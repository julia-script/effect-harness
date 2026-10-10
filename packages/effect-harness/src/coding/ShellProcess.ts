import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import type * as ChildProcess from 'effect/process/ChildProcess'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'

export const Cleanup = Schema.Literals(['best-effort-process-group', 'owned-tree'])
export type Cleanup = typeof Cleanup.Type
export class ShellProcessError extends Schema.TaggedError<ShellProcessError>()(
  'ShellProcessError',
  {
    reason: Schema.Literals(['notFound', 'spawn', 'closed']),
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}
/**
 * Scoped process capability. Inject an owned-tree implementation for strict
 * isolation. Implementations must register cleanup in the caller's Scope and
 * keep stdout/stderr independent, including when the leader exits early.
 */
export class ShellProcess extends Context.Service<
  ShellProcess,
  {
    readonly cleanup: Cleanup
    readonly spawn: (
      command: ChildProcess.Command,
    ) => Effect.Effect<ChildProcessSpawner.ChildProcessHandle, ShellProcessError, Scope.Scope>
  }
>()('effect-harness/coding/ShellProcess') {}

/**
 * Delegate a bound native Node/Bun 4.0.1 spawner. POSIX process-group cleanup
 * and Windows taskkill are best effort. Group ID reuse, detached descendants,
 * permissions, zombies and backend time bounds prevent an exclusive owned-tree
 * guarantee. This Layer never claims that guarantee, even on successful tests.
 * Supply this only for a backend with the documented scoped group capability.
 */
export const layerNative: Layer.Layer<
  ShellProcess,
  never,
  ChildProcessSpawner.ChildProcessSpawner
> = Layer.effect(
  ShellProcess,
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
    let closed = false
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        closed = true
      }),
    )
    return ShellProcess.of({
      cleanup: 'best-effort-process-group',
      spawn: (command) =>
        Effect.suspend(() => {
          if (closed)
            return Effect.fail(
              new ShellProcessError({
                reason: 'closed',
                message: 'Shell process provider is closed.',
              }),
            )
          return spawner.spawn(command).pipe(
            Effect.mapError(
              (cause) =>
                new ShellProcessError({
                  reason:
                    cause.reason._tag === 'NotFound' && cause.reason.method === 'spawn'
                      ? 'notFound'
                      : 'spawn',
                  message: cause.message,
                  cause,
                }),
            ),
          )
        }),
    })
  }),
)
