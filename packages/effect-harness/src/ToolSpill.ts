import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import type * as PlatformError from 'effect/PlatformError'
import * as Schema from 'effect/Schema'
import * as Semaphore from 'effect/Semaphore'
import type * as Scope from 'effect/Scope'

export const Reference = Schema.Struct({
  provider: Schema.String,
  lifetime: Schema.Literals(['persistent', 'toolkit']),
  path: Schema.String,
  stdoutPath: Schema.String,
  stderrPath: Schema.String,
  bytes: Schema.Finite,
  stdoutBytes: Schema.Finite,
  stderrBytes: Schema.Finite,
})
export type Reference = typeof Reference.Type
export class ToolSpillError extends Schema.TaggedError<ToolSpillError>()('ToolSpillError', {
  reason: Schema.Literals(['configuration', 'write', 'unavailable', 'closed']),
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}
export interface Writer {
  /** Writes complete raw chunks with backpressure, before decoded retention. */
  readonly append: (
    stream: 'stdout' | 'stderr',
    bytes: Uint8Array,
  ) => Effect.Effect<void, ToolSpillError>
  /** Publish only after streams settle. The reference survives this writer's call Scope. */
  readonly commit: Effect.Effect<Reference, ToolSpillError>
}
export class ToolSpill extends Context.Service<
  ToolSpill,
  {
    readonly open: Effect.Effect<Writer, ToolSpillError, Scope.Scope>
    readonly revalidate: (reference: Reference) => Effect.Effect<Reference, ToolSpillError>
    readonly remove: (reference: Reference) => Effect.Effect<void, ToolSpillError>
  }
>()('effect-harness/ToolSpill') {}

export interface Options {
  /** Stable provider ID and backend directory; no ambient host temp directory. */
  readonly provider: string
  readonly directory: string
  /** Persistent files survive toolkit shutdown/reopen until explicitly removed by their owner. */
  readonly lifetime: 'persistent' | 'toolkit'
}
const native = <A, R>(
  effect: Effect.Effect<A, PlatformError.PlatformError, R>,
  reason: 'write' | 'unavailable',
) =>
  effect.pipe(
    Effect.mapError((cause) => new ToolSpillError({ reason, message: cause.message, cause })),
  )

/**
 * File-backed provider. Every byte is streamed to disk immediately, including
 * before the retention threshold. Combined path records observed chunk order;
 * per-stream paths preserve exact bytes even when Unicode chunks interleave.
 * Toolkit files expire with this Layer's Scope; persistent files have explicit
 * owner-driven deletion. Neither uses a call-scoped temporary path for success.
 */
export const layer = (
  options: Options,
): Layer.Layer<ToolSpill, ToolSpillError, FileSystem.FileSystem | Path.Path> => {
  const { provider, directory, lifetime } = options
  return Layer.effect(
    ToolSpill,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      if (!provider || !path.isAbsolute(directory) || !['persistent', 'toolkit'].includes(lifetime))
        return yield* new ToolSpillError({
          reason: 'configuration',
          message: 'Spill requires a provider ID, absolute bound directory and explicit lifetime.',
        })
      const root = path.resolve(directory)
      yield* native(fs.makeDirectory(root, { recursive: true }), 'write')
      const owned = new Set<string>()
      let active = true
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          active = false
        }),
      )
      if (lifetime === 'toolkit')
        yield* Effect.addFinalizer(() =>
          Effect.forEach(owned, (p) => fs.remove(p, { recursive: true, force: true }), {
            discard: true,
          }).pipe(Effect.orDie),
        )
      const validate = Effect.fnUntraced(function* (ref: Reference) {
        const paths = [ref.path, ref.stdoutPath, ref.stderrPath]
        const folder = path.dirname(ref.path)
        if (
          ref.provider !== provider ||
          ref.lifetime !== lifetime ||
          path.dirname(folder) !== root ||
          !path.basename(folder).startsWith('tool-output-') ||
          ref.path !== path.join(folder, 'combined.bin') ||
          ref.stdoutPath !== path.join(folder, 'stdout.bin') ||
          ref.stderrPath !== path.join(folder, 'stderr.bin')
        )
          return yield* new ToolSpillError({
            reason: 'unavailable',
            message: 'Spill reference belongs to another provider or lifetime.',
          })
        const expected = [ref.bytes, ref.stdoutBytes, ref.stderrBytes]
        if (ref.bytes !== ref.stdoutBytes + ref.stderrBytes)
          return yield* new ToolSpillError({
            reason: 'unavailable',
            message: 'Spill totals do not match its independent streams.',
          })
        for (let i = 0; i < paths.length; i++) {
          const p = paths[i]
          const size = expected[i]
          if (p === undefined || size === undefined || !Number.isSafeInteger(size) || size < 0)
            return yield* new ToolSpillError({
              reason: 'unavailable',
              message: 'Invalid spill byte counts.',
            })
          const stat = yield* native(fs.stat(p), 'unavailable')
          if (stat.type !== 'File' || stat.size !== BigInt(size))
            return yield* new ToolSpillError({
              reason: 'unavailable',
              message: 'Spill is missing, expired or its size changed.',
            })
        }
        return ref
      })
      return ToolSpill.of({
        revalidate: validate,
        remove: Effect.fnUntraced(function* (ref) {
          yield* validate(ref)
          const folder = path.dirname(ref.path)
          yield* native(fs.remove(folder, { recursive: true, force: true }), 'write')
          owned.delete(folder)
        }),
        open: Effect.gen(function* () {
          if (!active)
            return yield* new ToolSpillError({
              reason: 'closed',
              message: 'Spill provider scope is closed.',
            })
          let committed = false
          let closed = false
          const folder = yield* Effect.acquireRelease(
            native(fs.makeTempDirectory({ directory: root, prefix: 'tool-output-' }), 'write'),
            (folder) =>
              Effect.gen(function* () {
                closed = true
                if (!committed)
                  yield* fs.remove(folder, { recursive: true, force: true }).pipe(Effect.orDie)
              }),
          )
          const allocate = Effect.fnUntraced(function* (name: string) {
            const p = path.join(folder, name)
            const file = yield* native(fs.open(p, { flag: 'w' }), 'write')
            return { p, file }
          })
          const combined = yield* allocate('combined.bin')
          const stdout = yield* allocate('stdout.bin')
          const stderr = yield* allocate('stderr.bin')
          let stdoutBytes = 0
          let stderrBytes = 0
          let failed = false
          const mutex = yield* Semaphore.make(1)
          const check = Effect.suspend(() => {
            if (failed)
              return Effect.fail(
                new ToolSpillError({
                  reason: 'write',
                  message: 'Spill writer has an incomplete failed write; publication is refused.',
                }),
              )
            if (!active || closed || committed)
              return Effect.fail(
                new ToolSpillError({
                  reason: 'closed',
                  message: 'Spill writer/provider is closed or already committed.',
                }),
              )
            return Effect.void
          })
          return {
            append: (stream, bytes) =>
              mutex.withPermit(
                Effect.uninterruptible(
                  Effect.gen(function* () {
                    yield* check
                    const target = stream === 'stdout' ? stdout : stderr
                    yield* native(target.file.writeAll(bytes), 'write')
                    yield* native(combined.file.writeAll(bytes), 'write')
                    if (stream === 'stdout') stdoutBytes += bytes.length
                    else stderrBytes += bytes.length
                  }),
                ).pipe(
                  Effect.tapError(() =>
                    Effect.sync(() => {
                      failed = true
                    }),
                  ),
                ),
              ),
            commit: mutex.withPermit(
              Effect.uninterruptible(
                Effect.gen(function* () {
                  yield* check
                  yield* native(combined.file.sync, 'write')
                  yield* native(stdout.file.sync, 'write')
                  yield* native(stderr.file.sync, 'write')
                  // Native writeAll completes before publication; call Scope closes handles only.
                  const ref: Reference = {
                    provider,
                    lifetime,
                    path: combined.p,
                    stdoutPath: stdout.p,
                    stderrPath: stderr.p,
                    bytes: stdoutBytes + stderrBytes,
                    stdoutBytes,
                    stderrBytes,
                  }
                  yield* validate(ref)
                  committed = true
                  if (lifetime === 'toolkit') owned.add(folder)
                  return ref
                }),
              ),
            ),
          } satisfies Writer
        }),
      })
    }),
  )
}
