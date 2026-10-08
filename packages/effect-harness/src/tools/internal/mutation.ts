import * as Effect from 'effect/Effect'
import * as RcMap from 'effect/RcMap'
import { MutationLocks } from '../../MutationLocks.ts'
import { Env } from '../../Env.ts'
import type { FileError } from '../../FileError.ts'

/** Namespace and canonical path mutex; noncancelable writes retain the permit until actual settlement. */
export const withFile: <A, E, R>(
  absolute: string,
  effect: Effect.Effect<A, E, R>,
) => Effect.Effect<A, E | FileError, R | Env | MutationLocks> = Effect.fnUntraced(
  function* <A, E, R>(
    absolute: string,
    _effect: Effect.Effect<A, E, R>,
  ): Effect.fn.Return<
    { readonly manager: MutationLocks['Service']; readonly key: string },
    FileError,
    Env | MutationLocks
  > {
    const env = yield* Env
    const canonical = (absolute: string): Effect.Effect<string, FileError> =>
      Effect.suspend(() =>
        env.canonicalPath(absolute).pipe(
          Effect.catchIf(
            (error) => error.code === 'not_supported',
            () => Effect.succeed(absolute),
          ),
          Effect.catchIf(
            (error) => error.code === 'not_found',
            () => {
              const parent = env.path.dirname(absolute)
              return parent === absolute
                ? Effect.succeed(absolute)
                : canonical(parent).pipe(
                    Effect.map((base) => env.path.join(base, env.path.basename(absolute))),
                  )
            },
          ),
        ),
      )
    const manager = yield* MutationLocks
    const key = JSON.stringify([env.id, yield* canonical(absolute)])
    return { manager, key }
  },
  (planning, _absolute, effect) =>
    Effect.flatMap(planning, ({ manager, key }) =>
      // Only lease admission and the caller operation receive the child Scope.
      // Canonical resolution above keeps the caller's ambient Scope unchanged.
      RcMap.get(manager, key).pipe(
        Effect.flatMap((lock) => lock.withPermit(effect)),
        Effect.scoped,
      ),
    ),
)
