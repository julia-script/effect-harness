import * as Effect from 'effect/Effect'
import * as RcMap from 'effect/RcMap'
import { MutationLocks } from '../../MutationLocks.ts'
import { Env, type FileError } from '../../Env.ts'
const canonical = (self: Env['Service'], absolute: string): Effect.Effect<string, FileError> =>
  Effect.suspend(() =>
    self.canonicalPath(absolute).pipe(
      Effect.catchIf(
        (error) => error.code === 'not_supported',
        () => Effect.succeed(absolute),
      ),
      Effect.catchIf(
        (error) => error.code === 'not_found',
        () => {
          const parent = self.path.dirname(absolute)
          return parent === absolute
            ? Effect.succeed(absolute)
            : canonical(self, parent).pipe(
                Effect.map((base) => self.path.join(base, self.path.basename(absolute))),
              )
        },
      ),
    ),
  )
/** Namespace and canonical path mutex; noncancelable writes retain the permit until actual settlement. */
export const withFile = Effect.fnUntraced(function* <A, E, R>(
  absolute: string,
  effect: Effect.Effect<A, E, R>,
): Effect.fn.Return<A, E | FileError, R | Env | MutationLocks> {
  const env = yield* Env
  const manager = yield* MutationLocks
  const key = JSON.stringify([env.id, yield* canonical(env, absolute)])
  return yield* RcMap.get(manager, key).pipe(
    Effect.flatMap((lock) => lock.withPermit(effect)),
    Effect.scoped,
  )
})
