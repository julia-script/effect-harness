import * as Effect from 'effect/Effect'
import * as RcMap from 'effect/RcMap'
import { MutationLocks } from '../MutationLocks.ts'
import { Env, type FileError } from '../Env.ts'
const canonical = Effect.fnUntraced(function* (
  env: Env['Service'],
  absolute: string,
): Effect.fn.Return<string, FileError> {
  return yield* env.canonicalPath(absolute).pipe(
    Effect.catch((error) => {
      if (error.code === 'not_supported') return Effect.succeed(absolute)
      if (error.code !== 'not_found') return Effect.fail(error)
      const parent = env.path.dirname(absolute)
      if (parent === absolute) return Effect.succeed(absolute)
      return canonical(env, parent).pipe(
        Effect.map((base) => env.path.join(base, env.path.basename(absolute))),
      )
    }),
  )
})
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
