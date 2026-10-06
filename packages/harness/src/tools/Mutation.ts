import * as Effect from 'effect/Effect'
import * as Semaphore from 'effect/Semaphore'
import { Env, type FileError } from '../Env.ts'
const queues = new Map<string, { readonly lock: Semaphore.Semaphore; users: number }>()
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
export const withFile = <A, E, R>(
  absolute: string,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E | FileError, R | Env> =>
  Effect.gen(function* () {
    const env = yield* Env
    const key = JSON.stringify([env.id, yield* canonical(env, absolute)])
    return yield* Effect.acquireUseRelease(
      Effect.sync(() => {
        let value = queues.get(key)
        if (value === undefined) {
          value = { lock: Semaphore.makeUnsafe(1), users: 0 }
          queues.set(key, value)
        }
        value.users++
        return value
      }),
      (queue) => queue.lock.withPermit(effect),
      (queue) =>
        Effect.sync(() => {
          queue.users--
          if (queue.users === 0 && queues.get(key) === queue) queues.delete(key)
        }),
    )
  })
