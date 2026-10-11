import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Semaphore from 'effect/Semaphore'
import { CodingFileError } from './FileError.js'
import * as FilePath from '../internal/CodingFilePath.js'

/** One scoped coordinator per bound backend, shared by write and later edit handlers. */
export class MutationCoordinator extends Context.Service<
  MutationCoordinator,
  {
    readonly withPath: <A, E, R>(
      path: string,
      use: (canonicalPath: string) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | CodingFileError, R>
  }
>()('effect-harness/coding/MutationCoordinator') {}

/**
 * Serializes lexical/symlink aliases by realPath, including missing files via
 * their nearest existing ancestor. Unrelated paths and backend instances stay
 * independent. References include interrupted waiters and are removed on exit.
 * This is cooperative coordination, not a lock against shell/external writers,
 * symlink replacement or hard-link aliases. Share this Layer within each binding.
 */
export const layer: Layer.Layer<MutationCoordinator, never, FileSystem.FileSystem | Path.Path> =
  Layer.effect(
    MutationCoordinator,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const locks = new Map<string, { semaphore: Semaphore.Semaphore; users: number }>()
      let closed = false
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          closed = true
          locks.clear()
        }),
      )
      const canonical = Effect.fn('MutationCoordinator.canonical')(function* (input: string) {
        const resolved = yield* FilePath.resolve(input).pipe(Effect.provideService(Path.Path, path))
        let ancestor = resolved
        const suffix: string[] = []
        while (true) {
          const real = yield* fs.realPath(ancestor).pipe(
            Effect.catch((cause) => {
              if (cause.reason._tag === 'NotFound') return Effect.void
              return Effect.fail(
                new CodingFileError({
                  reason: 'filesystem',
                  path: input,
                  message: cause.message,
                  cause,
                }),
              )
            }),
          )
          if (real !== undefined) return path.join(real, ...suffix)
          const parent = path.dirname(ancestor)
          if (parent === ancestor)
            return yield* new CodingFileError({
              reason: 'filesystem',
              path: input,
              message: 'No existing path ancestor',
            })
          suffix.unshift(path.basename(ancestor))
          ancestor = parent
        }
      })
      return {
        withPath: <A, E, R>(
          input: string,
          use: (canonicalPath: string) => Effect.Effect<A, E, R>,
        ) =>
          Effect.scoped(
            Effect.gen(function* () {
              if (closed)
                return yield* new CodingFileError({
                  reason: 'closed',
                  path: input,
                  message: 'The mutation coordinator scope is closed',
                })
              const key = yield* canonical(input)
              const candidate = yield* Semaphore.make(1)
              const entry = yield* Effect.acquireRelease(
                Effect.sync(() => {
                  const current = locks.get(key) ?? { semaphore: candidate, users: 0 }
                  current.users++
                  locks.set(key, current)
                  return current
                }),
                (entry) =>
                  Effect.sync(() => {
                    entry.users--
                    if (entry.users === 0 && locks.get(key) === entry) locks.delete(key)
                  }),
              )
              return yield* entry.semaphore.withPermit(
                Effect.suspend((): Effect.Effect<A, E | CodingFileError, R> => {
                  if (closed)
                    return Effect.fail(
                      new CodingFileError({
                        reason: 'closed',
                        path: input,
                        message: 'The mutation coordinator scope is closed',
                      }),
                    )
                  return use(key)
                }),
              )
            }),
          ),
      }
    }),
  )
