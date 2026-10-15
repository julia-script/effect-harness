/** Optional Node/Bun host store. No ambient paths or import-time filesystem access. */
import * as Fs from 'node:fs/promises'
import * as Path from 'node:path'
import * as Effect from 'effect/Effect'
import * as Clock from 'effect/Clock'
import * as Schema from 'effect/Schema'

export interface Service<A, E> {
  readonly read: (key: string) => Effect.Effect<A | undefined, E>
  readonly list: Effect.Effect<ReadonlyArray<A>, E>
  readonly modify: <B, E2, R>(
    key: string,
    f: (current: A | undefined) => Effect.Effect<readonly [B, A | undefined], E2, R>,
  ) => Effect.Effect<B, E | E2, R>
}
const isCode = (error: unknown, code: string) =>
  typeof error === 'object' && error !== null && 'code' in error && error.code === code
/**
 * Atomic replacement and an exclusive lock file coordinate independent instances/processes.
 * Use an application-owned private directory on a local filesystem. Files contain plaintext JSON
 * with mode 0600. Callers choose separate files for public state and credentials.
 * A crash may leave a lock: never steal it by age (a paused process could still rotate a token).
 * Remove stale locks only after establishing that their owning process has stopped.
 */
export const make = Effect.fnUntraced(function* <A, I, E>(
  schema: Schema.Codec<A, I>,
  failure: (reason: 'StoreFailure' | 'StoreCommitUncertain' | 'InvalidConfiguration') => E,
  options: {
    readonly path: string
    readonly lockTimeoutMs?: number
  },
) {
  const storeError = () => failure('StoreFailure')
  const io = <B>(run: () => Promise<B>) => Effect.tryPromise({ try: run, catch: storeError })
  const codec = Schema.fromJsonString(Schema.Record(Schema.String, Schema.toCodecJson(schema)))
  const path = Path.resolve(options.path)
  const lockTimeout = options.lockTimeoutMs ?? 20_000
  if (!Number.isSafeInteger(lockTimeout) || lockTimeout <= 0)
    return yield* Effect.fail(failure('InvalidConfiguration'))
  yield* io(() => Fs.mkdir(Path.dirname(path), { recursive: true, mode: 0o700 }))
  const read = Effect.gen(function* () {
    const text = yield* Effect.tryPromise({
      try: async () => {
        try {
          return await Fs.readFile(path, 'utf8')
        } catch (error) {
          if (isCode(error, 'ENOENT')) return '{}'
          throw error
        }
      },
      catch: storeError,
    })
    return yield* Schema.decodeEffect(codec)(text).pipe(Effect.mapError(storeError))
  })
  const acquire = (wait: (effect: Effect.Effect<void>) => Effect.Effect<void>) =>
    Effect.gen(function* () {
      const deadline = (yield* Clock.currentTimeMillis) + lockTimeout
      while (true) {
        const file = yield* Effect.tryPromise({
          try: async () => {
            try {
              return await Fs.open(`${path}.lock`, 'wx', 0o600)
            } catch (error) {
              if (isCode(error, 'EEXIST')) return undefined
              throw error
            }
          },
          catch: storeError,
        })
        if (file !== undefined) return file
        const remaining = deadline - (yield* Clock.currentTimeMillis)
        if (remaining <= 0) return yield* Effect.fail(storeError())
        yield* wait(Effect.sleep(Math.min(25, remaining)))
      }
    })
  const service: Service<A, E> = {
    read: (key) =>
      Effect.map(read, (values) => (Object.hasOwn(values, key) ? values[key] : undefined)),
    list: Effect.map(read, (values) => Object.values(values)),
    modify: (key, f) =>
      Effect.suspend(() => {
        let published = false
        let releaseError: E | undefined
        const commitFailure = () => failure(published ? 'StoreCommitUncertain' : 'StoreFailure')
        return Effect.uninterruptibleMask((restore) =>
          Effect.acquireUseRelease(
            acquire(restore),
            () =>
              Effect.uninterruptible(
                Effect.gen(function* () {
                  const values = yield* read
                  const current = Object.hasOwn(values, key) ? values[key] : undefined
                  const [result, next] = yield* f(current)
                  if (next === current) return result
                  if (next === undefined) Reflect.deleteProperty(values, key)
                  else
                    Object.defineProperty(values, key, {
                      value: next,
                      enumerable: true,
                      configurable: true,
                      writable: true,
                    })
                  const encoded = yield* Schema.encodeEffect(codec)(values).pipe(
                    Effect.mapError(storeError),
                  )
                  yield* Effect.tryPromise({
                    try: async () => {
                      // Acquire the durability handle before publishing anything. A directory
                      // may permit create/rename while denying the read access needed by open.
                      const directory = await Fs.open(Path.dirname(path), 'r')
                      try {
                        const temporary = `${path}.${crypto.randomUUID()}.tmp`
                        const file = await Fs.open(temporary, 'wx', 0o600)
                        try {
                          await file.writeFile(encoded, 'utf8')
                          await file.sync()
                          await file.close()
                          await Fs.rename(temporary, path)
                          published = true
                          await directory.sync()
                        } finally {
                          await file.close()
                          await Fs.rm(temporary, { force: true })
                        }
                      } finally {
                        await directory.close()
                      }
                    },
                    catch: commitFailure,
                  })
                  return result
                }),
              ),
            (file) =>
              io(async () => {
                await file.close()
                await Fs.unlink(`${path}.lock`)
              }).pipe(
                Effect.catch(() =>
                  Effect.sync(() => {
                    releaseError = commitFailure()
                  }),
                ),
              ),
          ).pipe(
            Effect.filterOrFail(
              () => releaseError === undefined,
              () => releaseError!,
            ),
          ),
        )
      }),
  }
  return service
})
