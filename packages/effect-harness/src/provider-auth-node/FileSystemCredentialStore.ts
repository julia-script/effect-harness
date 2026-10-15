/** Optional Node/Bun host store. No ambient paths or import-time filesystem access. */
import * as Fs from 'node:fs/promises'
import * as Path from 'node:path'
import * as Effect from 'effect/Effect'
import * as Clock from 'effect/Clock'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Credential from '../provider-auth/Credential.js'
import { AuthError } from '../provider-auth/AuthError.js'
import { CredentialStore } from '../provider-auth/CredentialStore.js'

const storeError = () => new AuthError({ reason: 'StoreFailure' })
const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: storeError })
const isCode = (error: unknown, code: string) =>
  typeof error === 'object' && error !== null && 'code' in error && error.code === code
const codec = Schema.fromJsonString(
  Schema.Record(Schema.String, Schema.toCodecJson(Credential.Credential)),
)

/**
 * Atomic replacement and an exclusive lock file coordinate independent instances/processes.
 * Use an application-owned private directory on a local filesystem. Files contain plaintext secrets
 * with mode 0600; encrypting/keychain stores can implement CredentialStore instead.
 * A crash may leave a lock: never steal it by age (a paused process could still rotate a token).
 * Remove stale locks only after establishing that their owning process has stopped.
 */
export const make = Effect.fnUntraced(function* (options: {
  readonly path: string
  readonly lockTimeoutMs?: number
}) {
  const path = Path.resolve(options.path)
  const lockTimeout = options.lockTimeoutMs ?? 20_000
  if (!Number.isSafeInteger(lockTimeout) || lockTimeout <= 0)
    return yield* new AuthError({ reason: 'InvalidConfiguration' })
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
        if (remaining <= 0) return yield* storeError()
        yield* wait(Effect.sleep(Math.min(25, remaining)))
      }
    })
  return CredentialStore.of({
    read: (key) => Effect.map(read, (values) => values[key]),
    list: Effect.map(read, (values) => Object.values(values).map(Credential.metadata)),
    modify: (key, f) =>
      Effect.suspend(() => {
        let published = false
        let releaseError: AuthError | undefined
        const commitFailure = () =>
          new AuthError({ reason: published ? 'StoreCommitUncertain' : 'StoreFailure' })
        return Effect.uninterruptibleMask((restore) =>
          Effect.acquireUseRelease(
            acquire(restore),
            () =>
              Effect.uninterruptible(
                Effect.gen(function* () {
                  const values = yield* read
                  const [result, next] = yield* f(values[key])
                  if (next === values[key]) return result
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
  })
})

export const layer = (options: Parameters<typeof make>[0]) =>
  Layer.effect(CredentialStore, make(options))
