/** Bun's OS credential store, coordinated by an application-owned local lock file. */
import * as Fs from 'node:fs/promises'
import * as Path from 'node:path'
import * as Clock from 'effect/Clock'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Credential from '../provider-auth/Credential.js'
import { AuthError } from '../provider-auth/AuthError.js'
import { CredentialStore } from '../provider-auth/CredentialStore.js'

const storeError = () => new AuthError({ reason: 'StoreFailure' })
const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: storeError })
const codec = Schema.fromJsonString(
  Schema.Record(Schema.String, Schema.toCodecJson(Credential.Stored)),
)

export interface Options {
  /** Application namespace in the OS credential manager. */
  readonly service: string
  /** Entry containing the credential collection. Defaults to "credentials". */
  readonly name?: string
  /**
   * Explicit lock filename in a private local directory. Every process accessing
   * this service/name pair must use the same path. Contains no credentials.
   */
  readonly lockPath: string
  /** Maximum wait for another writer, in milliseconds. Defaults to 20,000. */
  readonly lockTimeoutMs?: number
}

/**
 * Store a schema-validated collection in one Bun.secrets entry. A single native
 * replacement keeps credentials and listing consistent without a separate index.
 * The entire collection must fit the platform's secret-size limit.
 *
 * Requires Bun.secrets when evaluated, with no import-time keychain access.
 * Native operations may require OS interaction and have no cancellation API:
 * an acquired transaction waits for settlement before releasing its lock.
 * Never remove a stale lock until its owning process is known to have stopped.
 */
export const make = Effect.fnUntraced(function* (options: Options) {
  const name = options.name ?? 'credentials'
  const lockTimeout = options.lockTimeoutMs ?? 20_000
  const secrets = typeof Bun === 'undefined' ? undefined : Bun.secrets
  if (
    options.service.length === 0 ||
    name.length === 0 ||
    options.lockPath.length === 0 ||
    !Number.isSafeInteger(lockTimeout) ||
    lockTimeout <= 0 ||
    secrets === undefined
  )
    return yield* new AuthError({ reason: 'InvalidConfiguration' })
  const lockPath = Path.resolve(options.lockPath)
  const address = { service: options.service, name }
  yield* io(() => Fs.mkdir(Path.dirname(lockPath), { recursive: true, mode: 0o700 }))
  const read = Effect.gen(function* () {
    const text = yield* io(() => secrets.get(address))
    return yield* Schema.decodeEffect(codec)(text ?? '{}').pipe(Effect.mapError(storeError))
  })
  const acquire = (wait: (effect: Effect.Effect<void>) => Effect.Effect<void>) =>
    Effect.gen(function* () {
      const deadline = (yield* Clock.currentTimeMillis) + lockTimeout
      while (true) {
        const file = yield* io(async () => {
          try {
            return await Fs.open(lockPath, 'wx', 0o600)
          } catch (error) {
            if (
              typeof error === 'object' &&
              error !== null &&
              'code' in error &&
              error.code === 'EEXIST'
            )
              return undefined
            throw error
          }
        })
        if (file !== undefined) return file
        const remaining = deadline - (yield* Clock.currentTimeMillis)
        if (remaining <= 0) return yield* storeError()
        yield* wait(Effect.sleep(Math.min(25, remaining)))
      }
    })
  return CredentialStore.of({
    read: (key) =>
      Effect.map(read, (values) => (Object.hasOwn(values, key) ? values[key] : undefined)),
    list: Effect.map(read, (values) => Object.values(values).map(Credential.metadata)),
    modify: (key, f) =>
      Effect.suspend(() => {
        let attempted = false
        let releaseError: AuthError | undefined
        const commitFailure = () =>
          new AuthError({ reason: attempted ? 'StoreCommitUncertain' : 'StoreFailure' })
        return Effect.uninterruptibleMask((restore) =>
          Effect.acquireUseRelease(
            acquire(restore),
            () =>
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
                // Bun does not specify whether a rejected mutation has already
                // changed the OS entry. Conservatively require a reread.
                yield* Effect.tryPromise({
                  try: async () => {
                    attempted = true
                    if (Object.keys(values).length === 0) await secrets.delete(address)
                    else await secrets.set({ ...address, value: encoded })
                  },
                  catch: commitFailure,
                })
                return result
              }),
            (file) =>
              io(async () => {
                await file.close()
                await Fs.unlink(lockPath)
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

/** Provide the Bun Secrets implementation of CredentialStore. */
export const layer = (options: Options) => Layer.effect(CredentialStore, make(options))
