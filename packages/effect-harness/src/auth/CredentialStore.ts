/**
 * Locked in-memory and protected-file credential transactions with scoped atomic persistence.
 */
import * as Array from 'effect/Array'
import * as Function from 'effect/Function'
import { HostId } from './HostId.ts'
import * as SynchronizedRef from 'effect/SynchronizedRef'
import * as Config from 'effect/Config'
import * as Context from 'effect/Context'
import * as Crypto from 'effect/Crypto'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import * as Schedule from 'effect/Schedule'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Semaphore from 'effect/Semaphore'
import { Credential } from './Credential.ts'
import { AuthBusyError, AuthConfigurationError, AuthStorageError, AuthError } from './AuthError.ts'

/**
 * Type-level contracts for `CredentialStore`.
 */
export declare namespace CredentialStore {
  /**
   * Credential lookup and serialized updates for application-owned accounts.
   *
   * @category models
   */
  export interface Service {
    /**
     * Reads an application-owned credential by key, returning None when absent.
     */
    readonly get: (key: string) => Effect.Effect<Option.Option<Credential>, AuthError>
    /**
     * Returns saved account keys and their credentials; secrets remain Redacted.
     */
    readonly list: Effect.Effect<Array<readonly [string, Credential]>, AuthError>
    /**
     * Replaces a credential under the store’s update lock.
     */
    readonly set: (key: string, value: Credential) => Effect.Effect<void, AuthError>
    /**
     * Removes the saved credential under the store’s update lock.
     */
    readonly remove: (key: string) => Effect.Effect<void, AuthError>
    /**
     * Returns or persists a stable host UUID for this provider.
     */
    readonly hostId: (provider: string) => Effect.Effect<HostId, AuthError>
    /** Holds the per-store lock over read, callback and atomic replacement. Callback failure preserves credentials. */
    readonly modify: <A extends Credential | undefined, E, R>(
      key: string,
      update: (current: Option.Option<Credential>) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | AuthError, R>
  }
}

/**
 * Service for application-owned credentials and serialized token updates.
 *
 * **Details**
 *
 * modify holds the store lock across read, callback and atomic replacement. A failed
 * callback preserves the previous credential. hostId retains a provider-specific host
 * identity.
 *
 * **Gotchas**
 *
 * Credential values contain Redacted secrets. Do not log or unwrap them for ordinary
 * application output.
 *
 * @category services
 */
export class CredentialStore extends Context.Service<CredentialStore, CredentialStore.Service>()(
  'effect-harness/auth/CredentialStore',
) {}

const Snapshot = Schema.Struct({
  version: Schema.Literal(1),
  entries: Schema.Array(Schema.Struct({ key: Schema.String, value: Credential })),
  hosts: Schema.Array(Schema.Struct({ provider: Schema.String, id: HostId })),
})
type Snapshot = typeof Snapshot.Type
const SnapshotJson = Schema.fromJsonString(Snapshot)
const empty: Snapshot = { version: 1, entries: [], hosts: [] }
const find = (snapshot: Snapshot, key: string) =>
  Array.findFirst(snapshot.entries, (entry) => entry.key === key).pipe(
    Option.map((entry) => entry.value),
  )
const replace = (snapshot: Snapshot, key: string, value: Credential | undefined): Snapshot => ({
  ...snapshot,
  entries: [
    ...Array.filter(snapshot.entries, (entry) => entry.key !== key),
    ...(value === undefined ? [] : [{ key, value }]),
  ],
})
const storageError = (cause?: unknown) =>
  AuthError.make({
    reason: AuthStorageError.make({
      message: 'Protected credential storage failed',
      ...(cause === undefined ? {} : { cause }),
    }),
  })

interface Backend {
  readonly read: Effect.Effect<Snapshot, AuthError>
  readonly modify: <A>(
    update: (snapshot: Snapshot) => readonly [A, Snapshot],
  ) => Effect.Effect<A, AuthError>
  readonly modifyEffect: <A, E, R>(
    update: (snapshot: Snapshot) => Effect.Effect<readonly [A, Snapshot], E, R>,
  ) => Effect.Effect<A, E | AuthError, R>
}

const makeService = (
  backend: Backend,
  uuid: Effect.Effect<string, AuthError>,
): CredentialStore.Service => {
  const modify = <A extends Credential | undefined, E, R>(
    key: string,
    update: (current: Option.Option<Credential>) => Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | AuthError, R> =>
    backend.modifyEffect((snapshot) =>
      Effect.map(
        update(find(snapshot, key)),
        (value) => [value, replace(snapshot, key, value)] as const,
      ),
    )
  return CredentialStore.of({
    get: (key) => backend.read.pipe(Effect.map((snapshot) => find(snapshot, key))),
    list: backend.read.pipe(
      Effect.map((snapshot) =>
        Array.map(snapshot.entries, ({ key, value }) => [key, value] as const),
      ),
      Effect.withSpan('CredentialStore.list'),
    ),
    set: (key, value) => backend.modify((snapshot) => [undefined, replace(snapshot, key, value)]),
    remove: (key) => backend.modify((snapshot) => [undefined, replace(snapshot, key, undefined)]),
    modify,
    hostId: (provider) =>
      backend.modifyEffect(
        Effect.fnUntraced(function* (snapshot) {
          const existing = Array.findFirst(snapshot.hosts, (host) => host.provider === provider)
          if (Option.isSome(existing)) return [existing.value.id, snapshot] as const
          const id = yield* Schema.decodeEffect(HostId)(`urn:uuid:${yield* uuid}`).pipe(
            Effect.mapError(storageError),
          )
          return [id, { ...snapshot, hosts: [...snapshot.hosts, { provider, id }] }] as const
        }),
      ),
  })
}

/**
 * Provides empty process-local credential storage with serialized updates.
 *
 * **Details**
 *
 * Consumes native Crypto to create stable host IDs within the store lifetime.
 *
 * **Gotchas**
 *
 * Credentials and host IDs are lost when this service is rebuilt or the process exits.
 *
 * @category layers
 */
export const layerMemory: Layer.Layer<CredentialStore, never, Crypto.Crypto> = Layer.effect(
  CredentialStore,
)(
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto
    const snapshot = yield* SynchronizedRef.make(empty)
    return makeService(
      {
        read: SynchronizedRef.get(snapshot),
        modify: (update) => SynchronizedRef.modify(snapshot, update),
        modifyEffect: (update) => SynchronizedRef.modifyEffect(snapshot, update),
      },
      crypto.randomUUIDv4.pipe(Effect.mapError(storageError)),
    )
  }),
)

/**
 * Provides locked credential storage in an owner-only file and directory.
 *
 * **Details**
 *
 * Uses native FileSystem, Path and Crypto. Writes replace the saved file atomically while a
 * cross-process lock serializes updates.
 *
 * **Gotchas**
 *
 * The file is permission-protected, not encrypted. Stale locks fail as busy; confirm the
 * prior process is gone before recovering a lock. Do not nest another update to this store
 * inside modify.
 *
 * @category layers
 */
export const layerProtectedFile = (options: {
  readonly path: string
  readonly lockRetries?: number | undefined
}): Layer.Layer<CredentialStore, AuthError, Crypto.Crypto | FileSystem.FileSystem | Path.Path> =>
  Layer.effect(CredentialStore)(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const crypto = yield* Crypto.Crypto
      const mutex = yield* Semaphore.make(1)
      const file = path.resolve(options.path)
      const directory = path.dirname(file)
      if (file === directory)
        return yield* AuthError.make({
          reason: AuthConfigurationError.make({
            message: 'Credential path must name a file',
          }),
        })
      if (
        options.lockRetries !== undefined &&
        (!Number.isSafeInteger(options.lockRetries) || options.lockRetries < 0)
      )
        return yield* AuthError.make({
          reason: AuthConfigurationError.make({
            message: 'Lock retries must be a nonnegative integer',
          }),
        })
      if (Option.isSome(yield* fs.readLink(directory).pipe(Effect.option)))
        return yield* AuthError.make({
          reason: AuthStorageError.make({
            message: 'Credential directory must not be a symbolic link',
          }),
        })
      yield* fs
        .makeDirectory(directory, { recursive: true, mode: 0o700 })
        .pipe(Effect.mapError(storageError))
      const directoryStat = yield* fs.stat(directory).pipe(Effect.mapError(storageError))
      if (directoryStat.type !== 'Directory' || (directoryStat.mode & 0o077) !== 0)
        return yield* AuthError.make({
          reason: AuthStorageError.make({
            message: 'Credential directory must be owner-only',
          }),
        })
      const lockDirectory = `${file}.lock`
      const acquire = fs.makeDirectory(lockDirectory, { mode: 0o700 }).pipe(
        Effect.catch((cause) =>
          fs.exists(lockDirectory).pipe(
            Effect.mapError(storageError),
            Effect.flatMap((exists) =>
              Effect.fail(
                exists
                  ? AuthError.make({
                      reason: AuthBusyError.make({
                        message: 'Credential store is locked by another process',
                        cause,
                      }),
                    })
                  : storageError(cause),
              ),
            ),
          ),
        ),
        Effect.retry({
          times: options.lockRetries ?? 100,
          schedule: Schedule.spaced('20 millis'),
          while: (error) => error.reason._tag === 'AuthBusyError',
        }),
      )
      const diskLock = <A, E, R>(
        effect: Effect.Effect<A, E, R>,
      ): Effect.Effect<A, E | AuthError, R> =>
        mutex.withPermit(
          Effect.acquireUseRelease(acquire, Function.constant(effect), () =>
            fs.remove(lockDirectory, { recursive: true }).pipe(Effect.orDie),
          ),
        )
      const read = Effect.gen(function* () {
        if (Option.isSome(yield* fs.readLink(file).pipe(Effect.option)))
          return yield* AuthError.make({
            reason: AuthStorageError.make({
              message: 'Credential file must not be a symbolic link',
            }),
          })
        if (!(yield* fs.exists(file).pipe(Effect.mapError(storageError)))) return empty
        const stat = yield* fs.stat(file).pipe(Effect.mapError(storageError))
        if ((stat.mode & 0o077) !== 0 || stat.type !== 'File')
          return yield* AuthError.make({
            reason: AuthStorageError.make({
              message: 'Credential file must be a regular owner-only file',
            }),
          })
        const text = yield* fs.readFileString(file).pipe(Effect.mapError(storageError))
        return yield* Schema.decodeEffect(SnapshotJson)(text).pipe(Effect.mapError(storageError))
      })
      const write = Effect.fnUntraced(function* (snapshot: Snapshot) {
        const encoded = yield* Schema.encodeEffect(SnapshotJson)(snapshot).pipe(
          Effect.mapError(storageError),
        )
        const uuid = yield* crypto.randomUUIDv4.pipe(Effect.mapError(storageError))
        const temporary = `${file}.${uuid}.tmp`
        yield* Effect.scoped(
          Effect.gen(function* () {
            const stagingScope = yield* Effect.acquireRelease(Scope.make(), (scope, exit) =>
              Scope.close(scope, exit),
            )
            // Exclusive open establishes ownership before writing; close the handle before removal.
            const handle = yield* Effect.acquireRelease(
              fs
                .open(temporary, { flag: 'wx', mode: 0o600 })
                .pipe(Scope.provide(stagingScope), Effect.mapError(storageError)),
              (_, exit) =>
                Scope.close(stagingScope, exit).pipe(
                  Effect.andThen(fs.remove(temporary, { force: true }).pipe(Effect.orDie)),
                ),
            )
            yield* handle
              .writeAll(new TextEncoder().encode(encoded))
              .pipe(Effect.mapError(storageError))
            yield* handle.sync.pipe(Effect.mapError(storageError))
            yield* Scope.close(stagingScope, Exit.void)
            yield* fs.rename(temporary, file).pipe(Effect.mapError(storageError))
            const dir = yield* fs.open(directory, { flag: 'r' }).pipe(Effect.mapError(storageError))
            yield* dir.sync.pipe(Effect.mapError(storageError))
          }),
        )
      })
      const modifyEffect = <A, E, R>(
        update: (snapshot: Snapshot) => Effect.Effect<readonly [A, Snapshot], E, R>,
      ): Effect.Effect<A, E | AuthError, R> =>
        diskLock(
          Effect.gen(function* () {
            const snapshot = yield* read
            const [value, next] = yield* update(snapshot)
            if (next !== snapshot) yield* write(next)
            return value
          }),
        )
      return makeService(
        {
          read,
          modify: (update) => modifyEffect((snapshot) => Effect.sync(() => update(snapshot))),
          modifyEffect,
        },
        crypto.randomUUIDv4.pipe(Effect.mapError(storageError)),
      )
    }),
  )

/**
 * Resolves all layerProtectedFile options through the caller's ConfigProvider.
 *
 * @category layers
 */
export const layerProtectedFileConfig = (
  config: Config.Wrap<NonNullable<Parameters<typeof layerProtectedFile>[0]>>,
): Layer.Layer<
  CredentialStore,
  AuthError | Config.ConfigError,
  Crypto.Crypto | FileSystem.FileSystem | Path.Path
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layerProtectedFile(yield* Config.unwrap(config))
    }),
  )
