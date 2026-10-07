import * as Arr from 'effect/Array'
import * as Ref from 'effect/Ref'
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
import {
  AuthBusyError,
  AuthConfigurationError,
  AuthStorageError,
  AuthError,
  Credential,
} from './Credential.ts'

export interface Service {
  readonly get: (key: string) => Effect.Effect<Option.Option<Credential>, AuthError>
  readonly list: Effect.Effect<ReadonlyArray<readonly [string, Credential]>, AuthError>
  readonly set: (key: string, value: Credential) => Effect.Effect<void, AuthError>
  readonly remove: (key: string) => Effect.Effect<void, AuthError>
  readonly hostId: (provider: string) => Effect.Effect<string, AuthError>
  /** Holds the per-store lock over read, callback and atomic replacement. Callback failure preserves credentials. */
  readonly modify: <R>(
    key: string,
    update: (
      current: Option.Option<Credential>,
    ) => Effect.Effect<Credential | undefined, AuthError, R>,
  ) => Effect.Effect<Credential | undefined, AuthError, R>
}
export class CredentialStore extends Context.Service<CredentialStore, Service>()(
  '@effect-harness/auth/CredentialStore',
) {}

const Snapshot = Schema.Struct({
  version: Schema.Literal(1),
  entries: Schema.Array(Schema.Struct({ key: Schema.String, value: Credential })),
  hosts: Schema.Array(Schema.Struct({ provider: Schema.String, id: Schema.String })),
})
type Snapshot = typeof Snapshot.Type
const empty: Snapshot = { version: 1, entries: [], hosts: [] }
const find = (snapshot: Snapshot, key: string) =>
  Arr.findFirst(snapshot.entries, (entry) => entry.key === key).pipe(
    Option.map((entry) => entry.value),
  )
const replace = (snapshot: Snapshot, key: string, value: Credential | undefined): Snapshot => ({
  ...snapshot,
  entries: [
    ...snapshot.entries.filter((entry) => entry.key !== key),
    ...(value === undefined ? [] : [{ key, value }]),
  ],
})
const storageError = (cause?: unknown) =>
  new AuthError({
    reason: new AuthStorageError({
      message: 'Protected credential storage failed',
      ...(cause === undefined ? {} : { cause }),
    }),
  })

const makeService = (
  read: Effect.Effect<Snapshot, AuthError>,
  write: (snapshot: Snapshot) => Effect.Effect<void, AuthError>,
  lock: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E | AuthError, R>,
  uuid: Effect.Effect<string, AuthError>,
): Effect.Effect<Service> =>
  Effect.sync(() => {
    const modify: Service['modify'] = Effect.fnUntraced(function* (key, update) {
      const snapshot = yield* read
      const value = yield* update(find(snapshot, key))
      yield* write(replace(snapshot, key, value))
      return value
    }, lock)
    return CredentialStore.of({
      get: (key) => read.pipe(Effect.map((snapshot) => find(snapshot, key))),
      list: read.pipe(
        Effect.map((snapshot) => snapshot.entries.map(({ key, value }) => [key, value] as const)),
      ),
      set: Effect.fnUntraced(function* (key, value) {
        const snapshot = yield* read
        yield* write(replace(snapshot, key, value))
      }, lock),
      remove: (key) =>
        lock(Effect.flatMap(read, (snapshot) => write(replace(snapshot, key, undefined)))),
      modify,
      hostId: Effect.fnUntraced(function* (provider) {
        const snapshot = yield* read
        const existing = Arr.findFirst(snapshot.hosts, (host) => host.provider === provider)
        if (Option.isSome(existing)) return existing.value.id
        const id = `urn:uuid:${yield* uuid}`
        yield* write({ ...snapshot, hosts: [...snapshot.hosts, { provider, id }] })
        return id
      }, lock),
    })
  })

export const layerMemory: Layer.Layer<CredentialStore, never, Crypto.Crypto> = Layer.effect(
  CredentialStore,
)(
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto
    const mutex = yield* Semaphore.make(1)
    const snapshot = yield* Ref.make(empty)
    return yield* makeService(
      Ref.get(snapshot),
      (next) => Ref.set(snapshot, next),
      (effect) => mutex.withPermit(effect),
      crypto.randomUUIDv4.pipe(Effect.mapError(storageError)),
    )
  }),
)

/** A dedicated directory is required. A stale crash lock fails busy; it is never stolen from a live owner. */
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
        return yield* new AuthError({
          reason: new AuthConfigurationError({
            message: 'Credential path must name a file',
          }),
        })
      if (
        options.lockRetries !== undefined &&
        (!Number.isSafeInteger(options.lockRetries) || options.lockRetries < 0)
      )
        return yield* new AuthError({
          reason: new AuthConfigurationError({
            message: 'Lock retries must be a nonnegative integer',
          }),
        })
      if (Option.isSome(yield* fs.readLink(directory).pipe(Effect.option)))
        return yield* new AuthError({
          reason: new AuthStorageError({
            message: 'Credential directory must not be a symbolic link',
          }),
        })
      yield* fs
        .makeDirectory(directory, { recursive: true, mode: 0o700 })
        .pipe(Effect.mapError(storageError))
      const directoryStat = yield* fs.stat(directory).pipe(Effect.mapError(storageError))
      if (directoryStat.type !== 'Directory' || (directoryStat.mode & 0o077) !== 0)
        return yield* new AuthError({
          reason: new AuthStorageError({
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
                  ? new AuthError({
                      reason: new AuthBusyError({
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
          Effect.acquireUseRelease(
            acquire,
            () => effect,
            () => fs.remove(lockDirectory, { recursive: true }).pipe(Effect.orDie),
          ),
        )
      const read = Effect.gen(function* () {
        if (Option.isSome(yield* fs.readLink(file).pipe(Effect.option)))
          return yield* new AuthError({
            reason: new AuthStorageError({
              message: 'Credential file must not be a symbolic link',
            }),
          })
        if (!(yield* fs.exists(file).pipe(Effect.mapError(storageError)))) return empty
        const stat = yield* fs.stat(file).pipe(Effect.mapError(storageError))
        if ((stat.mode & 0o077) !== 0 || stat.type !== 'File')
          return yield* new AuthError({
            reason: new AuthStorageError({
              message: 'Credential file must be a regular owner-only file',
            }),
          })
        const text = yield* fs.readFileString(file).pipe(Effect.mapError(storageError))
        const json: unknown = yield* Effect.try({
          try: () => JSON.parse(text),
          catch: storageError,
        })
        return yield* Schema.decodeUnknownEffect(Snapshot)(json).pipe(Effect.mapError(storageError))
      })
      const write = Effect.fnUntraced(function* (snapshot: Snapshot) {
        const encoded = yield* Schema.encodeEffect(Snapshot)(snapshot).pipe(
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
              .writeAll(new TextEncoder().encode(JSON.stringify(encoded)))
              .pipe(Effect.mapError(storageError))
            yield* handle.sync.pipe(Effect.mapError(storageError))
            yield* Scope.close(stagingScope, Exit.void)
            yield* fs.rename(temporary, file).pipe(Effect.mapError(storageError))
            const dir = yield* fs.open(directory, { flag: 'r' }).pipe(Effect.mapError(storageError))
            yield* dir.sync.pipe(Effect.mapError(storageError))
          }),
        )
      })
      return yield* makeService(
        read,
        write,
        diskLock,
        crypto.randomUUIDv4.pipe(Effect.mapError(storageError)),
      )
    }),
  )

/** Resolves all layerProtectedFile options through the caller's ConfigProvider. */
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
