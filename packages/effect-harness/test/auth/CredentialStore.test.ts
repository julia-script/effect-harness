import { assertNone, assertSome } from '@effect/vitest/utils'
import * as DateTime from 'effect/DateTime'
import * as Time from 'effect-harness/auth/Time'
import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as BunFileSystem from '@effect/platform-bun/BunFileSystem'
import * as BunPath from '@effect/platform-bun/BunPath'
import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as PlatformError from 'effect/PlatformError'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import {
  AuthIdentityError,
  AuthNetworkError,
  AuthError,
  Credential,
} from 'effect-harness/auth/Credential'
import * as CredentialStore from 'effect-harness/auth/CredentialStore'

describe('CredentialStore', () => {
  it.effect(
    'memory transactions serialize effectful updates and instances keep separate snapshots',
    () =>
      Effect.gen(function* () {
        const first = Context.get(
          yield* Layer.build(CredentialStore.layerMemory),
          CredentialStore.CredentialStore,
        )
        const second = Context.get(
          yield* Layer.build(CredentialStore.layerMemory),
          CredentialStore.CredentialStore,
        )
        const value = {
          _tag: 'apiKey' as const,
          provider: 'openai',
          apiKey: Redacted.make('token'),
        }
        const held = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const owner = yield* Effect.forkChild(
          first.modify('first', () =>
            Deferred.succeed(held, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as(value),
            ),
          ),
        )
        yield* Deferred.await(held)
        const attempting = yield* Deferred.make<void>()
        const writer = yield* Effect.forkChild(
          Deferred.succeed(attempting, undefined).pipe(Effect.andThen(first.set('second', value))),
        )
        yield* Deferred.await(attempting)
        yield* Effect.yieldNow
        assert.isUndefined(writer.pollUnsafe())
        yield* Deferred.succeed(release, undefined)
        assert.strictEqual(yield* Fiber.join(owner), value)
        yield* Fiber.join(writer)
        assert.deepStrictEqual(
          (yield* first.list).map(([key]) => key),
          ['first', 'second'],
        )
        assert.deepStrictEqual(yield* second.list, [])
      }).pipe(Effect.provide(BunCrypto.layer)),
  )
  it.effect(
    'fractional domain instants retain exact numeric credential bytes across fresh stores',
    () =>
      Effect.gen(function* () {
        for (const millis of [0.25, -0.5, 1700000000000.125]) {
          const instant = yield* Schema.decodeEffect(Time.EpochMillis)(millis)
          assert.isTrue(DateTime.isUtc(instant))
          assert.strictEqual(DateTime.toEpochMillis(instant), millis)
          assert.strictEqual(yield* Schema.encodeEffect(Time.EpochMillis)(instant), millis)
        }
        for (const millis of [NaN, Infinity, -Infinity])
          assert.isTrue(Option.isNone(Schema.decodeOption(Time.EpochMillis)(millis)))
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const path = `${directory}/private/credentials.json`
        const wire = {
          kind: 'opaqueOAuth' as const,
          provider: 'anthropic',
          authorizationServer: 'https://issuer.test',
          clientId: 'client',
          accessToken: 'sensitive',
          refreshToken: 'refresh',
          scopes: [],
          expiresAt: 1700000000000.125,
        }
        const credential = yield* Schema.decodeEffect(Credential)(wire)
        const first = Context.get(
          yield* Layer.build(CredentialStore.layerProtectedFile({ path })),
          CredentialStore.CredentialStore,
        )
        yield* first.set('account', credential)
        const bytes = yield* fs.readFileString(path)
        assert.include(bytes, '1700000000000.125')
        const second = Context.get(
          yield* Layer.build(CredentialStore.layerProtectedFile({ path })),
          CredentialStore.CredentialStore,
        )
        const value = yield* second.get('account')
        assertSome(value, credential)
        if (value.value._tag === 'opaqueOAuth') {
          assert.isTrue(DateTime.isUtc(value.value.expiresAt))
          assert.strictEqual(DateTime.toEpochMillis(value.value.expiresAt), wire.expiresAt)
          assert.deepStrictEqual(yield* Schema.encodeEffect(Credential)(value.value), wire)
        }
      }).pipe(Effect.provide(Layer.mergeAll(BunCrypto.layer, BunFileSystem.layer, BunPath.layer))),
  )
  it.effect(
    'memory store keeps host identity and failed atomic updates keep prior credentials',
    () =>
      Effect.gen(function* () {
        const store = yield* CredentialStore.CredentialStore
        const value = {
          _tag: 'apiKey',
          provider: 'openai',
          apiKey: Redacted.make('old'),
        } as const
        yield* store.set('key', value)
        assert.strictEqual(yield* store.hostId('openai'), yield* store.hostId('openai'))
        const failure = yield* store
          .modify('key', () =>
            Effect.fail(new AuthError({ reason: new AuthNetworkError({ message: 'test' }) })),
          )
          .pipe(Effect.flip)
        assert.strictEqual(failure.code, 'network')
        assert.deepStrictEqual(yield* store.get('key'), Option.some(value))
        yield* store.remove('key')
        assertNone(yield* store.get('key'))
      }).pipe(Effect.provide(CredentialStore.layerMemory.pipe(Layer.provide(BunCrypto.layer)))),
  )
  it.effect('protected store reloads owner-only credentials and host across fresh layers', () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped()
      const path = `${directory}/private/credentials.json`
      const layer = CredentialStore.layerProtectedFile({ path })
      const first = yield* Layer.build(layer)
      const store = Context.get(first, CredentialStore.CredentialStore)
      const host = yield* store.hostId('openai')
      yield* store.set('key', {
        _tag: 'apiKey',
        provider: 'openai',
        apiKey: Redacted.make('private-key'),
      })
      assert.strictEqual((yield* fs.stat(path)).mode & 0o077, 0)
      const second = yield* Layer.build(CredentialStore.layerProtectedFile({ path }))
      const reopened = Context.get(second, CredentialStore.CredentialStore)
      assert.strictEqual(yield* reopened.hostId('openai'), host)
      const read = yield* reopened.get('key')
      assertSome(read, {
        _tag: 'apiKey',
        provider: 'openai',
        apiKey: Redacted.make('private-key'),
      })
      yield* reopened
        .modify('key', () =>
          Effect.fail(
            new AuthError({ reason: new AuthIdentityError({ message: 'bad identity' }) }),
          ),
        )
        .pipe(Effect.flip)
      assert.deepStrictEqual(yield* reopened.get('key'), read)
      yield* fs.chmod(path, 0o644)
      assert.strictEqual((yield* reopened.get('key').pipe(Effect.flip)).code, 'storage')
    }).pipe(Effect.provide(Layer.mergeAll(BunFileSystem.layer, BunPath.layer, BunCrypto.layer))),
  )
  it.effect(
    'interruption during exclusive temporary creation removes staging and preserves the previous grant',
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const path = `${directory}/private/credentials.json`
        const previous = {
          _tag: 'apiKey',
          provider: 'openai',
          apiKey: Redacted.make('original'),
        } as const
        const initial = Context.get(
          yield* Layer.build(CredentialStore.layerProtectedFile({ path })),
          CredentialStore.CredentialStore,
        )
        yield* initial.set('key', previous)
        const created = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        let temporary = ''
        const delayed = FileSystem.FileSystem.of({
          ...fs,
          open: (name, options) =>
            fs.open(name, options).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  temporary = name
                  assert.strictEqual(options?.flag, 'wx')
                  assert.strictEqual(options?.mode, 0o600)
                }),
              ),
              Effect.tap(() => Deferred.succeed(created, undefined)),
              Effect.tap(() => Deferred.await(release)),
            ),
        })
        const store = Context.get(
          yield* Layer.build(
            CredentialStore.layerProtectedFile({ path }).pipe(
              Layer.provide(Layer.succeed(FileSystem.FileSystem, delayed)),
            ),
          ),
          CredentialStore.CredentialStore,
        )
        const owner = yield* Effect.forkChild(
          store.set('key', { ...previous, apiKey: Redacted.make('replacement') }),
        )
        yield* Deferred.await(created)
        assert.isTrue(yield* fs.exists(temporary))
        const interrupted = yield* Effect.forkChild(Fiber.interrupt(owner), {
          startImmediately: true,
        })
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(interrupted)
        assert.isFalse(yield* fs.exists(temporary))
        assert.isFalse(yield* fs.exists(`${path}.lock`))
        assert.deepStrictEqual(yield* initial.get('key'), Option.some(previous))
      }).pipe(Effect.provide(Layer.mergeAll(BunFileSystem.layer, BunPath.layer, BunCrypto.layer))),
  )
  it.effect(
    'a partial staging write retains its failure cause and closes the handle before removing its file',
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const path = `${directory}/private/credentials.json`
        const previous = {
          _tag: 'apiKey',
          provider: 'openai',
          apiKey: Redacted.make('original'),
        } as const
        const initial = Context.get(
          yield* Layer.build(CredentialStore.layerProtectedFile({ path })),
          CredentialStore.CredentialStore,
        )
        yield* initial.set('key', previous)
        const caught = new PlatformError.PlatformError(
          new PlatformError.SystemError({
            _tag: 'Unknown',
            module: 'FileSystem',
            method: 'writeAll',
            description: 'injected partial write',
          }),
        )
        let temporary = ''
        let closed = false
        let removed = false
        const failing = FileSystem.FileSystem.of({
          ...fs,
          open: (name, options) =>
            options?.flag !== 'wx'
              ? fs.open(name, options)
              : Effect.gen(function* () {
                  temporary = name
                  yield* Effect.addFinalizer(() =>
                    Effect.sync(() => {
                      closed = true
                    }),
                  )
                  const handle = yield* fs.open(name, options)
                  return {
                    ...handle,
                    writeAll: (bytes: Uint8Array) =>
                      handle.write(bytes.subarray(0, 4)).pipe(Effect.andThen(Effect.fail(caught))),
                  }
                }),
          remove: (name, options) =>
            Effect.gen(function* () {
              if (name === temporary) {
                assert.isTrue(closed)
                assert.strictEqual((yield* fs.stat(name)).size, 4n)
                removed = true
              }
              yield* fs.remove(name, options)
            }),
        })
        const store = Context.get(
          yield* Layer.build(
            CredentialStore.layerProtectedFile({ path }).pipe(
              Layer.provide(Layer.succeed(FileSystem.FileSystem, failing)),
            ),
          ),
          CredentialStore.CredentialStore,
        )
        const error = yield* store
          .set('key', { ...previous, apiKey: Redacted.make('replacement') })
          .pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, 'AuthStorageError')
        assert.strictEqual(error.cause, caught)
        assert.isTrue(removed)
        assert.isFalse(yield* fs.exists(temporary))
        assert.isFalse(yield* fs.exists(`${path}.lock`))
        assert.deepStrictEqual(yield* initial.get('key'), Option.some(previous))
      }).pipe(Effect.provide(Layer.mergeAll(BunFileSystem.layer, BunPath.layer, BunCrypto.layer))),
  )
  it.effect("exclusive staging collision preserves the other owner's file", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped()
      const path = `${directory}/private/credentials.json`
      let collision = ''
      const occupied = FileSystem.FileSystem.of({
        ...fs,
        open: (name, options) =>
          options?.flag !== 'wx'
            ? fs.open(name, options)
            : Effect.gen(function* () {
                collision = name
                yield* fs.writeFileString(name, 'another owner', { flag: 'wx', mode: 0o600 })
                return yield* fs.open(name, options)
              }),
      })
      const store = Context.get(
        yield* Layer.build(
          CredentialStore.layerProtectedFile({ path }).pipe(
            Layer.provide(Layer.succeed(FileSystem.FileSystem, occupied)),
          ),
        ),
        CredentialStore.CredentialStore,
      )
      const error = yield* store
        .set('key', { _tag: 'apiKey', provider: 'openai', apiKey: Redacted.make('replacement') })
        .pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, 'AuthStorageError')
      assert.strictEqual(yield* fs.readFileString(collision), 'another owner')
      assert.isFalse(yield* fs.exists(path))
      assert.isFalse(yield* fs.exists(`${path}.lock`))
    }).pipe(Effect.provide(Layer.mergeAll(BunFileSystem.layer, BunPath.layer, BunCrypto.layer))),
  )
  it.effect(
    'independent protected stores serialize modifications and interruption releases the lock',
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const path = `${directory}/private/credentials.json`
        const first = Context.get(
          yield* Layer.build(CredentialStore.layerProtectedFile({ path, lockRetries: 0 })),
          CredentialStore.CredentialStore,
        )
        const second = Context.get(
          yield* Layer.build(CredentialStore.layerProtectedFile({ path, lockRetries: 0 })),
          CredentialStore.CredentialStore,
        )
        const previous = {
          _tag: 'apiKey',
          provider: 'openai',
          apiKey: Redacted.make('original'),
        } as const
        yield* first.set('key', previous)
        const started = yield* Deferred.make<void>()
        const owner = yield* Effect.forkChild(
          first.modify('key', () =>
            Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
          ),
        )
        yield* Deferred.await(started)
        assert.strictEqual(
          (yield* second
            .set('key', { ...previous, apiKey: Redacted.make('contender') })
            .pipe(Effect.flip)).code,
          'busy',
        )
        yield* Fiber.interrupt(owner)
        assert.isFalse(yield* fs.exists(`${path}.lock`))
        assert.deepStrictEqual(yield* second.get('key'), Option.some(previous))
        yield* second.set('key', { ...previous, apiKey: Redacted.make('winner') })
        const latest = yield* first.get('key')
        assertSome(latest, { ...previous, apiKey: Redacted.make('winner') })
        if (latest.value._tag === 'apiKey')
          assert.strictEqual(Redacted.value(latest.value.apiKey), 'winner')
      }).pipe(Effect.provide(Layer.mergeAll(BunFileSystem.layer, BunPath.layer, BunCrypto.layer))),
  )
  it.effect(
    'protected stores reject corrupt documents, symlinks and public directories without chmod',
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const path = `${directory}/private/credentials.json`
        const store = Context.get(
          yield* Layer.build(CredentialStore.layerProtectedFile({ path })),
          CredentialStore.CredentialStore,
        )
        yield* fs.writeFileString(path, 'private malformed document', { mode: 0o600 })
        const corrupt = yield* store.get('key').pipe(Effect.flip)
        assert.strictEqual(corrupt.code, 'storage')
        assert.isFalse(JSON.stringify(corrupt).includes('private malformed document'))
        yield* fs.remove(path)
        yield* fs.symlink(`${directory}/missing-target`, path)
        assert.strictEqual((yield* store.get('key').pipe(Effect.flip)).code, 'storage')
        const publicDirectory = `${directory}/public`
        yield* fs.makeDirectory(publicDirectory, { mode: 0o755 })
        assert.strictEqual(
          (yield* Layer.build(
            CredentialStore.layerProtectedFile({ path: `${publicDirectory}/credentials.json` }),
          ).pipe(Effect.flip)).code,
          'storage',
        )
        assert.strictEqual((yield* fs.stat(publicDirectory)).mode & 0o777, 0o755)
        const link = `${directory}/linked-directory`
        yield* fs.symlink(`${directory}/private`, link)
        assert.strictEqual(
          (yield* Layer.build(
            CredentialStore.layerProtectedFile({ path: `${link}/credentials.json` }),
          ).pipe(Effect.flip)).code,
          'storage',
        )
      }).pipe(Effect.provide(Layer.mergeAll(BunFileSystem.layer, BunPath.layer, BunCrypto.layer))),
  )
})
