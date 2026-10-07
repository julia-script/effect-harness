import * as AuthDuration from '../src/Duration.ts'
import * as Duration from 'effect/Duration'
import * as DateTime from 'effect/DateTime'
import * as Time from '../src/Time.ts'
import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as BunFileSystem from '@effect/platform-bun/BunFileSystem'
import * as BunPath from '@effect/platform-bun/BunPath'
import { assert, describe, it } from '@effect/vitest'
import * as Clock from 'effect/Clock'
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
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import { exportJWK, generateKeyPair, SignJWT } from 'jose'
import { AuthIdentityError, AuthNetworkError, AuthError, Credential } from '../src/Credential.ts'
import * as Store from '../src/CredentialStore.ts'
import * as Jwt from '../src/Jwt.ts'
import * as JoseJwt from '../src/JoseJwt.ts'
import * as Pkce from '../src/Pkce.ts'
import * as Token from '../src/Token.ts'

describe('auth', () => {
  it.effect(
    'duration normalization retains exact bigint nanos and detaches foreign native values',
    () =>
      Effect.gen(function* () {
        const nanos = 9007199254740993123456789n
        const input = {
          '~effect/Duration': '~effect/Duration',
          value: { _tag: 'Nanos', nanos },
        } as const
        const normalized = yield* AuthDuration.fromInput(
          input as unknown as Duration.Input,
          'Invalid duration',
        )
        assert.isFalse(Object.is(normalized, input))
        assert.deepEqual(normalized.value, { _tag: 'Nanos', nanos })
        assert.deepEqual(
          (yield* AuthDuration.fromInput(Duration.nanos(nanos), 'Invalid duration')).value,
          { _tag: 'Nanos', nanos },
        )
        assert.strictEqual(
          Duration.toMillis(
            yield* AuthDuration.fromInput({ milliseconds: 0.25 }, 'Invalid duration'),
          ),
          0.25,
        )
      }),
  )

  it.effect(
    'memory transactions serialize effectful updates and instances keep separate snapshots',
    () =>
      Effect.gen(function* () {
        const first = Context.get(yield* Layer.build(Store.layerMemory), Store.CredentialStore)
        const second = Context.get(yield* Layer.build(Store.layerMemory), Store.CredentialStore)
        const value = {
          kind: 'apiKey' as const,
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
        assert.deepEqual(
          (yield* first.list).map(([key]) => key),
          ['first', 'second'],
        )
        assert.deepEqual(yield* second.list, [])
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
          yield* Layer.build(Store.layerProtectedFile({ path })),
          Store.CredentialStore,
        )
        yield* first.set('account', credential)
        const bytes = yield* fs.readFileString(path)
        assert.include(bytes, '1700000000000.125')
        const second = Context.get(
          yield* Layer.build(Store.layerProtectedFile({ path })),
          Store.CredentialStore,
        )
        const value = yield* second.get('account')
        assert.isTrue(Option.isSome(value))
        if (Option.isSome(value) && value.value.kind === 'opaqueOAuth') {
          assert.isTrue(DateTime.isUtc(value.value.expiresAt))
          assert.strictEqual(DateTime.toEpochMillis(value.value.expiresAt), wire.expiresAt)
          assert.deepEqual(yield* Schema.encodeEffect(Credential)(value.value), wire)
        }
      }).pipe(Effect.provide(Layer.mergeAll(BunCrypto.layer, BunFileSystem.layer, BunPath.layer))),
  )

  it.effect('JWKS reads observe key rotation and recover after a failed observation', () =>
    Effect.gen(function* () {
      const first = yield* Effect.tryPromise(() => generateKeyPair('RS256', { extractable: true }))
      const second = yield* Effect.tryPromise(() => generateKeyPair('RS256', { extractable: true }))
      const keys = yield* Effect.tryPromise(() =>
        Promise.all([exportJWK(first.publicKey), exportJWK(second.publicKey)]),
      )
      const expiry = (yield* Clock.currentTimeMillis) / 1000 + 3600.00025
      const sign = (privateKey: typeof first.privateKey, kid: string) =>
        Effect.tryPromise(() =>
          new SignJWT({ sub: kid })
            .setProtectedHeader({ alg: 'RS256', kid })
            .setIssuer('https://issuer.test')
            .setAudience('client')
            .setExpirationTime(expiry)
            .sign(privateKey),
        )
      const tokens = [
        Redacted.make(yield* sign(first.privateKey, 'first')),
        Redacted.make(yield* sign(second.privateKey, 'second')),
      ]
      let reads = 0
      const http = HttpClient.make((request) => {
        const index = reads++
        return Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            Response.json(
              index === 1
                ? { keys: [] }
                : {
                    keys: [{ ...keys[index === 0 ? 0 : 1], kid: index === 0 ? 'first' : 'second' }],
                  },
            ),
          ),
        )
      })
      const verifier = yield* JoseJwt.make.pipe(Effect.provideService(HttpClient.HttpClient, http))
      const options = {
        issuer: 'https://issuer.test',
        audience: 'client',
        jwksUrl: 'https://issuer.test/jwks',
      }
      const identity = yield* verifier.verify(tokens[0]!, options)
      assert.strictEqual(identity.sub, 'first')
      assert.strictEqual(DateTime.toEpochMillis(identity.exp), expiry * 1000)
      assert.strictEqual(
        (yield* verifier.verify(tokens[1]!, options).pipe(Effect.flip)).code,
        'identity',
      )
      assert.strictEqual((yield* verifier.verify(tokens[1]!, options)).sub, 'second')
      assert.strictEqual(reads, 3)
    }),
  )

  it.effect('redacts credentials while the explicit persistence codec roundtrips them', () =>
    Effect.gen(function* () {
      const credential = yield* Schema.decodeEffect(Credential)({
        kind: 'apiKey',
        provider: 'openai',
        apiKey: 'private-key',
      })
      assert.isFalse(JSON.stringify(credential).includes('private-key'))
      const encoded = yield* Schema.encodeEffect(Credential)(credential)
      assert.deepStrictEqual(encoded, { kind: 'apiKey', provider: 'openai', apiKey: 'private-key' })
      const decoded = yield* Schema.decodeEffect(Credential)(encoded)
      assert.strictEqual(decoded.kind, 'apiKey')
      if (decoded.kind === 'apiKey')
        assert.strictEqual(Redacted.value(decoded.apiKey), 'private-key')
    }),
  )

  it.effect('PKCE uses secure fresh state/nonce and a 43-character S256 verifier', () =>
    Effect.gen(function* () {
      const first = yield* Pkce.make
      const second = yield* Pkce.make
      assert.match(Redacted.value(first.verifier), /^[a-zA-Z0-9_-]{43}$/)
      assert.match(first.challenge, /^[a-zA-Z0-9_-]{43}$/)
      assert.notStrictEqual(first.state, second.state)
      assert.notStrictEqual(first.nonce, first.state)
      assert.isFalse(JSON.stringify(first).includes(Redacted.value(first.verifier)))
    }).pipe(Effect.provide(BunCrypto.layer)),
  )

  it.effect(
    'memory store keeps host identity and failed atomic updates keep prior credentials',
    () =>
      Effect.gen(function* () {
        const store = yield* Store.CredentialStore
        const value = { kind: 'apiKey', provider: 'openai', apiKey: Redacted.make('old') } as const
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
        assert.isTrue(Option.isNone(yield* store.get('key')))
      }).pipe(Effect.provide(Store.layerMemory.pipe(Layer.provide(BunCrypto.layer)))),
  )

  it.effect('protected store reloads owner-only credentials and host across fresh layers', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const path = `${directory}/private/credentials.json`
        const layer = Store.layerProtectedFile({ path })
        const first = yield* Layer.build(layer)
        const store = Context.get(first, Store.CredentialStore)
        const host = yield* store.hostId('openai')
        yield* store.set('key', {
          kind: 'apiKey',
          provider: 'openai',
          apiKey: Redacted.make('private-key'),
        })
        assert.strictEqual((yield* fs.stat(path)).mode & 0o077, 0)
        const second = yield* Layer.build(Store.layerProtectedFile({ path }))
        const reopened = Context.get(second, Store.CredentialStore)
        assert.strictEqual(yield* reopened.hostId('openai'), host)
        const read = yield* reopened.get('key')
        assert.isTrue(Option.isSome(read))
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
      }),
    ).pipe(Effect.provide(Layer.mergeAll(BunFileSystem.layer, BunPath.layer, BunCrypto.layer))),
  )

  it.effect('token requests decode valid grants and sanitize error bodies', () =>
    Effect.gen(function* () {
      const request = yield* Token.request('https://auth.example/token', {
        grant_type: 'refresh_token',
        refresh_token: Redacted.make('not-logged'),
      }).pipe(
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make((req) => {
            assert.strictEqual(req.body._tag, 'Uint8Array')
            return Effect.succeed(
              HttpClientResponse.fromWeb(
                req,
                new Response(
                  JSON.stringify({
                    access_token: 'access',
                    refresh_token: 'rotated',
                    expires_in: 3600,
                    token_type: 'Bearer',
                    scope: 'read',
                  }),
                ),
              ),
            )
          }),
        ),
      )
      assert.strictEqual(Redacted.value(request.refresh_token), 'rotated')
      const rejected = yield* Token.request('https://auth.example/token', {
        refresh_token: Redacted.make('not-logged'),
      }).pipe(
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make((req) =>
            Effect.succeed(
              HttpClientResponse.fromWeb(req, new Response('secret-echo', { status: 400 })),
            ),
          ),
        ),
        Effect.flip,
      )
      assert.strictEqual(rejected.status, 400)
      assert.isFalse(JSON.stringify(rejected).includes('secret-echo'))
      assert.isFalse(JSON.stringify(rejected).includes('not-logged'))
    }),
  )

  it.effect(
    'interruption during exclusive temporary creation removes staging and preserves the previous grant',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped()
          const path = `${directory}/private/credentials.json`
          const previous = {
            kind: 'apiKey',
            provider: 'openai',
            apiKey: Redacted.make('original'),
          } as const
          const initial = Context.get(
            yield* Layer.build(Store.layerProtectedFile({ path })),
            Store.CredentialStore,
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
              Store.layerProtectedFile({ path }).pipe(
                Layer.provide(Layer.succeed(FileSystem.FileSystem, delayed)),
              ),
            ),
            Store.CredentialStore,
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
        }),
      ).pipe(Effect.provide(Layer.mergeAll(BunFileSystem.layer, BunPath.layer, BunCrypto.layer))),
  )

  it.effect(
    'a partial staging write retains its failure cause and closes the handle before removing its file',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped()
          const path = `${directory}/private/credentials.json`
          const previous = {
            kind: 'apiKey',
            provider: 'openai',
            apiKey: Redacted.make('original'),
          } as const
          const initial = Context.get(
            yield* Layer.build(Store.layerProtectedFile({ path })),
            Store.CredentialStore,
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
                        handle
                          .write(bytes.subarray(0, 4))
                          .pipe(Effect.andThen(Effect.fail(caught))),
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
              Store.layerProtectedFile({ path }).pipe(
                Layer.provide(Layer.succeed(FileSystem.FileSystem, failing)),
              ),
            ),
            Store.CredentialStore,
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
        }),
      ).pipe(Effect.provide(Layer.mergeAll(BunFileSystem.layer, BunPath.layer, BunCrypto.layer))),
  )

  it.effect("exclusive staging collision preserves the other owner's file", () =>
    Effect.scoped(
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
            Store.layerProtectedFile({ path }).pipe(
              Layer.provide(Layer.succeed(FileSystem.FileSystem, occupied)),
            ),
          ),
          Store.CredentialStore,
        )
        const error = yield* store
          .set('key', { kind: 'apiKey', provider: 'openai', apiKey: Redacted.make('replacement') })
          .pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, 'AuthStorageError')
        assert.strictEqual(yield* fs.readFileString(collision), 'another owner')
        assert.isFalse(yield* fs.exists(path))
        assert.isFalse(yield* fs.exists(`${path}.lock`))
      }),
    ).pipe(Effect.provide(Layer.mergeAll(BunFileSystem.layer, BunPath.layer, BunCrypto.layer))),
  )

  it.effect(
    'independent protected stores serialize modifications and interruption releases the lock',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped()
          const path = `${directory}/private/credentials.json`
          const first = Context.get(
            yield* Layer.build(Store.layerProtectedFile({ path, lockRetries: 0 })),
            Store.CredentialStore,
          )
          const second = Context.get(
            yield* Layer.build(Store.layerProtectedFile({ path, lockRetries: 0 })),
            Store.CredentialStore,
          )
          const previous = {
            kind: 'apiKey',
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
          assert.isTrue(Option.isSome(latest))
          if (Option.isSome(latest) && latest.value.kind === 'apiKey')
            assert.strictEqual(Redacted.value(latest.value.apiKey), 'winner')
        }),
      ).pipe(Effect.provide(Layer.mergeAll(BunFileSystem.layer, BunPath.layer, BunCrypto.layer))),
  )

  it.effect(
    'protected stores reject corrupt documents, symlinks and public directories without chmod',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped()
          const path = `${directory}/private/credentials.json`
          const store = Context.get(
            yield* Layer.build(Store.layerProtectedFile({ path })),
            Store.CredentialStore,
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
              Store.layerProtectedFile({ path: `${publicDirectory}/credentials.json` }),
            ).pipe(Effect.flip)).code,
            'storage',
          )
          assert.strictEqual((yield* fs.stat(publicDirectory)).mode & 0o777, 0o755)
          const link = `${directory}/linked-directory`
          yield* fs.symlink(`${directory}/private`, link)
          assert.strictEqual(
            (yield* Layer.build(
              Store.layerProtectedFile({ path: `${link}/credentials.json` }),
            ).pipe(Effect.flip)).code,
            'storage',
          )
        }),
      ).pipe(Effect.provide(Layer.mergeAll(BunFileSystem.layer, BunPath.layer, BunCrypto.layer))),
  )

  it.effect(
    'JWT boundary verifies signature, issuer, audience, expiration, nonce and subject',
    () =>
      Effect.gen(function* () {
        const pair = yield* Effect.tryPromise(() => generateKeyPair('RS256', { extractable: true }))
        const key = yield* Effect.tryPromise(() => exportJWK(pair.publicKey))
        const now = Math.floor((yield* Clock.currentTimeMillis) / 1000)
        const sign = (overrides: Record<string, unknown>) =>
          Effect.tryPromise(() =>
            new SignJWT({ sub: 'verified', nonce: 'expected', ...overrides })
              .setProtectedHeader({ alg: 'RS256', kid: 'test' })
              .setIssuer('https://issuer.test')
              .setAudience('issued-client')
              .setExpirationTime(now + 3600)
              .sign(pair.privateKey),
          )
        const http = HttpClient.make((req) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(JSON.stringify({ keys: [{ ...key, kid: 'test' }] })),
            ),
          ),
        )
        const verifier = yield* Jwt.Jwt.pipe(
          Effect.provide(
            JoseJwt.layer.pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, http))),
          ),
        )
        const signed = Redacted.make(yield* sign({}))
        const options = {
          issuer: 'https://issuer.test',
          audience: 'issued-client',
          jwksUrl: 'https://issuer.test/jwks',
          nonce: 'expected',
        }
        assert.strictEqual((yield* verifier.verify(signed, options)).sub, 'verified')
        for (const bad of [
          { ...options, nonce: 'wrong' },
          { ...options, issuer: 'https://other.test' },
          { ...options, audience: 'wrong' },
        ])
          assert.strictEqual(
            (yield* verifier.verify(signed, bad).pipe(Effect.flip)).code,
            'identity',
          )
        const missing = Redacted.make(yield* sign({ sub: '' }))
        assert.strictEqual(
          (yield* verifier.verify(missing, options).pipe(Effect.flip)).code,
          'identity',
        )
        const segments = Redacted.value(signed).split('.')
        const tampered = Redacted.make(`${segments[0]}.${segments[1]}.bad-signature`)
        assert.strictEqual(
          (yield* verifier.verify(tampered, options).pipe(Effect.flip)).code,
          'identity',
        )
        const expired = Redacted.make(
          yield* Effect.tryPromise(() =>
            new SignJWT({ sub: 'verified', nonce: 'expected' })
              .setProtectedHeader({ alg: 'RS256', kid: 'test' })
              .setIssuer(options.issuer)
              .setAudience(options.audience)
              .setExpirationTime(now - 1)
              .sign(pair.privateKey),
          ),
        )
        assert.strictEqual(
          (yield* verifier.verify(expired, options).pipe(Effect.flip)).code,
          'identity',
        )
      }),
  )
})
