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
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import { exportJWK, generateKeyPair, SignJWT } from 'jose'
import { AuthError, Credential } from '../src/Credential.ts'
import * as Store from '../src/CredentialStore.ts'
import * as Jwt from '../src/Jwt.ts'
import * as Pkce from '../src/Pkce.ts'
import * as Token from '../src/Token.ts'

describe('auth', () => {
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
      const first = yield* Pkce.make()
      const second = yield* Pkce.make()
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
          .modify('key', () => Effect.fail(new AuthError({ reason: 'network', message: 'test' })))
          .pipe(Effect.flip)
        assert.strictEqual(failure.reason, 'network')
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
            Effect.fail(new AuthError({ reason: 'identity', message: 'bad identity' })),
          )
          .pipe(Effect.flip)
        assert.deepStrictEqual(yield* reopened.get('key'), read)
        yield* fs.chmod(path, 0o644)
        assert.strictEqual((yield* reopened.get('key').pipe(Effect.flip)).reason, 'storage')
      }),
    ).pipe(Effect.provide(Layer.mergeAll(BunFileSystem.layer, BunPath.layer, BunCrypto.layer))),
  )

  it.effect('token requests decode valid grants and sanitize error bodies', () =>
    Effect.gen(function* () {
      const request = yield* Token.request('https://auth.example/token', {
        grant_type: 'refresh_token',
        refresh_token: 'not-logged',
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
        refresh_token: 'not-logged',
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
              .pipe(Effect.flip)).reason,
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
          assert.strictEqual(corrupt.reason, 'storage')
          assert.isFalse(JSON.stringify(corrupt).includes('private malformed document'))
          yield* fs.remove(path)
          yield* fs.symlink(`${directory}/missing-target`, path)
          assert.strictEqual((yield* store.get('key').pipe(Effect.flip)).reason, 'storage')
          const publicDirectory = `${directory}/public`
          yield* fs.makeDirectory(publicDirectory, { mode: 0o755 })
          assert.strictEqual(
            (yield* Layer.build(
              Store.layerProtectedFile({ path: `${publicDirectory}/credentials.json` }),
            ).pipe(Effect.flip)).reason,
            'storage',
          )
          assert.strictEqual((yield* fs.stat(publicDirectory)).mode & 0o777, 0o755)
          const link = `${directory}/linked-directory`
          yield* fs.symlink(`${directory}/private`, link)
          assert.strictEqual(
            (yield* Layer.build(
              Store.layerProtectedFile({ path: `${link}/credentials.json` }),
            ).pipe(Effect.flip)).reason,
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
          Effect.provide(Jwt.layer.pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, http)))),
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
            (yield* verifier.verify(signed, bad).pipe(Effect.flip)).reason,
            'identity',
          )
        const missing = Redacted.make(yield* sign({ sub: '' }))
        assert.strictEqual(
          (yield* verifier.verify(missing, options).pipe(Effect.flip)).reason,
          'identity',
        )
        const segments = Redacted.value(signed).split('.')
        const tampered = Redacted.make(`${segments[0]}.${segments[1]}.bad-signature`)
        assert.strictEqual(
          (yield* verifier.verify(tampered, options).pipe(Effect.flip)).reason,
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
          (yield* verifier.verify(expired, options).pipe(Effect.flip)).reason,
          'identity',
        )
      }),
  )
})
