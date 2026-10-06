import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as BunFileSystem from '@effect/platform-bun/BunFileSystem'
import * as BunPath from '@effect/platform-bun/BunPath'
import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import { AuthError, Credential, OpaqueOAuth } from '../src/Credential.ts'
import * as Store from '../src/CredentialStore.ts'
const encoded = {
  kind: 'opaqueOAuth',
  provider: 'opaque-provider',
  authorizationServer: 'https://auth.example',
  clientId: 'public-client',
  accessToken: 'secret-access',
  refreshToken: 'secret-refresh',
  scopes: ['inference'],
  expiresAt: 3600000,
} as const

describe('provider-neutral opaque OAuth credentials', () => {
  it.effect('redacts and persistence-roundtrips without inventing an OIDC identity', () =>
    Effect.gen(function* () {
      const value = yield* Schema.decodeEffect(Credential)(encoded)
      assert.strictEqual(value.kind, 'opaqueOAuth')
      assert.isFalse(JSON.stringify(value).includes('secret-'))
      assert.isFalse('subject' in value)
      assert.isFalse('idToken' in value)
      assert.deepEqual(yield* Schema.encodeEffect(Credential)(value), encoded)
      assert.isTrue(
        Option.isNone(
          Schema.decodeUnknownOption(OpaqueOAuth, { onExcessProperty: 'error' })({
            ...encoded,
            subject: 'invented',
          }),
        ),
      )
      assert.isTrue(
        Option.isNone(Schema.decodeOption(OpaqueOAuth)({ ...encoded, accessToken: '' })),
      )
    }),
  )
  it.effect('memory locked updates preserve an opaque grant on refresh failure', () =>
    Effect.gen(function* () {
      const store = yield* Store.CredentialStore
      const value = yield* Schema.decodeEffect(Credential)(encoded)
      yield* store.set('caller-owned-key', value)
      yield* store
        .modify('caller-owned-key', () =>
          Effect.fail(new AuthError({ reason: 'token', message: 'refresh failed' })),
        )
        .pipe(Effect.flip)
      assert.deepEqual(yield* store.get('caller-owned-key'), Option.some(value))
    }).pipe(Effect.provide(Store.layerMemory.pipe(Layer.provide(BunCrypto.layer)))),
  )
  it.effect('protected file reopens opaque grants with owner-only atomic persistence', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const path = `${directory}/private/credentials.json`
        const first = Context.get(
          yield* Layer.build(Store.layerProtectedFile({ path })),
          Store.CredentialStore,
        )
        yield* first.set('account', yield* Schema.decodeEffect(Credential)(encoded))
        assert.strictEqual((yield* fs.stat(path)).mode & 0o077, 0)
        const reopened = Context.get(
          yield* Layer.build(Store.layerProtectedFile({ path })),
          Store.CredentialStore,
        )
        const saved = yield* reopened.get('account')
        assert.isTrue(Option.isSome(saved))
        if (Option.isSome(saved) && saved.value.kind === 'opaqueOAuth')
          assert.strictEqual(Redacted.value(saved.value.accessToken), encoded.accessToken)
        yield* reopened
          .modify('account', () =>
            Effect.fail(new AuthError({ reason: 'network', message: 'failure' })),
          )
          .pipe(Effect.flip)
        assert.deepEqual(yield* reopened.get('account'), saved)
      }),
    ).pipe(Effect.provide(Layer.mergeAll(BunCrypto.layer, BunFileSystem.layer, BunPath.layer))),
  )
})
