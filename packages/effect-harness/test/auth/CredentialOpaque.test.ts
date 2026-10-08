import * as TestSchema from 'effect/testing/TestSchema'

import * as Time from 'effect-harness/auth/Time'

import { assertSome } from '@effect/vitest/utils'

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

import { AuthNetworkError, AuthTokenError, AuthError } from 'effect-harness/auth/AuthError'
import { Credential, OpaqueOAuth } from 'effect-harness/auth/Credential'

import * as CredentialStore from 'effect-harness/auth/CredentialStore'

const encoded = {
  _tag: 'opaqueOAuth',
  provider: 'opaque-provider',
  authorizationServer: 'https://auth.example',
  clientId: 'public-client',
  accessToken: 'secret-access',
  refreshToken: 'secret-refresh',
  scopes: ['inference'],
  expiresAt: 3600000,
} as const

describe('CredentialOpaque', () => {
  describe('provider-neutral opaque OAuth credentials', () => {
    it.effect('redacts and persistence-roundtrips without inventing an OIDC identity', () =>
      Effect.gen(function* () {
        const value = {
          _tag: 'opaqueOAuth' as const,
          provider: encoded.provider,
          authorizationServer: encoded.authorizationServer,
          clientId: encoded.clientId,
          accessToken: Redacted.make(encoded.accessToken),
          refreshToken: Redacted.make(encoded.refreshToken),
          scopes: encoded.scopes,
          expiresAt: Time.fromEpochMillis(encoded.expiresAt),
        }
        assert.strictEqual(value._tag, 'opaqueOAuth')
        assert.isFalse(JSON.stringify(value).includes('secret-'))
        assert.isFalse('subject' in value)
        assert.isFalse('idToken' in value)
        const checks = new TestSchema.Asserts(OpaqueOAuth)
        yield* checks.decoding().succeedEffect(encoded, value)
        yield* checks.encoding().succeedEffect(value, encoded)
        yield* checks
          .decoding({ parseOptions: { onExcessProperty: 'error' } })
          .failEffect(
            { ...encoded, subject: 'invented' },
            'Expected no excess property\n  at ["subject"]',
          )
        yield* checks
          .decoding()
          .failEffect(
            { ...encoded, accessToken: '' },
            'Expected a value with a length of at least 1\n  at ["accessToken"]',
          )
      }),
    )
    it.effect('memory locked updates preserve an opaque grant on refresh failure', () =>
      Effect.gen(function* () {
        const store = yield* CredentialStore.CredentialStore
        const value = yield* Schema.decodeEffect(Credential)(encoded)
        yield* store.set('caller-owned-key', value)
        yield* store
          .modify('caller-owned-key', () =>
            Effect.fail(
              new AuthError({ reason: new AuthTokenError({ message: 'refresh failed' }) }),
            ),
          )
          .pipe(Effect.flip)
        assert.deepStrictEqual(yield* store.get('caller-owned-key'), Option.some(value))
      }).pipe(Effect.provide(CredentialStore.layerMemory.pipe(Layer.provide(BunCrypto.layer)))),
    )
    it.effect('protected file reopens opaque grants with owner-only atomic persistence', () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const path = `${directory}/private/credentials.json`
        const first = Context.get(
          yield* Layer.build(CredentialStore.layerProtectedFile({ path })),
          CredentialStore.CredentialStore,
        )
        yield* first.set('account', yield* Schema.decodeEffect(Credential)(encoded))
        assert.strictEqual((yield* fs.stat(path)).mode & 0o077, 0)
        const reopened = Context.get(
          yield* Layer.build(CredentialStore.layerProtectedFile({ path })),
          CredentialStore.CredentialStore,
        )
        const saved = yield* reopened.get('account')
        assertSome(saved, yield* Schema.decodeEffect(Credential)(encoded))
        if (saved.value._tag === 'opaqueOAuth')
          assert.strictEqual(Redacted.value(saved.value.accessToken), encoded.accessToken)
        yield* reopened
          .modify('account', () =>
            Effect.fail(new AuthError({ reason: new AuthNetworkError({ message: 'failure' }) })),
          )
          .pipe(Effect.flip)
        assert.deepStrictEqual(yield* reopened.get('account'), saved)
      }).pipe(Effect.provide(Layer.mergeAll(BunCrypto.layer, BunFileSystem.layer, BunPath.layer))),
    )
  })
})
