import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as BunFileSystem from '@effect/platform-bun/BunFileSystem'
import * as BunPath from '@effect/platform-bun/BunPath'
import { assert, describe, it } from '@effect/vitest'
import { assertSome } from '@effect/vitest/utils'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as TestSchema from 'effect/testing/TestSchema'
import * as Credential from 'effect-harness/auth/Credential'
import * as CredentialStore from 'effect-harness/auth/CredentialStore'
import * as Time from 'effect-harness/auth/Time'

describe('Credential', () => {
  it.effect('singleton variants require encoded tags and default native constructor tags', () =>
    Effect.gen(function* () {
      const expected = {
        _tag: 'apiKey' as const,
        provider: 'openai',
        apiKey: Redacted.make('key'),
      }
      const checks = new TestSchema.Asserts(Credential.ApiKey)
      yield* checks
        .decoding()
        .succeedEffect({ _tag: 'apiKey', provider: 'openai', apiKey: 'key' }, expected)
      yield* checks
        .decoding()
        .failEffect({ provider: 'openai', apiKey: 'key' }, 'Missing key\n  at ["_tag"]')
      assert.deepStrictEqual(
        Credential.ApiKey.make({ provider: 'openai', apiKey: Redacted.make('key') }),
        expected,
      )
      yield* checks
        .encoding()
        .succeedEffect(expected, { _tag: 'apiKey', provider: 'openai', apiKey: 'key' })
      assert.isTrue(Credential.isApiKey(expected))
      assert.isFalse(Credential.isApiKey({ _tag: 'apiKey', provider: 'openai', apiKey: 'key' }))
    }),
  )
  it.effect('reads canonical tagged storage and retains redacted runtime values', () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const dir = yield* fs.makeTempDirectoryScoped()
      const path = `${dir}/credentials.json`
      const wire = {
        _tag: 'opaqueOAuth',
        provider: 'anthropic',
        authorizationServer: 'https://auth.example',
        clientId: 'client',
        accessToken: 'access-secret',
        refreshToken: 'refresh-secret',
        scopes: ['inference'],
        expiresAt: 0.125,
      }
      const snapshot = { version: 1, entries: [{ key: 'account', value: wire }], hosts: [] }
      const original = JSON.stringify(snapshot)
      yield* fs.writeFileString(path, original, { mode: 0o600 })
      yield* fs.chmod(path, 0o600)
      const store = Context.get(
        yield* Layer.build(CredentialStore.layerProtectedFile({ path })),
        CredentialStore.CredentialStore,
      )
      const expected = {
        _tag: 'opaqueOAuth' as const,
        provider: wire.provider,
        authorizationServer: wire.authorizationServer,
        clientId: wire.clientId,
        accessToken: Redacted.make(wire.accessToken),
        refreshToken: Redacted.make(wire.refreshToken),
        scopes: wire.scopes,
        expiresAt: Time.fromEpochMillis(wire.expiresAt),
      }
      assertSome(yield* store.get('account'), expected)
      assert.isFalse(JSON.stringify(expected).includes('secret'))
      yield* store.set('account', expected)
      assert.strictEqual(yield* fs.readFileString(path), original)
      assert.strictEqual((yield* fs.stat(path)).mode & 0o077, 0)
    }).pipe(Effect.provide(Layer.mergeAll(BunCrypto.layer, BunFileSystem.layer, BunPath.layer))),
  )

  it.effect('redacts credentials while the explicit persistence codec roundtrips them', () =>
    Effect.gen(function* () {
      const credential = {
        _tag: 'apiKey' as const,
        provider: 'openai',
        apiKey: Redacted.make('private-key'),
      }
      const encoded = { _tag: 'apiKey' as const, provider: 'openai', apiKey: 'private-key' }
      assert.isFalse(JSON.stringify(credential).includes('private-key'))
      const checks = new TestSchema.Asserts(Credential.Credential)
      yield* checks.decoding().succeedEffect(encoded, credential)
      yield* checks.encoding().succeedEffect(credential, encoded)
      assert.strictEqual(credential._tag, 'apiKey')
      assert.strictEqual(Redacted.value(credential.apiKey), 'private-key')
    }),
  )
})
