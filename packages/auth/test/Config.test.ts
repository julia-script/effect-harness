import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as BunFileSystem from '@effect/platform-bun/BunFileSystem'
import * as BunPath from '@effect/platform-bun/BunPath'
import { assert, describe, it } from '@effect/vitest'
import * as Config from 'effect/Config'
import * as ConfigProvider from 'effect/ConfigProvider'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Store from '../src/CredentialStore.ts'
import * as JoseJwt from '../src/JoseJwt.ts'
import * as Jwt from '../src/Jwt.ts'
import * as Token from '../src/Token.ts'

class UpdateInput extends Context.Service<UpdateInput, { readonly value: string }>()(
  'UpdateInput',
) {}

describe('portable auth construction and configuration', () => {
  it.effect(
    'protected file Config uses the injected provider and keeps generic callback requirements',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped()
          const path = `${directory}/private/configured.json`
          const layer = Store.layerProtectedFileConfig({
            path: Config.String('CREDENTIAL_PATH'),
            lockRetries: Config.Int('LOCK_RETRIES'),
          })
          const context = yield* Layer.build(layer).pipe(
            Effect.provideService(
              ConfigProvider.ConfigProvider,
              ConfigProvider.fromUnknown({ CREDENTIAL_PATH: path, LOCK_RETRIES: 0 }),
            ),
          )
          const store = Context.get(context, Store.CredentialStore)
          yield* store
            .modify('selected', () =>
              Effect.gen(function* () {
                const input = yield* UpdateInput
                return {
                  kind: 'apiKey',
                  provider: 'test',
                  apiKey: Redacted.make(input.value),
                } as const
              }),
            )
            .pipe(Effect.provideService(UpdateInput, UpdateInput.of({ value: 'injected-key' })))
          assert.isTrue(yield* fs.exists(path))
          const failure = yield* Layer.build(
            Store.layerProtectedFileConfig({ path: Config.String('ABSENT') }),
          ).pipe(
            Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({})),
            Effect.flip,
          )
          assert.strictEqual(failure._tag, 'ConfigError')
        }),
      ).pipe(Effect.provide(Layer.mergeAll(BunFileSystem.layer, BunPath.layer, BunCrypto.layer))),
  )

  it.effect(
    'sensitive helper arguments stay redacted and serialize only into URL-encoded bodies',
    () =>
      Effect.gen(function* () {
        const fields: Token.Fields = {
          grant_type: 'authorization_code',
          client_id: 'public-client',
          code: Redacted.make('code &?#'),
          code_verifier: Redacted.make('private-verifier'),
          state: Redacted.make('private-state'),
          refresh_token: Redacted.make('private-refresh'),
        }
        assert.isFalse(JSON.stringify(fields).includes('private-'))
        assert.isFalse(JSON.stringify(fields).includes('code &?#'))
        let requests = 0
        const http = HttpClient.make((request) => {
          requests++
          assert.strictEqual(request.headers['content-type'], 'application/x-www-form-urlencoded')
          if (request.body._tag !== 'Uint8Array') return Effect.die('Expected URL-encoded body')
          const serialized = new URLSearchParams(new TextDecoder().decode(request.body.body))
          assert.strictEqual(serialized.get('client_id'), 'public-client')
          assert.strictEqual(serialized.get('code'), 'code &?#')
          assert.strictEqual(serialized.get('code_verifier'), 'private-verifier')
          assert.strictEqual(serialized.get('state'), 'private-state')
          assert.strictEqual(serialized.get('refresh_token'), 'private-refresh')
          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              Response.json({
                access_token: 'access',
                refresh_token: 'refresh',
                token_type: 'Bearer',
                expires_in: 60,
              }),
            ),
          )
        })
        yield* Token.request('https://auth.example/token', fields).pipe(
          Effect.provideService(HttpClient.HttpClient, http),
        )
        yield* Token.revoke('https://auth.example/revoke', fields).pipe(
          Effect.provideService(HttpClient.HttpClient, http),
        )
        assert.strictEqual(requests, 2)
        if (fields.code_verifier === undefined) return yield* Effect.die('Expected verifier')
        assert.strictEqual(Redacted.value(fields.code_verifier), 'private-verifier')
      }),
  )

  it.effect('Jose construction captures the substituted HTTP client while Jwt stays portable', () =>
    Effect.gen(function* () {
      assert.isFalse('layer' in Jwt)
      let requests = 0
      const http = HttpClient.make((request) => {
        requests++
        return Effect.succeed(
          HttpClientResponse.fromWeb(request, Response.json({}, { status: 503 })),
        )
      })
      const service = yield* JoseJwt.make.pipe(Effect.provideService(HttpClient.HttpClient, http))
      const error = yield* service
        .verify(Redacted.make('not-a-token'), {
          issuer: 'issuer',
          audience: 'audience',
          jwksUrl: 'https://auth.example/keys',
        })
        .pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, 'AuthIdentityError')
      assert.strictEqual(requests, 1)
    }),
  )
})
