/**
 * The OpenAI adapter's implementation behind the account registry: a key is checked against the
 * endpoint's model list, and a custom endpoint serves what it listed.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, Layer, Redacted } from 'effect'
import { FetchHttpClient } from 'effect/http'
import { KeyValueStore } from 'effect/persistence'
import { Accounts, AccountError, Adapter } from '../../accounts/index.ts'
import { CredentialStore } from '../../credentials/index.ts'
import { Model } from '../../model/index.ts'
import { serve } from '../../test/fakeApi.ts'
import { OpenAi, OpenAiCodex } from '../index.ts'

const adapters = Adapter.group(OpenAi.adapter, OpenAiCodex.adapter)

const Services = Layer.mergeAll(
  KeyValueStore.layerMemory,
  CredentialStore.layerMemory(),
  FetchHttpClient.layer,
  OpenAi.layerAdapter,
  OpenAiCodex.layerAdapter(),
)

describe('OpenAi.implementation', () => {
  test('checks the key, and serves what a custom endpoint listed', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const api = yield* serve(({ headers }) =>
            headers['authorization'] === 'Bearer sk-good'
              ? { json: { object: 'list', data: [{ id: 'llama-3.3-70b' }, { id: 'qwen-3-32b' }] } }
              : { status: 401, json: { error: { message: 'Incorrect API key' } } },
          )
          const accounts = yield* Accounts.make(adapters)
          const input = {
            adapter: 'openai',
            name: 'local',
            label: 'Local',
            settings: { apiUrl: `${api.url}/v1` },
          }
          const refused = yield* Effect.flip(
            accounts.add({ ...input, apiKey: Redacted.make('sk-bad') }),
          )
          expect(refused.reason).toBeInstanceOf(AccountError.VerifyFailed)

          yield* accounts.add({ ...input, apiKey: Redacted.make('sk-good') })
          const model = yield* Model.makeDynamic(accounts.providers)
          expect((yield* model.models).map(({ provider, id }) => [provider, id])).toEqual([
            ['local', 'llama-3.3-70b'],
            ['local', 'qwen-3-32b'],
          ])
        }),
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- the test's entry point
      ).pipe(Effect.provide(Services)),
    ))
})
