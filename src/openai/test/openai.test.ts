/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * The OpenAI provider against a local stand-in for the Responses API: the request it sends for the
 * catalog model and options, and the response it maps back.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, Redacted } from 'effect'
import { FetchHttpClient } from 'effect/http'
import { CredentialStore } from '../../credentials/index.ts'
import { Model, ModelError } from '../../model/index.ts'
import type { Message } from '../../model/index.ts'
import { serve } from '../../model/test/fakeApi.ts'
import { OpenAi } from '../index.ts'
import { events } from './support.ts'

const user = (text: string): Message.Message => ({ role: 'user', content: text, timestamp: 0 })

/** Ask `modelId` through a Model over `provider`, with `options`. */
const ask = <R>(provider: Model.Provider<R>, modelId: string, options: Model.Options = {}) =>
  Effect.gen(function* () {
    const model = yield* Model.make([provider])
    const info = yield* model.info(OpenAi.ref(modelId))
    if (info === undefined) return yield* Effect.die(`no ${modelId}`)
    const request = { model: OpenAi.ref(modelId), messages: [user('hi')], options }
    return yield* Model.collect(model.stream(request), info)
  })

describe('OpenAi.provider', () => {
  test('sends the catalog model, its effort and the key, and maps the response', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const api = yield* serve(() => events)
          const provider = OpenAi.provider({
            apiKey: Redacted.make('sk-given'),
            apiUrl: `${api.url}/v1`,
          })
          const answer = yield* ask(provider, 'gpt-5.5', { reasoning: 'minimal', maxTokens: 500 })
          expect(answer).toMatchObject({
            role: 'assistant',
            content: [{ type: 'text', text: 'Hello' }],
            provider: 'openai',
            model: 'gpt-5.5',
            stopReason: 'stop',
          })
          expect(api.requests).toHaveLength(1)
          expect(api.requests[0]).toMatchObject({
            path: '/v1/responses',
            headers: { authorization: 'Bearer sk-given' },
            // gpt-5.5 has no `minimal`: the nearest effort it has, rising, is `low`.
            body: { model: 'gpt-5.5', max_output_tokens: 500, reasoning: { effort: 'low' } },
          })
        }),
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- each test is an entry point
      ).pipe(Effect.provide(FetchHttpClient.layer)),
    ))

  test('without a key, reads the stored one for each request', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const api = yield* serve(() => events)
          const provider = OpenAi.provider({ apiUrl: `${api.url}/v1` })
          const store = yield* CredentialStore.CredentialStore
          yield* store.set('openai', { _tag: 'ApiKey', key: Redacted.make('sk-first') })
          yield* ask(provider, 'gpt-5')
          yield* store.set('openai', { _tag: 'ApiKey', key: Redacted.make('sk-second') })
          yield* ask(provider, 'gpt-5')
          expect(api.requests.map((request) => request.headers['authorization'])).toEqual([
            'Bearer sk-first',
            'Bearer sk-second',
          ])
        }),
      ).pipe(Effect.provide([FetchHttpClient.layer, CredentialStore.layerMemory()]), Effect.orDie),
    ))

  test('listModels lists what the stored key reaches, marking what the provider serves', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const api = yield* serve(() => ({
            json: { object: 'list', data: [{ id: 'gpt-5.5' }, { id: 'gpt-next' }] },
          }))
          const store = yield* CredentialStore.CredentialStore
          yield* store.set('openai-team', { _tag: 'ApiKey', key: Redacted.make('sk-team') })
          const provider = OpenAi.provider({ name: 'openai-team', apiUrl: `${api.url}/v1` })
          const listed = yield* provider.listModels
          expect(api.requests[0]).toMatchObject({
            path: '/v1/models',
            headers: { authorization: 'Bearer sk-team' },
          })
          expect(listed).toEqual([
            {
              provider: 'openai-team',
              id: 'gpt-5.5',
              name: 'GPT-5.5',
              served: true,
              thinkingLevels: ['off', 'low', 'medium', 'high', 'xhigh'],
              serviceTiers: [],
              contextWindow: 272_000,
            },
            {
              provider: 'openai-team',
              id: 'gpt-next',
              name: 'gpt-next',
              served: false,
              serviceTiers: [],
            },
          ])
        }),
      ).pipe(Effect.provide([FetchHttpClient.layer, CredentialStore.layerMemory()]), Effect.orDie),
    ))

  test('listModels fails Rejected on a refused key', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const api = yield* serve(() => ({ status: 401, json: { error: { message: 'bad key' } } }))
          const provider = OpenAi.provider({ apiKey: Redacted.make('sk-bad'), apiUrl: api.url })
          const error = yield* Effect.flip(provider.listModels)
          expect(error.reason).toBeInstanceOf(ModelError.Rejected)
          expect(error.message).toContain('401')
        }),
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- each test is an entry point
      ).pipe(Effect.provide(FetchHttpClient.layer)),
    ))
})
