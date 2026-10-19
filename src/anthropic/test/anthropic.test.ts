/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * The Anthropic provider against a local stand-in for the Messages API: the request it sends for
 * the catalog model and options, and the response it maps back.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, Redacted } from 'effect'
import { FetchHttpClient } from 'effect/http'
import type { HttpClient } from 'effect/http'
import { Model } from '../../model/index.ts'
import type { Message } from '../../model/index.ts'
import { serve } from '../../model/test/fakeApi.ts'
import { Anthropic } from '../index.ts'

const usage = {
  cache_creation: null,
  cache_creation_input_tokens: 0,
  cache_read_input_tokens: 0,
  input_tokens: 10,
  output_tokens: 0,
  service_tier: null,
}

const events = [
  {
    type: 'message_start',
    message: {
      id: 'msg_1',
      type: 'message',
      role: 'assistant',
      model: 'claude-sonnet-5-5',
      content: [],
      stop_reason: null,
      stop_sequence: null,
      usage,
    },
  },
  { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } },
  { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'Hello' } },
  { type: 'content_block_stop', index: 0 },
  {
    type: 'message_delta',
    delta: { stop_reason: 'end_turn', stop_sequence: null },
    usage: {
      cache_creation_input_tokens: null,
      cache_read_input_tokens: null,
      input_tokens: null,
      output_tokens: 5,
    },
  },
  { type: 'message_stop' },
]

const user = (text: string): Message.Message => ({ role: 'user', content: text, timestamp: 0 })

/** Ask `modelId` with `options` through a Model over a provider at `url`, and return the body. */
const ask = (url: string, modelId: string, options: Model.Options) =>
  Effect.gen(function* () {
    const model = yield* Model.make([
      Anthropic.provider({ apiKey: Redacted.make('sk-ant'), apiUrl: url }),
    ])
    const info = yield* model.info(Anthropic.ref(modelId))
    if (info === undefined) return yield* Effect.die(`no ${modelId}`)
    const request = { model: Anthropic.ref(modelId), messages: [user('hi')], options }
    return yield* Model.collect(model.stream(request), info)
  })

const run = <A, E>(body: (url: string) => Effect.Effect<A, E, HttpClient.HttpClient>) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.flatMap(
        serve(() => events),
        (api) => Effect.as(body(api.url), api.requests),
      ),
    ).pipe(
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- each test is an entry point
      Effect.provide(FetchHttpClient.layer),
      Effect.orDie,
    ),
  )

describe('Anthropic.provider', () => {
  test('gives an adaptive model an effort, and maps the response', async () => {
    const requests = await run((url) =>
      Effect.tap(ask(url, 'claude-sonnet-5-5', { reasoning: 'xhigh' }), (answer) =>
        Effect.sync(() => {
          expect(answer).toMatchObject({
            content: [{ type: 'text', text: 'Hello' }],
            provider: 'anthropic',
            model: 'claude-sonnet-5-5',
            stopReason: 'stop',
          })
        }),
      ),
    )
    expect(requests[0]).toMatchObject({
      path: '/v1/messages',
      headers: { 'x-api-key': 'sk-ant' },
      body: {
        model: 'claude-sonnet-5-5',
        max_tokens: 128_000,
        thinking: { type: 'adaptive' },
        output_config: { effort: 'high' },
      },
    })
  })

  test('gives a budget model a thinking budget under its ceiling, and no temperature', async () => {
    const requests = await run((url) =>
      ask(url, 'claude-opus-4-5', { reasoning: 'medium', maxTokens: 4096, temperature: 0.5 }),
    )
    expect(requests[0]).toMatchObject({
      body: { max_tokens: 4096, thinking: { type: 'enabled', budget_tokens: 3072 } },
    })
    expect(requests[0]?.body).not.toHaveProperty('temperature')
  })

  test('sends temperature when the model is not thinking', async () => {
    const requests = await run((url) => ask(url, 'claude-opus-4-5', { temperature: 0.5 }))
    expect(requests[0]?.body).toMatchObject({ temperature: 0.5 })
    expect(requests[0]?.body).not.toHaveProperty('thinking')
  })

  test('listModels follows the pages and names the catalog levels', async () => {
    const requests = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const api = yield* serve((request) => ({
            json: request.search.includes('after_id')
              ? {
                  data: [{ type: 'model', id: 'claude-new', display_name: 'Claude New' }],
                  has_more: false,
                  first_id: 'claude-new',
                  last_id: 'claude-new',
                }
              : {
                  data: [
                    { type: 'model', id: 'claude-sonnet-5-5', display_name: 'Claude Sonnet 5.5' },
                  ],
                  has_more: true,
                  first_id: 'claude-sonnet-5-5',
                  last_id: 'claude-sonnet-5-5',
                },
          }))
          const provider = Anthropic.provider({ apiKey: Redacted.make('sk-ant'), apiUrl: api.url })
          const listed = yield* provider.listModels
          expect(
            listed.map(({ id, served, thinkingLevels }) => ({ id, served, thinkingLevels })),
          ).toEqual([
            {
              id: 'claude-sonnet-5-5',
              served: true,
              thinkingLevels: ['low', 'medium', 'high', 'xhigh', 'max'],
            },
            { id: 'claude-new', served: false, thinkingLevels: undefined },
          ])
          return api.requests
        }),
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- each test is an entry point
      ).pipe(Effect.provide(FetchHttpClient.layer)),
    )
    expect(requests.map(({ path, search }) => `${path}${search}`)).toEqual([
      '/v1/models?limit=1000',
      '/v1/models?limit=1000&after_id=claude-sonnet-5-5',
    ])
    expect(requests[0]?.headers).toMatchObject({
      'x-api-key': 'sk-ant',
      'anthropic-version': '2023-06-01',
    })
  })
})
