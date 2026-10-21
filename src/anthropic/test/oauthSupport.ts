/** Shared HTTP fixtures and model entry point for Claude OAuth contract tests. */
import { Effect, Redacted } from 'effect'
import { FetchHttpClient } from 'effect/http'
import type { HttpClient } from 'effect/http'
import { Credentials } from '../../credentials/index.ts'
import { Model } from '../../model/index.ts'
import { serve } from '../../model/test/fakeApi.ts'
import type { FakeApi, Recorded, Reply } from '../../model/test/fakeApi.ts'
import { AnthropicOAuth } from '../index.ts'
export const token = { access_token: 'access-new', refresh_token: 'refresh-new', expires_in: 3600 }
export const existing = {
  type: 'oauth' as const,
  access: Redacted.make('access-old'),
  refresh: Redacted.make('refresh-old'),
  expires: Number.MAX_SAFE_INTEGER,
  identity: { issuer: 'https://platform.claude.com', subject: 'existing-account' },
  revision: 'existing-sign-in',
  extra: { hostSetting: 'preserved' },
}
export const usage = {
  cache_creation: null,
  cache_creation_input_tokens: 0,
  cache_read_input_tokens: 0,
  input_tokens: 10,
  output_tokens: 0,
  service_tier: null,
}
export const events = [
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
  {
    type: 'content_block_start',
    index: 0,
    content_block: { type: 'tool_use', id: 'tool_1', name: 'Read', input: {} },
  },
  {
    type: 'content_block_delta',
    index: 0,
    delta: { type: 'input_json_delta', partial_json: '{"path":"file.txt"}' },
  },
  { type: 'content_block_stop', index: 0 },
  {
    type: 'message_delta',
    delta: { stop_reason: 'tool_use', stop_sequence: null },
    usage: {
      cache_creation_input_tokens: null,
      cache_read_input_tokens: null,
      input_tokens: null,
      output_tokens: 5,
    },
  },
  { type: 'message_stop' },
]
export const run = <A, E>(
  respond: (request: Recorded) => ReadonlyArray<unknown> | Reply,
  body: (api: FakeApi) => Effect.Effect<A, E, HttpClient.HttpClient | Credentials.CredentialStore>,
) =>
  Effect.runPromise(
    Effect.scoped(Effect.flatMap(serve(respond), body)).pipe(
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- test entry point
      Effect.provide([FetchHttpClient.layer, Credentials.layerMemory()]),
    ),
  )

export const ask = (
  api: FakeApi,
  modelId = 'claude-sonnet-5-5',
  options?: Model.Options,
  providerOptions?: AnthropicOAuth.Options,
) =>
  Effect.gen(function* () {
    const model = yield* Model.make([
      AnthropicOAuth.provider({ ...providerOptions, apiUrl: api.url, authUrl: api.url }),
    ])
    const ref = AnthropicOAuth.ref(modelId)
    const info = yield* model.info(ref)
    if (info === undefined) return yield* Effect.die('Missing catalog model')
    return yield* Model.collect(
      model.stream({
        model: ref,
        messages: [
          {
            role: 'system',
            content: 'Keep my instructions.',
            timestamp: 0,
            toolsAdded: [
              {
                name: 'read',
                description: 'Read a file',
                parameters: { type: 'object', properties: { path: { type: 'string' } } },
              },
            ],
          },
          { role: 'user', content: 'Read file.txt', timestamp: 0 },
        ],
        options: options ?? { reasoning: 'high', cacheRetention: 'long' },
      }),
      info,
    )
  })
