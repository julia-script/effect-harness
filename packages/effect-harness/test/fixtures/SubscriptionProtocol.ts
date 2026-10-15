import { assert } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import type * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as Store from '../../src/provider-auth/CredentialStore.js'
import * as MemoryCredentialStore from '../../src/provider-auth/MemoryCredentialStore.js'
export const codexToken = `header.${btoa(JSON.stringify({ 'https://api.openai.com/auth': { chatgpt_account_id: 'account-1' } }))}.SECRET-token`
export const binding = { provider: 'openai-codex', profile: 'fixture', identity: 'login' } as const
export const textItem = {
  type: 'message',
  id: 'message-1',
  role: 'assistant',
  status: 'completed',
  content: [{ type: 'output_text', text: 'Olá 🌍', annotations: [], logprobs: [] }],
}
export const codexResponse = {
  id: 'response-1',
  model: 'gpt-5.4',
  created_at: 0,
  output: [textItem],
  usage: {
    input_tokens: 12,
    input_tokens_details: { cached_tokens: 4 },
    output_tokens: 5,
    output_tokens_details: { reasoning_tokens: 2 },
    total_tokens: 17,
  },
}
export const codexEvents = [
  { type: 'response.created', response: { ...codexResponse, output: [] } },
  { type: 'response.output_item.added', output_index: 0, item: { ...textItem, content: [] } },
  {
    type: 'response.output_text.delta',
    item_id: 'message-1',
    output_index: 0,
    content_index: 0,
    delta: 'Olá 🌍',
    logprobs: [],
  },
  { type: 'response.output_item.done', output_index: 0, item: textItem },
  { type: 'response.completed', response: codexResponse },
]
export const claudeUsage = {
  input_tokens: 12,
  output_tokens: 5,
  cache_creation: null,
  cache_creation_input_tokens: 0,
  cache_read_input_tokens: 4,
  service_tier: 'standard',
}
export const claudeResponse = {
  id: 'message-1',
  type: 'message',
  role: 'assistant',
  model: 'claude-sonnet-4-6',
  content: [{ type: 'text', text: 'Olá 🌍', citations: null }],
  stop_reason: 'end_turn',
  stop_sequence: null,
  usage: claudeUsage,
}
export const claudeEvents = [
  { type: 'message_start', message: { ...claudeResponse, content: [], stop_reason: null } },
  {
    type: 'content_block_start',
    index: 0,
    content_block: { type: 'text', text: '', citations: null },
  },
  { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'Olá 🌍' } },
  { type: 'content_block_stop', index: 0 },
  {
    type: 'message_delta',
    delta: { stop_reason: 'end_turn', stop_sequence: null },
    usage: claudeUsage,
  },
  { type: 'message_stop' },
]
export const sse = (events: ReadonlyArray<unknown>) => {
  const bytes = new TextEncoder().encode(
    events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join(''),
  )
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const byte of bytes) controller.enqueue(Uint8Array.of(byte))
      controller.close()
    },
  })
}
export const payload = (request: HttpClientRequest.HttpClientRequest) => {
  assert.strictEqual(request.body._tag, 'Uint8Array')
  if (request.body._tag !== 'Uint8Array') throw new Error('Expected JSON body')
  return Schema.decodeUnknownSync(Schema.JsonObject)(
    JSON.parse(new TextDecoder().decode(request.body.body)),
  )
}
export const credentials = (provider: 'openai-codex' | 'anthropic') =>
  Effect.gen(function* () {
    const store = yield* MemoryCredentialStore.make
    yield* Store.save({
      type: 'oauth',
      binding: { ...binding, provider },
      accessToken: Redacted.make(provider === 'openai-codex' ? codexToken : 'SECRET-access'),
      refreshToken: Redacted.make('SECRET-refresh'),
      expiresAt: 9_000_000_000_000,
      ...(provider === 'openai-codex' ? { accountId: 'account-1' } : {}),
    }).pipe(Effect.provideService(Store.CredentialStore, store))
    return store
  })
