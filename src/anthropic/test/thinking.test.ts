/** Claude thinking keeps its signature, so the next request can send it back. */
import { expect, test } from 'bun:test'
import { Effect } from 'effect'
import { CredentialStore } from '../../credentials/index.ts'
import { Model } from '../../model/index.ts'
import type { Message } from '../../model/index.ts'
import { AnthropicOAuth } from '../index.ts'
import { events, existing, run, usage } from './oauthSupport.ts'

const thinking = [
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
    content_block: { type: 'thinking', thinking: '', signature: '' },
  },
  { type: 'content_block_delta', index: 0, delta: { type: 'thinking_delta', thinking: 'Plan.' } },
  { type: 'content_block_delta', index: 0, delta: { type: 'signature_delta', signature: 'sig' } },
  { type: 'content_block_stop', index: 0 },
  {
    type: 'content_block_start',
    index: 1,
    content_block: { type: 'redacted_thinking', data: 'hidden' },
  },
  { type: 'content_block_stop', index: 1 },
  ...events
    .slice(1)
    .map((event) =>
      'index' in event && typeof event.index === 'number'
        ? { ...event, index: event.index + 2 }
        : event,
    ),
]

test('thinking and redacted thinking keep their signatures and go back on the next request', () =>
  run(
    () => thinking,
    (api) =>
      Effect.gen(function* () {
        const store = yield* CredentialStore.CredentialStore
        yield* store.set(AnthropicOAuth.name, existing)
        const model = yield* Model.make([
          AnthropicOAuth.provider({ apiUrl: api.url, authUrl: api.url }),
        ])
        const ref = AnthropicOAuth.ref('claude-sonnet-5-5')
        const info = yield* model.info(ref)
        if (info === undefined) return yield* Effect.die('Missing catalog model')
        const ask = (messages: ReadonlyArray<Message.Message>) =>
          Model.collect(
            model.stream({ model: ref, messages, options: { reasoning: 'high' } }),
            info,
          )
        const user: Message.Message = { role: 'user', content: 'Read file.txt', timestamp: 0 }
        const answer = yield* ask([user])
        expect(answer.content).toMatchObject([
          {
            type: 'thinking',
            thinking: 'Plan.',
            thinkingSignature: JSON.stringify({
              anthropic: { info: { type: 'thinking', signature: 'sig' } },
            }),
          },
          {
            type: 'thinking',
            thinking: '',
            thinkingSignature: JSON.stringify({
              anthropic: { info: { type: 'redacted_thinking', redactedData: 'hidden' } },
            }),
          },
          { type: 'toolCall', id: 'tool_1' },
        ])
        yield* ask([
          user,
          answer,
          {
            role: 'toolResult',
            toolCallId: 'tool_1',
            toolName: 'read',
            content: [{ type: 'text', text: 'hello' }],
            isError: false,
            timestamp: 0,
          },
        ])
        expect(api.requests[1]?.body).toMatchObject({
          messages: [
            { role: 'user' },
            {
              role: 'assistant',
              content: [
                { type: 'thinking', thinking: 'Plan.', signature: 'sig' },
                { type: 'redacted_thinking', data: 'hidden' },
                { type: 'tool_use', id: 'tool_1' },
              ],
            },
            { role: 'user' },
          ],
        })
        return yield* Effect.void
      }),
  ))
