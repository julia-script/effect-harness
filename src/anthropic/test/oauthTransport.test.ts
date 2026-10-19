/** Native Claude message transport compatibility and terminal stream validation. */
import { expect, test } from 'bun:test'
import { AnthropicClient } from '@effect/ai-anthropic'
import { Effect, Stream } from 'effect'
import type { Request } from '../../model/Model.ts'
import { AnthropicOAuth } from '../index.ts'
import * as transport from '../internal/oauth/transport.ts'
import { events, run, usage } from './oauthSupport.ts'

const imageResult: Request = {
  model: AnthropicOAuth.ref('claude-opus-4-5'),
  options: { cacheRetention: 'none' },
  messages: [
    {
      role: 'toolResult',
      toolCallId: 'old',
      toolName: 'read',
      content: [
        { type: 'text', text: 'Screenshot' },
        { type: 'image', mimeType: 'image/png', data: 'aW1hZ2U=' },
      ],
      isError: false,
      timestamp: 0,
    },
  ],
}

const cachedPayload: Parameters<AnthropicClient.Service['createMessageStream']>[0] = {
  payload: {
    model: 'claude-opus-4-5',
    max_tokens: 1024,
    tools: [
      {
        name: 'read',
        input_schema: { type: 'object', properties: { cache_control: { type: 'string' } } },
        cache_control: { type: 'ephemeral' },
      },
    ],
    messages: [
      {
        role: 'assistant',
        content: [
          {
            type: 'tool_use',
            id: 'old',
            name: 'read',
            input: { cache_control: 'application value' },
          },
        ],
      },
      {
        role: 'user',
        content: [
          {
            type: 'tool_result',
            tool_use_id: 'old',
            content: 'Screenshot',
            cache_control: { type: 'ephemeral' },
          },
        ],
      },
    ],
  },
}

test('tool-result images remain native image blocks and cache stripping does not edit tool input JSON', () =>
  run(
    () => events,
    (api) =>
      Effect.gen(function* () {
        const native = yield* AnthropicClient.make({ apiUrl: api.url })
        const client = transport.client(native, imageResult)
        const [, stream] = yield* client.createMessageStream(cachedPayload)
        yield* Stream.runDrain(stream)
        expect(api.requests[0]?.body).toMatchObject({
          messages: [
            {
              content: [
                { type: 'tool_use', name: 'Read', input: { cache_control: 'application value' } },
              ],
            },
            {
              content: [
                {
                  type: 'tool_result',
                  content: [
                    { type: 'text', text: 'Screenshot' },
                    {
                      type: 'image',
                      source: { type: 'base64', media_type: 'image/png', data: 'aW1hZ2U=' },
                    },
                  ],
                },
              ],
            },
          ],
          tools: [{ input_schema: { properties: { cache_control: { type: 'string' } } } }],
        })
        expect(api.requests[0]?.body).not.toHaveProperty('tools.0.cache_control')
        expect(api.requests[0]?.body).not.toHaveProperty('messages.1.content.0.cache_control')
      }),
  ))

test('a truncated stream fails instead of reporting successful completion', () =>
  run(
    () => events.slice(0, -1),
    (api) =>
      Effect.gen(function* () {
        const native = yield* AnthropicClient.make({ apiUrl: api.url })
        const client = transport.client(native, {
          model: AnthropicOAuth.ref('claude-opus-4-5'),
          messages: [],
        })
        const [, stream] = yield* client.createMessageStream({
          payload: {
            model: 'claude-opus-4-5',
            max_tokens: 1024,
            messages: [{ role: 'user', content: 'Hi' }],
          },
        })
        const result = yield* Effect.result(Stream.runDrain(stream))
        expect(result).toMatchObject({
          _tag: 'Failure',
          failure: { reason: { description: 'Claude stream ended before message_stop' } },
        })
      }),
  ))

test('normalization collisions fail before sending a Messages request', () =>
  run(
    () => events,
    (api) =>
      Effect.gen(function* () {
        const native = yield* AnthropicClient.make({ apiUrl: api.url })
        const client = transport.client(native, {
          model: AnthropicOAuth.ref('claude-opus-4-5'),
          messages: [],
        })
        const result = yield* Effect.result(
          client.createMessageStream({
            payload: {
              model: 'claude-opus-4-5',
              max_tokens: 1024,
              messages: [{ role: 'user', content: 'Hi' }],
              tools: [
                { name: 'read', input_schema: { type: 'object' } },
                { name: 'Read', input_schema: { type: 'object' } },
              ],
            },
          }),
        )
        expect(result).toMatchObject({
          _tag: 'Failure',
          failure: { reason: { _tag: 'InvalidUserInputError' } },
        })
        expect(api.requests).toHaveLength(0)
      }),
  ))

test('nonstreaming tool names and forced tool choice round-trip without touching input properties', () =>
  run(
    () => ({
      json: {
        id: 'msg_2',
        type: 'message',
        role: 'assistant',
        model: 'claude-opus-4-5',
        content: [{ type: 'tool_use', id: 'tool_2', name: 'Read', input: { name: 'Read' } }],
        stop_reason: 'tool_use',
        stop_sequence: null,
        usage,
      },
    }),
    (api) =>
      Effect.gen(function* () {
        const native = yield* AnthropicClient.make({ apiUrl: api.url })
        const client = transport.client(native, {
          model: AnthropicOAuth.ref('claude-opus-4-5'),
          messages: [],
        })
        const [message] = yield* client.createMessage({
          payload: {
            model: 'claude-opus-4-5',
            max_tokens: 1024,
            messages: [{ role: 'user', content: 'Hi' }],
            tools: [
              {
                name: 'read',
                input_schema: { type: 'object', properties: { name: { type: 'string' } } },
              },
            ],
            tool_choice: { type: 'tool', name: 'read' },
          },
        })
        expect(api.requests[0]?.body).toMatchObject({ tool_choice: { type: 'tool', name: 'Read' } })
        expect(message.content).toEqual([
          { type: 'tool_use', id: 'tool_2', name: 'read', input: { name: 'Read' } },
        ])
      }),
  ))

test('native nonstreaming serialization retains the subscription thinking display field', () =>
  run(
    () => ({
      json: {
        id: 'msg_3',
        type: 'message',
        role: 'assistant',
        model: 'claude-opus-4-5',
        content: [{ type: 'text', text: 'Hello', citations: null }],
        stop_reason: 'end_turn',
        stop_sequence: null,
        usage,
      },
    }),
    (api) =>
      Effect.gen(function* () {
        const native = yield* AnthropicClient.make({ apiUrl: api.url })
        const client = transport.client(native, {
          model: AnthropicOAuth.ref('claude-opus-4-5'),
          messages: [],
        })
        yield* client.createMessage({
          payload: {
            model: 'claude-opus-4-5',
            max_tokens: 2048,
            messages: [{ role: 'user', content: 'Hi' }],
            thinking: { type: 'enabled', budget_tokens: 1024 },
          },
        })
        expect(api.requests[0]?.body).toMatchObject({
          thinking: { type: 'enabled', budget_tokens: 1024, display: 'summarized' },
        })
      }),
  ))
