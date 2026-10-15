import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as HttpClient from 'effect/http/HttpClient'
import type * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Store from '../src/provider-auth/CredentialStore.js'
import * as Claude from '../src/provider-anthropic/OAuthLanguageModel.js'
import {
  binding,
  claudeEvents,
  claudeResponse,
  sse,
  payload,
  credentials,
} from './fixtures/SubscriptionProtocol.js'

describe('Claude OAuth public LanguageModel protocol', () => {
  it.effect(
    'Claude completion/streaming preserve application instructions, normalize tool choice, and reverse returned names',
    () =>
      Effect.gen(function* () {
        const store = yield* credentials('anthropic')
        const requests: HttpClientRequest.HttpClientRequest[] = []
        const http = HttpClient.make((request) =>
          Effect.sync(() => {
            requests.push(request)
            const body = payload(request)
            return HttpClientResponse.fromWeb(
              request,
              new Response(
                body.stream
                  ? sse(claudeEvents)
                  : JSON.stringify({
                      ...claudeResponse,
                      stop_reason: 'tool_use',
                      content: [
                        { type: 'tool_use', id: 'call-1', name: 'Read', input: { path: 'hello' } },
                      ],
                    }),
              ),
            )
          }),
        )
        const model = yield* Claude.make({
          model: 'claude-sonnet-4-6',
          binding: { ...binding, provider: 'anthropic' },
        }).pipe(
          Effect.provideService(Store.CredentialStore, store),
          Effect.provideService(HttpClient.HttpClient, http),
        )
        const toolkit = Toolkit.make(
          Tool.make('read', {
            parameters: Schema.Struct({ path: Schema.String }),
            success: Schema.String,
          }),
        )
        const result = yield* model.generateText({
          prompt: Prompt.make([
            { role: 'system', content: 'Actual application' },
            { role: 'user', content: 'hello' },
          ]),
          toolkit,
          toolChoice: { tool: 'read' } as const,
          disableToolCallResolution: true,
        })
        assert.strictEqual(result.toolCalls[0]?.name, 'read')
        assert.deepEqual(result.toolCalls[0]?.params, { path: 'hello' })
        const streamed = yield* model.streamText({ prompt: 'hello' }).pipe(Stream.runCollect)
        assert.strictEqual(
          streamed
            .filter((part) => part.type === 'text-delta')
            .map((part) => part.delta)
            .join(''),
          'Olá 🌍',
        )
        const request = requests[0]!
        assert.strictEqual(request.headers.authorization, 'Bearer SECRET-access')
        assert.isUndefined(request.headers['x-api-key'])
        assert.include(request.headers['anthropic-beta'], 'oauth-2025-04-20')
        const body = payload(request)
        assert.include(JSON.stringify(body.system), 'Actual application')
        assert.include(JSON.stringify(body.system), 'You are Claude Code')
        assert.deepEqual(body.tool_choice, { type: 'tool', name: 'Read' })
        assert.include(JSON.stringify(body.tools), '"name":"Read"')
        assert.notInclude(JSON.stringify(result), 'SECRET')
      }),
  )

  it.effect('rejects Claude wire-name collisions before dispatch or tool execution', () =>
    Effect.gen(function* () {
      const store = yield* credentials('anthropic')
      const model = yield* Claude.make({
        model: 'claude-sonnet-4-6',
        binding: { ...binding, provider: 'anthropic' },
      }).pipe(
        Effect.provideService(Store.CredentialStore, store),
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make(() => Effect.die('Must not dispatch a colliding registry')),
        ),
      )
      const toolkit = Toolkit.make(
        Tool.make('read', { parameters: Schema.Struct({}) }),
        Tool.make('Read', { parameters: Schema.Struct({}) }),
      )
      const error = yield* Effect.flip(
        model.generateText({ prompt: 'hello', toolkit, disableToolCallResolution: true }),
      )
      assert.strictEqual(error.reason._tag, 'InvalidRequestError')
    }),
  )
})

it.effect('Claude preserves prepared dynamic tool schemas without enabling strict validation', () =>
  Effect.gen(function* () {
    const store = yield* credentials('anthropic')
    const requests: HttpClientRequest.HttpClientRequest[] = []
    const parameters = {
      type: 'object',
      properties: {
        command: { type: 'string' },
        env: { type: 'object', additionalProperties: { type: 'string' } },
      },
      required: ['command'],
      additionalProperties: true,
    }
    const model = yield* Claude.make({
      model: 'claude-sonnet-4-6',
      binding: { ...binding, provider: 'anthropic' },
    }).pipe(
      Effect.provideService(Store.CredentialStore, store),
      Effect.provideService(
        HttpClient.HttpClient,
        HttpClient.make((request) =>
          Effect.sync(() => {
            requests.push(request)
            return HttpClientResponse.fromWeb(request, new Response(sse(claudeEvents)))
          }),
        ),
      ),
    )
    const parts = yield* model
      .streamText({
        prompt: 'hello',
        toolkit: Toolkit.make(Tool.dynamic('bash', { parameters })),
        disableToolCallResolution: true,
      })
      .pipe(Stream.runCollect)
    const tools = yield* Schema.decodeUnknownEffect(
      Schema.Array(
        Schema.Struct({
          strict: Schema.Boolean,
          input_schema: Schema.JsonObject,
        }),
      ),
    )(payload(requests[0]!).tools)
    assert.strictEqual(tools[0]?.strict, false)
    assert.deepEqual(tools[0]?.input_schema, parameters)
    assert.strictEqual(
      parts
        .filter((p) => p.type === 'text-delta')
        .map((p) => p.delta)
        .join(''),
      'Olá 🌍',
    )
  }),
)
