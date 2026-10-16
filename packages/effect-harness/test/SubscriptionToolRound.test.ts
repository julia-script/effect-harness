import * as TemporaryStorage from './fixtures/Storage.js'
import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Hook from '../src/Hook.js'
import * as Session from '../src/Session.js'

import * as Tool from '../src/Tool.js'
import * as ToolResult from '../src/ToolResult.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Store from '../src/provider-auth/CredentialStore.js'
import * as Codex from '../src/provider-openai-codex/CodexLanguageModel.js'
import * as Claude from '../src/provider-anthropic/OAuthLanguageModel.js'
import {
  binding,
  codexEvents,
  codexResponse,
  claudeEvents,
  claudeResponse,
  sse,
  payload,
  credentials,
} from './fixtures/SubscriptionProtocol.js'

for (const provider of ['openai-codex', 'anthropic'] as const)
  it.live(
    `${provider}: harness executes one tool round and preserves canonical media and original names`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* credentials(provider)
          let calls = 0
          let executions = 0
          const bodies: Schema.JsonObject[] = []
          const http = HttpClient.make((request) =>
            Effect.sync(() => {
              const body = payload(request)
              bodies.push(body)
              calls++
              if (provider === 'openai-codex') {
                const tool = {
                  type: 'function_call',
                  id: 'fc-1',
                  call_id: 'call-1',
                  name: 'read',
                  arguments: '{"path":"hello"}',
                  status: 'completed',
                }
                const events =
                  calls > 1
                    ? codexEvents
                    : [
                        { type: 'response.created', response: { ...codexResponse, output: [] } },
                        {
                          type: 'response.output_item.added',
                          output_index: 0,
                          item: { ...tool, arguments: '' },
                        },
                        {
                          type: 'response.function_call_arguments.delta',
                          output_index: 0,
                          item_id: 'fc-1',
                          delta: '{"path":',
                        },
                        {
                          type: 'response.function_call_arguments.delta',
                          output_index: 0,
                          item_id: 'fc-1',
                          delta: '"hello"}',
                        },
                        { type: 'response.output_item.done', output_index: 0, item: tool },
                        {
                          type: 'response.completed',
                          response: { ...codexResponse, output: [tool] },
                        },
                      ]
                return HttpClientResponse.fromWeb(request, new Response(sse(events)))
              }
              const tool = {
                type: 'tool_use',
                id: 'call-1',
                name: 'Read',
                input: { path: 'hello' },
              }
              const answer =
                calls > 1
                  ? claudeResponse
                  : { ...claudeResponse, stop_reason: 'tool_use', content: [tool] }
              const events =
                calls > 1
                  ? claudeEvents
                  : [
                      {
                        type: 'message_start',
                        message: { ...claudeResponse, content: [], stop_reason: null },
                      },
                      {
                        type: 'content_block_start',
                        index: 0,
                        content_block: { ...tool, input: {} },
                      },
                      {
                        type: 'content_block_delta',
                        index: 0,
                        delta: { type: 'input_json_delta', partial_json: '{"path":' },
                      },
                      {
                        type: 'content_block_delta',
                        index: 0,
                        delta: { type: 'input_json_delta', partial_json: '"hello"}' },
                      },
                      { type: 'content_block_stop', index: 0 },
                      {
                        type: 'message_delta',
                        delta: { stop_reason: 'tool_use', stop_sequence: null },
                        usage: claudeResponse.usage,
                      },
                      { type: 'message_stop' },
                    ]
              return HttpClientResponse.fromWeb(
                request,
                new Response(body.stream ? sse(events) : JSON.stringify(answer)),
              )
            }),
          )
          const native = yield* (
            provider === 'openai-codex'
              ? Codex.make({ model: 'gpt-5.4', binding })
              : Claude.make({
                  model: 'claude-sonnet-4-6',
                  binding: binding,
                })
          ).pipe(
            Effect.provideService(Store.CredentialStore, store),
            Effect.provideService(HttpClient.HttpClient, http),
          )
          const toolkit = Toolkit.make(
            Tool.make('read', {
              parameters: Schema.Struct({ path: Schema.String }),
              success: Schema.String,
            }),
          )
          const png =
            'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aZkwAAAAASUVORK5CYII='
          const runtime = yield* HarnessRuntime.make({
            tools: toolkit,
            hooks: [
              Hook.make({
                event: 'afterTool',
                execute: () =>
                  Schema.decodeEffect(ToolResult.ResultSchema)({
                    content: [
                      Prompt.textPart({ text: 'visible-result' }),
                      Prompt.filePart({ mediaType: 'image/png', data: png }),
                    ],
                    isError: false,
                    diagnostics: [],
                  }),
              }),
            ],
          }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, native),
            Effect.provide(
              toolkit.toLayer({
                read: ({ path }) =>
                  Effect.sync(() => {
                    executions++
                    assert.strictEqual(path, 'hello')
                    return 'handler-internal'
                  }),
              }),
            ),
          )
          const root = yield* runtime.backend.root
          const job = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'Use read once, then answer.' },
          })
          assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
          assert.strictEqual(calls, 2)
          assert.strictEqual(executions, 1)
          assert.include(JSON.stringify(bodies[1]), 'visible-result')
          assert.include(JSON.stringify(bodies[1]), png)
          assert.notInclude(JSON.stringify(bodies[1]), 'handler-internal')
          const entries = yield* Session.scanEntries(runtime.session, {
            conversationId: root,
          }).pipe(Stream.runCollect)
          assert.strictEqual(entries.filter((entry) => entry.kind === 'tool.result').length, 1)
          assert.notInclude(JSON.stringify(entries), 'SECRET')
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
