import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Prepared from '../src/PreparedRequest.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import * as Tool from '../src/Tool.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Search from '../src/ToolSearch.js'
import * as Store from '../src/provider-auth/CredentialStore.js'
import * as Codex from '../src/provider-openai-codex/Model.js'
import * as Claude from '../src/provider-anthropic/OAuthModel.js'
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
    `${provider}: search loads an ordinary function and the next model call executes its real handler`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* credentials(provider)
          const bodies: Schema.JsonObject[] = []
          const preparations: Prepared.Snapshot[] = []
          let executions = 0
          const http = HttpClient.make((request) =>
            Effect.gen(function* () {
              const body = payload(request)
              bodies.push(body)
              preparations.push(
                Option.getOrThrow(yield* Effect.serviceOption(Prepared.PreparedRequest)).current,
              )
              if (provider === 'anthropic') {
                assert.include(
                  request.headers['anthropic-beta']!,
                  'thinking-binding-controls-2026-08-01',
                )
                assert.deepStrictEqual(body.thinking, {
                  type: 'adaptive',
                  block_binding: { prefix_mismatch_behavior: 'drop_block' },
                })
              }
              const turn = bodies.length
              const name = turn === 1 ? 'tool_search' : 'read'
              const args = turn === 1 ? { query: 'filesystem' } : { path: '/meeting.txt' }
              if (provider === 'openai-codex') {
                if (turn > 2)
                  return HttpClientResponse.fromWeb(request, new Response(sse(codexEvents)))
                const tool = {
                  type: 'function_call',
                  id: `fc-${turn}`,
                  call_id: `call-${turn}`,
                  name,
                  arguments: JSON.stringify(args),
                  status: 'completed',
                }
                return HttpClientResponse.fromWeb(
                  request,
                  new Response(
                    sse([
                      { type: 'response.created', response: { ...codexResponse, output: [] } },
                      {
                        type: 'response.output_item.added',
                        output_index: 0,
                        item: { ...tool, arguments: '' },
                      },
                      {
                        type: 'response.function_call_arguments.delta',
                        output_index: 0,
                        item_id: tool.id,
                        delta: tool.arguments,
                      },
                      { type: 'response.output_item.done', output_index: 0, item: tool },
                      {
                        type: 'response.completed',
                        response: { ...codexResponse, output: [tool] },
                      },
                    ]),
                  ),
                )
              }
              if (turn > 2)
                return HttpClientResponse.fromWeb(request, new Response(sse(claudeEvents)))
              const tool = {
                type: 'tool_use',
                id: `call-${turn}`,
                name: name === 'read' ? 'Read' : name,
                input: args,
              }
              const index = turn === 1 ? 1 : 0
              return HttpClientResponse.fromWeb(
                request,
                new Response(
                  sse([
                    {
                      type: 'message_start',
                      message: {
                        ...claudeResponse,
                        model: 'claude-sonnet-5-5',
                        content: [],
                        stop_reason: null,
                      },
                    },
                    ...(turn === 1
                      ? [
                          {
                            type: 'content_block_start',
                            index: 0,
                            content_block: { type: 'thinking', thinking: '', signature: '' },
                          },
                          {
                            type: 'content_block_delta',
                            index: 0,
                            delta: { type: 'thinking_delta', thinking: 'search reasoning' },
                          },
                          {
                            type: 'content_block_delta',
                            index: 0,
                            delta: { type: 'signature_delta', signature: 'signed-search' },
                          },
                          { type: 'content_block_stop', index: 0 },
                        ]
                      : []),
                    {
                      type: 'content_block_start',
                      index,
                      content_block: { ...tool, input: {} },
                    },
                    {
                      type: 'content_block_delta',
                      index,
                      delta: { type: 'input_json_delta', partial_json: JSON.stringify(args) },
                    },
                    { type: 'content_block_stop', index },
                    {
                      type: 'message_delta',
                      delta: { stop_reason: 'tool_use', stop_sequence: null },
                      usage: claudeResponse.usage,
                    },
                    { type: 'message_stop' },
                  ]),
                ),
              )
            }),
          )
          const modelId = provider === 'openai-codex' ? 'gpt-5.4' : 'claude-sonnet-5-5'
          const definition = {
            ref: { provider, modelId },
            capabilities: { tools: true, images: false, reasoning: true, structuredOutput: false },
          }
          const model =
            provider === 'openai-codex'
              ? yield* Codex.make({ model: modelId, binding, definition }).pipe(
                  Effect.provideService(Store.CredentialStore, store),
                  Effect.provideService(HttpClient.HttpClient, http),
                )
              : yield* Claude.make({
                  model: modelId,
                  binding: { ...binding, provider },
                  definition,
                  thinkingModes: ['adaptive'],
                  config: { thinking: { type: 'adaptive' } },
                }).pipe(
                  Effect.provideService(Store.CredentialStore, store),
                  Effect.provideService(HttpClient.HttpClient, http),
                )
          const files = Toolkit.make(
            Tool.make('read', {
              deferred: true,
              description: 'Read a filesystem document',
              parameters: Schema.Struct({ path: Schema.String }),
              success: Schema.String,
            }),
          )
          const runtime = yield* HarnessRuntime.make({
            models: [model],
            tools: Toolkit.merge(Search.toolkit, files),
          }).pipe(
            Effect.provide(
              Layer.merge(
                Search.layer,
                files.toLayer({
                  read: ({ path }) =>
                    Effect.sync(() => {
                      executions++
                      assert.strictEqual(path, '/meeting.txt')
                      return 'meeting at noon'
                    }),
                }),
              ),
            ),
          )
          const root = yield* runtime.backend.root
          const job = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'Find and read the meeting file' },
          })
          assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
          assert.strictEqual(executions, 1)
          assert.strictEqual(bodies.length, 3)
          const toolNames = bodies.map((body) =>
            (body.tools as Schema.JsonObject[]).map((tool) => tool.name),
          )
          const wireName = provider === 'anthropic' ? 'Read' : 'read'
          assert.deepStrictEqual(toolNames, [
            ['tool_search'],
            ['tool_search', wireName],
            ['tool_search', wireName],
          ])
          assert.deepStrictEqual(
            preparations.map((preparation) =>
              preparation.tools.map((tool) => tool.declaration.name),
            ),
            [['tool_search'], ['tool_search', 'read'], ['tool_search', 'read']],
          )
          assert.deepStrictEqual(
            (bodies[0]!.tools as Schema.JsonObject[])[0],
            (bodies[1]!.tools as Schema.JsonObject[])[0],
          )
          assert.include(JSON.stringify(bodies[2]), 'meeting at noon')
          assert.notInclude(JSON.stringify(bodies), 'addTools')
          assert.notInclude(JSON.stringify(bodies), 'defer_loading')
          const entries = yield* Session.scanEntries(runtime.session, {
            conversationId: root,
          }).pipe(Stream.runCollect)
          assert.strictEqual(entries.filter((entry) => entry.kind === 'tool.result').length, 2)
          assert.include(JSON.stringify(entries), '"loaded":["read"]')
          if (provider === 'anthropic') {
            assert.include(JSON.stringify(bodies[1]), 'signed-search')
            assert.include(JSON.stringify(bodies[2]), 'signed-search')
            assert.include(JSON.stringify(entries), 'search reasoning')
          }
          assert.notInclude(JSON.stringify(entries), 'SECRET')
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
