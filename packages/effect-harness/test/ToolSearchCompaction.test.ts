/** Durable discovery and subscription compaction acceptance through the public OAuth transports. */
import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import { CompactionRequest } from '../src/CompactionRequest.js'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Prepared from '../src/PreparedRequest.js'
import * as Session from '../src/Session.js'
import * as Settings from '../src/Settings.js'
import * as Storage from '../src/Storage.js'
import * as Tool from '../src/Tool.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Search from '../src/ToolSearch.js'
import * as Transaction from '../src/Transaction.js'
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
    `${provider}: search/load/invoke/compact/reopen/invoke preserves selection, account and thinking policy`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* credentials(provider)
          const bodies: Schema.JsonObject[] = []
          const preparations: Prepared.Snapshot[] = []
          let executions = 0
          let generations = 0
          let summaries = 0
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
                  display: 'summarized',
                })
              }
              if (Option.isSome(yield* Effect.serviceOption(CompactionRequest))) {
                summaries++
                assert.deepStrictEqual(body.tools ?? [], [])
                assert.notInclude(JSON.stringify(body), 'private-search-reasoning')
                return HttpClientResponse.fromWeb(
                  request,
                  new Response(
                    provider === 'openai-codex'
                      ? sse(codexEvents)
                      : JSON.stringify({ ...claudeResponse, model: 'claude-sonnet-5-5' }),
                  ),
                )
              }
              const turn = ++generations
              if (turn > 2 && turn !== 4)
                return HttpClientResponse.fromWeb(
                  request,
                  new Response(sse(provider === 'openai-codex' ? codexEvents : claudeEvents)),
                )
              const name = turn === 1 ? 'tool_search' : 'read'
              const args = turn === 1 ? { query: 'filesystem' } : { path: '/meeting.txt' }
              if (provider === 'openai-codex') {
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
              const tool = {
                type: 'tool_use',
                id: `call-${turn}`,
                name: name === 'read' ? 'Read' : name,
                input: args,
              }
              const index = turn === 1 ? 2 : 0
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
                            delta: { type: 'thinking_delta', thinking: 'private-search-reasoning' },
                          },
                          {
                            type: 'content_block_delta',
                            index: 0,
                            delta: { type: 'signature_delta', signature: 'signed-search' },
                          },
                          { type: 'content_block_stop', index: 0 },
                          {
                            type: 'content_block_start',
                            index: 1,
                            content_block: { type: 'redacted_thinking', data: 'redacted-search' },
                          },
                          { type: 'content_block_stop', index: 1 },
                        ]
                      : []),
                    { type: 'content_block_start', index, content_block: { ...tool, input: {} } },
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
            maxOutputTokens: 1024,
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
          const handlers = Layer.merge(
            Search.layer,
            files.toLayer({
              read: ({ path }) =>
                Effect.sync(() => {
                  executions++
                  assert.strictEqual(path, '/meeting.txt')
                  return 'meeting at noon'
                }),
            }),
          )
          const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
          const storageLayer = Storage.layerJsonl({ filePath: `${directory}/session.jsonl` })
          const firstScope = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
          const firstStorage = yield* Layer.buildWithScope(Layer.fresh(storageLayer), firstScope)
          const settings = yield* Settings.make({
            compaction: {
              enabled: false,
              reserveTokens: 20,
              keepRecentTokens: 600,
              backgroundTokens: 0,
            },
          })
          const runtime = yield* HarnessRuntime.make({
            models: [model],
            tools: Toolkit.merge(Search.toolkit, files),
          }).pipe(
            Effect.provide(handlers),
            Effect.provideContext(firstStorage),
            Effect.provideService(Settings.Settings, settings),
            Effect.provideService(Scope.Scope, firstScope),
          )
          const root = yield* runtime.backend.root
          yield* Session.commit(runtime.session, (tx) =>
            Effect.gen(function* () {
              for (const text of [
                'old background '.repeat(2000),
                'earlier conversation '.repeat(2000),
              ])
                yield* Transaction.appendEntry(tx, root, {
                  kind: 'input',
                  model: yield* Schema.encodeEffect(
                    Schema.Array(Schema.toCodecJson(Prompt.Message)),
                  )([Prompt.userMessage({ content: [Prompt.textPart({ text })] })]),
                })
            }),
          )
          const job = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'Find and read the meeting file' },
          })
          assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
          assert.strictEqual(executions, 1)
          const selection = (yield* runtime.backend.agent(root)).modelTools
          const handle = yield* runtime.backend.compact!(root)
          const compacted = yield* runtime.backend.waitCompaction!(handle)
          assert.strictEqual(compacted.status, 'completed', JSON.stringify(compacted))
          if (compacted.status !== 'completed' || compacted.result._tag !== 'Queued')
            return assert.fail('Expected queued summary')
          assert.strictEqual(
            (yield* runtime.backend.wait(compacted.result.submissionId)).status,
            'done',
          )
          assert.strictEqual(summaries, 1)
          yield* Scope.close(firstScope, Exit.void)
          const secondStorage = yield* Layer.build(Layer.fresh(storageLayer))
          const second = yield* HarnessRuntime.make({
            models: [model],
            tools: Toolkit.merge(Search.toolkit, files),
          }).pipe(
            Effect.provide(handlers),
            Effect.provideContext(secondStorage),
            Effect.provideService(Settings.Settings, settings),
          )
          assert.deepStrictEqual((yield* second.backend.agent(root)).modelTools, selection)
          const next = yield* second.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'Read the meeting file again' },
          })
          assert.strictEqual((yield* second.backend.wait(next.id)).status, 'done')
          assert.strictEqual(executions, 2)
          assert.strictEqual(summaries, 1)
          assert.deepStrictEqual((yield* second.backend.agent(root)).modelTools, selection)
          assert.strictEqual(bodies.length, 6)
          const wireName = provider === 'anthropic' ? 'Read' : 'read'
          assert.deepStrictEqual(
            bodies.map((body) =>
              ((body.tools ?? []) as Schema.JsonObject[]).map((tool) => tool.name),
            ),
            [
              ['tool_search'],
              ['tool_search', wireName],
              ['tool_search', wireName],
              [],
              ['tool_search', wireName],
              ['tool_search', wireName],
            ],
          )
          assert.deepStrictEqual(
            preparations.map((preparation) =>
              preparation.tools.map((tool) => tool.declaration.name),
            ),
            [
              ['tool_search'],
              ['tool_search', 'read'],
              ['tool_search', 'read'],
              [],
              ['tool_search', 'read'],
              ['tool_search', 'read'],
            ],
          )
          for (const preparation of preparations) {
            const configuration = preparation.configuration as Schema.JsonObject
            assert.deepStrictEqual(configuration.binding, { ...binding, provider })
          }
          const entries = yield* Session.scanEntries(second.session, { conversationId: root }).pipe(
            Stream.runCollect,
          )
          assert.strictEqual(entries.filter((entry) => entry.kind === 'compaction').length, 1)
          assert.strictEqual(entries.filter((entry) => entry.kind === 'tool.result').length, 3)
          assert.notInclude(JSON.stringify(bodies[4]), 'old background')
          if (provider === 'anthropic') {
            assert.include(JSON.stringify(bodies[1]), 'signed-search')
            assert.include(JSON.stringify(bodies[4]), 'signed-search')
            assert.include(JSON.stringify(bodies[4]), 'redacted-search')
            assert.include(JSON.stringify(entries), 'private-search-reasoning')
          }
          assert.notInclude(JSON.stringify(entries), 'SECRET')
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )
