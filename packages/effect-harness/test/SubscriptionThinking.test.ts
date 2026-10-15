import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Session from '../src/Session.js'
import * as Settings from '../src/Settings.js'
import * as Storage from '../src/Storage.js'
import * as Transaction from '../src/Transaction.js'
import * as Tool from '../src/Tool.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Store from '../src/provider-auth/CredentialStore.js'
import * as Claude from '../src/provider-anthropic/OAuthModel.js'
import * as Native from '../src/provider-anthropic/OAuthLanguageModel.js'
import {
  binding,
  credentials,
  claudeResponse,
  claudeUsage,
  payload,
  sse,
} from './fixtures/SubscriptionProtocol.js'
const modelId = 'claude-sonnet-5-5'
const definition = {
  ref: { provider: 'anthropic', modelId },
  maxOutputTokens: 4096,
  capabilities: { tools: true, images: false, reasoning: true, structuredOutput: false },
}
const thinking = (signature: string) =>
  Prompt.reasoningPart({
    text: `thinking-${signature}`,
    options: { anthropic: { info: { type: 'thinking', signature } } },
  })
const read = Tool.make('read', { parameters: Schema.Struct({}), success: Schema.String })
const extra = Tool.make('extra', { parameters: Schema.Struct({}), success: Schema.String })

it.live(
  'Claude compaction and tool-prefix changes preserve durable thinking and request model-specific mismatch handling after restart',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
        const storage = Storage.layerJsonl({ filePath: `${directory}/history.jsonl` })
        const store = yield* credentials('anthropic')
        const bodies: Schema.JsonObject[] = []
        const headers: string[] = []
        const http = HttpClient.make((request) =>
          Effect.sync(() => {
            const body = payload(request)
            bodies.push(body)
            headers.push(request.headers['anthropic-beta']!)
            const summary = JSON.stringify(body).includes('context summarization assistant')
            if (summary) {
              assert.notInclude(JSON.stringify(body), 'thinking-secret-summary')
              assert.notInclude(JSON.stringify(body), 'redacted-summary')
              return HttpClientResponse.fromWeb(
                request,
                new Response(
                  JSON.stringify({
                    ...claudeResponse,
                    content: [{ type: 'text', text: 'Saved summary', citations: null }],
                  }),
                ),
              )
            }
            const events = [
              {
                type: 'message_start',
                message: { ...claudeResponse, model: modelId, content: [], stop_reason: null },
              },
              {
                type: 'content_block_start',
                index: 0,
                content_block: { type: 'thinking', thinking: '', signature: '' },
              },
              {
                type: 'content_block_delta',
                index: 0,
                delta: { type: 'thinking_delta', thinking: 'new valid reasoning' },
              },
              {
                type: 'content_block_delta',
                index: 0,
                delta: { type: 'signature_delta', signature: 'new-signature' },
              },
              { type: 'content_block_stop', index: 0 },
              {
                type: 'content_block_start',
                index: 1,
                content_block: { type: 'text', text: '', citations: null },
              },
              {
                type: 'content_block_delta',
                index: 1,
                delta: { type: 'text_delta', text: 'Done' },
              },
              { type: 'content_block_stop', index: 1 },
              {
                type: 'message_delta',
                delta: { stop_reason: 'end_turn', stop_sequence: null },
                usage: claudeUsage,
              },
              { type: 'message_stop' },
            ]
            return HttpClientResponse.fromWeb(request, new Response(sse(events)))
          }),
        )
        const settings = yield* Settings.make({
          compaction: { enabled: false, reserveTokens: 2048, keepRecentTokens: 50 },
        })
        const open = (grow: boolean) =>
          Effect.gen(function* () {
            const descriptor = yield* Claude.make({
              model: modelId,
              binding: binding,
              definition,
              thinkingModes: ['adaptive'],
              config: { thinking: { type: 'adaptive' } },
            }).pipe(
              Effect.provideService(Store.CredentialStore, store),
              Effect.provideService(HttpClient.HttpClient, http),
            )
            const toolkit = Toolkit.make(read, ...(grow ? [extra] : []))
            return yield* HarnessRuntime.make({ models: [descriptor], tools: toolkit }).pipe(
              Effect.provide(
                toolkit.toLayer({
                  read: () => Effect.succeed('read'),
                  extra: () => Effect.succeed('extra'),
                }),
              ),
              Effect.provideContext(yield* Layer.build(Layer.fresh(storage))),
              Effect.provideService(Settings.Settings, settings),
            )
          })
        const firstScope = yield* Scope.make()
        yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
        const first = yield* open(false).pipe(Effect.provideService(Scope.Scope, firstScope))
        const root = yield* first.backend.root
        const messages = [
          Prompt.userMessage({ content: [Prompt.textPart({ text: 'old '.repeat(300) })] }),
          Prompt.assistantMessage({
            content: [thinking('secret-summary'), Prompt.textPart({ text: 'old answer' })],
          }),
          Prompt.assistantMessage({
            content: [
              thinking('stale-signature'),
              Prompt.reasoningPart({
                text: '',
                options: {
                  anthropic: { info: { type: 'redacted_thinking', redactedData: 'opaque-stale' } },
                },
              }),
              Prompt.toolCallPart({
                id: 'call-1',
                name: 'read',
                params: {},
                providerExecuted: false,
              }),
            ],
          }),
          Prompt.toolMessage({
            content: [
              Prompt.toolResultPart({
                id: 'call-1',
                name: 'read',
                result: 'retained '.repeat(100),
                isFailure: false,
                providerExecuted: false,
              }),
            ],
          }),
        ]
        yield* Session.commit(first.session, (tx) =>
          Effect.forEach(messages, (message) =>
            Effect.gen(function* () {
              yield* Transaction.appendEntry(tx, root, {
                kind: 'custom',
                model: yield* Schema.encodeEffect(Schema.Array(Schema.toCodecJson(Prompt.Message)))(
                  [message],
                ),
              })
            }),
          ),
        )
        const handle = yield* first.backend.compact!(root)
        const outcome = yield* first.backend.waitCompaction!(handle)
        assert.strictEqual(outcome.status, 'completed', JSON.stringify(outcome))
        if (outcome.status === 'completed' && outcome.result._tag === 'Queued')
          yield* first.backend.wait(outcome.result.submissionId)
        const submit = (runtime: HarnessRuntime.HarnessRuntimeService) =>
          Effect.gen(function* () {
            const job = yield* runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'Continue' },
            })
            assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
          })
        yield* submit(first)
        assert.include(JSON.stringify(bodies[1]), 'Saved summary')
        assert.include(JSON.stringify(bodies[1]), 'stale-signature')
        assert.include(JSON.stringify(bodies[1]), 'opaque-stale')
        assert.include(JSON.stringify(bodies[1]), 'call-1')
        assert.include(JSON.stringify(bodies[1]), 'tool_result')
        yield* Scope.close(firstScope, Exit.void)
        const second = yield* open(true)
        yield* submit(second)
        yield* submit(second)
        for (const body of bodies.slice(2)) {
          assert.include(JSON.stringify(body.tools), 'extra')
          assert.include(JSON.stringify(body), 'new-signature')
          assert.include(JSON.stringify(body), 'stale-signature')
        }
        for (const [index, body] of bodies.entries()) {
          assert.include(headers[index]!, 'thinking-binding-controls-2026-08-01')
          assert.deepEqual(body.thinking, {
            type: 'adaptive',
            display: 'summarized',
            block_binding: { prefix_mismatch_behavior: 'drop_block' },
          })
        }
        const entries = yield* Session.scanEntries(second.session, { conversationId: root }).pipe(
          Stream.runCollect,
        )
        assert.include(JSON.stringify(entries), 'secret-summary')
        assert.include(JSON.stringify(entries), 'stale-signature')
        assert.include(JSON.stringify(entries), 'new-signature')
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
)

it.effect(
  'Claude rejects too-small summary budgets before I/O; policy capability changes reject restoration',
  () =>
    Effect.gen(function* () {
      const store = yield* credentials('anthropic')
      let calls = 0
      const http = HttpClient.make(() =>
        Effect.sync(() => {
          calls++
          throw new Error('No I/O expected')
        }),
      )
      const make = (model: string, modes: ReadonlyArray<'enabled' | 'adaptive'> = ['enabled']) =>
        Claude.make({
          model,
          binding: binding,
          definition: { ...definition, ref: { provider: 'anthropic', modelId: model } },
          thinkingModes: modes,
          config: { thinking: { type: 'enabled', budget_tokens: 1024 }, maxTokens: 4096 },
        }).pipe(
          Effect.provideService(Store.CredentialStore, store),
          Effect.provideService(HttpClient.HttpClient, http),
        )
      const original = yield* make('claude-sonnet-4-6')
      const configured = Context.make(Native.Config, {
        thinking: { type: 'enabled' as const, budget_tokens: 1024 },
        maxTokens: 4096,
      })
      const settings = yield* Settings.make()
      const policy = {
        requestId: Identity.RequestId.make('summary'),
        ordinal: 0,
        attempt: Usage.AttemptId.make(0),
        settings: yield* settings.get,
        maxOutputTokens: 1024,
        cacheRetention: 'none' as const,
        deferred: false as const,
      }
      assert.strictEqual(
        (yield* Effect.flip(original.compaction!.configure(configured, policy))).reason._tag,
        'ModelUnsupportedError',
      )
      const valid = yield* original.compaction!.configure(configured, {
        ...policy,
        maxOutputTokens: 1025,
      })
      assert.strictEqual(Context.getOrUndefined(valid, Native.Config)?.maxTokens, 1025)
      const encoded = yield* original.compaction!.configuration.encode(valid)
      const changed = yield* make('claude-sonnet-4-6', ['adaptive'])
      assert.strictEqual(
        (yield* Effect.flip(changed.compaction!.configuration.restore(encoded))).reason._tag,
        'ModelUnsupportedError',
      )
      assert.strictEqual(calls, 0)
    }),
)
import * as Identity from '../src/Identity.js'
import * as Usage from '../src/Usage.js'
