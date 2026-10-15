import { encodeCredential, decodeOAuth } from './fixtures/Credentials.js'
import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Record from '../src/Record.js'
import * as Session from '../src/Session.js'
import * as Transaction from '../src/Transaction.js'
import * as Settings from '../src/Settings.js'
import * as Storage from '../src/Storage.js'
import * as Usage from '../src/Usage.js'
import * as Tool from '../src/Tool.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Store from '../src/provider-auth/CredentialStore.js'
import * as Codex from '../src/provider-openai-codex/Model.js'
import * as Claude from '../src/provider-anthropic/OAuthModel.js'
import {
  binding,
  credentials,
  codexEvents,
  codexResponse,
  claudeEvents,
  claudeResponse,
  payload,
  sse,
} from './fixtures/SubscriptionProtocol.js'

type Provider = 'openai-codex' | 'anthropic'
const definition = (provider: Provider, contextWindow?: number) => ({
  ref: { provider, modelId: provider === 'anthropic' ? 'claude-sonnet-4-6' : 'gpt-5.4' },
  maxOutputTokens: 100,
  ...(contextWindow === undefined ? {} : { contextWindow }),
  capabilities: { tools: true, images: false, reasoning: true, structuredOutput: false },
})
const model = Effect.fnUntraced(function* (provider: Provider, contextWindow?: number) {
  if (provider === 'anthropic')
    return yield* Claude.make({
      model: 'claude-sonnet-4-6',
      binding: binding,
      definition: definition(provider, contextWindow),
      config: { cache: 'short', maxTokens: 4096 },
    })
  return yield* Codex.make({
    model: 'gpt-5.4',
    binding,
    definition: definition(provider, contextWindow),
    config: { cache: 'affinity' },
  })
})
const seed = (runtime: HarnessRuntime.HarnessRuntimeService) =>
  Session.commit(runtime.session, (tx) =>
    Effect.gen(function* () {
      const root = Record.ROOT_CONVERSATION_ID
      for (const text of ['old '.repeat(500), 'recent '.repeat(100)])
        yield* Transaction.appendEntry(tx, root, {
          kind: 'input',
          model: yield* Schema.encodeEffect(Schema.Array(Schema.toCodecJson(Prompt.Message)))([
            Prompt.userMessage({ content: [Prompt.textPart({ text })] }),
          ]),
        })
    }),
  )

for (const provider of ['openai-codex', 'anthropic'] as const) {
  for (const mode of ['manual', 'threshold', 'background', 'overflow'] as const) {
    it.live(
      `${provider}: ${mode} compacts through the public constructor and continues with summary plus tail`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* credentials(provider)
            const requests: Schema.JsonObject[] = []
            let summaries = 0
            let overflowed = false
            const http = HttpClient.make((request) =>
              Effect.sync(() => {
                const body = payload(request)
                requests.push(body)
                const summary = JSON.stringify(body).includes('context summarization assistant')
                assert.match(request.headers.authorization!, /^Bearer /)
                if (summary) {
                  summaries++
                  assert.notInclude(JSON.stringify(body), 'cache_control')
                  assert.isUndefined(body.prompt_cache_key)
                  assert.isUndefined(request.headers['session-id'])
                  assert.deepEqual(body.tools ?? [], [])
                  if (provider === 'anthropic') assert.strictEqual(body.max_tokens, 80)
                  else assert.isUndefined(body.max_output_tokens)
                } else if (mode === 'overflow' && !overflowed) {
                  overflowed = true
                  return HttpClientResponse.fromWeb(
                    request,
                    new Response(
                      JSON.stringify({
                        error:
                          provider === 'anthropic'
                            ? {
                                type: 'invalid_request_error',
                                message: 'prompt is too long: 3000 tokens > 2000 maximum',
                              }
                            : {
                                code: 'context_length_exceeded',
                                message: 'SECRET provider message',
                              },
                      }),
                      { status: 400 },
                    ),
                  )
                }
                if (provider === 'openai-codex') {
                  if (!summary)
                    return HttpClientResponse.fromWeb(request, new Response(sse(codexEvents)))
                  const response = {
                    ...codexResponse,
                    output: [
                      {
                        ...codexResponse.output[0]!,
                        content: [
                          {
                            type: 'output_text',
                            text: 'Remember old work.',
                            annotations: [],
                            logprobs: [],
                          },
                        ],
                      },
                    ],
                  }
                  return HttpClientResponse.fromWeb(
                    request,
                    new Response(sse([{ type: 'response.completed', response }])),
                  )
                }
                return HttpClientResponse.fromWeb(
                  request,
                  new Response(
                    summary
                      ? JSON.stringify({
                          ...claudeResponse,
                          content: [{ type: 'text', text: 'Remember old work.', citations: null }],
                        })
                      : sse(claudeEvents),
                  ),
                )
              }),
            )
            const descriptor = yield* model(provider, mode === 'threshold' ? 500 : 2000).pipe(
              Effect.provideService(Store.CredentialStore, store),
              Effect.provideService(HttpClient.HttpClient, http),
            )
            const settings = yield* Settings.make({
              compaction: {
                enabled: mode !== 'manual',
                reserveTokens: 100,
                keepRecentTokens: 50,
                backgroundTokens: mode === 'background' ? 1500 : 0,
              },
            })
            const toolkit = Toolkit.make(
              Tool.make('lookup', { parameters: Schema.Struct({}), success: Schema.String }),
            )
            const runtime = yield* HarnessRuntime.make({
              models: [descriptor],
              tools: toolkit,
            }).pipe(
              Effect.provide(toolkit.toLayer({ lookup: () => Effect.succeed('found') })),
              Effect.provideService(Settings.Settings, settings),
            )
            const root = yield* runtime.backend.root
            yield* seed(runtime)
            if (mode === 'manual') {
              const handle = yield* runtime.backend.compact!(root)
              const outcome = yield* runtime.backend.waitCompaction!(handle)
              assert.strictEqual(outcome.status, 'completed', JSON.stringify(outcome))
              if (outcome.status === 'completed' && outcome.result._tag === 'Queued')
                yield* runtime.backend.wait(outcome.result.submissionId)
            }
            const submit = () =>
              Effect.gen(function* () {
                const job = yield* runtime.backend.submit({
                  conversationId: root,
                  draft: { type: 'input', content: 'Continue.' },
                })
                const outcome = yield* runtime.backend.wait(job.id)
                assert.strictEqual(outcome.status, 'done', JSON.stringify(outcome))
              })
            yield* submit()
            if (mode === 'background') {
              const tasks = yield* Session.scanTasks(runtime.session).pipe(Stream.runCollect)
              for (const task of tasks.filter((task) => task.kind === 'harness.compaction')) {
                const outcome = yield* runtime.backend.waitCompaction!({
                  conversationId: root,
                  id: task.id,
                })
                if (outcome.status === 'completed' && outcome.result._tag === 'Queued')
                  yield* runtime.backend.wait(outcome.result.submissionId)
              }
              yield* submit()
            }
            assert.strictEqual(summaries, 1)
            const continued = requests
              .filter((body) => !JSON.stringify(body).includes('context summarization assistant'))
              .at(-1)!
            assert.include(JSON.stringify(continued.tools).toLowerCase(), 'lookup')
            for (const request of requests.filter((body) =>
              JSON.stringify(body).includes('context summarization assistant'),
            )) {
              assert.deepEqual(request.tools ?? [], [])
            }
            assert.include(JSON.stringify(continued), 'Remember old work.')
            assert.include(JSON.stringify(continued), 'recent ')
            assert.notInclude(JSON.stringify(continued), 'old old old')
            const entries = yield* Session.scanEntries(runtime.session, {
              conversationId: root,
            }).pipe(Stream.runCollect)
            assert.strictEqual(entries.filter((entry) => entry.kind === 'compaction').length, 1)
            assert.notInclude(JSON.stringify(entries), 'SECRET')
            const receipts = yield* Session.commit(runtime.session, (tx) =>
              Usage.receipts(tx, root),
            )
            assert.strictEqual(
              receipts.filter(
                (receipt) =>
                  receipt.identity._tag === 'requestAttempt' &&
                  receipt.identity.purpose === 'compaction',
              ).length,
              1,
            )
          }),
        ).pipe(Effect.provide(Storage.layerMemory)),
    )
  }
}

for (const provider of ['openai-codex', 'anthropic'] as const) {
  for (const failure of [
    'blank',
    'length',
    'tool',
    'malformed',
    'failed',
    'truncated',
    'oversized',
  ] as const) {
    it.live(`${provider}: ${failure} summary never advances the context head`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* credentials(provider)
          const http = HttpClient.make((request) =>
            Effect.sync(() => {
              let body: string | ReadableStream<Uint8Array>
              if (provider === 'openai-codex') {
                const response = {
                  ...codexResponse,
                  output: [
                    {
                      ...codexResponse.output[0]!,
                      content: [
                        {
                          type: 'output_text',
                          text:
                            failure === 'blank'
                              ? '  '
                              : 'x'.repeat(failure === 'oversized' ? 500 : 20),
                          annotations: [],
                          logprobs: [],
                        },
                      ],
                    },
                  ],
                }
                if (failure === 'tool')
                  body = sse([
                    {
                      type: 'response.completed',
                      response: {
                        ...response,
                        output: [
                          {
                            type: 'function_call',
                            id: 'fc',
                            call_id: 'call',
                            name: 'rogue',
                            arguments: '{}',
                            status: 'completed',
                          },
                        ],
                      },
                    },
                  ])
                else if (failure === 'length')
                  body = sse([
                    {
                      type: 'response.incomplete',
                      response: {
                        ...response,
                        status: 'incomplete',
                        incomplete_details: { reason: 'max_output_tokens' },
                      },
                    },
                  ])
                else if (failure === 'failed')
                  body = sse([
                    {
                      type: 'response.failed',
                      response: { error: { code: 'invalid_request_error', message: 'SECRET' } },
                    },
                  ])
                else if (failure === 'truncated')
                  body = sse([{ type: 'response.created', response }])
                else if (failure === 'malformed') body = 'data: {broken}\n\n'
                else body = sse([{ type: 'response.completed', response }])
              } else {
                const response = {
                  ...claudeResponse,
                  content: [
                    { type: 'text', text: failure === 'blank' ? '  ' : 'summary', citations: null },
                  ],
                }
                if (failure === 'tool')
                  body = JSON.stringify({
                    ...response,
                    stop_reason: 'tool_use',
                    content: [{ type: 'tool_use', id: 'call', name: 'rogue', input: {} }],
                  })
                else if (failure === 'length' || failure === 'oversized')
                  body = JSON.stringify({ ...response, stop_reason: 'max_tokens' })
                else if (failure === 'failed')
                  body = JSON.stringify({
                    type: 'error',
                    error: { type: 'invalid_request_error', message: 'SECRET' },
                  })
                else if (failure === 'truncated') body = JSON.stringify(response).slice(0, -2)
                else if (failure === 'malformed') body = '{}'
                else body = JSON.stringify(response)
              }
              return HttpClientResponse.fromWeb(request, new Response(body))
            }),
          )
          const descriptor = yield* model(provider).pipe(
            Effect.provideService(Store.CredentialStore, store),
            Effect.provideService(HttpClient.HttpClient, http),
          )
          const settings = yield* Settings.make({
            retry: { enabled: false },
            compaction: { enabled: false, reserveTokens: 100, keepRecentTokens: 50 },
          })
          const runtime = yield* HarnessRuntime.make({ models: [descriptor] }).pipe(
            Effect.provideService(Settings.Settings, settings),
          )
          const root = yield* runtime.backend.root
          yield* seed(runtime)
          const handle = yield* runtime.backend.compact!(root)
          const outcome = yield* runtime.backend.waitCompaction!(handle)
          assert.strictEqual(outcome.status, 'failed', JSON.stringify(outcome))
          const entries = yield* Session.scanEntries(runtime.session, {
            conversationId: root,
          }).pipe(Stream.runCollect)
          assert.strictEqual(entries.filter((entry) => entry.kind === 'compaction').length, 0)
          const receipts = yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root))
          assert.strictEqual(receipts.length, 1)
          assert.notInclude(JSON.stringify(receipts), 'SECRET')
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
    )
  }
}

for (const provider of ['openai-codex', 'anthropic'] as const)
  it.live(
    `${provider}: cold summary restart pins request/budget, refreshes credentials and never regenerates committed work`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
          const storageLayer = Storage.layerJsonl({ filePath: `${directory}/session.jsonl` })
          const store = yield* credentials(provider)
          const entered = yield* Deferred.make<void>()
          let summaryCalls = 0
          let refreshes = 0
          const ids: string[] = []
          const attempts: number[] = []
          const bodies: Schema.JsonObject[] = []
          const http = HttpClient.make((request) =>
            Effect.gen(function* () {
              if (request.url.endsWith('/oauth/token')) {
                refreshes++
                return HttpClientResponse.fromWeb(
                  request,
                  new Response(
                    JSON.stringify({
                      access_token: provider === 'openai-codex' ? codexToken : 'SECRET-renewed',
                      refresh_token: 'SECRET-rotated',
                      expires_in: 3600,
                    }),
                  ),
                )
              }
              const body = payload(request)
              if (!JSON.stringify(body).includes('context summarization assistant'))
                return HttpClientResponse.fromWeb(
                  request,
                  new Response(sse(provider === 'anthropic' ? claudeEvents : codexEvents)),
                )
              bodies.push(body)
              const prepared = Option.getOrThrow(
                yield* Effect.serviceOption(Prepared.PreparedRequest),
              )
              const policy = Option.getOrThrow(yield* Effect.serviceOption(CompactionRequest))
              ids.push(prepared.current.requestId)
              attempts.push(policy.attempt)
              summaryCalls++
              if (summaryCalls === 1) {
                yield* Deferred.succeed(entered, undefined)
                return yield* Effect.never
              }
              assert.strictEqual(policy.maxOutputTokens, 80)
              assert.strictEqual(refreshes, 1)
              assert.strictEqual(
                request.headers.authorization,
                `Bearer ${provider === 'anthropic' ? 'SECRET-renewed' : codexToken}`,
              )
              return HttpClientResponse.fromWeb(
                request,
                new Response(
                  provider === 'anthropic'
                    ? JSON.stringify(claudeResponse)
                    : sse([{ type: 'response.completed', response: codexResponse }]),
                ),
              )
            }),
          )
          const settings = yield* Settings.make({
            compaction: { enabled: false, reserveTokens: 100, keepRecentTokens: 50 },
          })
          const open = Effect.gen(function* () {
            const storage = yield* Layer.build(Layer.fresh(storageLayer))
            const descriptor = yield* model(provider).pipe(
              Effect.provideService(Store.CredentialStore, store),
              Effect.provideService(HttpClient.HttpClient, http),
            )
            return yield* HarnessRuntime.make({ models: [descriptor] }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(Settings.Settings, settings),
            )
          })
          const firstScope = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
          const first = yield* open.pipe(Effect.provideService(Scope.Scope, firstScope))
          const root = yield* first.backend.root
          yield* seed(first)
          const handle = yield* first.backend.compact!(root)
          yield* Deferred.await(entered).pipe(Effect.timeout('5 seconds'))
          yield* Scope.close(firstScope, Exit.void)
          yield* store.modify(binding.credentialId, (current) =>
            Effect.succeed([
              undefined,
              current === undefined
                ? undefined
                : encodeCredential({ ...decodeOAuth(current)!, expiresAt: 1 }),
            ] as const),
          )
          yield* settings.update({ compaction: { reserveTokens: 200, keepRecentTokens: 90 } })
          const secondScope = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void))
          const second = yield* open.pipe(Effect.provideService(Scope.Scope, secondScope))
          const outcome = yield* second.backend.waitCompaction!(handle).pipe(
            Effect.timeout('5 seconds'),
          )
          assert.strictEqual(outcome.status, 'completed', JSON.stringify(outcome))
          if (outcome.status === 'completed' && outcome.result._tag === 'Queued')
            yield* second.backend.wait(outcome.result.submissionId)
          assert.deepEqual(ids, [ids[0], ids[0]])
          assert.deepEqual(attempts, [0, 1])
          assert.deepEqual(bodies[0], bodies[1])
          const receipts = yield* Session.commit(second.session, (tx) => Usage.receipts(tx, root))
          assert.deepEqual(
            receipts.map((receipt) => receipt.outcome._tag),
            ['interrupted', 'completed'],
          )
          assert.strictEqual(receipts[1]?.usage?._tag, 'model')
          if (receipts[1]?.usage?._tag === 'model')
            assert.strictEqual(receipts[1].usage.tokens.input, provider === 'anthropic' ? 16 : 12)
          yield* Scope.close(secondScope, Exit.void)
          const third = yield* open
          assert.deepEqual(yield* third.backend.waitCompaction!(handle), outcome)
          assert.strictEqual(summaryCalls, 2)
          const job = yield* third.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'Continue' },
          })
          assert.strictEqual((yield* third.backend.wait(job.id)).status, 'done')
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )

import { NodeServices } from '@effect/platform-node'
import * as Deferred from 'effect/Deferred'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Scope from 'effect/Scope'
import * as Prepared from '../src/PreparedRequest.js'
import { CompactionRequest } from '../src/CompactionRequest.js'
import { codexToken } from './fixtures/SubscriptionProtocol.js'

import * as Option from 'effect/Option'

for (const provider of ['openai-codex', 'anthropic'] as const)
  it.live(
    `${provider}: threshold reuses normalized input usage and discards it after a leading instruction rewrite`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* credentials(provider)
          let summaries = 0
          const http = HttpClient.make((request) =>
            Effect.sync(() => {
              const body = payload(request)
              const summary = JSON.stringify(body).includes('context summarization assistant')
              if (summary) summaries++
              if (provider === 'anthropic')
                return HttpClientResponse.fromWeb(
                  request,
                  new Response(summary ? JSON.stringify(claudeResponse) : sse(claudeEvents)),
                )
              return HttpClientResponse.fromWeb(
                request,
                new Response(
                  sse(
                    summary
                      ? [{ type: 'response.completed', response: codexResponse }]
                      : codexEvents,
                  ),
                ),
              )
            }),
          )
          const descriptor = yield* model(provider, 500).pipe(
            Effect.provideService(Store.CredentialStore, store),
            Effect.provideService(HttpClient.HttpClient, http),
          )
          const settings = yield* Settings.make({
            compaction: {
              enabled: false,
              reserveTokens: 100,
              keepRecentTokens: 50,
              backgroundTokens: 0,
            },
          })
          const runtime = yield* HarnessRuntime.make({ models: [descriptor] }).pipe(
            Effect.provideService(Settings.Settings, settings),
          )
          const root = yield* runtime.backend.root
          yield* seed(runtime)
          const submit = () =>
            Effect.gen(function* () {
              const job = yield* runtime.backend.submit({
                conversationId: root,
                draft: { type: 'input', content: 'Continue' },
              })
              assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
            })
          yield* submit()
          yield* settings.update({ compaction: { enabled: true } })
          yield* submit()
          assert.strictEqual(
            summaries,
            0,
            'Matching input usage must supersede the whole-history estimate',
          )
          yield* runtime.backend.configure({
            conversationId: root,
            change: { instructions: 'Changed leading instructions' },
          })
          yield* submit()
          assert.strictEqual(
            summaries,
            1,
            'A changed leading prefix must invalidate historical usage',
          )
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
