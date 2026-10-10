import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import { SqliteClient } from '@effect/sql-sqlite-node'
import * as Effect from 'effect/Effect'
import * as Context from 'effect/Context'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Scope from 'effect/Scope'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Response from 'effect/ai/Response'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as LiveState from 'effect-harness/LiveState'
import * as Model from 'effect-harness/Model'
import * as Prepared from 'effect-harness/PreparedRequest'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Settings from 'effect-harness/Settings'
import * as Storage from 'effect-harness/Storage'
import { StorageError } from 'effect-harness/StorageError'
import { StorageFormat } from 'effect-harness/StorageFormat'
import * as Usage from 'effect-harness/Usage'

const root = Record.ROOT_CONVERSATION_ID
const answer: Response.StreamPartEncoded[] = [
  { type: 'text-start', id: 't' },
  { type: 'text-delta', id: 't', delta: 'paid response' },
  { type: 'text-end', id: 't' },
  {
    type: 'finish',
    reason: 'stop',
    usage: {
      inputTokens: { total: 13, uncached: 13, cacheRead: undefined, cacheWrite: undefined },
      outputTokens: { total: 7, text: 7, reasoning: undefined },
    },
    response: undefined,
  },
]

for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  describe(`${backend} rejected stream usage publication`, () => {
    for (const boundary of ['trailing', 'body'] as const)
      it.live(
        `${boundary}: retains observed usage once in failed settlement and a cold runtime without replay`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
              const memory = yield* Layer.build(Storage.layerMemory)
              const disk =
                backend === 'sqlite'
                  ? Storage.layerSql.pipe(
                      Layer.provide(SqliteClient.layer({ filename: `${directory}/usage.sqlite` })),
                    )
                  : Storage.layerJsonl({ filePath: `${directory}/usage.jsonl` })
              const store =
                backend === 'memory' ? Effect.succeed(memory) : Layer.build(Layer.fresh(disk))
              const owner = yield* Scope.make()
              const context = yield* store.pipe(Effect.provideService(Scope.Scope, owner))
              const real = yield* Effect.service(Storage.Storage).pipe(
                Effect.provideContext(context),
              )
              const format = yield* Effect.service(StorageFormat).pipe(
                Effect.provideContext(context),
              )
              const error = new StorageError({
                reason: 'io',
                operation: 'test.known.partial',
                message: 'deterministic rejection of final usage snapshot',
              })
              let rejected = 0
              let calls = 0
              let observed: Prepared.Snapshot | undefined
              const wrapped = Storage.Storage.of({
                ...real,
                commit: (input) =>
                  Effect.suspend(() => {
                    const writes = Array.from(input)
                    const known = writes.some((write) => {
                      if (write._tag !== 'task') return false
                      return Option.isSome(
                        Schema.decodeUnknownOption(
                          Schema.Struct({
                            phase: Schema.Literal('streaming'),
                            partial: Schema.Struct({ usage: Usage.RecordSchema }),
                          }),
                        )(write.value.state.checkpoint),
                      )
                    })
                    if (known && rejected === 0) {
                      rejected++
                      return Effect.fail(error)
                    }
                    return real.commit(writes)
                  }),
              })
              const model = yield* LanguageModel.make({
                generateText: () => Effect.die('No generation fallback'),
                streamText: () =>
                  Stream.unwrap(
                    Effect.gen(function* () {
                      calls++
                      observed = Option.getOrThrow(
                        yield* Effect.serviceOption(Prepared.PreparedRequest),
                      ).current
                      const parts = Stream.fromIterable(answer)
                      return boundary === 'trailing'
                        ? parts
                        : Stream.concat(parts, Stream.fromEffect(Effect.never))
                    }),
                  ),
              })
              const pricing = {
                inputToken: 0.01,
                outputToken: 0.02,
                currency: 'USD',
                source: 'known-usage-fixture',
              }
              const ref = { provider: 'fixture', modelId: 'observed' }
              const models =
                boundary === 'trailing'
                  ? []
                  : [
                      Model.make({
                        definition: {
                          ref,
                          pricing,
                          capabilities: {
                            tools: true,
                            images: false,
                            reasoning: false,
                            structuredOutput: false,
                          },
                        },
                        languageModel: model,
                        options: Schema.Struct({}),
                        configure: () => Effect.succeed(Context.empty()),
                      }),
                    ]
              const settings = yield* Settings.make({
                progress: { partialIntervalMs: boundary === 'trailing' ? 100000 : 0 },
              })
              const first = yield* HarnessRuntime.make({ models }).pipe(
                Effect.provideContext(
                  context.pipe(
                    Context.add(Storage.Storage, wrapped),
                    Context.add(StorageFormat, { ...format, storage: wrapped }),
                  ),
                ),
                Effect.provideService(Scope.Scope, owner),
                Effect.provideService(LanguageModel.LanguageModel, model),
                Effect.provideService(Settings.Settings, settings),
              )
              yield* first.backend.root
              const job = yield* first.backend.submit({
                conversationId: root,
                draft: { type: 'input', content: 'go' },
              })
              const result = yield* first.backend.wait(job.id)
              assert.strictEqual(result.status, 'unanswered')
              assert.strictEqual(rejected, 1)
              assert.strictEqual(calls, 1)
              const receipts = yield* Session.commit(first.session, (tx) =>
                Usage.receipts(tx, root),
              )
              assert.strictEqual(receipts.length, 1)
              const receipt = receipts[0]
              assert.isDefined(receipt)
              assert.strictEqual(receipt?.identity._tag, 'requestAttempt')
              if (receipt?.identity._tag === 'requestAttempt') {
                assert.strictEqual(receipt.identity.requestId, observed?.requestId)
                assert.strictEqual(receipt.identity.attempt, 0)
                assert.strictEqual(receipt.identity.purpose, 'generation')
              }
              assert.strictEqual(receipts[0]?.outcome._tag, 'failed')
              assert.deepEqual(
                receipts[0]?.usage,
                Usage.model(
                  { input: 13, inputUncached: 13, output: 7, outputText: 7 },
                  boundary === 'body' ? ref : undefined,
                  boundary === 'body' ? pricing : undefined,
                ),
              )
              const report = yield* Session.usage(first.session, root)
              assert.strictEqual(report.models[0]?.responses, 1)
              assert.strictEqual(report.models[0]?.tokens.input, 13)
              assert.strictEqual(report.models[0]?.tokens.output, 7)
              assert.strictEqual(report.unknownAttempts ?? 0, 0)
              assert.deepEqual(
                report.costs,
                boundary === 'body' ? [{ currency: 'USD', model: 0.27, tool: 0 }] : [],
              )
              const entries = yield* Session.scanEntries(first.session, {
                conversationId: root,
              }).pipe(Stream.runCollect)
              assert.strictEqual(entries.filter((entry) => entry.kind === 'assistant').length, 0)
              assert.deepEqual(
                (yield* Session.snapshot(
                  first.session,
                  LiveState.document,
                  LiveState.target(root),
                )).pipe(Option.map((value) => value.value)),
                Option.some({}),
              )
              assert.deepEqual(yield* Session.usage(first.session, root), report)
              yield* Scope.close(owner, Exit.void)
              const second = yield* HarnessRuntime.make({ models }).pipe(
                Effect.provideContext(yield* store),
                Effect.provideService(LanguageModel.LanguageModel, model),
              )
              yield* second.backend.wait(job.id)
              assert.deepEqual(
                yield* Session.commit(second.session, (tx) => Usage.receipts(tx, root)),
                receipts,
              )
              assert.deepEqual(yield* Session.usage(second.session, root), report)
              assert.strictEqual(calls, 1)
            }),
          ).pipe(Effect.provide(NodeServices.layer)),
      )
  })
