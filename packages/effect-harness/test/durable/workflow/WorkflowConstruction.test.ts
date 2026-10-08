import { assert, describe, it } from '@effect/vitest'
import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Record from 'effect-harness/durable/Record'
import * as Session from 'effect-harness/durable/Session'
import * as Store from 'effect-harness/durable/Store'
import * as Identity from 'effect-harness/durable/Identity'
import * as Directory from 'effect-harness/durable/SessionDirectory'
import * as Conversation from 'effect-harness/durable/Conversation'
import * as Benchmark from 'effect-harness/durable/testing/Benchmark'
import { sessionLayer } from 'effect-harness/durable/testing/Storage'
import { withLayer } from '../StorageFixture.ts'
import { Submission } from 'effect-harness/durable/workflow/Submission'
import * as SubmissionExecutor from 'effect-harness/durable/workflow/SubmissionExecutor'

const fixture = sessionLayer(Store.layerMemory)

describe('WorkflowConstruction', () => {
  it.effect(
    'benchmark counter reads reject numeric strings and preserve valid/missing behavior',
    () =>
      withLayer(
        Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          const id = yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              const id = yield* tx.mint(Record.DocumentId)
              yield* tx.write({
                _tag: 'document.create',
                record: {
                  id,
                  kind: 'benchmark.replay',
                  scope: { _tag: 'session' },
                },
                content: { _tag: 'base', version: 1, value: { count: '12' } },
              })
              return id
            }),
          )
          const dataset: Benchmark.Dataset = {
            firstEntryId: Record.EntryId.make(1),
            filteredTaskCount: 0,
            exactDocumentId: id,
            exactDocumentKey: '',
            replayDocumentIds: new Map([[0, id]]),
            historicalDocumentId: id,
            ancientAt: Record.Seq.make(1),
            recentAt: Record.Seq.make(1),
            deepestConversationId: Record.ROOT_CONVERSATION_ID,
            ancestorHeadEntryId: Record.EntryId.make(1),
          }
          const benchmark = Benchmark.STORAGE_READ_BENCHMARKS.find(
            (value) => value.name === 'document replay tail (0)',
          )
          if (benchmark === undefined) return yield* Effect.die('Missing replay workload')
          const error = yield* Effect.flip(benchmark.run(dataset))
          assert.match(error.message, /Invalid benchmark counter document/)
          yield* session.transaction((tx) =>
            tx.write({
              _tag: 'document.change',
              id,
              content: { _tag: 'delta', version: 1, ops: [['set', ['count'], 12]] },
            }),
          )
          assert.strictEqual(yield* benchmark.run(dataset), 12)
          const missing = {
            ...dataset,
            replayDocumentIds: new Map([[0, Record.DocumentId.make(9999)]]),
          }
          assert.strictEqual(Number.isNaN(yield* benchmark.run(missing)), true)
        }),
        fixture,
      ),
  )

  it.effect(
    'native submission keeps selected Session and replay/conflict/absence precedence over bad settings',
    () =>
      withLayer(
        Effect.gen(function* () {
          const selected = yield* Session.Session
          const otherContext = yield* Layer.build(Layer.fresh(fixture))
          const other = Context.get(otherContext, Session.Session)
          assert.notStrictEqual(other, selected)
          yield* selected.root()
          const requestId = Identity.RequestId.make('already-settled')
          const old = yield* selected.transaction(
            Effect.fnUntraced(function* (tx) {
              const admitted = yield* tx.createSubmission({
                _tag: 'WriteQueued',
                conversationId: Record.ROOT_CONVERSATION_ID,
                requestId,
                type: 'write',
                status: 'queued',
              })
              yield* tx.settleSubmission(admitted.id, {
                status: 'unanswered',
                reason: 'preexisting',
              })
              return admitted.id
            }),
          )
          const configuration = yield* Conversation.makeConfiguration()
          let samples = 0
          const broken: Conversation.Configuration['Service'] = {
            ...configuration,
            settings: Effect.suspend(() => {
              samples++
              return Schema.decodeUnknownEffect(Schema.String)(0).pipe(
                Effect.flatMap(() => configuration.settings),
              )
            }),
          }
          const sessionId = Identity.SessionId.make('selected')
          const directory = Directory.SessionDirectory.of({
            resolve: () => Effect.succeed(selected),
          })
          const runtime = SubmissionExecutor.layer.pipe(
            Layer.provide(Layer.succeed(Directory.SessionDirectory, directory)),
            Layer.provide(Layer.succeed(Conversation.Configuration, broken)),
            Layer.provideMerge(WorkflowEngine.layerMemory),
          )
          yield* Effect.gen(function* () {
            const payload = {
              sessionId,
              conversationId: Record.ROOT_CONVERSATION_ID,
              requestId,
              submission: {
                _tag: 'write' as const,
                entry: { kind: 'test' },
              },
            }
            const receipt = yield* Submission.execute(payload)
            assert.strictEqual(receipt.id, old)
            assert.strictEqual(receipt.status, 'unanswered')
            assert.strictEqual(samples, 0)
            const conflict = yield* Effect.flip(
              Submission.execute({
                ...payload,
                submission: {
                  _tag: 'input',
                  message: Prompt.userMessage({
                    content: [Prompt.textPart({ text: 'changed kind' })],
                  }),
                },
              }),
            )
            assert.strictEqual(conflict.reason._tag, 'RequestConflictError')
            assert.strictEqual(samples, 0)
            const absent = yield* Effect.flip(
              Submission.execute({
                ...payload,
                conversationId: Record.ConversationId.make(9999),
                requestId: Identity.RequestId.make('absent'),
              }),
            )
            assert.strictEqual(absent.reason._tag, 'InvalidStateError')
            assert.strictEqual(samples, 0)
            const bad = yield* Effect.flip(
              Submission.execute({ ...payload, requestId: Identity.RequestId.make('new') }),
            )
            assert.strictEqual(bad.reason._tag, 'StorageError')
            assert.strictEqual(samples, 1)
            assert.strictEqual((yield* other.scanSubmissions({}, 10)).items.length, 0)
          }).pipe(
            Effect.provide(runtime),
            Effect.provideService(Session.Session, other),
            Effect.provideService(Conversation.Configuration, configuration),
          )
        }),
        fixture,
      ).pipe(Effect.provide(BunCrypto.layer)),
  )
})
