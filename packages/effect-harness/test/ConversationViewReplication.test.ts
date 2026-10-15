import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Context from 'effect/Context'
import * as FileSystem from 'effect/FileSystem'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Layer from 'effect/Layer'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Schema from 'effect/Schema'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as View from 'effect-harness/ConversationView'
import * as Events from 'effect-harness/AgentEvent'
import * as Conversation from 'effect-harness/Conversation'
import * as Record from 'effect-harness/Record'
import * as Registry from 'effect-harness/Registry'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Task from 'effect-harness/Task'
import { TaskExecution } from 'effect-harness/TaskExecution'
import * as Tool from 'effect-harness/Tool'
import * as Invocation from 'effect-harness/ToolInvocation'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Transaction from 'effect-harness/Transaction'
import { ToolExecution } from 'effect-harness/ToolExecution'
import * as Document from 'effect-harness/Document'

const bun = 'Bun' in globalThis
const services = bun ? (await import('@effect/platform-bun')).BunServices.layer : NodeServices.layer

describe('durable semantic replica', () => {
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    it.live(
      `actual nested payload survives parent retirement and cold reopen without replay or charge: ${backend}`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'nested-observation-' })
            const sql = bun
              ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
              : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient'))).layer
            const storageLayer = (() => {
              if (backend === 'memory') return Storage.layerMemory
              if (backend === 'sqlite')
                return Storage.layerJournal().pipe(
                  Layer.provide(SqlEventJournal.layer()),
                  Layer.provide(sql({ filename: `${directory}/native.sqlite` })),
                )
              return Storage.layerJsonl({ filePath: `${directory}/native.jsonl` })
            })()
            const storageScope = yield* Scope.make()
            const context = yield* Layer.buildWithScope(storageLayer, storageScope)
            const model = yield* LanguageModel.make({
              generateText: () => Effect.die('No provider'),
              streamText: () => Stream.die('No provider'),
            })
            const leaf = Tool.make('observed-leaf', {
              parameters: Schema.Struct({ value: Schema.String }),
              success: Schema.String,
              replay: 'safe',
            })
            const media = Tool.makeResult('observed-media', { replay: 'safe' })
            const toolkit = Toolkit.make(leaf, media)
            let executions = 0
            const contract = {
              kind: 'observation.parent',
              version: 1,
              input: Schema.Void,
              checkpoint: Schema.Struct({ phase: Schema.Literal('execute') }),
              success: Schema.String,
              failure: Schema.String,
            } as const
            const parent = Task.make({
              ...contract,
              initial: () => ({ phase: 'execute' as const }),
              phases: {
                execute: () =>
                  Effect.gen(function* () {
                    const execution = yield* TaskExecution
                    const result = yield* Invocation.invoke(
                      execution,
                      leaf,
                      { value: 'public-actual-nested' },
                      { key: 'owned-payload' },
                    )
                    assert.strictEqual(result.status, 'succeeded')
                    const second = yield* Invocation.invoke(
                      execution,
                      media,
                      {},
                      { key: 'owned-media' },
                    )
                    assert.strictEqual(second.status, 'succeeded')
                    yield* execution.commit((tx) =>
                      execution.progress(tx, contract, {
                        status: 'completing',
                        outcome: { status: 'completed', result: 'parent-result' },
                      }),
                    )
                  }),
              },
            })
            const registry = yield* Registry.make
            yield* registry.install(
              Registry.withTask(
                Registry.toolkit({ id: 'observation-parent', version: 1 }, Toolkit.make()),
                parent,
              ),
            )
            const scope = yield* Scope.make()
            const runtime = yield* HarnessRuntime.make({ registry, tools: toolkit }).pipe(
              Effect.provideContext(context),
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provideService(Scope.Scope, scope),
              Effect.provide(
                toolkit.toLayer({
                  'observed-leaf': ({ value }) =>
                    Effect.sync(() => {
                      executions++
                      return value
                    }),
                  'observed-media': () =>
                    Effect.gen(function* () {
                      executions++
                      const access = yield* ToolExecution
                      yield* access.output('retained native output')
                      yield* access.details({ actual: 'retained details' })
                      yield* access.diagnostic({ severity: 'info', message: 'actual diagnostic' })
                      return {
                        content: [
                          Prompt.filePart({
                            mediaType: 'image/png',
                            data: new Uint8Array([3, 8, 13]),
                          }),
                        ],
                        details: { privateDetail: 'native UI' },
                      }
                    }),
                }),
              ),
            )
            const client = yield* Harness.make.pipe(
              Effect.provideService(HarnessBackend, runtime.backend),
            )
            const conversation = yield* client.root
            yield* Session.commit(runtime.session, (tx) =>
              Transaction.appendEntry(tx, conversation.id, {
                kind: 'observation.anchor',
                data: { text: 'Actual fork anchor' },
              }),
            )
            const observing = yield* Scope.make()
            const pull = yield* Stream.toPull(Events.watch(conversation)).pipe(
              Effect.provideService(Scope.Scope, observing),
            )
            const initial = (yield* pull)[0]
            if (initial._tag !== 'Snapshot') return yield* Effect.die('Initial snapshot required')
            const ref = yield* runtime.scheduler.start(parent, undefined, {
              conversationId: conversation.id,
            })
            assert.strictEqual((yield* runtime.scheduler.wait(ref)).status, 'completed')
            const latest = yield* View.snapshot(conversation)
            const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(View.SnapshotSchema))(
              latest,
            )
            assert.deepStrictEqual(
              yield* Schema.decodeEffect(Schema.toCodecJson(View.SnapshotSchema))(encoded),
              latest,
            )
            assert.strictEqual(latest.value.nested.length, 2)
            assert.strictEqual(latest.value.nested[0]!.availability, 'available')
            assert.strictEqual(latest.value.nested[0]!.receipts[0]!.result.valueKind, 'content')
            const receipt = latest.value.nested[0]!.receipts[0]!
            if (receipt.result.valueKind === 'content')
              assert.strictEqual(receipt.result.structuredOutput, 'public-actual-nested')
            assert.notProperty(receipt, 'legacyValue')
            assert.notProperty(receipt.result, 'control')
            const mediaReceipt = latest.value.nested[1]!.receipts[0]!
            assert.strictEqual(mediaReceipt.result.valueKind, 'content')
            if (mediaReceipt.result.valueKind !== 'content')
              return yield* Effect.die('Native media required')
            const file = yield* Schema.decodeUnknownEffect(Prompt.FilePart)(
              mediaReceipt.result.structuredOutput,
            )
            assert.deepStrictEqual(file.data, new Uint8Array([3, 8, 13]))
            assert.notProperty(mediaReceipt, 'legacyValue')
            assert.notProperty(mediaReceipt.result, 'control')
            assert.isTrue(
              latest.value.progress.some((item) =>
                JSON.stringify(item.value).includes('retained native output'),
              ),
            )
            assert.deepStrictEqual(latest.value.graph.tasks, [])
            assert.deepStrictEqual(latest.value.usage, yield* Conversation.usage(conversation))
            assert.deepStrictEqual(
              yield* Session.scanDocuments(runtime.session, {
                scope: { _tag: 'task', taskId: ref.id },
              }).pipe(Stream.runCollect),
              [],
            )
            let replica = initial
            const actualEvents: Events.Event[] = []
            while (replica.revision.cursor < latest.revision.cursor)
              for (const frame of yield* pull) {
                if (frame._tag === 'Batch') actualEvents.push(...frame.events)
                replica = yield* Events.reduce(replica, frame)
              }
            assert.deepStrictEqual(replica, latest)
            assert.strictEqual(actualEvents.filter((event) => event._tag === 'NestedEnd').length, 2)
            assert.isBelow(
              actualEvents.findIndex((event) => event._tag === 'NestedEnd'),
              actualEvents.findIndex(
                (event) => event._tag === 'TaskEnd' && event.outcome.taskId === ref.id,
              ),
            )
            yield* Scope.close(observing, Exit.void)
            const remount = yield* View.snapshot(conversation)
            assert.notStrictEqual(remount.revision.epoch, latest.revision.epoch)
            assert.deepStrictEqual(remount.value, latest.value)
            const fork = yield* Conversation.fork(conversation)
            const forkView = yield* View.snapshot(fork)
            assert.deepStrictEqual(forkView.value.nested, [])
            assert.deepStrictEqual(forkView.value.run, {})
            assert.deepStrictEqual(forkView.value.live, {})
            assert.deepStrictEqual(forkView.value.graph.tasks, [])
            assert.deepStrictEqual(forkView.value.usage, yield* Conversation.usage(fork))
            assert.strictEqual(forkView.value.usage.tools.length, 0)
            yield* Scope.close(scope, Exit.void)
            const coldStorageScope = yield* Scope.make()
            if (backend !== 'memory') yield* Scope.close(storageScope, Exit.void)
            const coldContext =
              backend === 'memory'
                ? context
                : yield* Layer.buildWithScope(storageLayer, coldStorageScope)
            const before = yield* Context.get(coldContext, Storage.Storage)
              .scanDocuments({
                scope: { _tag: 'conversation', conversationId: Record.ROOT_CONVERSATION_ID },
              })
              .pipe(Stream.runCollect)
            const nextScope = yield* Scope.make()
            const next = yield* HarnessRuntime.make().pipe(
              Effect.provideContext(coldContext),
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provideService(Scope.Scope, nextScope),
            )
            const cold = yield* next.backend.observations!.conversationSnapshot!(
              Record.ROOT_CONVERSATION_ID,
            )
            assert.deepStrictEqual(cold.value, latest.value)
            assert.deepStrictEqual(
              yield* Context.get(coldContext, Storage.Storage)
                .scanDocuments({
                  scope: { _tag: 'conversation', conversationId: Record.ROOT_CONVERSATION_ID },
                })
                .pipe(Stream.runCollect),
              before,
            )
            assert.strictEqual(executions, 2)
            const retention = Document.family({
              kind: 'harness.tool.nested.receipt',
              version: 1,
              scope: 'conversation',
              history: 'latest',
              fork: 'initial',
              schema: Schema.JsonObject,
              initial: () => ({}),
            })
            const retained = before.filter((record) => record.kind === retention.definition.kind)
            assert.strictEqual(retained.length, 2)
            yield* Session.commit(next.session, (tx) =>
              Effect.forEach(
                retained,
                (record) =>
                  record.key === undefined
                    ? Effect.die('Retained family identity required')
                    : Transaction.retireDocument(tx, retention, {
                        scope: record.scope,
                        key: record.key,
                      }),
                { discard: true },
              ),
            )
            const oldMissing = yield* next.backend.observations!.conversationSnapshot!(
              Record.ROOT_CONVERSATION_ID,
            )
            assert.deepStrictEqual(
              oldMissing.value.nested.map((nested) => nested.availability),
              ['unavailable', 'unavailable'],
            )
            assert.deepStrictEqual(
              oldMissing.value.nested.flatMap((nested) => nested.receipts),
              [],
            )
            assert.deepStrictEqual(oldMissing.value.usage, cold.value.usage)
            assert.strictEqual(executions, 2)
            yield* Scope.close(nextScope, Exit.void)
            yield* Scope.close(coldStorageScope, Exit.void)
            yield* Scope.close(storageScope, Exit.void)
          }),
        ).pipe(Effect.provide(services)),
    )

  it.effect('foreign targets and out-of-order event batches fail typed continuity', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const model = yield* LanguageModel.make({
          generateText: () => Effect.die('No provider'),
          streamText: () => Stream.die('No provider'),
        })
        const runtime = yield* HarnessRuntime.make().pipe(
          Effect.provideService(LanguageModel.LanguageModel, model),
        )
        yield* Session.commit(runtime.session, Transaction.ensureRoot)
        const snapshot = yield* runtime.backend.observations!.conversationSnapshot!(
          Record.ROOT_CONVERSATION_ID,
        )
        const batch: Events.Frame = {
          _tag: 'Batch',
          protocol: 1,
          target: snapshot.target,
          epoch: snapshot.revision.epoch,
          from: 0,
          to: 1,
          events: [],
          value: snapshot.value,
        }
        const after = yield* Events.reduce(snapshot, batch)
        assert.strictEqual(
          (yield* Events.reduce(after, batch).pipe(Effect.flip)).reason,
          'continuity',
        )
        assert.strictEqual(
          (yield* Events.reduce(snapshot, {
            ...batch,
            target: { _tag: 'conversation', conversationId: Record.ConversationId.make(999) },
          }).pipe(Effect.flip)).reason,
          'continuity',
        )
        assert.strictEqual(
          (yield* Events.reduce(snapshot, { ...batch, epoch: 'other' }).pipe(Effect.flip)).reason,
          'continuity',
        )
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )
})
