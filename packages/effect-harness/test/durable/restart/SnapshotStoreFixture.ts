import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as BunRuntime from '@effect/platform-bun/BunRuntime'
import * as SqliteClient from '@effect/sql-sqlite-bun/SqliteClient'
import * as Record from 'effect-harness/durable/Record'
import { StorageError } from 'effect-harness/durable/StorageError'
import { Store, makeCandidate } from 'effect-harness/durable/Store'
import * as SnapshotStore from 'effect-harness/durable/storage/SnapshotStore'
import * as Config from 'effect/Config'
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as KeyValueStore from 'effect/persistence/KeyValueStore'
import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import * as ClusterWorkflowEngine from 'effect/cluster/ClusterWorkflowEngine'
import * as SingleRunner from 'effect/cluster/SingleRunner'
import * as Activity from 'effect/workflow/Activity'
import * as Workflow from 'effect/workflow/Workflow'

const Recovery = Workflow.make('SnapshotStoreRecovery', {
  payload: {},
  success: Record.Seq,
  error: StorageError,
  idempotencyKey: () => 'same-execution',
})
const Handler = Recovery.toLayer(() =>
  Effect.gen(function* () {
    const store = yield* Store
    const phase = yield* Config.String('SQL_STORE_PHASE').pipe(Effect.orDie)
    if (phase !== 'start') yield* Effect.sleep('100 millis')
    yield* Activity.make({
      name: 'first',
      success: Record.Seq,
      error: StorageError,
      execute: store.commit(
        [
          {
            _tag: 'conversation',
            type: 'conversation',
            value: { id: Record.ROOT_CONVERSATION_ID },
          },
        ],
        { key: 'first' },
      ),
    })
    return yield* Activity.make({
      name: 'second',
      success: Record.Seq,
      error: StorageError,
      execute: Effect.gen(function* () {
        const seq = yield* store.transact(
          (state) =>
            phase === 'start'
              ? Effect.succeed(
                  makeCandidate({ state, writes: [], result: Record.Seq.make(state.nextSeq) }),
                )
              : Effect.die('Committed Activity callback must not run again'),
          { key: 'second' },
        )
        if (phase === 'start') {
          yield* Console.log('SQL_STORE_READY:committed')
          return yield* Effect.never
        }
        return seq
      }),
    })
  }),
)

const program = Effect.gen(function* () {
  const filename = yield* Config.String('SQL_STORE_DB')
  const Database = SqliteClient.layer({ filename })
  const Cluster = SingleRunner.layer({
    runnerStorage: 'memory',
    shardingConfig: {
      shardsPerGroup: 1,
      entityMessagePollInterval: '25 millis',
      entityReplyPollInterval: '25 millis',
    },
  }).pipe(Layer.provideMerge(Database), Layer.provide(BunCrypto.layer))
  const Live = Handler.pipe(
    Layer.provideMerge(
      SnapshotStore.layer.pipe(
        Layer.provide(Layer.mergeAll(KeyValueStore.layerSql(), SqlEventJournal.layer())),
      ),
    ),
    Layer.provideMerge(ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(Cluster))),
  )
  yield* Effect.gen(function* () {
    const result = yield* Recovery.execute({})
    const state = yield* (yield* Store).committed
    yield* Console.log(`SQL_STORE_DONE:${result}:${state.nextSeq}:${state.receipts.length}`)
  }).pipe(Effect.provide(Live))
})

BunRuntime.runMain(program)
