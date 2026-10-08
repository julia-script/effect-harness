import * as Conversation from '@effect-harness/durable/Conversation'
import * as DurableExecutor from '@effect-harness/durable/Executor'
import * as Identity from '@effect-harness/durable/Identity'
import * as Session from '@effect-harness/durable/Session'
import * as SessionDirectory from '@effect-harness/durable/SessionDirectory'
import * as SnapshotStore from '@effect-harness/durable/storage/SnapshotStore'
import * as HarnessExecutor from '@effect-harness/harness/Executor'
import * as Layer from 'effect/Layer'
import * as KeyValueStore from 'effect/persistence/KeyValueStore'
import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import * as ClusterWorkflowEngine from 'effect/cluster/ClusterWorkflowEngine'
import * as SingleRunner from 'effect/cluster/SingleRunner'
import * as Database from './Database.ts'
import * as DemoModel from './DemoModel.ts'
import * as Greeting from './Greeting.ts'
import * as Uppercase from './Uppercase.ts'

export const sessionId = Identity.SessionId.make('example')

export const Configuration = Conversation.layerConfiguration({
  settings: {
    retry: { enabled: false },
    compaction: { enabled: false },
    progress: { partialIntervalMs: '0 millis', outputIntervalMs: '0 millis' },
  },
})

// Runner membership is local; messages and Activity replies remain in SQLite.
// Both the native engine and domain Store receive the same database Layer.
const Cluster = SingleRunner.layer({
  runnerStorage: 'memory',
  shardingConfig: {
    shardsPerGroup: 1,
    entityMessagePollInterval: '25 millis',
    entityReplyPollInterval: '25 millis',
  },
}).pipe(Layer.provideMerge(Database.layer))

const Engine = ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(Cluster))

const Creation = Conversation.layerCreation.pipe(Layer.provide(Configuration))

const Persistence = Layer.mergeAll(KeyValueStore.layerSql(), SqlEventJournal.layer())
const Storage = SnapshotStore.layer.pipe(Layer.provide(Persistence))
const SessionLive = Session.layer.pipe(Layer.provideMerge(Storage), Layer.provide(Creation))

const Directory = SessionDirectory.layerSingle(sessionId).pipe(Layer.provideMerge(SessionLive))

// The same registration graph works with application-supplied models and tools.
export const layerNoDeps = Layer.mergeAll(DurableExecutor.layer, Greeting.layer).pipe(
  Layer.provide(HarnessExecutor.layer),
  Layer.provideMerge(Directory),
  Layer.provide(Configuration),
  Layer.provideMerge(Engine),
)

const Catalogue = DemoModel.layerCatalogue.pipe(Layer.provide(DemoModel.layer))
const Tools = Uppercase.layerRegistry.pipe(Layer.provide(Uppercase.layerHandlers))

export const layer = layerNoDeps.pipe(Layer.provide(Layer.mergeAll(Catalogue, Tools)))
