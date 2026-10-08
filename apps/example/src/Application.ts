import {
  Conversation,
  Executor as DurableExecutor,
  Identity,
  Session,
  SessionDirectory,
  type Ownership,
  type Store,
  type StorageError,
} from 'effect-harness/durable'
import type { Cancellation, Structured } from 'effect-harness/durable/workflow'
import { SnapshotStore } from 'effect-harness/durable/storage'
import { Executor as HarnessExecutor, type Model, type Registry } from 'effect-harness'
import {
  Layer,
  type Config,
  type Crypto,
  type FileSystem,
  type Path,
  type PlatformError,
  type Schema,
} from 'effect'
import { KeyValueStore } from 'effect/persistence'
import { SqlEventJournal } from 'effect/eventlog'
import {
  ClusterWorkflowEngine,
  SingleRunner,
  type MessageStorage,
  type Runners,
  type Sharding,
} from 'effect/cluster'
import type { SqlClient, SqlError } from 'effect/sql'
import type { WorkflowEngine } from 'effect/workflow'
import type { SqliteClient } from '@effect/sql-sqlite-bun'
import * as Database from './Database.ts'
import * as DemoModel from './DemoModel.ts'
import * as Greeting from './Greeting.ts'
import * as Uppercase from './Uppercase.ts'

export const sessionId = Identity.SessionId.make('example')

// effect-nit-allow P3-layer-naming-layer-prefix: CreationLayer is application wiring that combines Configuration and CreationHook; the suffix distinguishes the composition from a service-module layer export.
export const CreationLayer: Layer.Layer<
  Conversation.Configuration | Session.CreationHook,
  Schema.SchemaError,
  Crypto.Crypto
> = Conversation.layer({
  settings: {
    retry: { enabled: false },
    compaction: { enabled: false },
    progress: { partialInterval: '0 millis', outputInterval: '0 millis' },
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

const Persistence = Layer.mergeAll(KeyValueStore.layerSql(), SqlEventJournal.layer())
const Storage = SnapshotStore.layer.pipe(Layer.provide(Persistence))
// effect-nit-allow P3-layer-naming-layer-prefix: SessionLayer is application wiring that retains the same shared storage, configuration and creation-hook instances.
const SessionLayer = Session.layer.pipe(
  Layer.provideMerge(Storage),
  Layer.provideMerge(CreationLayer),
)

const Directory = SessionDirectory.layerSingle(sessionId).pipe(Layer.provideMerge(SessionLayer))

// The same registration graph works with application-supplied models and tools.
export const layerNoDeps: Layer.Layer<
  | Cancellation.Cancellation
  | Conversation.Configuration
  | Session.CreationHook
  | Ownership.Declarations
  | Structured.DrainConversations
  | MessageStorage.MessageStorage
  | Runners.Runners
  | Session.Session
  | SessionDirectory.SessionDirectory
  | Sharding.Sharding
  | SqlClient.SqlClient
  | SqliteClient.SqliteClient
  | Store.Store
  | WorkflowEngine.WorkflowEngine,
  | Config.ConfigError
  | Database.ConfigurationError
  | PlatformError.PlatformError
  | Schema.SchemaError
  | SqlError.SqlError
  | StorageError.StorageError,
  Model.Catalog | Crypto.Crypto | FileSystem.FileSystem | Path.Path | Registry.Registry
> = Layer.mergeAll(DurableExecutor.layer, Greeting.layer).pipe(
  Layer.provide(HarnessExecutor.layer),
  Layer.provideMerge(Directory),
  Layer.provideMerge(Engine),
)

const Catalogue = DemoModel.layerCatalogue.pipe(Layer.provide(DemoModel.layer))
const Tools = Uppercase.layerRegistry.pipe(Layer.provide(Uppercase.layerHandlers))

export const layer: Layer.Layer<
  | Cancellation.Cancellation
  | Conversation.Configuration
  | Session.CreationHook
  | Ownership.Declarations
  | Structured.DrainConversations
  | MessageStorage.MessageStorage
  | Runners.Runners
  | Session.Session
  | SessionDirectory.SessionDirectory
  | Sharding.Sharding
  | SqlClient.SqlClient
  | SqliteClient.SqliteClient
  | Store.Store
  | WorkflowEngine.WorkflowEngine,
  | Config.ConfigError
  | Database.ConfigurationError
  | PlatformError.PlatformError
  | Schema.SchemaError
  | SqlError.SqlError
  | StorageError.StorageError
  | Registry.RegistryError,
  Crypto.Crypto | FileSystem.FileSystem | Path.Path
> = layerNoDeps.pipe(Layer.provide(Layer.mergeAll(Catalogue, Tools)))
