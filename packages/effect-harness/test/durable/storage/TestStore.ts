import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as KeyValueStore from 'effect/persistence/KeyValueStore'
import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import * as SnapshotStore from 'effect-harness/durable/storage/SnapshotStore'
import { StorageError, rejected, Io } from 'effect-harness/durable/StorageError'

export const persistence = Layer.mergeAll(KeyValueStore.layerSql(), SqlEventJournal.layer())
export const make = SnapshotStore.make().pipe(
  Effect.provide(persistence),
  Effect.mapError((cause) =>
    cause instanceof StorageError ? cause : rejected('Cannot acquire persistence', Io, cause),
  ),
)
export const layer = SnapshotStore.layer.pipe(Layer.provide(persistence))
