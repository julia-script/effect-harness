import { assert, describe, it } from '@effect/vitest'

import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'

import * as KeyValueStore from 'effect/persistence/KeyValueStore'

import * as Layer from 'effect/Layer'

import * as SnapshotStore from 'effect-harness/durable/storage/SnapshotStore'

import * as Record from 'effect-harness/durable/Record'

import * as TestStore from './TestStore.ts'

describe('SnapshotStoreSchema', () => {
  it.effect('validates saved snapshot counters and rejects malformed values', () =>
    Effect.gen(function* () {
      const values = yield* KeyValueStore.KeyValueStore
      const store = yield* SnapshotStore.make()
      const exhausted = {
        ...Record.emptyState(),
        nextId: Number.MAX_SAFE_INTEGER + 1,
        nextSeq: Number.MAX_SAFE_INTEGER + 1,
      }
      yield* KeyValueStore.toSchemaStore(values, SnapshotStore.Snapshot).set(
        '@effect-harness/durable/session',
        { version: 1, state: exhausted, frames: [] },
      )
      assert.strictEqual((yield* store.read).nextSeq, exhausted.nextSeq)
      yield* values.set('@effect-harness/durable/session', '{"version":1,"state":null,"frames":[]}')
      const error = yield* store.read.pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, 'CorruptError')
      assert.ok(error.cause instanceof Schema.SchemaError)
    }).pipe(
      Effect.provide(
        TestStore.persistence.pipe(Layer.provide(SqliteClient.layer({ filename: ':memory:' }))),
      ),
    ),
  )
})
