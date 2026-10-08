import type * as Schema from 'effect/Schema'
import * as Option from 'effect/Option'

import { assert, describe, it } from '@effect/vitest'

import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'

import * as Effect from 'effect/Effect'

import * as KeyValueStore from 'effect/persistence/KeyValueStore'

import * as Layer from 'effect/Layer'

import * as SnapshotStore from 'effect-harness/durable/storage/SnapshotStore'

import * as Store from 'effect-harness/durable/Store'

import * as TestStore from './TestStore.ts'

describe('SnapshotStoreSerialization', () => {
  it.effect('preserves receipt strings and state/frames through schema serialization', () =>
    Effect.gen(function* () {
      const store = yield* TestStore.make
      const result: Schema.JsonObject = { z: ['first', 1], a: { text: 'unicode \ud800' } }
      Object.defineProperty(result, '__proto__', { value: 'own', enumerable: true })
      const key = 'key:\ud800'
      const fingerprint = 'fingerprint:"\\'
      yield* store.transact(
        (state) => Effect.succeed(Store.makeCandidate({ state, writes: [], result })),
        {
          key,
          fingerprint,
        },
      )
      const state = yield* store.read
      const frame = (yield* store.journal(0)).frames[0]
      assert.ok(frame)
      const values = yield* KeyValueStore.KeyValueStore
      const saved = yield* KeyValueStore.toSchemaStore(values, SnapshotStore.Snapshot).get(
        '@effect-harness/durable/session',
      )
      assert.deepStrictEqual(Option.getOrUndefined(saved), { version: 1, state, frames: [frame] })
      const reopened = yield* TestStore.make
      assert.deepStrictEqual(
        yield* reopened.transact(() => Effect.die('receipt replay callback'), {
          key,
          fingerprint,
        }),
        result,
      )
    }).pipe(
      Effect.provide(
        TestStore.persistence.pipe(
          Layer.provideMerge(SqliteClient.layer({ filename: ':memory:' })),
        ),
      ),
    ),
  )
})
