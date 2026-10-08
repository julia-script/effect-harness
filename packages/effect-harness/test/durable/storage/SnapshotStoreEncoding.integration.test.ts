import * as SchemaIssue from 'effect/SchemaIssue'

import { assert, describe, it } from '@effect/vitest'

import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'

import * as SchemaGetter from 'effect/SchemaGetter'

import * as KeyValueStore from 'effect/persistence/KeyValueStore'

import * as Layer from 'effect/Layer'

import * as SnapshotStore from 'effect-harness/durable/storage/SnapshotStore'

import * as Document from 'effect-harness/durable/Document'

import * as Session from 'effect-harness/durable/Session'

import * as Store from 'effect-harness/durable/Store'

import * as TestStore from './TestStore.ts'

describe('SnapshotStoreEncoding', () => {
  it.effect(
    'rejects a schema encoding failure before any key/value write and retains its cause',
    () =>
      Effect.gen(function* () {
        const values = yield* KeyValueStore.KeyValueStore
        let mutations = 0
        const wrapped = KeyValueStore.make({
          ...values,
          set: (key, value) => {
            mutations++
            return values.set(key, value)
          },
        })
        const store = yield* SnapshotStore.make().pipe(
          Effect.provideService(KeyValueStore.KeyValueStore, wrapped),
        )
        mutations = 0
        const shape = Schema.Struct({ value: Schema.String })
        const rejectedCodec = shape.pipe(
          Schema.decodeTo(shape, {
            decode: SchemaGetter.passthrough(),
            encode: SchemaGetter.transformEffect(() =>
              Effect.fail(new SchemaIssue.InvalidValue({ message: 'encoder rejected' })),
            ),
          }),
        )
        const token = Document.defineUnsafe({
          kind: 'encoding-failure',
          version: 1,
          scope: 'session',
          schema: rejectedCodec,
          initial: () => ({ value: 'decode succeeds' }),
        })
        const session = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
        const before = yield* store.read
        const error = yield* session
          .transaction((tx) => tx.doc(token).pipe(Effect.asVoid))
          .pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, 'InvalidError')
        assert.strictEqual(error.certainty, 'rejected')
        assert.ok(error.cause instanceof Schema.SchemaError)
        assert.strictEqual(mutations, 0)
        assert.deepStrictEqual(yield* store.read, before)
      }).pipe(
        Effect.provide(
          TestStore.persistence.pipe(
            Layer.provideMerge(SqliteClient.layer({ filename: ':memory:' })),
          ),
        ),
      ),
  )
})
