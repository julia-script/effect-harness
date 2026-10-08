import * as Serialization from 'effect-harness/durable/Serialization'
import { assertSome } from '@effect/vitest/utils'
import * as TestSchema from 'effect/testing/TestSchema'
import * as Option from 'effect/Option'

import { assert, describe, it } from '@effect/vitest'

import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'

import * as KeyValueStore from 'effect/persistence/KeyValueStore'

import * as Layer from 'effect/Layer'

import * as Document from 'effect-harness/durable/Document'

import * as Session from 'effect-harness/durable/Session'

import * as Store from 'effect-harness/durable/Store'

import * as TestStore from './storage/TestStore.ts'

describe('DocumentEncoding', () => {
  it.effect(
    'omits optional undefined at the schema storage boundary and reopens the decoded document',
    () =>
      Effect.gen(function* () {
        const store = yield* TestStore.make
        const schema = Schema.Struct({ value: Schema.String, note: Schema.optional(Schema.String) })
        const codec = Serialization.object(schema)
        const assertions = new TestSchema.Asserts(codec)
        yield* assertions
          .encoding()
          .succeedEffect({ value: 'kept', note: undefined }, { value: 'kept' })
        yield* assertions.decoding().succeedEffect({ value: 'kept' }, { value: 'kept' })
        const token = Document.defineUnsafe({
          kind: 'undefined-friendly',
          version: 1,
          scope: 'session',
          schema: codec,
          initial: () => ({ value: 'kept', note: undefined }),
        })
        const session = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
        yield* session.transaction((tx) => tx.doc(token).pipe(Effect.asVoid))
        const encoded = yield* Document.encode(token, { value: 'kept', note: undefined })
        assert.deepStrictEqual(encoded, { value: 'kept' })
        const values = yield* KeyValueStore.KeyValueStore
        assert.isFalse((yield* values.get('@effect-harness/durable/session'))?.includes('null'))
        const reopened = yield* TestStore.make
        const next = yield* Session.make.pipe(Effect.provideService(Store.Store, reopened))
        const snapshot = yield* next.snapshot(token)
        assertSome(
          Option.map(snapshot, (snapshot) => snapshot.value),
          { value: 'kept' },
        )
        const copied: { value: string; note?: string | undefined } = Document.copyUnsafe({
          value: 'kept',
          note: undefined,
        })
        assert.isTrue(Object.hasOwn(copied, 'note'))
      }).pipe(
        Effect.provide(
          TestStore.persistence.pipe(
            Layer.provideMerge(SqliteClient.layer({ filename: ':memory:' })),
          ),
        ),
      ),
  )
})
