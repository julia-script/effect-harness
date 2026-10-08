import { assert, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import type * as Layer from 'effect/Layer'

import * as Schema from 'effect/Schema'

import * as Document from 'effect-harness/durable/Document'

import * as Record from 'effect-harness/durable/Record'

import * as Session from 'effect-harness/durable/Session'

import { Store } from 'effect-harness/durable/Store'

import { type StorageError } from 'effect-harness/durable/StorageError'

import * as StoreModule from 'effect-harness/durable/Store'

import { sessionLayer } from 'effect-harness/durable/testing/Storage'

import type * as SqlError from 'effect/sql/SqlError'

const token = Document.defineUnsafe({
  kind: 'ids',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ id: Record.TaskId, items: Schema.Array(Record.SubmissionId) }),
  initial: () => ({ id: Schema.decodeSync(Record.TaskId)(2), items: [] }),
})
export const cases = (backend: Layer.Layer<Store, StorageError | SqlError.SqlError>) => {
  it.effect(
    `preserves arbitrary callback results, void receipt metadata and branded primitive drafts (${backend === StoreModule.layerMemory ? 'memory' : 'sqlite'})`,
    () =>
      Effect.gen(function* () {
        const session = yield* Session.Session
        const store = yield* Store
        assert.strictEqual(
          yield* session.transaction((tx) => tx.ensureRoot.pipe(Effect.asVoid)),
          undefined,
        )
        const arbitrary = { service: store, date: new Date(), callback: () => true }
        assert.strictEqual(yield* session.transaction(() => Effect.succeed(arbitrary)), arbitrary)
        let calls = 0
        const voidReceipt = () =>
          session.transaction(
            Effect.fnUntraced(function* (tx) {
              calls++
              yield* tx.doc(token)
              return undefined
            }),
            { key: 'void' },
          )
        assert.strictEqual(yield* voidReceipt(), undefined)
        assert.strictEqual(yield* voidReceipt(), undefined)
        assert.strictEqual(calls, 1)
        assert.strictEqual((yield* store.read).receipts[0]?.resultIsVoid, true)
        assert.strictEqual(
          yield* session.transaction(() => Effect.succeed(null), { key: 'null' }),
          null,
        )
        assert.strictEqual(
          yield* session.transaction(() => Effect.die('unexpected replay'), { key: 'null' }),
          null,
        )
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const d = yield* tx.doc(token)
            const taskId: Record.TaskId = d.id
            d.id = taskId
            const id = yield* tx.mint(Record.SubmissionId)
            d.items.push(id)
            const copied: Record.SubmissionId | undefined = d.items[0]
            assert.strictEqual(copied, id)
          }),
        )
        const before = yield* store.read
        // Deliberate JavaScript misuse enters through this isolated unknown boundary; normal APIs remain strict.
        const runtimeReceipt = session.transaction as unknown as (
          change: (tx: Session.Transaction) => Effect.Effect<unknown, StorageError>,
          options: { readonly key: string },
        ) => Effect.Effect<unknown, StorageError>
        const namedArray: Array<Schema.Json> = [1]
        Object.defineProperty(namedArray, 'note', { value: 'lost by JSON', enumerable: true })
        const noncanonicalArray: Array<Schema.Json> = [1]
        Object.defineProperty(noncanonicalArray, '01', { value: 2, enumerable: true })
        for (const [key, invalid] of [
          ['invalid', { bad: () => true }],
          ['date', new Date()],
          ['named-array', namedArray],
          ['noncanonical-array', noncanonicalArray],
        ] as const) {
          // oxlint-disable-next-line effecttsgo/any-unknown-in-error-context -- Deliberate malformed JavaScript has unknown success; flipping it here asserts the exact typed Invalid rejection.
          const error = yield* runtimeReceipt((tx) => tx.ensureRoot.pipe(Effect.as(invalid)), {
            key,
          }).pipe(Effect.flip)
          assert.strictEqual(error._tag, 'StorageError')
          assert.strictEqual(error.reason._tag, 'InvalidError')
          assert.deepStrictEqual(yield* store.read, before)
        }
      }).pipe(Effect.provide(sessionLayer(backend))),
  )
}
