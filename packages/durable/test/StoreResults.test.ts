import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Document from '@effect-harness/durable/Document'
import * as Record from '@effect-harness/durable/Record'
import * as Session from '@effect-harness/durable/Session'
import { Store } from '@effect-harness/durable/Store'
import { StorageError } from '@effect-harness/durable/StorageError'
import * as Memory from '@effect-harness/durable/storage/Memory'
import * as Sqlite from './storage/TestStore.ts'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import { sessionLayer } from '@effect-harness/durable/testing/Storage'
const token = Document.defineUnsafe({
  kind: 'ids',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ id: Record.TaskId, items: Schema.Array(Record.SubmissionId) }),
  initial: () => ({ id: Schema.decodeSync(Record.TaskId)(2), items: [] }),
})
for (const backend of [
  Memory.layer,
  Sqlite.layer.pipe(Layer.provide(SqliteClient.layer({ filename: ':memory:' }))),
]) {
  it.effect(
    `preserves arbitrary callback results, void receipt metadata and branded primitive drafts (${backend === Memory.layer ? 'memory' : 'sqlite'})`,
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
        for (const [key, invalid] of [
          ['invalid', { bad: () => true }],
          ['date', new Date()],
        ] as const) {
          // oxlint-disable-next-line effecttsgo/any-unknown-in-error-context -- Deliberate malformed JavaScript has unknown success; flipping it here asserts the exact typed Invalid rejection.
          const error = yield* runtimeReceipt((tx) => tx.ensureRoot.pipe(Effect.as(invalid)), {
            key,
          }).pipe(Effect.flip)
          assert.strictEqual(error._tag, 'StorageError')
          assert.strictEqual(error.reason._tag, 'Invalid')
          assert.deepStrictEqual(yield* store.read, before)
        }
      }).pipe(Effect.provide(sessionLayer(backend))),
  )
}
