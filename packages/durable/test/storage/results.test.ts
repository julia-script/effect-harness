import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Document from '../../src/Document.ts'
import * as Record from '../../src/Record.ts'
import * as Session from '../../src/Session.ts'
import { Store } from '../../src/Store.ts'
import { StorageError } from '../../src/StorageError.ts'
import * as Memory from '../../src/storage/Memory.ts'
import * as Sqlite from '../../src/storage/Sqlite.ts'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import { sessionLayer } from '../../src/testing/Storage.ts'
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
      Effect.scoped(
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
          const invalid = { bad: () => true }
          const rejected: Effect.Effect<unknown, StorageError> = session.transaction(
            // @ts-expect-error Receipt results must be JSON-safe or void; runtime also rejects JavaScript misuse.
            (tx) => tx.ensureRoot.pipe(Effect.as(invalid)),
            { key: 'invalid' },
          )
          const error = yield* rejected.pipe(Effect.flip, Effect.orDie)
          assert.ok(error instanceof StorageError)
          assert.strictEqual(error.reason._tag, 'Invalid')
          assert.deepStrictEqual(yield* store.read, before)
          const classReceipt: Effect.Effect<unknown, StorageError> = session.transaction(
            // @ts-expect-error Class instances cannot satisfy the JSON receipt result contract.
            () => Effect.succeed(new Date()),
            { key: 'date' },
          )
          assert.strictEqual(
            (yield* classReceipt.pipe(Effect.flip, Effect.orDie)).reason._tag,
            'Invalid',
          )
        }).pipe(Effect.provide(sessionLayer(backend))),
      ),
  )
}
