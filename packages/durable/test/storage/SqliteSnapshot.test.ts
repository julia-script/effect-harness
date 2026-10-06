import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Record from '@effect-harness/durable/Record'
import * as Sqlite from '@effect-harness/durable/storage/Sqlite'

describe('Sqlite.load', () => {
  it.effect('keeps state, journal and receipts coherent across an admitted concurrent commit', () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      const entered = yield* Deferred.make<boolean>()
      const release = yield* Deferred.make<void>()
      let gate = false
      // Intercept only scheduling after the actual state SELECT; all SQL and persisted data
      // remain native. An inherited lease blocks the competing writer, so release that
      // reader first. Without a lease, finish the writer before the remaining SELECTs.
      const wrapped = new Proxy(sql, {
        apply(target, self, args) {
          const statement = Reflect.apply(target, self, args)
          if (!gate || !String(args[0]).includes('SELECT state,')) return statement
          gate = false
          return statement.pipe(
            Effect.tap(() =>
              Effect.gen(function* () {
                const context = yield* Effect.context<never>()
                yield* Deferred.succeed(
                  entered,
                  Option.isSome(Context.getOption(context, sql.transactionService)),
                )
                yield* Deferred.await(release)
              }),
            ),
          )
        },
      })
      const store = yield* Sqlite.make().pipe(Effect.provideService(SqlClient.SqlClient, wrapped))
      gate = true
      const reading = yield* store.read.pipe(Effect.forkScoped)
      const leased = yield* Deferred.await(entered)
      const writing = yield* store
        .commit([{ type: 'conversation', value: { id: Record.ROOT_CONVERSATION_ID } }], {
          key: 'new-receipt',
        })
        .pipe(Effect.forkScoped)
      if (!leased) yield* Fiber.join(writing)
      yield* Deferred.succeed(release, undefined)
      const snapshot = yield* Fiber.join(reading)
      assert.strictEqual(snapshot.nextSeq, 1)
      assert.deepStrictEqual(snapshot.receipts, [])
      yield* Fiber.join(writing)
      const committed = yield* store.committed
      assert.strictEqual(committed.nextSeq, 2)
      assert.deepStrictEqual(
        committed.receipts.map((receipt) => receipt.key),
        ['new-receipt'],
      )
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
  )

  it.effect('preserves native transaction-local preview and excludes rolled-back receipts', () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      const store = yield* Sqlite.make()
      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* store.commit(
              [{ type: 'conversation', value: { id: Record.ROOT_CONVERSATION_ID } }],
              { key: 'preview' },
            )
            assert.deepStrictEqual(
              (yield* store.read).receipts.map((receipt) => receipt.key),
              ['preview'],
            )
            return yield* Effect.fail('rollback')
          }),
        )
        .pipe(Effect.flip)
      assert.deepStrictEqual((yield* store.committed).receipts, [])
      assert.strictEqual((yield* store.read).nextSeq, 1)
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
  )
})
