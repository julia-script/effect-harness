import * as Option from 'effect/Option'
import * as Layer from 'effect/Layer'
import { RestartWorker } from '../restart/RestartWorker.ts'
import { assert, describe, it } from '@effect/vitest'
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Effect from 'effect/Effect'
import * as SqlClient from 'effect/sql/SqlClient'

describe('GenerationRestart', () => {
  for (const faultWindow of [false, true])
    // SIGKILL, child stdout and independent SQL COMMIT progress use host processes outside TestClock.
    it.live(
      `restarts after SIGKILL ${faultWindow ? 'inside a transaction after a SQL-reading workflow prefix' : 'without repeating a completed activity'}`,
      () =>
        Effect.gen(function* () {
          const worker = yield* RestartWorker
          const filename = worker.filename
          const first = yield* worker.spawn('start')
          const ready = yield* worker.marker(first, 'PROBE_READY:')
          assert.deepStrictEqual(
            ready,
            Option.some(faultWindow ? 'PROBE_READY:uncommitted' : 'PROBE_READY:1'),
          )
          // An independent writable transaction waits for the child commit, closing
          // the native reply notification vs. physical COMMIT timing gap.
          if (!faultWindow) {
            const committed = yield* Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient
              return yield* sql.withTransaction(
                sql<{ count: number }>`SELECT count FROM restart_probe WHERE id = 1`,
              )
            }).pipe(Effect.provide(SqliteClient.layer({ filename })))
            assert.strictEqual(committed[0]?.count, 1)
          }
          yield* first.kill({ killSignal: 'SIGKILL' })
          const killed = yield* Effect.result(first.exitCode)
          assert.strictEqual(killed._tag, 'Failure')
          assert.strictEqual(yield* first.isRunning, false)
          if (faultWindow) {
            const tables = yield* Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient
              return yield* sql.withTransaction(
                sql<{
                  name: string
                }>`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'restart_probe'`,
              )
            }).pipe(Effect.provide(SqliteClient.layer({ filename })))
            assert.deepStrictEqual(tables, [])
          }
          const second = yield* worker.spawn('resume')
          const done = yield* worker.marker(second, 'PROBE_DONE:')
          assert.deepStrictEqual(
            done,
            Option.some('PROBE_DONE:{"cachedCount":1,"committedCount":1}'),
          )
          assert.strictEqual(yield* second.exitCode, 0)
        }).pipe(
          Effect.provide(
            RestartWorker.layer({
              fixture: new URL('../restart/fixture.ts', import.meta.url).pathname,
              databaseEnv: 'DURABLE_TEST_DB',
              phaseEnv: 'DURABLE_TEST_PHASE',
              extraEnv: { DURABLE_TEST_WINDOW: String(faultWindow) },
            }).pipe(Layer.provide(NodeServices.layer)),
          ),
        ),
      30000,
    )
})
