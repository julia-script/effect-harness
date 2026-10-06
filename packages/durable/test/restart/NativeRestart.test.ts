import { assert, describe, it } from '@effect/vitest'
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Path from 'effect/Path'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Stream from 'effect/Stream'

const marker = (handle: ChildProcessSpawner.ChildProcessHandle, prefix: string) =>
  handle.stdout.pipe(
    Stream.decodeText,
    Stream.splitLines,
    Stream.filter((line) => line.startsWith(prefix)),
    Stream.runHead,
    Effect.timeout('15 seconds'),
  )

describe('native SQL Workflow persistence', () => {
  for (const faultWindow of [false, true])
    it.live(
      `restarts after SIGKILL ${faultWindow ? 'inside a transaction after a SQL-reading workflow prefix' : 'without repeating a completed activity'}`,
      () =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
          const directory = yield* fs.makeTempDirectoryScoped()
          const filename = path.join(directory, 'workflow.sqlite')
          const fixture = new URL('./fixture.ts', import.meta.url).pathname
          const command = (phase: string) =>
            ChildProcess.make('bun', [fixture], {
              env: {
                DURABLE_TEST_DB: filename,
                DURABLE_TEST_PHASE: phase,
                DURABLE_TEST_WINDOW: String(faultWindow),
              },
              extendEnv: true,
              stdin: 'ignore',
              stderr: 'inherit',
            })
          const first = yield* spawner.spawn(command('start'))
          const ready = yield* marker(first, 'PROBE_READY:')
          assert.strictEqual(ready._tag, 'Some')
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
          const second = yield* spawner.spawn(command('resume'))
          const done = yield* marker(second, 'PROBE_DONE:')
          assert.strictEqual(done._tag, 'Some')
          if (done._tag === 'Some')
            assert.strictEqual(done.value, 'PROBE_DONE:{"cachedCount":1,"committedCount":1}')
          assert.strictEqual(yield* second.exitCode, 0)
        }).pipe(Effect.provide(NodeServices.layer)),
      30000,
    )
})
