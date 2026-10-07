import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as BunRuntime from '@effect/platform-bun/BunRuntime'
import * as SqliteClient from '@effect/sql-sqlite-bun/SqliteClient'
import * as ClusterSchema from 'effect/cluster/ClusterSchema'
import * as ClusterWorkflowEngine from 'effect/cluster/ClusterWorkflowEngine'
import * as SingleRunner from 'effect/cluster/SingleRunner'
import * as Config from 'effect/Config'
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Activity from 'effect/workflow/Activity'
import * as DurableDeferred from 'effect/workflow/DurableDeferred'
import { Probe, Resume } from './Probe.ts'

const executor = Probe.toLayer(() =>
  Effect.gen(function* () {
    const phase = yield* Config.String('DURABLE_TEST_PHASE').pipe(Effect.orDie)
    const faultWindow = yield* Config.String('DURABLE_TEST_WINDOW').pipe(
      Config.withDefault('false'),
      Effect.orDie,
    )
    if (faultWindow === 'true') {
      const sql = yield* SqlClient.SqlClient
      if (phase !== 'start') yield* Effect.sleep('100 millis')
      yield* sql.withTransaction(sql`SELECT 1`).pipe(Effect.orDie)
    }
    const count = yield* Activity.make({
      name: 'domain-commit',
      success: Schema.Int,
      error: Schema.String,
      execute: Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        yield* sql`CREATE TABLE IF NOT EXISTS restart_probe (id INTEGER PRIMARY KEY CHECK (id = 1), count INTEGER NOT NULL)`
        yield* sql`INSERT OR IGNORE INTO restart_probe (id, count) VALUES (1, 0)`
        yield* sql`UPDATE restart_probe SET count = count + 1 WHERE id = 1`
        const rows = yield* sql<{ count: number }>`SELECT count FROM restart_probe WHERE id = 1`
        if (faultWindow === 'true' && phase === 'start') {
          yield* Console.log('PROBE_READY:uncommitted')
          return yield* Effect.never
        }
        return rows[0]?.count ?? 0
      }).pipe(Effect.mapError((error) => error.message)),
    }).annotate(ClusterSchema.WithTransaction, true)
    yield* Console.log(`PROBE_READY:${count}`)
    yield* DurableDeferred.await(Resume)
    return count
  }),
)

const program = Effect.gen(function* () {
  const filename = yield* Config.String('DURABLE_TEST_DB')
  const phase = yield* Config.String('DURABLE_TEST_PHASE')
  const database = SqliteClient.layer({ filename })
  const cluster = SingleRunner.layer({
    runnerStorage: 'memory',
    shardingConfig: {
      shardsPerGroup: 1,
      entityMessagePollInterval: '25 millis',
      entityReplyPollInterval: '25 millis',
    },
  }).pipe(Layer.provideMerge(database), Layer.provide(BunCrypto.layer))
  const runtime = executor.pipe(
    Layer.provideMerge(ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(cluster))),
  )
  yield* Effect.gen(function* () {
    const payload = { key: 'persistent' }
    yield* Probe.execute(payload, { discard: true })
    if (phase === 'start') return yield* Effect.never
    const executionId = yield* Probe.executionId(payload)
    yield* DurableDeferred.succeed(Resume, {
      token: DurableDeferred.tokenFromExecutionId(Resume, { workflow: Probe, executionId }),
      value: undefined,
    })
    const cachedCount = yield* Probe.execute(payload)
    const sql = yield* SqlClient.SqlClient
    const rows = yield* sql<{ count: number }>`SELECT count FROM restart_probe WHERE id = 1`
    yield* Console.log(
      `PROBE_DONE:${JSON.stringify({ cachedCount, committedCount: rows[0]?.count })}`,
    )
  }).pipe(Effect.provide(runtime))
})

BunRuntime.runMain(program)
