import * as NodeServices from '@effect/platform-node/NodeServices'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as SnapshotStore from './TestStore.ts'
import { Store } from 'effect-harness/durable/Store'
import { RestartWorker } from '../restart/RestartWorker.ts'

const Worker = RestartWorker.layer({
  fixture: new URL('../restart/SnapshotStoreFixture.ts', import.meta.url).pathname,
  databaseEnv: 'SQL_STORE_DB',
  phaseEnv: 'SQL_STORE_PHASE',
}).pipe(Layer.provide(NodeServices.layer))

it.live(
  'recovers a killed Activity after its domain commit using Effect key/value storage and native Workflow',
  () =>
    Effect.gen(function* () {
      const worker = yield* RestartWorker
      const first = yield* worker.spawn('start')
      assert.strictEqual(
        Option.getOrUndefined(yield* worker.marker(first, 'SQL_STORE_READY:')),
        'SQL_STORE_READY:committed',
      )
      yield* first.kill({ killSignal: 'SIGKILL' })
      assert.strictEqual((yield* Effect.result(first.exitCode))._tag, 'Failure')

      const inspect = Effect.flatMap(Store, (store) => store.committed).pipe(
        Effect.provide(
          SnapshotStore.layer.pipe(
            Layer.provide(SqliteClient.layer({ filename: worker.filename })),
          ),
        ),
      )
      const before = yield* inspect
      assert.strictEqual(before.nextSeq, 3)
      assert.deepStrictEqual(
        before.receipts.map((receipt) => receipt.key),
        ['first', 'second'],
      )

      for (const phase of ['resume', 'verify']) {
        const process = yield* worker.spawn(phase)
        assert.strictEqual(
          Option.getOrUndefined(yield* worker.marker(process, 'SQL_STORE_DONE:')),
          'SQL_STORE_DONE:2:3:2',
        )
        assert.strictEqual(yield* process.exitCode, 0)
      }
      const after = yield* inspect
      assert.strictEqual(after.nextSeq, 3)
      assert.deepStrictEqual(
        after.receipts.map((receipt) => receipt.key),
        ['first', 'second'],
      )
    }).pipe(Effect.provide(Worker)),
)
