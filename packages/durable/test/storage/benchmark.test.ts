import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Session from '../../src/Session.ts'
import { Store } from '../../src/Store.ts'
import * as Memory from '../../src/storage/Memory.ts'
import * as Benchmark from '../../src/testing/Benchmark.ts'
import { sessionLayer } from '../../src/testing/Storage.ts'

it.effect(
  'benchmark fixtures verify every read and write scenario through public Effect services',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const scale = { name: 'test', entryCount: 100, taskCount: 61, documentCount: 2 }
        const dataset = yield* Benchmark.seedStorageBenchmark(scale)
        for (const benchmark of Benchmark.STORAGE_READ_BENCHMARKS)
          assert.strictEqual(
            yield* benchmark.run(dataset),
            benchmark.expected(dataset),
            benchmark.name,
          )
        const state = yield* Store.use((store) => store.read)
        assert.strictEqual(
          state.conversations.length +
            state.entries.length +
            state.tasks.length +
            state.documents.length,
          Benchmark.storageBenchmarkPrimaryRecordCount(scale),
        )
        for (const benchmark of Benchmark.STORAGE_WRITE_BENCHMARKS)
          assert.strictEqual(yield* benchmark.run, benchmark.expected, benchmark.name)
        assert.ok(
          (yield* Session.Session.use((session) => session.scanSubmissions({}, 10))).items.length >
            0,
        )
      }).pipe(Effect.provide(sessionLayer(Memory.layer))),
    ),
  30000,
)
