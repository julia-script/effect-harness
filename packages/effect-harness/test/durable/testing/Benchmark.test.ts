import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Session from 'effect-harness/durable/Session'
import { Store } from 'effect-harness/durable/Store'
import * as Memory from 'effect-harness/durable/storage/Memory'
import * as Benchmark from 'effect-harness/durable/testing/Benchmark'
import { sessionLayer } from 'effect-harness/durable/testing/Storage'

describe('Benchmark', () => {
  it.effect(
    'benchmark fixtures verify every read and write scenario through public Effect services',
    () =>
      Effect.gen(function* () {
        const scale = { name: 'test', entryCount: 100, taskCount: 61, documentCount: 2 }
        const dataset = yield* Benchmark.seedStorageBenchmark(scale)
        for (const benchmark of Benchmark.STORAGE_READ_BENCHMARKS) {
          assert.strictEqual(
            yield* benchmark.run(dataset),
            benchmark.expected(dataset),
            benchmark.name,
          )
        }
        const state = yield* Store.use((store) => store.read)
        assert.strictEqual(
          state.conversations.length +
            state.entries.length +
            state.tasks.length +
            state.documents.length,
          Benchmark.storageBenchmarkPrimaryRecordCount(scale),
        )
        for (const benchmark of Benchmark.STORAGE_WRITE_BENCHMARKS) {
          assert.strictEqual(yield* benchmark.run, benchmark.expected, benchmark.name)
        }
        assert.ok(
          (yield* Session.Session.use((session) => session.scanSubmissions({}, 10))).items.length >
            0,
        )
      }).pipe(Effect.provide(sessionLayer(Memory.layer))),
    30000,
  )
})
