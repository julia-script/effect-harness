import { NodeRuntime } from '@effect/platform-node'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Storage from 'effect-harness/Storage'
import * as Testing from 'effect-harness/Testing'

// Run after building: node apps/example/dist/tour/StorageConformance.js
// Supply your own scoped adapter layer here. No test runner is needed.
const cases = Testing.storageConformance({
  make: Effect.succeed({
    open: Layer.build(Storage.layerMemory).pipe(
      Effect.map((context) => Context.get(context, Storage.Storage)),
    ),
  }),
  capabilities: { history: true, reopen: false },
})

NodeRuntime.runMain(
  Effect.gen(function* () {
    for (const test of cases) {
      yield* test.run
      yield* Effect.log(test.name)
    }
  }),
)
