import { NodeRuntime } from '@effect/platform-node'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Storage from 'effect-harness/Storage'
import * as Testing from 'effect-harness/Testing'

// Replace layerMemory with your own scoped adapter Layer. Each case gets a fresh store.
// History is explicit: disabling it omits historical reconstruction and long-tail checks.
const cases = Testing.storageConformance({
  make: Effect.succeed({
    open: Layer.build(Storage.layerMemory).pipe(Effect.map((c) => Context.get(c, Storage.Storage))),
  }),
  capabilities: { history: true },
})
NodeRuntime.runMain(
  Effect.forEach(cases, (c) => c.run.pipe(Effect.tap(() => Effect.log(c.name))), { discard: true }),
)
