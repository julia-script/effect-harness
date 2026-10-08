import * as Harness from 'effect-harness/Harness'
import * as Executor from 'effect-harness/Executor'
import * as Layer from 'effect/Layer'
import * as Effect from 'effect/Effect'
import * as Database from './Database.ts'
import * as DemoModel from './DemoModel.ts'
import * as Greeting from './Greeting.ts'
import * as Uppercase from './Uppercase.ts'

/** Native model and registry services are supplied by the application. */
export const layerWith = (options: Harness.Options = {}) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const greeting = yield* Greeting.make
      return Harness.layer({
        agent: { model: DemoModel.ref },
        settings: {
          retry: { enabled: false },
          compaction: { enabled: false },
          progress: { partialInterval: '0 millis', outputInterval: '0 millis' },
        },
        ...options,
        tasks: [greeting, ...(options.tasks ?? [])],
      }).pipe(Layer.provide(Database.layer), Layer.provide(Executor.layer))
    }),
  )

export const layerNoDeps = layerWith()
const catalogue = DemoModel.layerCatalogue.pipe(Layer.provide(DemoModel.layer))
const tools = Uppercase.layerRegistry.pipe(Layer.provide(Uppercase.layerHandlers))
export const layer = layerNoDeps.pipe(Layer.provide(Layer.merge(catalogue, tools)))
