/**
 * The default loop: `HarnessServer.layer` with an extension, settings and the built-in tasks, over
 * only Storage, a Model and the extension's handlers. Leaving out a handler is a type error at the
 * provide site, which `tsc` and the `missing-layer-context` lint both report.
 */
import { describe, expect, test } from 'bun:test'
import { Context, Effect, Layer, Schema } from 'effect'
import { HarnessClient, HarnessTransport } from '../../client/index.ts'
import { Faux, Model } from '../../model/index.ts'
import * as Storage from '../../storage/Storage.ts'
import { Tool, Toolkit } from '../../tool/index.ts'
import { Extension, Extensions, HarnessServer } from '../index.ts'
import { runTest } from '../../test/harness.ts'

const Weather = Tool.make('weather', {
  description: 'The weather in a city.',
  parameters: Schema.Struct({ city: Schema.String }),
  success: Schema.Struct({ celsius: Schema.Finite }),
})
const toolkit = Toolkit.make(Weather)
const weather = Extension.make('weather', { toolkit })
const WeatherLive = toolkit.toLayer({ weather: () => Effect.succeed({ celsius: 21 }) })

/** The default loop with the weather extension, and a client, over one memory transport. */
const app = (faux: Faux.Faux, storage: Storage.Service) => {
  const loop = HarnessServer.layer({
    extensions: [weather],
    settings: { model: faux.ref(), retry: { baseDelayMs: 1, maxDelayMs: 1 } },
  }).pipe(
    Layer.provide([
      WeatherLive,
      Model.layer([faux.provider]),
      Layer.succeed(Storage.Storage, storage),
    ]),
  )
  return Layer.merge(loop, HarnessClient.layer).pipe(Layer.provide(HarnessTransport.layerMemory))
}

describe('HarnessServer.layer', () => {
  test('runs a tool round with only storage, a model and the handlers provided', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const faux = yield* Faux.make()
        yield* faux.setResponses([
          Faux.message(Faux.toolCall('weather', { city: 'Lisbon' }), { stopReason: 'toolUse' }),
          Faux.message('21 degrees.'),
        ])
        const context = yield* Layer.build(app(faux, storage))
        const installed = Context.get(context, Extensions.Extensions)
        expect((yield* installed.snapshot).map(({ extension }) => extension.name)).toEqual([
          'weather',
        ])
        const client = Context.get(context, HarnessClient.HarnessClient)
        const root = yield* client.root()
        const submission = yield* root.submit({ type: 'input', content: 'Weather in Lisbon?' })
        expect(yield* submission.wait).toMatchObject({ status: 'done' })
        const { items } = yield* root.entries({ order: 'ascending' }, 50)
        expect(items.map((entry) => entry.kind)).toEqual([
          '$user',
          '$system',
          '$assistant',
          '$tool-result',
          '$assistant',
        ])
      }),
    ))
})
