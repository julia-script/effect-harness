import * as Layer from 'effect/Layer'
import * as Harness from 'effect-harness/Harness'
import * as Storage from 'effect-harness/Storage'
import * as DemoModel from './DemoModel.js'
import * as Uppercase from './Uppercase.js'

/** Only Storage remains for the application to choose. */
export const layer = Harness.layerLocal({ tools: Uppercase.tools }).pipe(
  Layer.provide(Uppercase.layer),
  Layer.provide(DemoModel.layer),
)
export const layerMemory = layer.pipe(Layer.provide(Storage.layerMemory))
