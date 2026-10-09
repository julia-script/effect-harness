import { NodeRuntime } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as Application from './Application.js'
import * as Forks from './tour/Forks.js'
import * as Documents from './tour/Documents.js'
import * as Extensions from './tour/Extensions.js'

const program = Effect.gen(function* () {
  yield* Effect.log('forks', yield* Forks.program.pipe(Effect.provide(Application.layerMemory)))
  yield* Effect.log('documents', yield* Documents.program.pipe(Effect.provide(Documents.layer)))
  yield* Effect.log('extensions', yield* Extensions.program.pipe(Effect.provide(Extensions.layer)))
})
NodeRuntime.runMain(program)
