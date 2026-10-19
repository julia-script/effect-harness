import * as Effect from 'effect/Effect'
import * as Images from 'effect-harness/images-bun'
import * as Checks from 'effect-harness/testing/Images'
import { imageFixture } from './Native.js'

const cases = Checks.imagesConformance({
  make: imageFixture(Images.layer),
  capabilities: { orientations: true },
})
await Effect.runPromise(
  Effect.forEach(cases, (c) => c.run.pipe(Effect.tap(() => Effect.log(c.name))), { discard: true }),
)
