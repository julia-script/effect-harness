import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Images from 'effect-harness/ImagePreparation'

// Runnable after the integration build. Policy demonstration; no pretend image codec.
const limits: Images.Limits = {
  maxWidth: 1024,
  maxHeight: 1024,
  maxBytes: 4_000_000,
  maxInputPixels: 40_000_000,
}
const original = { width: 4000, height: 3000 }
const oriented = Images.orientedDimensions(original, 6)
const program = Console.log({
  original,
  oriented,
  target: Images.fitDimensions(oriented, limits),
  binaryBudget: Images.binaryBudget(limits.maxBytes),
})

await Effect.runPromise(program)
