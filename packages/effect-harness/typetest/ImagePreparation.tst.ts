import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import type * as Scope from 'effect/Scope'
import * as Images from 'effect-harness/ImagePreparation'
import type { ImagePreparationError } from 'effect-harness/ImagePreparationError'

declare const adapter: Images.Adapter
const limits: Images.Limits = {
  maxWidth: 100,
  maxHeight: 100,
  maxBytes: 1000,
  maxInputPixels: 10000,
}
const program = Images.prepare(new Uint8Array(3), 'image/png', limits)

test('unprovided preparation retains the image requirement and semantic error union', () => {
  expect(program).type.toBe<
    Effect.Effect<Images.PreparedImage, ImagePreparationError, Images.ImagePreparation>
  >()
  expect(Effect.runPromise).type.not.toBeCallableWith(program)
  expect(adapter.decode).type.toBe<
    (
      bytes: Uint8Array,
      mimeType: Images.InputMimeType,
      maxInputPixels: number,
    ) => Effect.Effect<Images.DecodedImage, ImagePreparationError, Scope.Scope>
  >()
})

test('only the matching provided service removes the requirement', () => {
  expect(Images.layer(adapter)).type.toBe<Layer.Layer<Images.ImagePreparation>>()
  expect(program.pipe(Effect.provide(Images.layer(adapter)))).type.toBe<
    Effect.Effect<Images.PreparedImage, ImagePreparationError>
  >()
  class OtherService extends Context.Service<OtherService, { readonly value: string }>()(
    'typetest/image/Other',
  ) {}
  const unrelated = Layer.succeed(OtherService, { value: 'other' })
  expect(program.pipe(Effect.provide(unrelated))).type.toBe<
    Effect.Effect<Images.PreparedImage, ImagePreparationError, Images.ImagePreparation>
  >()
  expect(Images.ImagePreparation.of).type.not.toBeCallableWith({
    prepare: () => Effect.succeed('not an image'),
  })
  expect(Images.ImagePreparation.of).type.not.toBeCallableWith({})
})

test('adapter construction dependencies and errors remain visible until supplied', () => {
  class Decoder extends Context.Service<Decoder, Images.Adapter>()('typetest/image/Decoder') {}
  const bound = Layer.effect(Images.ImagePreparation, Effect.map(Decoder, Images.make))
  expect(bound).type.toBe<Layer.Layer<Images.ImagePreparation, never, Decoder>>()
  expect(program.pipe(Effect.provide(bound))).type.toBe<
    Effect.Effect<Images.PreparedImage, ImagePreparationError, Decoder>
  >()
  const ready = bound.pipe(Layer.provide(Layer.succeed(Decoder, adapter)))
  expect(program.pipe(Effect.provide(ready))).type.toBe<
    Effect.Effect<Images.PreparedImage, ImagePreparationError>
  >()
  const failed = Layer.effect(
    Images.ImagePreparation,
    Effect.fail('codec installation failed' as const),
  )
  expect(program.pipe(Effect.provide(failed))).type.toBe<
    Effect.Effect<Images.PreparedImage, ImagePreparationError | 'codec installation failed'>
  >()
})

test('byte ownership and optional metadata have precise public types', () => {
  expect<Images.PreparedImage['bytes']>().type.toBe<Uint8Array>()
  expect<Images.PreparedImage['convertedFrom']>().type.toBe<Images.InputMimeType | undefined>()
  expect(Images.make(adapter).prepare).type.toBe<Images.Service['prepare']>()
  expect(Images.validateLimits(limits)).type.toBe<
    Effect.Effect<
      Images.ValidatedLimits,
      import('effect-harness/ImagePreparationError').InvalidImageLimits
    >
  >()
})
