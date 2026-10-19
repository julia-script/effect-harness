import { expect, test } from 'tstyche'
import * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import type * as Scope from 'effect/Scope'
import * as Images from 'effect-harness/ImagePreparation'
import type { ImagePreparationError } from 'effect-harness/ImagePreparationError'
import type { UnsupportedImage } from 'effect-harness/ImagePreparationError'
import * as BunImages from 'effect-harness/images-bun'

declare const adapter: Images.Adapter

test('Bun Layer removes only its native semantic image requirement', () => {
  expect(BunImages.layer).type.toBe<Layer.Layer<Images.ImagePreparation, UnsupportedImage>>()
  expect(BunImages.adapter).type.toBe<Effect.Effect<Images.Adapter, UnsupportedImage>>()
  const program = Images.prepare(new Uint8Array(1), 'image/png', {
    maxWidth: 1,
    maxHeight: 1,
    maxBytes: 100,
    maxInputPixels: 1,
  })
  expect(program).type.toBe<
    Effect.Effect<Images.PreparedImage, ImagePreparationError, Images.ImagePreparation>
  >()
  expect(program.pipe(Effect.provide(BunImages.layer))).type.toBe<
    Effect.Effect<Images.PreparedImage, ImagePreparationError>
  >()
  expect(Effect.runPromise).type.not.toBeCallableWith(program)
  expect(adapter.decode(new Uint8Array(1), 'image/png', 1)).type.toBe<
    Effect.Effect<Images.DecodedImage, ImagePreparationError, Scope.Scope>
  >()
})
