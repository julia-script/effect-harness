import * as Effect from 'effect/Effect'
import type { Dimensions } from '../ImagePreparation.js'
import { ImageDecodeFailed } from '../ImagePreparationError.js'

/** Photon repairs missing JPEG entropy. Independently decode every MCU/scan in strict mode
 * before permitting passthrough. jpeg-js readBit rejects premature markers; decodeScan walks
 * baseline and progressive blocks. useTArray avoids its optional Node Buffer allocation path.
 * The decoder accounts for coefficients, component planes and output against this per-call cap.
 * See jpeg-js/lib/decoder.js (0.4.4): decodeScan, requestMemoryAllocation and decode.
 */
export const validate = Effect.fn('ImagesWasm.validateJpeg')(function* (
  bytes: Uint8Array,
  size: Dimensions,
  maxInputPixels: number,
) {
  const codec = yield* Effect.tryPromise({
    try: () => import('jpeg-js'),
    catch: (cause) =>
      new ImageDecodeFailed({
        mimeType: 'image/jpeg',
        message: 'Cannot load strict portable JPEG validator',
        cause,
      }),
  }).pipe(Effect.uninterruptible)
  yield* Effect.try({
    try: () => {
      const image = codec.decode(bytes, {
        useTArray: true,
        formatAsRGBA: true,
        tolerantDecoding: false,
        maxResolutionInMP: maxInputPixels / 1_000_000,
        // Coefficients and padded MCU planes require more space than the final RGBA image.
        maxMemoryUsageInMB: (maxInputPixels * 64 + 65_536) / 1_048_576,
      })
      if (
        image.width !== size.width ||
        image.height !== size.height ||
        image.data.length !== size.width * size.height * 4
      )
        throw new ImageDecodeFailed({
          mimeType: 'image/jpeg',
          message: 'Strict JPEG dimensions disagree with header',
        })
    },
    catch: (cause) =>
      new ImageDecodeFailed({
        mimeType: 'image/jpeg',
        message: 'Strict JPEG entropy validation failed',
        cause,
      }),
  })
})
