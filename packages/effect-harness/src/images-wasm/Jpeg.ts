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
      const blocksWide = Math.ceil(size.width / 8)
      const blocksHigh = Math.ceil(size.height / 8)
      // jpeg-js supports up to four output components. Valid sampling factors are 1–4;
      // ceil(width / (8 * maxH)) * h <= ceil(width / 8) + 3 (and likewise vertically).
      // Bound every component by that worst-case MCU padding, regardless of subsampling.
      const coefficients = 4 * (blocksWide + 3) * (blocksHigh + 3) * 256
      const planes = 4 * blocksWide * blocksHigh * 64
      const output = size.width * size.height * 8 // Four-component getData plus RGBA.
      // Fixed table allowance stays capped: repeated tables/frames and invalid sampling
      // still consume the decoder's cumulative allowance and cannot grow it from input.
      const memoryBytes = coefficients + planes + output + 65_536
      if (!Number.isSafeInteger(memoryBytes))
        throw new ImageDecodeFailed({
          mimeType: 'image/jpeg',
          message: 'Strict JPEG allocation budget exceeds safe integer range',
        })
      const image = codec.decode(bytes, {
        useTArray: true,
        formatAsRGBA: true,
        tolerantDecoding: false,
        maxResolutionInMP: maxInputPixels / 1_000_000,
        maxMemoryUsageInMB: memoryBytes / 1_048_576,
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
