/**
 * The `Bun.Image` codec: Bun's own JPEG, PNG and WebP codecs, decoding BMP, GIF and TIFF too; the
 * pipeline runs on Bun's worker threads, off the event loop.
 */
import { Effect, Option } from 'effect'
import type { Codec, Size, Target } from '../../ImageProcessor.ts'

/** A rejection is an image this codec cannot take. */
const attempt = <A>(run: () => Promise<A>): Effect.Effect<A | undefined> =>
  Effect.map(Effect.option(Effect.tryPromise(run)), Option.getOrUndefined)

/** `Bun.Image` rotates first, then mirrors, whatever the call order. */
const upright = (image: Bun.Image, orientation: number): Bun.Image => {
  if (orientation === 2) return image.flop()
  if (orientation === 3) return image.rotate(180)
  if (orientation === 4) return image.flip()
  if (orientation === 5) return image.rotate(90).flop()
  if (orientation === 6) return image.rotate(90)
  if (orientation === 7) return image.rotate(90).flip()
  return orientation === 8 ? image.rotate(270) : image
}

const encoded = (image: Bun.Image, target: Target) =>
  target.format === 'png' ? image.png() : image.jpeg({ quality: target.quality ?? 80 })

// Orientation is applied explicitly, the same way for JPEG and WebP.
const open = (bytes: Uint8Array) => new Bun.Image(bytes, { autoOrient: false })

export const codec: Codec = {
  // `metadata()` reads only the header: a 1x1 encoding first decodes the whole image.
  size: (bytes) =>
    Effect.map(
      attempt(() =>
        open(bytes)
          .resize(1, 1)
          .png()
          .bytes()
          .then(() => open(bytes).metadata()),
      ),
      (metadata): Size | undefined =>
        metadata === undefined ? undefined : { width: metadata.width, height: metadata.height },
    ),
  encode: (bytes, orientation, target) =>
    attempt(() =>
      encoded(
        upright(open(bytes), orientation).resize(target.width, target.height, { fit: 'fill' }),
        target,
      ).bytes(),
    ),
}
