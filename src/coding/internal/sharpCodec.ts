/**
 * The `sharp` codec, on libvips: native, off the event loop on libuv's thread pool.
 */
import { Effect, Option } from 'effect'
import sharp from 'sharp'
import type { Sharp } from 'sharp'
import type { Codec, Size, Target } from '../ImageProcessor.ts'

/** A rejection is an image this codec cannot take. */
const attempt = <A>(run: () => Promise<A>): Effect.Effect<A | undefined> =>
  Effect.map(Effect.option(Effect.tryPromise(run)), Option.getOrUndefined)

/** `sharp` mirrors first, then rotates, whatever the call order. */
const upright = (image: Sharp, orientation: number): Sharp => {
  if (orientation === 2) return image.flop()
  if (orientation === 3) return image.rotate(180)
  if (orientation === 4) return image.flip()
  if (orientation === 5) return image.flip().rotate(90)
  if (orientation === 6) return image.rotate(90)
  if (orientation === 7) return image.flop().rotate(90)
  return orientation === 8 ? image.rotate(270) : image
}

const encoded = (image: Sharp, target: Target) =>
  target.format === 'png' ? image.png() : image.jpeg({ quality: target.quality ?? 80 })

export const codec: Codec = {
  size: (bytes) =>
    Effect.map(
      attempt(() => sharp(bytes).metadata()),
      (metadata): Size | undefined =>
        metadata === undefined ? undefined : { width: metadata.width, height: metadata.height },
    ),
  encode: (bytes, orientation, target) =>
    Effect.map(
      attempt(() =>
        encoded(
          upright(sharp(bytes), orientation).resize(target.width, target.height, { fit: 'fill' }),
          target,
        ).toBuffer(),
      ),
      (buffer) => (buffer === undefined ? undefined : new Uint8Array(buffer)),
    ),
}
