/**
 * The fitting algorithm of every image processor, ported from pi's `images/index.ts`: try the
 * image as it is, then upright and scaled down, in falling encodings, until its base64 fits.
 */
import { Effect } from 'effect'
import type { Codec, Limits, Prepared, Size, Target } from '../ImageProcessor.ts'
import { orientationOf, transposes } from './exif.ts'
import { base64Length, INLINE_IMAGE_TYPES, toBase64 } from './image.ts'

/** JPEG qualities tried in order. */
const JPEG_QUALITIES = [80, 70, 55, 40]

const fits = (size: Size, bytes: number, limits: Limits) =>
  size.width <= limits.maxWidth &&
  size.height <= limits.maxHeight &&
  base64Length(bytes) <= limits.maxBytes

/** The encodings tried at one size: PNG, then JPEG at falling qualities, or the JPEGs first. */
const encodings = (size: Size, jpegFirst: boolean): ReadonlyArray<Target> => {
  const png: Target = { ...size, format: 'png' }
  const jpegs = JPEG_QUALITIES.map((quality): Target => ({
    width: size.width,
    height: size.height,
    format: 'jpeg',
    quality,
  }))
  return jpegFirst ? [...jpegs, png] : [png, ...jpegs]
}

/** The first encoding at `size` whose base64 fits `maxBytes`. */
const encodeWithin = (
  codec: Codec,
  bytes: Uint8Array,
  orientation: number,
  size: Size,
  maxBytes: number,
  jpegFirst: boolean,
) =>
  Effect.gen(function* () {
    let found: { readonly data: string; readonly mimeType: string } | undefined
    for (const target of encodings(size, jpegFirst)) {
      const encoded = yield* codec.encode(bytes, orientation, target)
      if (encoded !== undefined && base64Length(encoded.byteLength) <= maxBytes) {
        found = { data: toBase64(encoded), mimeType: `image/${target.format}` }
        break
      }
    }
    return found
  })

const shrink = (size: Size): Size => ({
  width: Math.max(1, Math.floor(size.width * 0.75)),
  height: Math.max(1, Math.floor(size.height * 0.75)),
})

/** The upright size scaled to fit the maximum width and height. */
const scaled = (upright: Size, limits: Limits): Size => {
  const scale = Math.min(1, limits.maxWidth / upright.width, limits.maxHeight / upright.height)
  return {
    width: Math.max(1, Math.round(upright.width * scale)),
    height: Math.max(1, Math.round(upright.height * scale)),
  }
}

/** The image upright, scaled, and encoded, smaller each round until it fits. */
const reencode = (
  codec: Codec,
  bytes: Uint8Array,
  mimeType: string,
  upright: Size,
  limits: Limits,
): Effect.Effect<Prepared | undefined> => {
  const orientation = orientationOf(bytes)
  const jpegFirst = mimeType === 'image/jpeg'
  const prepared = (
    encoded: { readonly data: string; readonly mimeType: string },
    target: Size,
  ) => {
    const same = target.width === upright.width && target.height === upright.height
    const result: Prepared = {
      ...encoded,
      ...(encoded.mimeType === mimeType ? {} : { convertedFrom: mimeType }),
      ...(same ? {} : { resized: { from: upright, to: target } }),
    }
    return result
  }
  const at = (target: Size): Effect.Effect<Prepared | undefined> =>
    Effect.flatMap(
      encodeWithin(codec, bytes, orientation, target, limits.maxBytes, jpegFirst),
      (encoded) => {
        if (encoded !== undefined) return Effect.succeed(prepared(encoded, target))
        const smallest = target.width === 1 && target.height === 1
        return smallest ? Effect.succeed(encoded) : at(shrink(target))
      },
    )
  return at(scaled(upright, limits))
}

export const prepare = (
  codec: Codec,
  bytes: Uint8Array,
  mimeType: string,
  limits: Limits,
): Effect.Effect<Prepared | undefined> =>
  Effect.gen(function* () {
    // Decoded even when it is returned as it is: a damaged file is an error, not an image the provider rejects.
    const stored = yield* codec.size(bytes)
    if (stored === undefined) return stored
    const orientation = orientationOf(bytes)
    if (INLINE_IMAGE_TYPES.has(mimeType) && orientation === 1 && fits(stored, bytes.length, limits))
      return { data: toBase64(bytes), mimeType }
    const upright = transposes(orientation)
      ? { width: stored.height, height: stored.width }
      : stored
    return yield* reencode(codec, bytes, mimeType, upright, limits)
  })
