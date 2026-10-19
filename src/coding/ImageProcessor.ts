/**
 * Turns images into ones the model can take: upright, in a format providers take inline, and
 * within the size limits. `read` uses the processor in context when there is one; without one, it
 * passes supported images within the limits through as they are and refuses the rest.
 *
 * One fitting algorithm, ported from pi's `images/index.ts`, runs over a `Codec`, the part that
 * decodes and encodes. `BunImageProcessor` (on `Bun.Image`) and `SharpImageProcessor` (on `sharp`)
 * are stock codecs (`sharp` cannot decode BMP); `make` builds a processor over any other.
 */
import { Context, Layer, Schema } from 'effect'
import type { Effect } from 'effect'
import { prepare } from './internal/fit.ts'
import { IMAGE_LIMITS } from './internal/image.ts'

export const Size = Schema.Struct({ width: Schema.Int, height: Schema.Int })
export type Size = typeof Size.Type

/** Limits an image the model sees must keep. */
export const Limits = Schema.Struct({
  /** Pixels. */
  maxWidth: Schema.Int,
  /** Pixels. */
  maxHeight: Schema.Int,
  /** Bytes of base64. */
  maxBytes: Schema.Int,
})
export type Limits = typeof Limits.Type

/** The coding agent's limits: 2000x2000, and 4.5 MiB of base64, below Anthropic's 5 MB. */
export const DEFAULT_LIMITS: Limits = IMAGE_LIMITS

/** An image the model can take: a supported format, within the limits, as base64. */
export const Prepared = Schema.Struct({
  data: Schema.String,
  mimeType: Schema.String,
  /** Set when the image was resized, for the model to map coordinates back. */
  resized: Schema.optionalKey(Schema.Struct({ from: Size, to: Size })),
  /** The original format, when it differs from `mimeType`: BMP becomes PNG, a large PNG may become JPEG. */
  convertedFrom: Schema.optionalKey(Schema.String),
})
export type Prepared = typeof Prepared.Type

export interface Service {
  /** The image prepared to fit `limits`, or `undefined` when it cannot be decoded or made to fit. */
  readonly prepare: (
    bytes: Uint8Array,
    mimeType: string,
    limits: Limits,
  ) => Effect.Effect<Prepared | undefined>
}

export class ImageProcessor extends Context.Service<ImageProcessor, Service>()(
  'effect-harness/coding/ImageProcessor',
) {}

/** One encoding a codec is asked for. */
export interface Target extends Size {
  readonly format: 'png' | 'jpeg'
  /** JPEG quality, 1 to 100. */
  readonly quality?: number
}

/**
 * What a processor needs from an image library. Orientation is the EXIF orientation, 1 to 8,
 * which the fitting algorithm reads itself, so every codec turns images upright the same way.
 */
export interface Codec {
  /** The size the file stores, before orientation; `undefined` when the whole image does not decode. */
  readonly size: (bytes: Uint8Array) => Effect.Effect<Size | undefined>
  /**
   * The image turned upright for `orientation`, scaled to exactly the target's size, which is
   * already upright, and encoded; `undefined` when it cannot be.
   */
  readonly encode: (
    bytes: Uint8Array,
    orientation: number,
    target: Target,
  ) => Effect.Effect<Uint8Array | undefined>
}

/**
 * A processor over `codec`. An image already upright, in a format providers take inline, and
 * within the limits is returned as it is, once it decodes. Otherwise it is turned upright, scaled
 * to the maximum width and height, and encoded as PNG or JPEG at falling qualities (JPEG first for
 * JPEG sources), then at smaller sizes, 3/4 at a time, until its base64 fits.
 */
export const make = (codec: Codec): Service => ({
  prepare: (bytes, mimeType, limits) => prepare(codec, bytes, mimeType, limits),
})

/** `make` as a Layer. */
export const layer = (codec: Codec): Layer.Layer<ImageProcessor> =>
  Layer.succeed(ImageProcessor, make(codec))
