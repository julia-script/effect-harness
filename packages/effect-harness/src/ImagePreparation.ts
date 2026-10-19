import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import type * as Scope from 'effect/Scope'
import {
  ImageCannotFit,
  ImageEncodeFailed,
  UnsupportedImage,
  type ImagePreparationError,
} from './ImagePreparationError.js'
import * as Plan from './internal/ImagePreparationPlan.js'

export {
  base64Length,
  binaryBudget,
  encodingCandidates,
  fitDimensions,
  orientPoint,
  orientedDimensions,
  smallerDimensions,
  validateLimits,
} from './internal/ImagePreparationPlan.js'

export type InputMimeType = 'image/png' | 'image/jpeg' | 'image/gif' | 'image/webp' | 'image/bmp'
export type OutputMimeType = 'image/png' | 'image/jpeg' | 'image/webp'
export type Orientation = 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8
export interface Dimensions {
  readonly width: number
  readonly height: number
}

/** All limits are positive safe integers. maxBytes counts padded base64 ASCII bytes, not binary bytes. */
export interface Limits {
  readonly maxWidth: number
  readonly maxHeight: number
  readonly maxBytes: number
  readonly maxInputPixels: number
  /** Preserve alpha by default; opt in to JPEG flattened against opaque white. */
  readonly transparency?: 'preserve' | 'flatten-white'
}
export interface ValidatedLimits extends Limits {
  readonly transparency: 'preserve' | 'flatten-white'
}

export interface ImageMetadata {
  readonly mimeType: InputMimeType
  /** Encoded dimensions before EXIF normalization; animated sources use the first frame's canvas. */
  readonly original: Dimensions
  readonly orientation: Orientation
  readonly animated: boolean
  readonly hasAlpha: boolean
}

export interface EncodingCandidate {
  readonly mimeType: OutputMimeType
  /** JPEG/WebP quality is 1–100; PNG is lossless and has no quality argument. */
  readonly quality?: number
  readonly transparency: 'preserve' | 'flatten-white'
}

export interface EncodingRequest extends EncodingCandidate {
  /** Exact integer target size, aspect-fitted by policy before encoding. Never crop or enlarge. */
  readonly dimensions: Dimensions
  /** Apply the original EXIF orientation exactly once, before resize; output must be upright. */
  readonly orientation: Orientation
  readonly frame: 'first'
}

/** Advertise actual bound capabilities, including any explicit decoder fallback. No global backend switch. */
export interface AdapterCapabilities {
  readonly decode: ReadonlyArray<InputMimeType>
  readonly encode: ReadonlyArray<OutputMimeType>
  readonly orientations: ReadonlyArray<Orientation>
  readonly preserveAlpha: boolean
  readonly flattenWhite: boolean
}

export interface DecodedImage {
  readonly metadata: ImageMetadata
  /** Return valid encoded bytes at exactly the requested size/MIME with normalized orientation.
   * Each retry starts from the decoded source, never the previous lossy candidate.
   * Only the first frame is transformed; strip stale orientation metadata. Other EXIF is not promised.
   */
  readonly encode: (request: EncodingRequest) => Effect.Effect<Uint8Array, ImagePreparationError>
}

/** Replaceable codec boundary. Bun, Node/sharp and WASM implementations belong in separate adapters. */
export interface Adapter {
  readonly capabilities: AdapterCapabilities
  /** Fully decode/validate even a passthrough; metadata-only inspection is insufficient.
   * Check maxInputPixels against headers BEFORE allocating pixels. Translate codec failures to semantic errors.
   * Borrow only these private bytes without mutation. Register native/WASM cleanup in Scope.
   * On interruption, noncancellable work must settle before releasing its borrowed buffers/resources;
   * use a scoped worker or completion lease if the codec has no abort API. Never mutate Bun.Image.backend.
   */
  readonly decode: (
    bytes: Uint8Array,
    mimeType: InputMimeType,
    maxInputPixels: number,
  ) => Effect.Effect<DecodedImage, ImagePreparationError, Scope.Scope>
}

/** Result bytes own a fresh fixed buffer; they alias neither caller input nor codec output.
 * Upright in-limit PNG/JPEG/GIF/WebP is validated and returned byte-identically, including animation.
 * BMP and all transformed animation are encoded as one upright frame. Metadata describes actual policy.
 */
export interface PreparedImage {
  readonly bytes: Uint8Array
  readonly mimeType: Exclude<InputMimeType, 'image/bmp'>
  readonly original: Dimensions
  readonly oriented: Dimensions
  readonly dimensions: Dimensions
  readonly orientation: Orientation
  readonly passthrough: boolean
  readonly animation: 'unchanged' | 'first-frame' | 'static'
  readonly transparency: 'unchanged' | 'preserved' | 'flattened-white' | 'opaque'
  readonly convertedFrom?: InputMimeType
  readonly resized?: { readonly from: Dimensions; readonly to: Dimensions }
}

export interface Service {
  readonly prepare: (
    bytes: Uint8Array,
    mimeType: string,
    limits: Limits,
  ) => Effect.Effect<PreparedImage, ImagePreparationError>
}

/** Image preparation is a toolkit-local service requirement, fulfilled by a bound adapter Layer. */
export class ImagePreparation extends Context.Service<ImagePreparation, Service>()(
  'effect-harness/ImagePreparation',
) {}

const isInputMime = (mimeType: string): mimeType is InputMimeType =>
  mimeType === 'image/png' ||
  mimeType === 'image/jpeg' ||
  mimeType === 'image/gif' ||
  mimeType === 'image/webp' ||
  mimeType === 'image/bmp'

/** Build policy around a scoped codec. This does not supply a native decoder or prove codec conformance. */
export const make = (adapter: Adapter): Service => {
  // Snapshot capability lists so rebinding another toolkit cannot alter this service's policy.
  const capabilities: AdapterCapabilities = {
    ...adapter.capabilities,
    decode: [...adapter.capabilities.decode],
    encode: [...adapter.capabilities.encode],
    orientations: [...adapter.capabilities.orientations],
  }
  return {
    prepare: Effect.fn('ImagePreparation.prepare')(function* (
      bytes: Uint8Array,
      mimeType: string,
      limits: Limits,
    ) {
      const validated = yield* Plan.validateLimits(limits)
      if (!isInputMime(mimeType) || !capabilities.decode.includes(mimeType)) {
        return yield* new UnsupportedImage({
          mimeType,
          operation: 'decode',
          message: 'No bound decoder for this MIME type',
        })
      }
      // Uint8Array.from also detaches Buffer/subarray/shared-buffer views.
      const owned = Uint8Array.from(bytes)
      return yield* Effect.scoped(
        Effect.gen(function* () {
          const decoded = yield* adapter.decode(owned, mimeType, validated.maxInputPixels)
          const metadata = decoded.metadata
          yield* Plan.validateMetadata(metadata, mimeType, validated)
          const original = { ...metadata.original }
          const oriented = Plan.orientedDimensions(original, metadata.orientation)
          if (
            mimeType !== 'image/bmp' &&
            Plan.canPassthrough(metadata, owned.byteLength, validated)
          ) {
            return {
              bytes: Uint8Array.from(owned),
              mimeType,
              original,
              oriented,
              dimensions: { ...oriented },
              orientation: metadata.orientation,
              passthrough: true,
              animation: metadata.animated ? 'unchanged' : 'static',
              transparency: 'unchanged',
            } satisfies PreparedImage
          }
          if (!capabilities.orientations.includes(metadata.orientation)) {
            return yield* new UnsupportedImage({
              mimeType,
              operation: 'orient',
              message: 'The bound adapter cannot normalize this orientation',
            })
          }
          const candidates = Plan.encodingCandidates(metadata, validated, capabilities)
          if (candidates.length === 0) {
            return yield* new UnsupportedImage({
              mimeType,
              operation: metadata.hasAlpha ? 'transparency' : 'encode',
              message: 'No encoder satisfies the transparency policy',
            })
          }
          let dimensions = Plan.fitDimensions(oriented, validated)
          for (;;) {
            for (const candidate of candidates) {
              const encoded = yield* decoded.encode({
                ...candidate,
                dimensions: { ...dimensions },
                orientation: metadata.orientation,
                frame: 'first',
              })
              if (encoded.byteLength === 0) {
                return yield* new ImageEncodeFailed({
                  mimeType: candidate.mimeType,
                  message: 'Encoder returned empty bytes',
                })
              }
              if (encoded.byteLength > Plan.binaryBudget(validated.maxBytes)) continue
              const resized =
                dimensions.width !== oriented.width || dimensions.height !== oriented.height
              let transparency: PreparedImage['transparency'] = 'opaque'
              if (metadata.hasAlpha)
                transparency =
                  candidate.transparency === 'flatten-white' ? 'flattened-white' : 'preserved'
              return {
                bytes: Uint8Array.from(encoded),
                mimeType: candidate.mimeType,
                original,
                oriented,
                dimensions: { ...dimensions },
                orientation: metadata.orientation,
                passthrough: false,
                animation: metadata.animated ? 'first-frame' : 'static',
                transparency,
                ...(mimeType === candidate.mimeType ? {} : { convertedFrom: mimeType }),
                ...(resized ? { resized: { from: { ...oriented }, to: { ...dimensions } } } : {}),
              } satisfies PreparedImage
            }
            if (dimensions.width === 1 && dimensions.height === 1) {
              return yield* new ImageCannotFit({ maxBytes: validated.maxBytes })
            }
            const smaller = Plan.smallerDimensions(dimensions)
            dimensions = Plan.fitDimensions(oriented, {
              maxWidth: smaller.width,
              maxHeight: smaller.height,
            })
          }
        }),
      )
    }),
  }
}

export const layer = (adapter: Adapter): Layer.Layer<ImagePreparation> =>
  Layer.succeed(ImagePreparation, make(adapter))

/** Prepare through the currently bound service. Input bytes are copied when the Effect is evaluated. */
export const prepare = Effect.fn('ImagePreparation.prepareWithService')(function* (
  bytes: Uint8Array,
  mimeType: string,
  limits: Limits,
) {
  const service = yield* ImagePreparation
  return yield* service.prepare(bytes, mimeType, limits)
})
