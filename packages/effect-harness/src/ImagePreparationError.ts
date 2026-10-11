import * as Schema from 'effect/Schema'

/** A caller supplied a non-positive, fractional, non-finite or unsafe image limit. */
export class InvalidImageLimits extends Schema.TaggedError<InvalidImageLimits>()(
  'InvalidImageLimits',
  { field: Schema.String, message: Schema.String },
) {}

/** The selected adapter cannot decode the MIME type or perform a required transformation. */
export class UnsupportedImage extends Schema.TaggedError<UnsupportedImage>()('UnsupportedImage', {
  mimeType: Schema.String,
  operation: Schema.Literals(['decode', 'orient', 'encode', 'transparency']),
  message: Schema.String,
}) {}

/** Input bytes, dimensions, orientation or declared MIME type failed full decode validation. */
export class ImageDecodeFailed extends Schema.TaggedError<ImageDecodeFailed>()(
  'ImageDecodeFailed',
  {
    mimeType: Schema.String,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** A codec failed while encoding a candidate; this is distinct from an oversized valid output. */
export class ImageEncodeFailed extends Schema.TaggedError<ImageEncodeFailed>()(
  'ImageEncodeFailed',
  {
    mimeType: Schema.String,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** Input pixel dimensions exceed the allocation guard, before pixel buffers are allocated. */
export class ImageInputPixelsExceeded extends Schema.TaggedError<ImageInputPixelsExceeded>()(
  'ImageInputPixelsExceeded',
  { width: Schema.Finite, height: Schema.Finite, maxInputPixels: Schema.Finite },
) {}

/** Every supported candidate, including 1×1, exceeded the base64 payload budget. */
export class ImageCannotFit extends Schema.TaggedError<ImageCannotFit>()('ImageCannotFit', {
  maxBytes: Schema.Finite,
}) {}

export type ImagePreparationError =
  | InvalidImageLimits
  | UnsupportedImage
  | ImageDecodeFailed
  | ImageEncodeFailed
  | ImageInputPixelsExceeded
  | ImageCannotFit
