import * as Effect from 'effect/Effect'
import type {
  AdapterCapabilities,
  Dimensions,
  EncodingCandidate,
  ImageMetadata,
  Limits,
  Orientation,
  OutputMimeType,
  ValidatedLimits,
} from '../ImagePreparation.js'
import {
  ImageDecodeFailed,
  ImageInputPixelsExceeded,
  InvalidImageLimits,
} from '../ImagePreparationError.js'

/** Number of ASCII bytes in the base64 payload, excluding a data URL prefix. */
export const base64Length = (byteLength: number): number => 4 * Math.ceil(byteLength / 3)

/** Largest binary payload whose padded base64 representation fits the validated budget. */
export const binaryBudget = (maxBytes: number): number => Math.floor(maxBytes / 4) * 3

export const validateLimits = Effect.fn('ImagePreparation.validateLimits')(function* (
  limits: Limits,
) {
  if (limits === null || typeof limits !== 'object') {
    return yield* new InvalidImageLimits({ field: 'limits', message: 'Limits must be an object' })
  }
  for (const field of ['maxWidth', 'maxHeight', 'maxBytes', 'maxInputPixels'] as const) {
    if (!Number.isSafeInteger(limits[field]) || limits[field] <= 0) {
      return yield* new InvalidImageLimits({
        field,
        message: `${field} must be a positive safe integer`,
      })
    }
  }
  const transparency = limits.transparency === undefined ? 'preserve' : limits.transparency
  if (transparency !== 'preserve' && transparency !== 'flatten-white') {
    return yield* new InvalidImageLimits({
      field: 'transparency',
      message: 'Unknown transparency policy',
    })
  }
  const validated: ValidatedLimits = { ...limits, transparency }
  return validated
})

export const validateMetadata = Effect.fn('ImagePreparation.validateMetadata')(function* (
  metadata: ImageMetadata,
  mimeType: string,
  limits: ValidatedLimits,
) {
  const { width, height } = metadata.original
  if (
    metadata.mimeType !== mimeType ||
    !Number.isSafeInteger(width) ||
    width <= 0 ||
    !Number.isSafeInteger(height) ||
    height <= 0 ||
    !Number.isInteger(metadata.orientation) ||
    metadata.orientation < 1 ||
    metadata.orientation > 8
  ) {
    return yield* new ImageDecodeFailed({
      mimeType,
      message: 'Invalid decoded metadata or MIME mismatch',
    })
  }
  // Division keeps the allocation check exact even when width*height would overflow.
  if (width > Math.floor(limits.maxInputPixels / height)) {
    return yield* new ImageInputPixelsExceeded({
      width,
      height,
      maxInputPixels: limits.maxInputPixels,
    })
  }
})

export const orientedDimensions = (size: Dimensions, orientation: Orientation): Dimensions =>
  orientation >= 5 ? { width: size.height, height: size.width } : { ...size }

/** Maps an integer pixel coordinate before resizing through all eight EXIF orientations. */
export const orientPoint = (
  size: Dimensions,
  orientation: Orientation,
  x: number,
  y: number,
): { readonly x: number; readonly y: number } => {
  switch (orientation) {
    case 1:
      return { x, y }
    case 2:
      return { x: size.width - 1 - x, y }
    case 3:
      return { x: size.width - 1 - x, y: size.height - 1 - y }
    case 4:
      return { x, y: size.height - 1 - y }
    case 5:
      return { x: y, y: x }
    case 6:
      return { x: size.height - 1 - y, y: x }
    case 7:
      return { x: size.height - 1 - y, y: size.width - 1 - x }
    case 8:
      return { x: y, y: size.width - 1 - x }
  }
}

/** Nearest integer aspect-fit dimensions with a one-pixel minimum and no enlargement. */
export const fitDimensions = (
  size: Dimensions,
  limits: Pick<Limits, 'maxWidth' | 'maxHeight'>,
): Dimensions => {
  const scale = Math.min(1, limits.maxWidth / size.width, limits.maxHeight / size.height)
  return {
    width: Math.min(size.width, limits.maxWidth, Math.max(1, Math.round(size.width * scale))),
    height: Math.min(size.height, limits.maxHeight, Math.max(1, Math.round(size.height * scale))),
  }
}

/** Retry at three quarters of each dimension, terminating at 1×1. */
export const smallerDimensions = (size: Dimensions): Dimensions => ({
  width: Math.max(1, Math.floor(size.width * 0.75)),
  height: Math.max(1, Math.floor(size.height * 0.75)),
})

export const canPassthrough = (
  metadata: ImageMetadata,
  byteLength: number,
  limits: ValidatedLimits,
): boolean =>
  metadata.mimeType !== 'image/bmp' &&
  metadata.orientation === 1 &&
  metadata.original.width <= limits.maxWidth &&
  metadata.original.height <= limits.maxHeight &&
  byteLength <= binaryBudget(limits.maxBytes)

/** Ordered PNG / WebP / JPEG trials; transparent JPEG requires explicit white flattening. */
export const encodingCandidates = (
  metadata: ImageMetadata,
  limits: ValidatedLimits,
  capabilities: AdapterCapabilities,
): ReadonlyArray<EncodingCandidate> => {
  const order: ReadonlyArray<OutputMimeType> =
    metadata.mimeType === 'image/jpeg'
      ? ['image/jpeg', 'image/png', 'image/webp']
      : ['image/png', 'image/webp', 'image/jpeg']
  const candidates: Array<EncodingCandidate> = []
  for (const mimeType of order) {
    if (!capabilities.encode.includes(mimeType)) continue
    if (metadata.hasAlpha && mimeType === 'image/jpeg' && limits.transparency === 'preserve')
      continue
    const transparency =
      metadata.hasAlpha && mimeType === 'image/jpeg' ? 'flatten-white' : 'preserve'
    if (transparency === 'flatten-white' && !capabilities.flattenWhite) continue
    if (metadata.hasAlpha && transparency === 'preserve' && !capabilities.preserveAlpha) continue
    if (mimeType === 'image/png') {
      candidates.push({ mimeType, transparency })
    } else {
      for (const quality of [80, 70, 55, 40]) candidates.push({ mimeType, quality, transparency })
    }
  }
  return candidates
}
