import {
  orientPoint,
  orientedDimensions,
  type Dimensions,
  type Orientation,
} from '../ImagePreparation.js'

/** A fresh RGBA source for each candidate, including mirrored/transposed EXIF orientations. */
export const orient = (
  pixels: Uint8Array,
  size: Dimensions,
  orientation: Orientation,
): Uint8Array => {
  const upright = orientedDimensions(size, orientation)
  const result = new Uint8Array(pixels.length)
  for (let y = 0; y < size.height; y++) {
    for (let x = 0; x < size.width; x++) {
      const target = orientPoint(size, orientation, x, y)
      const source = (y * size.width + x) * 4
      result.set(pixels.subarray(source, source + 4), (target.y * upright.width + target.x) * 4)
    }
  }
  return result
}

export const flattenWhite = (pixels: Uint8Array): void => {
  for (let i = 0; i < pixels.length; i += 4) {
    const alpha = pixels[i + 3]! / 255
    for (let channel = 0; channel < 3; channel++) {
      pixels[i + channel] = Math.round(pixels[i + channel]! * alpha + 255 * (1 - alpha))
    }
    pixels[i + 3] = 255
  }
}

export const hasAlpha = (pixels: Uint8Array): boolean => {
  for (let i = 3; i < pixels.length; i += 4) if (pixels[i] !== 255) return true
  return false
}
