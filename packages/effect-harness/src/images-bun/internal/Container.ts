import * as Effect from 'effect/Effect'
import type { InputMimeType, Orientation } from '../../ImagePreparation.js'
import { ImageDecodeFailed } from '../../ImagePreparationError.js'

const text = (bytes: Uint8Array, start: number, length: number): string =>
  String.fromCharCode(...bytes.subarray(start, start + length))

const orientation = (value: number): Orientation | undefined => {
  switch (value) {
    case 1:
    case 2:
    case 3:
    case 4:
    case 5:
    case 6:
    case 7:
    case 8:
      return value
    default:
      return undefined
  }
}

// JPEG APP1 and WebP EXIF may carry the six-byte Exif prefix; PNG eXIf carries TIFF directly.
const exifOrientation = (bytes: Uint8Array): Orientation | undefined => {
  const tiff = text(bytes, 0, 6) === 'Exif\0\0' ? bytes.subarray(6) : bytes
  if (tiff.length < 8) return undefined
  const order = text(tiff, 0, 2)
  if (order !== 'II' && order !== 'MM') return undefined
  const little = order === 'II'
  const view = new DataView(tiff.buffer, tiff.byteOffset, tiff.byteLength)
  if (view.getUint16(2, little) !== 42) return undefined
  const start = view.getUint32(4, little)
  const count = view.getUint16(start, little)
  for (let i = 0; i < count; i++) {
    const entry = start + 2 + i * 12
    if (view.getUint16(entry, little) !== 0x112) continue
    if (view.getUint16(entry + 2, little) !== 3 || view.getUint32(entry + 4, little) !== 1)
      return undefined
    return orientation(view.getUint16(entry + 8, little))
  }
  return 1
}

/** Inspect complete container framing, animation declarations and original orientation.
 * Pixel validation is deliberately separate: this is never passthrough decode proof.
 */
export const inspect = Effect.fn('BunImagePreparation.inspectContainer')(function* (
  bytes: Uint8Array,
  mimeType: InputMimeType,
) {
  const fail = () =>
    new ImageDecodeFailed({ mimeType, message: 'Malformed image container or EXIF' })
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  let orient: Orientation = 1
  let animated = false
  const readExif = (payload: Uint8Array) =>
    Effect.try({
      try: () => exifOrientation(payload),
      catch: fail,
    })
  if (mimeType === 'image/jpeg') {
    let p = 2
    while (p + 4 <= bytes.length) {
      if (bytes[p] !== 255) return yield* fail()
      const marker = bytes[p + 1]
      if (marker === 255) {
        p++
        continue
      }
      if (marker === 0xda || marker === 0xd9) break
      const length = view.getUint16(p + 2)
      if (length < 2 || p + 2 + length > bytes.length) return yield* fail()
      if (marker === 0xe1 && text(bytes, p + 4, 6) === 'Exif\0\0') {
        const value = yield* readExif(bytes.subarray(p + 4, p + 2 + length))
        if (value === undefined) return yield* fail()
        orient = value
      }
      p += length + 2
    }
    // Some Bun/libjpeg versions accept missing EOI with warnings. The strict validator also
    // rejects incomplete entropy scans; this cheap check is not its replacement.
    if (bytes[bytes.length - 2] !== 255 || bytes[bytes.length - 1] !== 217) return yield* fail()
  } else if (mimeType === 'image/png' || mimeType === 'image/webp') {
    const png = mimeType === 'image/png'
    let p = png ? 8 : 12
    const end = png ? bytes.length : view.getUint32(4, true) + 8
    if (end !== bytes.length) return yield* fail()
    let ended = false
    while (p < end) {
      if (p + (png ? 12 : 8) > end) return yield* fail()
      const length = view.getUint32(p + (png ? 0 : 4), !png)
      const kind = text(bytes, p + (png ? 4 : 0), 4)
      const start = p + 8
      const next = start + length + (png ? 4 : length % 2)
      if (next > end) return yield* fail()
      if (kind === 'eXIf' || kind === 'EXIF') {
        const value = yield* readExif(bytes.subarray(start, start + length))
        if (value === undefined) return yield* fail()
        orient = value
      }
      if (kind === 'acTL' || kind === 'ANIM') animated = true
      if (kind === 'IEND') {
        ended = true
        if (next !== end) return yield* fail()
      }
      p = next
    }
    if (png && !ended) return yield* fail()
  } else if (mimeType === 'image/gif') {
    if (bytes.length < 13) return yield* fail()
    let p = 13
    const packed = bytes[10] ?? 0
    if (packed & 128) p += 3 * 2 ** ((packed & 7) + 1)
    let frames = 0
    let ended = false
    while (p < bytes.length) {
      const block = bytes[p++]
      if (block === 0x3b) {
        ended = true
        break
      }
      if (block === 0x2c) {
        if (p + 9 > bytes.length) return yield* fail()
        const local = bytes[p + 8] ?? 0
        p += 9
        if (local & 128) p += 3 * 2 ** ((local & 7) + 1)
        p++ // LZW minimum code size
        frames++
      } else if (block === 0x21) p++ // extension label
      else return yield* fail()
      for (;;) {
        if (p >= bytes.length) return yield* fail()
        const length = bytes[p++] ?? 0
        if (length === 0) break
        p += length
        if (p > bytes.length) return yield* fail()
      }
    }
    if (!ended || frames === 0 || p !== bytes.length) return yield* fail()
    animated = frames > 1
  }
  return { orientation: orient, animated }
})
