/**
 * EXIF orientation of JPEG and WebP files, read from the bytes without decoding the image; ported
 * from pi's `images/exif.ts`.
 */

const byteAt = (bytes: Uint8Array, at: number) => bytes[at] ?? 0

const ascii = (bytes: Uint8Array, offset: number, text: string) => {
  for (let index = 0; index < text.length; index++)
    if (bytes[offset + index] !== text.codePointAt(index)) return false
  return true
}

const isExifHeader = (bytes: Uint8Array, offset: number) =>
  ascii(bytes, offset, 'Exif') && bytes[offset + 4] === 0 && bytes[offset + 5] === 0

/** Where the TIFF header of a JPEG's APP1 segment starts; -1 when there is none. */
const jpegTiff = (bytes: Uint8Array) => {
  let offset = 2
  while (offset + 4 <= bytes.length) {
    if (bytes[offset] !== 0xff) return -1
    const marker = byteAt(bytes, offset + 1)
    if (marker === 0xff) offset++
    else if (marker === 0xe1 && isExifHeader(bytes, offset + 4)) return offset + 10
    else offset += 2 + ((byteAt(bytes, offset + 2) << 8) | byteAt(bytes, offset + 3))
  }
  return -1
}

const uint32LE = (bytes: Uint8Array, at: number) =>
  (byteAt(bytes, at) |
    (byteAt(bytes, at + 1) << 8) |
    (byteAt(bytes, at + 2) << 16) |
    (byteAt(bytes, at + 3) << 24)) >>>
  0

/** Where the TIFF header of a WebP's EXIF chunk starts; -1 when there is none. */
const webpTiff = (bytes: Uint8Array) => {
  let offset = 12
  while (offset + 8 <= bytes.length) {
    const size = uint32LE(bytes, offset + 4)
    // Some WebP files prefix the TIFF header with "Exif\0\0".
    if (ascii(bytes, offset, 'EXIF'))
      return isExifHeader(bytes, offset + 8) ? offset + 14 : offset + 8
    // RIFF chunks are padded to an even size.
    offset += 8 + size + (size % 2)
  }
  return -1
}

const tiffStart = (bytes: Uint8Array) => {
  if (bytes[0] === 0xff && bytes[1] === 0xd8) return jpegTiff(bytes)
  return ascii(bytes, 0, 'RIFF') && ascii(bytes, 8, 'WEBP') ? webpTiff(bytes) : -1
}

/** The EXIF orientation of a JPEG or WebP, 1 to 8; 1 when absent or unreadable. */
export const orientationOf = (bytes: Uint8Array): number => {
  const tiff = tiffStart(bytes)
  if (tiff < 0 || tiff + 8 > bytes.length) return 1
  const little = bytes[tiff] === 0x49 && bytes[tiff + 1] === 0x49
  const u16 = (at: number) =>
    little
      ? byteAt(bytes, at) | (byteAt(bytes, at + 1) << 8)
      : (byteAt(bytes, at) << 8) | byteAt(bytes, at + 1)
  const u32 = (at: number) => (little ? uint32LE(bytes, at) : ((u16(at) << 16) | u16(at + 2)) >>> 0)
  const directory = tiff + u32(tiff + 4)
  if (directory + 2 > bytes.length) return 1
  const entries = u16(directory)
  for (let index = 0; index < entries; index++) {
    const entry = directory + 2 + index * 12
    if (entry + 12 > bytes.length) return 1
    // Tag 0x0112 is Orientation, a SHORT whose value sits in the entry.
    if (u16(entry) === 0x0112) {
      const value = u16(entry + 8)
      return value >= 1 && value <= 8 ? value : 1
    }
  }
  return 1
}

/** Whether `orientation` swaps width and height: 5 to 8. */
export const transposes = (orientation: number) => orientation >= 5
