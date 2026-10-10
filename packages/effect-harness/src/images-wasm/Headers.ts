import * as Effect from 'effect/Effect'
import type { Dimensions, InputMimeType, Orientation } from '../ImagePreparation.js'
import { ImageDecodeFailed, ImageInputPixelsExceeded } from '../ImagePreparationError.js'

export interface Header {
  readonly dimensions: Dimensions
  readonly orientation: Orientation
  readonly animated: boolean
}

// Header parsing has no pixel allocations. All reads are bounded by the borrowed input.
const read = (bytes: Uint8Array, mimeType: InputMimeType, maxInputPixels: number): Header => {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  const fail = (): never => {
    throw new ImageDecodeFailed({ mimeType, message: 'Invalid or truncated image header' })
  }
  const need = (offset: number, length: number) => {
    if (offset < 0 || length < 0 || offset > bytes.length - length) fail()
  }
  const u8 = (offset: number) => {
    need(offset, 1)
    return view.getUint8(offset)
  }
  const u16 = (offset: number, little = false) => {
    need(offset, 2)
    return view.getUint16(offset, little)
  }
  const u32 = (offset: number, little = false) => {
    need(offset, 4)
    return view.getUint32(offset, little)
  }
  const text = (offset: number, length: number) => {
    need(offset, length)
    let result = ''
    for (let i = offset; i < offset + length; i++) result += String.fromCharCode(u8(i))
    return result
  }
  const dimensions = (width: number, height: number): Dimensions => {
    if (!Number.isSafeInteger(width) || !Number.isSafeInteger(height) || width <= 0 || height <= 0)
      fail()
    if (width > Math.floor(maxInputPixels / height)) {
      throw new ImageInputPixelsExceeded({ width, height, maxInputPixels })
    }
    return { width, height }
  }
  const exif = (start: number, length: number): Orientation => {
    need(start, length)
    if (length >= 6 && text(start, 6) === 'Exif\0\0') {
      start += 6
      length -= 6
    }
    const end = start + length
    if (length < 8) fail()
    const order = text(start, 2)
    if (order !== 'II' && order !== 'MM') fail()
    const little = order === 'II'
    if (u16(start + 2, little) !== 42) fail()
    const directory = start + u32(start + 4, little)
    if (directory < start || directory > end - 2) fail()
    const count = u16(directory, little)
    if (count > Math.floor((end - directory - 2) / 12)) fail()
    for (let i = 0; i < count; i++) {
      const entry = directory + 2 + i * 12
      if (u16(entry, little) !== 0x112) continue
      if (u16(entry + 2, little) !== 3 || u32(entry + 4, little) !== 1) fail()
      const value = u16(entry + 8, little)
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
          return fail()
      }
    }
    return 1
  }
  let orientation: Orientation = 1
  let animated = false
  if (mimeType === 'image/png') {
    if (text(0, 8) !== '\x89PNG\r\n\x1a\n' || text(12, 4) !== 'IHDR' || u32(8) !== 13) fail()
    const size = dimensions(u32(16), u32(20))
    let ended = false
    for (let offset = 8; offset < bytes.length;) {
      const length = u32(offset)
      need(offset + 8, length + 4)
      const kind = text(offset + 4, 4)
      if (kind === 'eXIf') orientation = exif(offset + 8, length)
      if (kind === 'acTL') animated = u32(offset + 8) > 1
      if (kind === 'fcTL') dimensions(u32(offset + 12), u32(offset + 16))
      offset += length + 12
      if (kind === 'IEND') {
        ended = true
        break
      }
    }
    if (!ended) fail()
    return { dimensions: size, orientation, animated }
  }
  if (mimeType === 'image/jpeg') {
    if (u16(0) !== 0xffd8 || u16(bytes.length - 2) !== 0xffd9) fail()
    let size: Dimensions | undefined
    for (let offset = 2; offset < bytes.length;) {
      if (u8(offset++) !== 0xff) fail()
      while (u8(offset) === 0xff) offset++
      const marker = u8(offset++)
      if (marker === 0xda || marker === 0xd9) break
      if (marker === 0x01 || (marker >= 0xd0 && marker <= 0xd7)) continue
      const length = u16(offset)
      if (length < 2) fail()
      need(offset, length)
      if (
        [0xc0, 0xc1, 0xc2, 0xc3, 0xc5, 0xc6, 0xc7, 0xc9, 0xca, 0xcb, 0xcd, 0xce, 0xcf].includes(
          marker,
        )
      ) {
        if (length < 8) fail()
        size = dimensions(u16(offset + 5), u16(offset + 3))
      }
      if (marker === 0xe1 && length >= 8 && text(offset + 2, 6) === 'Exif\0\0') {
        orientation = exif(offset + 2, length - 2)
      }
      offset += length
    }
    if (!size) return fail()
    return { dimensions: size, orientation, animated: false }
  }
  if (mimeType === 'image/gif') {
    if (!['GIF87a', 'GIF89a'].includes(text(0, 6))) fail()
    const size = dimensions(u16(6, true), u16(8, true))
    let offset = 13
    const packed = u8(10)
    if (packed & 0x80) offset += 3 * 2 ** ((packed & 7) + 1)
    const blocks = () => {
      for (;;) {
        const length = u8(offset++)
        if (length === 0) return
        need(offset, length)
        offset += length
      }
    }
    let frames = 0
    let ended = false
    while (offset < bytes.length) {
      const marker = u8(offset++)
      if (marker === 0x3b) {
        ended = true
        break
      }
      if (marker === 0x21) {
        u8(offset++)
        blocks()
        continue
      }
      if (marker !== 0x2c) fail()
      const left = u16(offset, true),
        top = u16(offset + 2, true)
      const frame = dimensions(u16(offset + 4, true), u16(offset + 6, true))
      if (left + frame.width > size.width || top + frame.height > size.height) fail()
      const local = u8(offset + 8)
      offset += 9
      if (local & 0x80) offset += 3 * 2 ** ((local & 7) + 1)
      u8(offset++)
      blocks()
      frames++
    }
    if (!ended || frames === 0) fail()
    return { dimensions: size, orientation, animated: frames > 1 }
  }
  if (mimeType === 'image/bmp') {
    if (text(0, 2) !== 'BM') fail()
    const length = u32(14, true)
    let size: Dimensions
    if (length === 12) size = dimensions(u16(18, true), u16(20, true))
    else {
      if (length < 40) fail()
      need(14, length)
      size = dimensions(view.getInt32(18, true), Math.abs(view.getInt32(22, true)))
    }
    need(u32(10, true), 1)
    return { dimensions: size, orientation, animated: false }
  }
  if (text(0, 4) !== 'RIFF' || text(8, 4) !== 'WEBP' || u32(4, true) + 8 !== bytes.length) fail()
  const u24 = (offset: number) => u8(offset) + u8(offset + 1) * 256 + u8(offset + 2) * 65536
  const webpSize = (kind: string, start: number, length: number): Dimensions | undefined => {
    if (kind === 'VP8X') {
      if (length !== 10) fail()
      animated = (u8(start) & 2) !== 0
      return dimensions(u24(start + 4) + 1, u24(start + 7) + 1)
    }
    if (kind === 'VP8 ') {
      if (length < 10 || text(start + 3, 3) !== '\x9d\x01\x2a') fail()
      return dimensions(u16(start + 6, true) & 0x3fff, u16(start + 8, true) & 0x3fff)
    }
    if (kind === 'VP8L') {
      if (length < 5 || u8(start) !== 0x2f) fail()
      const bits = u32(start + 1, true)
      return dimensions((bits & 0x3fff) + 1, ((bits >>> 14) & 0x3fff) + 1)
    }
    return undefined
  }
  let size: Dimensions | undefined
  for (let offset = 12; offset < bytes.length;) {
    const kind = text(offset, 4),
      length = u32(offset + 4, true),
      start = offset + 8
    need(start, length)
    const found = webpSize(kind, start, length)
    if (found) {
      if (size && (found.width > size.width || found.height > size.height)) fail()
      size ??= found
    }
    if (kind === 'EXIF') orientation = exif(start, length)
    if (kind === 'ANMF') {
      if (length < 16) fail()
      const frame = dimensions(u24(start + 6) + 1, u24(start + 9) + 1)
      if (
        !size ||
        u24(start) * 2 + frame.width > size.width ||
        u24(start + 3) * 2 + frame.height > size.height
      )
        fail()
      for (let nested = start + 16; nested < start + length;) {
        const n = u32(nested + 4, true)
        if (nested + 8 + n > start + length) fail()
        webpSize(text(nested, 4), nested + 8, n)
        nested += n + 8 + (n & 1)
      }
    }
    offset += length + 8 + (length & 1)
  }
  if (!size) return fail()
  return { dimensions: size, orientation, animated }
}

export const inspect = Effect.fn('ImagesWasm.inspect')(function* (
  bytes: Uint8Array,
  mimeType: InputMimeType,
  maxInputPixels: number,
) {
  return yield* Effect.try({
    try: () => read(bytes, mimeType, maxInputPixels),
    catch: (cause) =>
      cause instanceof ImageInputPixelsExceeded || cause instanceof ImageDecodeFailed
        ? cause
        : new ImageDecodeFailed({ mimeType, message: 'Invalid image header', cause }),
  })
})
