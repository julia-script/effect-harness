/**
 * Image formats from file headers, ported from pi's `tools/image.ts`: which supported format a
 * file is, and the size its header declares, without decoding it.
 */
import * as Base64 from 'effect/encoding/Base64'

const PNG_SIGNATURE = [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]

/** Formats every provider takes inline; others, such as BMP, need converting. */
export const INLINE_IMAGE_TYPES: ReadonlySet<string> = new Set([
  'image/png',
  'image/jpeg',
  'image/gif',
  'image/webp',
])

/** Limits an image the model sees must keep: the coding agent's, below Anthropic's 5 MB. */
export const IMAGE_LIMITS = { maxWidth: 2000, maxHeight: 2000, maxBytes: 4.5 * 1024 * 1024 }

const byte = (buffer: Uint8Array, offset: number) => buffer[offset] ?? 0

const uint16LE = (buffer: Uint8Array, offset: number) =>
  byte(buffer, offset) + (byte(buffer, offset + 1) << 8)

const uint16BE = (buffer: Uint8Array, offset: number) =>
  (byte(buffer, offset) << 8) + byte(buffer, offset + 1)

const uint32BE = (buffer: Uint8Array, offset: number) =>
  byte(buffer, offset) * 0x1000000 +
  (byte(buffer, offset + 1) << 16) +
  (byte(buffer, offset + 2) << 8) +
  byte(buffer, offset + 3)

const uint32LE = (buffer: Uint8Array, offset: number) =>
  byte(buffer, offset) +
  (byte(buffer, offset + 1) << 8) +
  (byte(buffer, offset + 2) << 16) +
  byte(buffer, offset + 3) * 0x1000000

const startsWith = (buffer: Uint8Array, bytes: ReadonlyArray<number>) =>
  buffer.length >= bytes.length && bytes.every((each, index) => buffer[index] === each)

const ascii = (buffer: Uint8Array, offset: number, text: string) => {
  if (buffer.length < offset + text.length) return false
  for (let index = 0; index < text.length; index++)
    if (buffer[offset + index] !== text.codePointAt(index)) return false
  return true
}

const isPng = (buffer: Uint8Array) =>
  buffer.length >= 16 && uint32BE(buffer, PNG_SIGNATURE.length) === 13 && ascii(buffer, 12, 'IHDR')

/** An APNG has an `acTL` chunk before its first `IDAT`. */
const isAnimatedPng = (buffer: Uint8Array) => {
  let offset = PNG_SIGNATURE.length
  while (offset + 8 <= buffer.length) {
    if (ascii(buffer, offset + 4, 'acTL')) return true
    if (ascii(buffer, offset + 4, 'IDAT')) return false
    const next = offset + 8 + uint32BE(buffer, offset) + 4
    if (next <= offset || next > buffer.length) return false
    offset = next
  }
  return false
}

const BMP_DEPTHS = new Set([1, 4, 8, 16, 24, 32])

const isBmp = (buffer: Uint8Array) => {
  if (buffer.length < 26) return false
  const fileSize = uint32LE(buffer, 2)
  const pixels = uint32LE(buffer, 10)
  const header = uint32LE(buffer, 14)
  if ((fileSize !== 0 && fileSize < 26) || pixels < 14 + header) return false
  if (fileSize !== 0 && pixels >= fileSize) return false
  const at = header === 12 ? 22 : 26
  if (header !== 12 && (header < 40 || header > 124 || buffer.length < 30)) return false
  return uint16LE(buffer, at) === 1 && BMP_DEPTHS.has(uint16LE(buffer, at + 2))
}

/** The supported image format of a file, from its bytes; animated PNGs and others are not. */
export const mimeTypeOf = (buffer: Uint8Array): string | undefined => {
  if (startsWith(buffer, [0xff, 0xd8, 0xff])) return buffer[3] === 0xf7 ? undefined : 'image/jpeg'
  if (startsWith(buffer, PNG_SIGNATURE))
    return isPng(buffer) && !isAnimatedPng(buffer) ? 'image/png' : undefined
  if (ascii(buffer, 0, 'GIF87a') || ascii(buffer, 0, 'GIF89a')) return 'image/gif'
  if (ascii(buffer, 0, 'RIFF') && ascii(buffer, 8, 'WEBP')) return 'image/webp'
  return ascii(buffer, 0, 'BM') && isBmp(buffer) ? 'image/bmp' : undefined
}

interface Size {
  readonly width: number
  readonly height: number
}

const webpSize = (bytes: Uint8Array): Size | undefined => {
  if (bytes.length < 30) return undefined
  if (ascii(bytes, 12, 'VP8 '))
    return { width: uint16LE(bytes, 26) & 0x3fff, height: uint16LE(bytes, 28) & 0x3fff }
  if (ascii(bytes, 12, 'VP8L')) {
    const bits = uint32LE(bytes, 21)
    return { width: (bits & 0x3fff) + 1, height: ((bits >>> 14) & 0x3fff) + 1 }
  }
  if (!ascii(bytes, 12, 'VP8X')) return undefined
  const uint24 = (offset: number) => uint16LE(bytes, offset) + (byte(bytes, offset + 2) << 16)
  return { width: uint24(24) + 1, height: uint24(27) + 1 }
}

/** SOF0 to SOF15, except DHT (C4), JPG (C8) and DAC (CC), which share the range. */
const isFrameStart = (marker: number) =>
  marker >= 0xc0 && marker <= 0xcf && marker !== 0xc4 && marker !== 0xc8 && marker !== 0xcc

const jpegSize = (bytes: Uint8Array): Size | undefined => {
  let offset = 2
  while (offset + 9 <= bytes.length) {
    if (bytes[offset] !== 0xff) return undefined
    const marker = byte(bytes, offset + 1)
    if (marker === 0xff) offset++
    else if (isFrameStart(marker))
      return { width: uint16BE(bytes, offset + 7), height: uint16BE(bytes, offset + 5) }
    else offset += 2 + uint16BE(bytes, offset + 2)
  }
  return undefined
}

/** The size an image's header declares; `undefined` when it cannot be read. */
export const sizeOf = (bytes: Uint8Array, mimeType: string): Size | undefined => {
  if (mimeType === 'image/png')
    return bytes.length < 24
      ? undefined
      : { width: uint32BE(bytes, 16), height: uint32BE(bytes, 20) }
  if (mimeType === 'image/gif')
    return bytes.length < 10 ? undefined : { width: uint16LE(bytes, 6), height: uint16LE(bytes, 8) }
  if (mimeType === 'image/webp') return webpSize(bytes)
  return mimeType === 'image/jpeg' ? jpegSize(bytes) : undefined
}

/** The length of the base64 of `byteLength` bytes. */
export const base64Length = (byteLength: number) => Math.ceil(byteLength / 3) * 4

/** Standard base64 of `bytes`. */
export const toBase64 = (bytes: Uint8Array) => Base64.encode(bytes)
