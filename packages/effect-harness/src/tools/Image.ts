/**
 * Image signatures, MIME recognition and model-visible file parts.
 */
import * as Option from 'effect/Option'
// Adapted from pi-durable (MIT), pinned 636703a0; see package NOTICE.
const PNG_SIGNATURE = [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]
/** Bytes every check except the APNG chunk walk needs: BMP reads up to offset 29. */
const HEADER_BYTES = 32
const BLOCK_BYTES = 64 * 1024

import * as Effect from 'effect/Effect'
/**
 * Bounded random-access byte source used for image-signature recognition.
 *
 * @category models
 */
export interface ByteSource<out E, out R> {
  readonly size: number
  readonly read: (offset: number, length: number) => Effect.Effect<Uint8Array, E, R>
}
/**
 * Recognizes supported image MIME types using bounded reads, rejecting animated PNG.
 *
 * @category combinators
 */
export const detectSupportedImageMimeTypeOf = Effect.fnUntraced(function* <E, R>(
  self: ByteSource<E, R>,
): Effect.fn.Return<Option.Option<string>, E, R> {
  const header = yield* self.read(0, HEADER_BYTES)
  if (!startsWith(header, PNG_SIGNATURE)) return detectSupportedImageMimeType(header)
  if (!isPng(header)) return Option.none()
  let block: Uint8Array = new Uint8Array(0)
  let blockStart = 0
  let offset = PNG_SIGNATURE.length
  while (offset + 8 <= self.size) {
    if (offset < blockStart || offset + 8 > blockStart + block.length) {
      blockStart = offset
      block = yield* self.read(offset, BLOCK_BYTES)
    }
    const chunkHeader = block.subarray(offset - blockStart, offset - blockStart + 8)
    const length = readUint32BE(chunkHeader, 0)
    if (startsWithAscii(chunkHeader, 4, 'acTL')) return Option.none()
    if (startsWithAscii(chunkHeader, 4, 'IDAT')) return Option.some('image/png')
    const next = offset + 8 + length + 4
    if (next <= offset || next > self.size) break
    offset = next
  }
  return Option.some('image/png')
})

/**
 * Recognizes supported image MIME types from available signature bytes.
 *
 * @category combinators
 */
export function detectSupportedImageMimeType(self: Uint8Array): Option.Option<string> {
  if (startsWith(self, [0xff, 0xd8, 0xff]))
    return self[3] === 0xf7 ? Option.none() : Option.some('image/jpeg')
  if (startsWith(self, PNG_SIGNATURE))
    return isPng(self) && !isAnimatedPng(self) ? Option.some('image/png') : Option.none()
  if (startsWithAscii(self, 0, 'GIF87a') || startsWithAscii(self, 0, 'GIF89a'))
    return Option.some('image/gif')
  if (startsWithAscii(self, 0, 'RIFF') && startsWithAscii(self, 8, 'WEBP'))
    return Option.some('image/webp')
  if (startsWithAscii(self, 0, 'BM') && isBmp(self)) return Option.some('image/bmp')
  return Option.none()
}

function isPng(self: Uint8Array): boolean {
  return (
    self.length >= 16 &&
    readUint32BE(self, PNG_SIGNATURE.length) === 13 &&
    startsWithAscii(self, 12, 'IHDR')
  )
}

function isAnimatedPng(self: Uint8Array): boolean {
  let offset = PNG_SIGNATURE.length
  while (offset + 8 <= self.length) {
    const chunkLength = readUint32BE(self, offset)
    const chunkTypeOffset = offset + 4
    if (startsWithAscii(self, chunkTypeOffset, 'acTL')) return true
    if (startsWithAscii(self, chunkTypeOffset, 'IDAT')) return false
    const nextOffset = offset + 8 + chunkLength + 4
    if (nextOffset <= offset || nextOffset > self.length) return false
    offset = nextOffset
  }
  return false
}

function isBmp(self: Uint8Array): boolean {
  if (self.length < 26) return false
  const declaredFileSize = readUint32LE(self, 2)
  const pixelDataOffset = readUint32LE(self, 10)
  const dibHeaderSize = readUint32LE(self, 14)
  if (declaredFileSize !== 0 && declaredFileSize < 26) return false
  if (pixelDataOffset < 14 + dibHeaderSize) return false
  if (declaredFileSize !== 0 && pixelDataOffset >= declaredFileSize) return false

  let colorPlanes: number
  let bitsPerPixel: number
  if (dibHeaderSize === 12) {
    colorPlanes = readUint16LE(self, 22)
    bitsPerPixel = readUint16LE(self, 24)
  } else if (dibHeaderSize >= 40 && dibHeaderSize <= 124) {
    if (self.length < 30) return false
    colorPlanes = readUint16LE(self, 26)
    bitsPerPixel = readUint16LE(self, 28)
  } else {
    return false
  }
  return colorPlanes === 1 && [1, 4, 8, 16, 24, 32].includes(bitsPerPixel)
}

function readUint16LE(self: Uint8Array, offset: number): number {
  return (self[offset] ?? 0) + ((self[offset + 1] ?? 0) << 8)
}

function readUint32BE(self: Uint8Array, offset: number): number {
  return (
    (self[offset] ?? 0) * 0x1000000 +
    ((self[offset + 1] ?? 0) << 16) +
    ((self[offset + 2] ?? 0) << 8) +
    (self[offset + 3] ?? 0)
  )
}

function readUint32LE(self: Uint8Array, offset: number): number {
  return (
    (self[offset] ?? 0) +
    ((self[offset + 1] ?? 0) << 8) +
    ((self[offset + 2] ?? 0) << 16) +
    (self[offset + 3] ?? 0) * 0x1000000
  )
}

function startsWith(self: Uint8Array, bytes: ReadonlyArray<number>): boolean {
  if (self.length < bytes.length) return false
  return bytes.every((byte, index) => self[index] === byte)
}

function startsWithAscii(self: Uint8Array, offset: number, text: string): boolean {
  if (self.length < offset + text.length) return false
  for (let index = 0; index < text.length; index++) {
    if (self[offset + index] !== text.charCodeAt(index)) return false
  }
  return true
}
