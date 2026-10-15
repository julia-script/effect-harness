const prefix = (bytes: Uint8Array, signature: ReadonlyArray<number>, offset = 0) =>
  signature.every((value, index) => bytes[offset + index] === value)
const ascii = (bytes: Uint8Array, value: string, offset = 0) =>
  prefix(
    bytes,
    Array.from(value, (character) => character.charCodeAt(0)),
    offset,
  )

/** Only bounded header bytes are inspected; full content validation belongs to ImagePreparation. */
export const detect = (bytes: Uint8Array): string | undefined => {
  if (prefix(bytes, [0x89, 0x50, 0x4e, 0x47])) return 'image/png'
  if (prefix(bytes, [0xff, 0xd8])) return 'image/jpeg'
  if (ascii(bytes, 'GIF8')) return 'image/gif'
  if (ascii(bytes, 'RIFF') && ascii(bytes, 'WEBP', 8)) return 'image/webp'
  if (ascii(bytes, 'BM')) return 'image/bmp'
  if (prefix(bytes, [0x49, 0x49, 0x2a, 0]) || prefix(bytes, [0x4d, 0x4d, 0, 0x2a]))
    return 'image/tiff'
  if (prefix(bytes, [0, 0, 1, 0])) return 'image/x-icon'
  if (ascii(bytes, 'ftyp', 4)) return 'image/unsupported-container'
  return undefined
}
