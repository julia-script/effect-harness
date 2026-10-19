import * as Effect from 'effect/Effect'
import { ImageDecodeFailed } from '../../ImagePreparationError.js'

interface Frame {
  readonly width: number
  readonly height: number
  readonly left: number
  readonly top: number
  readonly data: ReadonlyArray<Uint8Array>
}

const crc32 = (bytes: Uint8Array): number => {
  let crc = 0xffffffff
  for (const byte of bytes) {
    crc ^= byte
    for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ ((crc & 1) === 0 ? 0 : 0xedb88320)
  }
  return (crc ^ 0xffffffff) >>> 0
}

const chunk = (kind: string, data: Uint8Array): Uint8Array => {
  const bytes = new Uint8Array(data.length + 12)
  const view = new DataView(bytes.buffer)
  view.setUint32(0, data.length)
  for (let i = 0; i < 4; i++) bytes[i + 4] = kind.charCodeAt(i)
  bytes.set(data, 8)
  view.setUint32(bytes.length - 4, crc32(bytes.subarray(4, bytes.length - 4)))
  return bytes
}

const join = (parts: ReadonlyArray<Uint8Array>): Uint8Array => {
  const bytes = new Uint8Array(parts.reduce((length, part) => length + part.length, 0))
  let offset = 0
  for (const part of parts) {
    bytes.set(part, offset)
    offset += part.length
  }
  return bytes
}

/** Verify PNG CRCs and APNG frame rectangles/sequences without allocating pixels.
 * Standalone frame containers retain colour/palette declarations. Each frame's
 * compressed pixels must still be decoded by the caller; framing alone is insufficient.
 */
export const read = Effect.fn('BunImagePreparation.readPngFrames')(function* (bytes: Uint8Array) {
  const fail = () =>
    new ImageDecodeFailed({
      mimeType: 'image/png',
      message: 'Malformed PNG checksum or animation frame',
    })
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  const colour: Array<Uint8Array> = []
  const frames: Array<Frame> = []
  let header: Uint8Array | undefined
  let canvasWidth = 0,
    canvasHeight = 0
  let count = 0,
    sequence = 0,
    startedData = false
  let current:
    | {
        width: number
        height: number
        left: number
        top: number
        useIdat: boolean
        data: Array<Uint8Array>
      }
    | undefined
  for (let p = 8; p < bytes.length;) {
    if (p + 12 > bytes.length) return yield* fail()
    const length = view.getUint32(p)
    const end = p + 12 + length
    if (end > bytes.length || crc32(bytes.subarray(p + 4, end - 4)) !== view.getUint32(end - 4))
      return yield* fail()
    const kind = String.fromCharCode(...bytes.subarray(p + 4, p + 8))
    const data = bytes.subarray(p + 8, end - 4)
    const dv = new DataView(data.buffer, data.byteOffset, data.byteLength)
    if (kind === 'IHDR') {
      if (header !== undefined || p !== 8 || length !== 13) return yield* fail()
      header = data
      canvasWidth = dv.getUint32(0)
      canvasHeight = dv.getUint32(4)
    } else if (kind === 'acTL') {
      if (length !== 8 || count !== 0 || startedData) return yield* fail()
      count = dv.getUint32(0)
      if (count === 0) return yield* fail()
    } else if (kind === 'fcTL') {
      if (length !== 26 || count === 0 || dv.getUint32(0) !== sequence++) return yield* fail()
      if (current !== undefined) {
        if (current.data.length === 0) return yield* fail()
        frames.push(current)
      }
      const width = dv.getUint32(4),
        height = dv.getUint32(8),
        left = dv.getUint32(12),
        top = dv.getUint32(16)
      if (
        width === 0 ||
        height === 0 ||
        left + width > canvasWidth ||
        top + height > canvasHeight ||
        (data[24] ?? 255) > 2 ||
        (data[25] ?? 255) > 1
      )
        return yield* fail()
      if (
        !startedData &&
        (width !== canvasWidth || height !== canvasHeight || left !== 0 || top !== 0)
      )
        return yield* fail()
      current = { width, height, left, top, useIdat: !startedData, data: [] }
    } else if (kind === 'IDAT') {
      if (current !== undefined && !current.useIdat) return yield* fail()
      if (current !== undefined) current.data.push(data)
      startedData = true
    } else if (kind === 'fdAT') {
      if (
        length < 4 ||
        current === undefined ||
        current.useIdat ||
        !startedData ||
        dv.getUint32(0) !== sequence++
      )
        return yield* fail()
      current.data.push(data.subarray(4))
    } else if (
      !startedData &&
      ['PLTE', 'tRNS', 'sRGB', 'gAMA', 'cHRM', 'iCCP', 'sBIT'].includes(kind)
    )
      colour.push(bytes.subarray(p, end))
    p = end
  }
  if (header === undefined) return yield* fail()
  if (current !== undefined) {
    if (current.data.length === 0) return yield* fail()
    frames.push(current)
  }
  if (frames.length !== count) return yield* fail()
  const originalHeader = header
  return frames.map((frame) => {
    const ihdr = Uint8Array.from(originalHeader)
    const dimensions = new DataView(ihdr.buffer)
    dimensions.setUint32(0, frame.width)
    dimensions.setUint32(4, frame.height)
    const png = join([
      bytes.subarray(0, 8),
      chunk('IHDR', ihdr),
      ...colour,
      ...frame.data.map((part) => chunk('IDAT', part)),
      chunk('IEND', new Uint8Array(0)),
    ])
    return { png, left: frame.left, top: frame.top }
  })
})
