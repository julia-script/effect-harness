import type { Dimensions } from '../ImagePreparation.js'
import { ImageDecodeFailed, ImageInputPixelsExceeded } from '../ImagePreparationError.js'

export interface Frame extends Dimensions {
  readonly left: number
  readonly top: number
  readonly blend: number
  readonly useIdat: boolean
  readonly data: Array<Uint8Array>
}

export interface Container {
  readonly header: Uint8Array
  readonly colour: ReadonlyArray<Uint8Array>
  readonly defaultData: ReadonlyArray<Uint8Array>
  readonly frames: ReadonlyArray<Frame>
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

/** Bounded container validation only. Callers must fully decode every returned frame.
 * Chunk payloads borrow the input; standalone containers are built one frame at a time.
 */
export const read = (bytes: Uint8Array, maxInputPixels: number): Container => {
  const fail = (): never => {
    throw new ImageDecodeFailed({
      mimeType: 'image/png',
      message: 'Malformed PNG animation or checksum',
    })
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  const colour: Array<Uint8Array> = []
  const frames: Array<Frame> = []
  const defaultData: Array<Uint8Array> = []
  let header: Uint8Array | undefined
  let width = 0,
    height = 0,
    count = 0,
    sequence = 0
  let startedData = false,
    endedData = false,
    ended = false
  let current: Frame | undefined
  const guard = (w: number, h: number) => {
    if (w === 0 || h === 0) fail()
    if (w > Math.floor(maxInputPixels / h))
      throw new ImageInputPixelsExceeded({ width: w, height: h, maxInputPixels })
  }
  for (let offset = 8; offset < bytes.length;) {
    if (offset > bytes.length - 12) fail()
    const length = view.getUint32(offset)
    if (length > bytes.length - offset - 12) fail()
    const end = offset + length + 12
    if (crc32(bytes.subarray(offset + 4, end - 4)) !== view.getUint32(end - 4)) fail()
    const kindBytes = bytes.subarray(offset + 4, offset + 8)
    if (
      kindBytes.some((byte) => !((byte >= 65 && byte <= 90) || (byte >= 97 && byte <= 122))) ||
      (kindBytes[2]! & 32) !== 0
    )
      fail()
    const kind = String.fromCharCode(...kindBytes)
    const data = bytes.subarray(offset + 8, end - 4)
    const dv = new DataView(data.buffer, data.byteOffset, data.byteLength)
    if (startedData && kind !== 'IDAT') endedData = true
    switch (kind) {
      case 'IHDR':
        if (header || offset !== 8 || length !== 13) fail()
        header = data
        width = dv.getUint32(0)
        height = dv.getUint32(4)
        guard(width, height)
        break
      case 'acTL':
        if (length !== 8 || count !== 0 || startedData) fail()
        count = dv.getUint32(0)
        if (count === 0) fail()
        break
      case 'fcTL': {
        if (length !== 26 || count === 0 || dv.getUint32(0) !== sequence++) fail()
        if (current && current.data.length === 0) fail()
        const w = dv.getUint32(4),
          h = dv.getUint32(8)
        const left = dv.getUint32(12),
          top = dv.getUint32(16)
        guard(w, h)
        if (w > width - left || h > height - top || data[24]! > 2 || data[25]! > 1) fail()
        if (!startedData && (w !== width || h !== height || left !== 0 || top !== 0)) fail()
        current = {
          width: w,
          height: h,
          left,
          top,
          blend: data[25]!,
          useIdat: !startedData,
          data: [],
        }
        frames.push(current)
        if (frames.length > count) fail()
        break
      }
      case 'IDAT':
        if (!header || endedData || (current && !current.useIdat)) fail()
        defaultData.push(data)
        if (current) current.data.push(data)
        startedData = true
        break
      case 'fdAT':
        if (
          length <= 4 ||
          !current ||
          current.useIdat ||
          !startedData ||
          dv.getUint32(0) !== sequence++
        )
          fail()
        current!.data.push(data.subarray(4))
        break
      case 'IEND':
        if (length !== 0 || end !== bytes.length || !startedData) fail()
        ended = true
        break
      default:
        if ((kindBytes[0]! & 32) === 0 && kind !== 'PLTE') fail()
        if (
          kind === 'PLTE' &&
          (startedData ||
            colour.some((part) => String.fromCharCode(...part.subarray(4, 8)) === 'PLTE') ||
            length === 0 ||
            length % 3 !== 0 ||
            length > 768)
        )
          fail()
        if (!startedData && ['PLTE', 'tRNS', 'sRGB', 'gAMA', 'cHRM', 'iCCP', 'sBIT'].includes(kind))
          colour.push(bytes.subarray(offset, end))
    }
    offset = end
  }
  if (!header || !ended || frames.length !== count || (current && current.data.length === 0)) fail()
  return { header: header!, colour, defaultData, frames }
}

export const image = (container: Container, frame?: Frame): Uint8Array => {
  const header = Uint8Array.from(container.header)
  if (frame) {
    const view = new DataView(header.buffer)
    view.setUint32(0, frame.width)
    view.setUint32(4, frame.height)
  }
  const parts = [
    new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10]),
    chunk('IHDR', header),
    ...container.colour,
    ...(frame?.data ?? container.defaultData).map((data) => chunk('IDAT', data)),
    chunk('IEND', new Uint8Array(0)),
  ]
  const bytes = new Uint8Array(parts.reduce((n, part) => n + part.length, 0))
  let offset = 0
  for (const part of parts) {
    bytes.set(part, offset)
    offset += part.length
  }
  return bytes
}

/** The first animation frame starts on a transparent canvas, even with an excluded default still.
 * SOURCE copies RGBA; OVER onto transparent leaves fully transparent pixels transparent black.
 */
export const firstCanvas = (pixels: Uint8Array, frame: Frame, size: Dimensions): Uint8Array => {
  if (pixels.length !== frame.width * frame.height * 4)
    throw new ImageDecodeFailed({
      mimeType: 'image/png',
      message: 'Invalid animation frame pixels',
    })
  const canvas = new Uint8Array(size.width * size.height * 4)
  for (let y = 0; y < frame.height; y++) {
    for (let x = 0; x < frame.width; x++) {
      const p = (y * frame.width + x) * 4
      if (frame.blend === 1 && pixels[p + 3] === 0) continue
      canvas.set(pixels.subarray(p, p + 4), ((y + frame.top) * size.width + x + frame.left) * 4)
    }
  }
  return canvas
}
