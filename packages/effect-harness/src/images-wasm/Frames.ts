import type { InputMimeType } from '../ImagePreparation.js'

/** Standalone encoded frames after the first, for full compressed-data validation.
 * Call only after Headers.inspect has bounded every frame and checked container structure.
 * Reconstruction copies encoded bytes, never pixel buffers, and does not alter the source.
 */
export function* additionalFrames(
  bytes: Uint8Array,
  mimeType: InputMimeType,
): Generator<Uint8Array> {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  if (mimeType === 'image/gif') {
    const table = bytes[10]! & 0x80 ? 3 * 2 ** ((bytes[10]! & 7) + 1) : 0
    const header = bytes.subarray(0, 13 + table)
    let offset = header.length
    let control = new Uint8Array(0)
    let first = true
    const blocks = () => {
      for (;;) {
        const length = bytes[offset++]!
        if (length === 0) return
        offset += length
      }
    }
    while (offset < bytes.length) {
      const start = offset
      const marker = bytes[offset++]!
      if (marker === 0x3b) break
      if (marker === 0x21) {
        const label = bytes[offset++]!
        blocks()
        if (label === 0xf9) control = bytes.slice(start, offset)
        continue
      }
      const local = bytes[offset + 8]!
      offset += 9
      if (local & 0x80) offset += 3 * 2 ** ((local & 7) + 1)
      offset++
      blocks()
      if (first) first = false
      else {
        const result = new Uint8Array(header.length + control.length + offset - start + 1)
        result.set(header)
        result.set(control, header.length)
        result.set(bytes.subarray(start, offset), header.length + control.length)
        result[result.length - 1] = 0x3b
        yield result
      }
      control = new Uint8Array(0)
    }
  }
  if (mimeType === 'image/webp') {
    let first = true
    for (let offset = 12; offset < bytes.length;) {
      const length = view.getUint32(offset + 4, true)
      if (view.getUint32(offset) === 0x414e4d46) {
        // ANMF
        if (first) first = false
        else {
          const start = offset + 8
          const payload = bytes.subarray(start + 16, start + length)
          const result = new Uint8Array(30 + payload.length)
          const out = new DataView(result.buffer)
          result.set([82, 73, 70, 70], 0)
          out.setUint32(4, result.length - 8, true)
          result.set([87, 69, 66, 80, 86, 80, 56, 88], 8)
          out.setUint32(16, 10, true)
          // Preserve ALPH signaling and exact frame dimensions from the guarded ANMF header.
          if (payload[0] === 65 && payload[1] === 76 && payload[2] === 80 && payload[3] === 72)
            result[20] = 16
          result.set(bytes.subarray(start + 6, start + 12), 24)
          result.set(payload, 30)
          yield result
        }
      }
      offset += 8 + length + (length & 1)
    }
  }
}
