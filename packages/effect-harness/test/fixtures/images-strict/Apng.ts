/** Test containers use real codec-produced IDAT streams and recomputed CRCs. */
export const chunk = (kind: string, data: Uint8Array): Buffer => {
  const result = Buffer.alloc(data.length + 12)
  result.writeUInt32BE(data.length)
  result.write(kind, 4)
  result.set(data, 8)
  let crc = 0xffffffff
  for (const byte of result.subarray(4, -4)) {
    crc ^= byte
    for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ (crc & 1 ? 0xedb88320 : 0)
  }
  result.writeUInt32BE((crc ^ 0xffffffff) >>> 0, result.length - 4)
  return result
}

export const parts = (bytes: Uint8Array) => {
  const result: Array<{ kind: string; data: Buffer }> = []
  const buffer = Buffer.from(bytes)
  for (let offset = 8; offset < buffer.length;) {
    const length = buffer.readUInt32BE(offset)
    result.push({
      kind: buffer.toString('ascii', offset + 4, offset + 8),
      data: buffer.subarray(offset + 8, offset + 8 + length),
    })
    offset += length + 12
  }
  return result
}

export interface Frame {
  readonly png: Uint8Array
  readonly left?: number
  readonly top?: number
  readonly blend?: number
  readonly corrupt?: boolean
}

export const apng = (
  still: Uint8Array,
  frames: ReadonlyArray<Frame>,
  included: boolean,
): Buffer => {
  const common = parts(still)
  const ihdr = common.find((part) => part.kind === 'IHDR')!.data
  const control = Buffer.alloc(8)
  control.writeUInt32BE(frames.length)
  const chunks = [Buffer.from(still.subarray(0, 8)), chunk('IHDR', ihdr), chunk('acTL', control)]
  for (const part of common) {
    if (['PLTE', 'tRNS'].includes(part.kind)) chunks.push(chunk(part.kind, part.data))
  }
  const idat = (png: Uint8Array) =>
    Buffer.concat(
      parts(png)
        .filter((part) => part.kind === 'IDAT')
        .map((part) => part.data),
    )
  if (!included) chunks.push(chunk('IDAT', idat(still)))
  let sequence = 0
  frames.forEach((frame, index) => {
    const header = parts(frame.png).find((part) => part.kind === 'IHDR')!.data
    const fc = Buffer.alloc(26)
    fc.writeUInt32BE(sequence++)
    fc.writeUInt32BE(header.readUInt32BE(0), 4)
    fc.writeUInt32BE(header.readUInt32BE(4), 8)
    fc.writeUInt32BE(frame.left ?? 0, 12)
    fc.writeUInt32BE(frame.top ?? 0, 16)
    fc.writeUInt16BE(1, 20)
    fc.writeUInt16BE(10, 22)
    fc[25] = frame.blend ?? 0
    chunks.push(chunk('fcTL', fc))
    const pixels = frame.corrupt ? Buffer.from([1, 2, 3, 4]) : idat(frame.png)
    if (included && index === 0) chunks.push(chunk('IDAT', pixels))
    else {
      const seq = Buffer.alloc(4)
      seq.writeUInt32BE(sequence++)
      chunks.push(chunk('fdAT', Buffer.concat([seq, pixels])))
    }
  })
  chunks.push(chunk('IEND', Buffer.alloc(0)))
  return Buffer.concat(chunks)
}

export const mutate = (bytes: Uint8Array, kind: string, change: (data: Buffer) => void) => {
  let changed = false
  return Buffer.concat([
    Buffer.from(bytes.subarray(0, 8)),
    ...parts(bytes).map((part) => {
      const data = Buffer.from(part.data)
      if (part.kind === kind && !changed) {
        change(data)
        changed = true
      }
      return chunk(part.kind, data)
    }),
  ])
}
