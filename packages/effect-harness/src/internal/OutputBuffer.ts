import type { Policy, Skip, Snapshot } from '../OutputRetention.js'

/** Private mutable byte ring. Each byte is visited once; no growing first-line string. */
export interface OutputBuffer {
  readonly policy: Policy
  readonly storage: Uint8Array
  readonly decoder: TextDecoder
  start: number
  length: number
  totalBytes: number
  newlines: number
  endsWithNewline: boolean
}
const encoder = new TextEncoder()
export const make = (policy: Policy): OutputBuffer => ({
  policy: { ...policy },
  storage: new Uint8Array(policy.maxBytes + 4),
  decoder: new TextDecoder('utf-8', { ignoreBOM: true }),
  start: 0,
  length: 0,
  totalBytes: 0,
  newlines: 0,
  endsWithNewline: true,
})
const accept = (self: OutputBuffer, text: string): void => {
  // Decoded chunks are bounded by the producer's chunk; no pending whole-line allocation.
  const bytes = encoder.encode(text)
  self.totalBytes += bytes.length
  for (const byte of bytes) {
    if (byte === 10) self.newlines++
    self.endsWithNewline = byte === 10
    if (self.length < self.storage.length) {
      self.storage[(self.start + self.length) % self.storage.length] = byte
      self.length++
    } else if (self.policy.retain === 'tail') {
      self.storage[self.start] = byte
      self.start = (self.start + 1) % self.storage.length
    }
  }
}
export const append = (self: OutputBuffer, chunk: string | Uint8Array, skip?: Skip): void => {
  if (typeof chunk === 'string' || skip !== undefined) accept(self, self.decoder.decode())
  if (skip !== undefined) {
    self.totalBytes += skip.bytes
    self.newlines += skip.newlines
    self.endsWithNewline = skip.endsWithNewline
    self.start = 0
    self.length = 0
  }
  accept(self, typeof chunk === 'string' ? chunk : self.decoder.decode(chunk, { stream: true }))
}
export const end = (self: OutputBuffer): void => accept(self, self.decoder.decode())
const continuation = (byte: number | undefined): boolean =>
  byte !== undefined && (byte & 0xc0) === 0x80
const lines = (bytes: Uint8Array): number => {
  let count = 0
  for (const byte of bytes) if (byte === 10) count++
  return count + (bytes.length > 0 && bytes[bytes.length - 1] !== 10 ? 1 : 0)
}
export const snapshot = (self: OutputBuffer): Snapshot => {
  const bytes = new Uint8Array(self.length)
  for (let i = 0; i < bytes.length; i++)
    bytes[i] = self.storage[(self.start + i) % self.storage.length] ?? 0
  let from = 0
  let to = bytes.length
  const { maxBytes, maxLines, retain } = self.policy
  if (maxBytes === 0 || maxLines === 0) to = 0
  else if (retain === 'tail') {
    while (continuation(bytes[from])) from++
    let count = 1
    const last = bytes[bytes.length - 1] === 10 ? bytes.length - 2 : bytes.length - 1
    for (let i = last; i >= from; i--) {
      if (bytes[i] !== 10) continue
      if (count === maxLines) {
        from = i + 1
        break
      }
      count++
    }
    if (bytes.length - from > maxBytes) {
      const cut = bytes.length - maxBytes
      const newline = bytes.indexOf(10, cut - 1)
      from = newline !== -1 && newline + 1 < bytes.length ? newline + 1 : cut
      while (continuation(bytes[from])) from++
    }
  } else {
    let count = 0
    for (let i = 0; i < bytes.length; i++)
      if (bytes[i] === 10 && ++count === maxLines) {
        to = i + 1
        break
      }
    if (to > maxBytes) {
      const newline = bytes.lastIndexOf(10, maxBytes - 1)
      to = retain === 'head' && newline !== -1 ? newline + 1 : maxBytes
      while (to > 0 && continuation(bytes[to])) to--
    }
  }
  const kept = bytes.subarray(from, Math.max(from, to))
  const totalLines = self.newlines + (self.totalBytes > 0 && !self.endsWithNewline ? 1 : 0)
  const retainedLines = lines(kept)
  return {
    text: new TextDecoder('utf-8', { ignoreBOM: true }).decode(kept),
    totalBytes: self.totalBytes,
    totalLines,
    retainedBytes: kept.length,
    retainedLines,
    skippedBytes: self.totalBytes - kept.length,
    skippedLines: totalLines - retainedLines,
    truncated: self.totalBytes > kept.length,
  }
}
