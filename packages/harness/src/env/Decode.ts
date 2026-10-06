// Adapted from pi-durable (MIT), pinned 636703a0; see ../LICENSE.pi.txt.
export const rangeDecoder = (): TextDecoder => new TextDecoder('utf-8', { ignoreBOM: true })
export const startsWithBom = (bytes: Uint8Array): boolean =>
  bytes[0] === 0xef && bytes[1] === 0xbb && bytes[2] === 0xbf
export interface Decoder {
  readonly decoder: TextDecoder
  started: boolean
}
export const make = (): Decoder => ({ decoder: rangeDecoder(), started: false })
export function decode(state: Decoder, bytes?: Uint8Array): string {
  const text =
    bytes === undefined ? state.decoder.decode() : state.decoder.decode(bytes, { stream: true })
  if (state.started || text === '') return text
  state.started = true
  return text.startsWith('\ufeff') ? text.slice(1) : text
}
