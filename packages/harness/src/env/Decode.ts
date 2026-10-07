import * as Predicate from 'effect/Predicate'
// Adapted from pi-durable (MIT), pinned 636703a0; see ../LICENSE.pi.txt.
export const rangeDecoder = (): TextDecoder => new TextDecoder('utf-8', { ignoreBOM: true })
export const startsWithBom = (bytes: Uint8Array): boolean =>
  bytes[0] === 0xef && bytes[1] === 0xbb && bytes[2] === 0xbf
const TypeId = '~@effect-harness/harness/env/Decode'
export interface Decoder {
  readonly [TypeId]: typeof TypeId
  readonly decoder: TextDecoder
  started: boolean
}
export const isDecoder = (input: unknown): input is Decoder => Predicate.hasProperty(input, TypeId)
export const make = (): Decoder => {
  const handle: Decoder = { [TypeId]: TypeId, decoder: rangeDecoder(), started: false }
  Object.defineProperty(handle, TypeId, { enumerable: false })
  return handle
}
export function decode(state: Decoder, bytes?: Uint8Array): string {
  const text =
    bytes === undefined ? state.decoder.decode() : state.decoder.decode(bytes, { stream: true })
  if (state.started || text === '') return text
  state.started = true
  return text.startsWith('\ufeff') ? text.slice(1) : text
}
