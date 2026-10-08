/**
 * Incremental UTF-8 decoding with explicit byte-order-mark handling.
 */
import * as Pipeable from 'effect/Pipeable'
import * as Inspectable from 'effect/Inspectable'
import { FileError, FileUnknown } from '../Env.ts'
import * as Effect from 'effect/Effect'
import * as Predicate from 'effect/Predicate'
// Adapted from pi-durable (MIT), pinned 636703a0; see package NOTICE.
/**
 * Creates a UTF-8 decoder that retains byte-order marks as range text.
 *
 * @category combinators
 */
export const rangeDecoder = (): TextDecoder => new TextDecoder('utf-8', { ignoreBOM: true })
/**
 * Checks the initial UTF-8 byte-order-mark bytes.
 *
 * @category guards
 */
export const hasBom = (u: Uint8Array): boolean => u[0] === 0xef && u[1] === 0xbb && u[2] === 0xbf
const TypeId = '~@effect-harness/harness/env/Decode'
/**
 * Incremental text decoder selected from the file’s leading byte window.
 *
 * @category models
 */
export interface Decoder extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [TypeId]: typeof TypeId
  readonly decoder: TextDecoder
  started: boolean
}
/**
 * Checks whether a value carries the nominal `Decoder` marker.
 *
 * **Gotchas**
 *
 * This checks library identity, not the validity of arbitrary fields or stored JSON.
 *
 * @category guards
 */
export const isDecoder = (u: unknown): u is Decoder => Predicate.hasProperty(u, TypeId)
/**
 * Creates a fresh mutable incremental UTF-8 decoder with BOM state.
 *
 * @category constructors
 */
export const make = (): Decoder => {
  const handle: Decoder = Object.assign(Object.create(DecoderProto), {
    [TypeId]: TypeId,
    decoder: rangeDecoder(),
    started: false,
  })
  Object.defineProperty(handle, TypeId, { enumerable: false })
  return handle
}
/**
 * Decodes and advances BOM state synchronously; native decoder or accessor faults can throw.
 *
 * @category unsafe
 */
export function decodeUnsafe(self: Decoder, bytes?: Uint8Array): string {
  const text =
    bytes === undefined ? self.decoder.decode() : self.decoder.decode(bytes, { stream: true })
  if (self.started || text === '') return text
  self.started = true
  return text.startsWith('\ufeff') ? text.slice(1) : text
}

/**
 * Decodes input through the typed boundary.
 *
 * @category combinators
 */
export const decode = (self: Decoder, bytes?: Uint8Array): Effect.Effect<string, FileError> =>
  Effect.try({
    try: () => decodeUnsafe(self, bytes),
    catch: (cause) =>
      new FileError({ reason: new FileUnknown({ message: 'Unable to decode file bytes', cause }) }),
  })

const DecoderProto = {
  ...Pipeable.Prototype,
  ...Inspectable.BaseProto,
  toJSON(): unknown {
    return { _id: '@effect-harness/harness/env/Decode/Decoder' }
  },
}
