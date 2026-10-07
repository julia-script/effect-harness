/**
 * Public namespaces for the retained sibling concepts.
 */
// @barrel
/**
 * Atomic native file replacement with canonical destinations and settled writes.
 *
 * @category re-exports
 */
export * as AtomicWrite from './AtomicWrite.ts'
/**
 * Incremental UTF-8 decoding with explicit byte-order-mark handling.
 *
 * @category re-exports
 */
export * as Decode from './Decode.ts'
/**
 * Incremental byte scanning for exact text-line selection offsets.
 *
 * @category re-exports
 */
export * as LineScan from './LineScan.ts'
/**
 * Safe coercion of foreign filesystem error codes.
 *
 * @category re-exports
 */
export * as NativeError from './NativeError.ts'
// effect-review-allow P9-barrel-namespace-only: the Node compatibility facade is host-specific and remains an explicit leaf import; portable decoding and scanning concepts are exported here.
