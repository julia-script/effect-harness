/**
 * Public namespaces for the retained sibling concepts.
 *
 * @since 0.0.0
 */
// @barrel
/**
 * @since 0.0.0
 */
export * as AtomicWrite from './AtomicWrite.ts'
/**
 * @since 0.0.0
 */
export * as Decode from './Decode.ts'
/**
 * @since 0.0.0
 */
export * as LineScan from './LineScan.ts'
/**
 * @since 0.0.0
 */
export * as NativeError from './NativeError.ts'
// effect-review-allow P9-barrel-namespace-only: the Node compatibility facade is host-specific and remains an explicit leaf import; portable decoding and scanning concepts are exported here.
