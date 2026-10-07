/**
 * Scoped storage service, allocation accessors and memory acquisition.
 *
 * @category re-exports
 */
// effect-review-allow P9-barrel-namespace-only: this supported leaf keeps named memory constructors while the implementation belongs to Store.
export { makeMemory as make, layerMemory as layer, layerStoreMemory } from '../Store.ts'
