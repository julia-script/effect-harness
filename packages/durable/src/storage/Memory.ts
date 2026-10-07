/**
 * Compatibility entrypoint for the scoped in-memory Store.
 *
 * @since 0.0.0
 */
// effect-review-allow P9-barrel-namespace-only: this supported leaf keeps named memory constructors while the implementation belongs to Store.
export { makeMemory as make, layerMemory as layer, layerStoreMemory } from '../Store.ts'
