import { register } from 'node:module'
register(new URL('./IsolationLoader.mjs', import.meta.url))
await import('../../../src/index.ts')
await import('../../../src/ImagePreparation.ts')
await import('../../../src/images-node/index.ts')
await import('../../../src/images-wasm/index.ts')
console.log('Core and adapter declarations import without sharp, Bun or Photon')
