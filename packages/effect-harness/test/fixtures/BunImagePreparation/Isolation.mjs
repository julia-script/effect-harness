import { register } from 'node:module'
import assert from 'node:assert/strict'

register(new URL('./IsolationLoader.mjs', import.meta.url))
await import('../../../src/index.ts')
await import('../../../src/ImagePreparation.ts')
const Native = await import('../../../src/images-bun/index.ts')
const Effect = await import('effect/Effect')
assert.deepEqual((await Effect.runPromise(Native.adapter)).capabilities.decode, [])
console.log('Portable core and Bun declarations import without native codecs')
