import assert from 'node:assert/strict'
import * as Effect from 'effect/Effect'
import * as Images from '../../../src/ImagePreparation.js'

assert.equal(typeof Bun.Image, 'function')
// Launched with Bun --no-addons: the actual optional native validator is
// unavailable, while Bun.Image itself remains real and available.
const Native = await import('../../../src/images-bun/index.js')
const adapter = await Effect.runPromise(Native.adapter.pipe(Effect.result))
assert.equal(adapter._tag, 'Failure')
if (adapter._tag === 'Failure') assert.equal(adapter.failure._tag, 'UnsupportedImage')
const prepared = await Effect.runPromise(
  Images.prepare(new Uint8Array(1), 'image/png', {
    maxWidth: 1,
    maxHeight: 1,
    maxInputPixels: 1,
    maxBytes: 100,
  }).pipe(Effect.provide(Native.layer), Effect.result),
)
assert.equal(prepared._tag, 'Failure')
if (prepared._tag === 'Failure') assert.equal(prepared.failure._tag, 'UnsupportedImage')
console.log('Unavailable validator is a typed Layer construction error')
