import { registerHooks } from 'node:module'
import { existsSync, readFileSync } from 'node:fs'
import assert from 'node:assert/strict'
import * as Effect from 'effect/Effect'
import * as Photon from '@cf-wasm/photon/node'

const missing = process.argv[2] === 'missing'
const jpeg = Uint8Array.from(readFileSync(new URL('../images-native/opaque.jpg', import.meta.url)))
const bad = Uint8Array.from(readFileSync(new URL('./truncated-entropy-90.jpg', import.meta.url)))
const png = Uint8Array.from(readFileSync(new URL('../images-native/opaque.png', import.meta.url)))
registerHooks({
  resolve(specifier, context, next) {
    if (
      specifier === 'sharp' ||
      specifier === 'bun' ||
      specifier.startsWith('node:') ||
      (missing && specifier === 'jpeg-js')
    )
      throw new Error(`Forbidden dependency ${specifier}`)
    if (
      specifier.startsWith('.') &&
      specifier.endsWith('.js') &&
      context.parentURL?.includes('/src/')
    ) {
      const ts = new URL(specifier.slice(0, -3) + '.ts', context.parentURL)
      if (existsSync(ts)) return next(ts.href, context)
    }
    return next(specifier, context)
  },
})
// Verify pure typed-array validation in a supplied, already initialized WASM environment.
// Node lazily constructs web globals through undici, which itself needs Buffer.
// Initialize the host web primitives before disabling Node-specific globals.
void globalThis.Request
void globalThis.Response
void globalThis.Headers
void globalThis.FormData
globalThis.Buffer = undefined
globalThis.Bun = undefined
const Images = await import('../../../src/ImagePreparation.ts')
const Wasm = await import('../../../src/images-wasm/index.ts')
const service = Images.make(Wasm.makeAdapter(Photon))
const limits = { maxWidth: 100, maxHeight: 100, maxBytes: 100000, maxInputPixels: 4096 }
assert.equal((await Effect.runPromise(service.prepare(png, 'image/png', limits))).passthrough, true)
const result = await Effect.runPromise(
  service.prepare(jpeg, 'image/jpeg', limits).pipe(Effect.result),
)
if (missing) {
  assert.equal(result._tag, 'Failure')
  assert.equal(result.failure._tag, 'ImageDecodeFailed')
  assert.match(result.failure.message, /Cannot load strict portable JPEG validator/)
} else {
  assert.equal(result._tag, 'Success')
  assert.equal(result.success.passthrough, true)
  const failure = await Effect.runPromise(
    service.prepare(bad, 'image/jpeg', limits).pipe(Effect.result),
  )
  assert.equal(failure._tag, 'Failure')
  assert.equal(failure.failure._tag, 'ImageDecodeFailed')
}
console.log(
  missing
    ? 'Missing optional JPEG validator fails semantically'
    : 'Portable JPEG validation works without Node, sharp, Bun or Buffer',
)
