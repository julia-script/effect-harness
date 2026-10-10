import assert from 'node:assert/strict'
import { existsSync } from 'node:fs'
import { registerHooks } from 'node:module'
import { fileURLToPath } from 'node:url'

registerHooks({
  resolve(specifier, context, next) {
    if (specifier === 'sharp' || specifier === 'bun' || specifier.startsWith('@cf-wasm/'))
      throw new Error(`Read declaration eagerly loaded optional codec ${specifier}`)
    try {
      return next(specifier, context)
    } catch (cause) {
      if (specifier.startsWith('.') && specifier.endsWith('.js') && context.parentURL) {
        const source = new URL(specifier.slice(0, -3) + '.ts', context.parentURL)
        if (existsSync(fileURLToPath(source))) return next(source.href, context)
      }
      throw cause
    }
  },
})
const Read = await import('../../../src/coding/Read.ts')
assert.equal(typeof Read.read, 'function')
assert.equal(typeof Read.withImages, 'function')
assert.equal(Read.tool.name, 'read')
console.log('Portable text and image Read declarations import without codecs')
