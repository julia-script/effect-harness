import { resolve as sourceResolve } from './SourceLoader.mjs'

// Deny codecs and Bun entirely while importing core and adapter declarations.
export async function resolve(specifier, context, next) {
  if (specifier === 'sharp' || specifier.startsWith('@cf-wasm/') || specifier === 'bun') {
    throw new Error(`Unexpected eager optional codec import: ${specifier}`)
  }
  return sourceResolve(specifier, context, next)
}
