import { existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

export async function resolve(specifier, context, next) {
  if (specifier === 'sharp' || specifier === 'bun' || specifier.startsWith('@cf-wasm/'))
    throw new Error(`Image declaration eagerly loaded optional codec ${specifier}`)
  try {
    return await next(specifier, context)
  } catch (cause) {
    if (specifier.startsWith('.') && specifier.endsWith('.js') && context.parentURL) {
      const source = new URL(specifier.slice(0, -3) + '.ts', context.parentURL)
      if (existsSync(fileURLToPath(source))) return next(source.href, context)
    }
    throw cause
  }
}
