import { existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

export async function resolve(specifier, context, next) {
  if (/sharp|photon|jpeg-js|platform-(node|bun)|bun:ffi|node:(fs|child_process)/.test(specifier)) {
    throw new Error(`Portable conformance route loaded native dependency: ${specifier}`)
  }
  if (process.env.CONFORMANCE_SOURCE_ROOT && specifier.startsWith('effect-harness/')) {
    const module = specifier.slice('effect-harness/'.length)
    return next(
      new URL(
        `./packages/effect-harness/src/${module}.ts`,
        new URL(`file://${process.env.CONFORMANCE_SOURCE_ROOT}/`),
      ).href,
      context,
    )
  }
  try {
    return await next(specifier, context)
  } catch (error) {
    if (
      process.env.CONFORMANCE_SOURCE_ROOT &&
      specifier.startsWith('.') &&
      specifier.endsWith('.js') &&
      context.parentURL
    ) {
      const candidate = new URL(specifier.slice(0, -3) + '.ts', context.parentURL)
      if (existsSync(fileURLToPath(candidate))) return next(candidate.href, context)
    }
    throw error
  }
}
