import { execFileSync } from 'node:child_process'
import { tmpdir } from 'node:os'
import { describe, expect, it } from 'vitest'
import * as Effect from 'effect/Effect'
import * as Images from '../src/ImagePreparation.js'
import * as BunImages from '../src/images-bun/index.js'

describe('Bun image preparation', () => {
  it('reports an unavailable optional validator before advertising a ready Layer', () => {
    const output = execFileSync(
      'bun',
      [
        '--no-addons',
        new URL('./fixtures/BunImagePreparation/UnavailableValidator.ts', import.meta.url).pathname,
      ],
      { encoding: 'utf8', timeout: 30000 },
    )
    expect(output).toContain('Unavailable validator is a typed Layer construction error')
  })
  it('preserves core imports when all optional native codecs are denied', () => {
    const output = execFileSync(
      process.execPath,
      [new URL('./fixtures/BunImagePreparation/Isolation.mjs', import.meta.url).pathname],
      { encoding: 'utf8', timeout: 30000, cwd: tmpdir() },
    )
    expect(output).toContain('Portable core and Bun declarations import without native codecs')
  })
  it('executes the real native codec, pixel and completion-lease matrix in Bun', () => {
    const output = execFileSync(
      'bun',
      [new URL('./fixtures/BunImagePreparation/verify.ts', import.meta.url).pathname],
      { encoding: 'utf8', timeout: 60000 },
    )
    expect(output).toContain('Bun native matrix passed')
  }, 65000)

  it('imports safely outside Bun and exposes a typed unavailable adapter', async () => {
    expect(typeof Bun).toBe('undefined')
    const adapter = await Effect.runPromise(BunImages.adapter)
    expect(adapter.capabilities.decode).toEqual([])
    const exit = await Effect.runPromise(
      Images.prepare(new Uint8Array(1), 'image/png', {
        maxWidth: 1,
        maxHeight: 1,
        maxBytes: 100,
        maxInputPixels: 1,
      }).pipe(Effect.provide(BunImages.layer), Effect.result),
    )
    expect(exit._tag).toBe('Failure')
    if (exit._tag === 'Failure') expect(exit.failure._tag).toBe('UnsupportedImage')
  })
})
