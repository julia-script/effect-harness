import { assert, it } from '@effect/vitest'
import { execFileSync } from 'node:child_process'
import {
  cpSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

it('blocks actual private Node package imports despite retained stale emitted files', () => {
  const directory = fileURLToPath(new URL('../../..', import.meta.url))
  const fixture = mkdtempSync(join(tmpdir(), 'durable-exports-'))
  try {
    cpSync(join(directory, 'package.json'), join(fixture, 'package.json'))
    cpSync(join(directory, 'dist'), join(fixture, 'dist'), { recursive: true })
    symlinkSync(join(directory, 'node_modules'), join(fixture, 'node_modules'), 'dir')
    cpSync(fileURLToPath(new URL('./exports.mjs', import.meta.url)), join(fixture, 'exports.mjs'))
    // A clean build need not contain old outputs. Recreate the exact moved emitted
    // bodies at their old depths inside this disposable package, without touching dist.
    for (const [old, current] of [
      ['State', 'state'],
      ['Backend', 'backend'],
    ] as const) {
      const emitted = readFileSync(
        join(directory, `dist/durable/storage/internal/${current}.js`),
        'utf8',
      )
      writeFileSync(
        join(fixture, `dist/durable/storage/${old}.js`),
        emitted
          .replaceAll('../../', '../')
          .replaceAll("'../StrictReceiptJson.js'", "'./StrictReceiptJson.js'")
          .replaceAll("'./state.js'", "'./State.js'"),
      )
    }
    mkdirSync(join(fixture, 'dist/durable/storage/internal/nested/deeper'), { recursive: true })
    writeFileSync(
      join(fixture, 'dist/durable/storage/internal/nested/deeper/state.js'),
      'export const privateProbe = true\n',
    )
    // A fresh Node process resolves this package self-reference. Source aliases
    // cannot manufacture the export-map denial, and every old output is present.
    const output = execFileSync(process.execPath, [join(fixture, 'exports.mjs')], {
      cwd: fixture,
      encoding: 'utf8',
    })
    assert.include(output, '"staleOutputsPresent":true')
    assert.include(output, '"publicStoreImported":true')
    assert.include(output, '"publicCloneErrorImported":true')
  } finally {
    rmSync(fixture, { recursive: true, force: true })
  }
})
