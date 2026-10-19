/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * The `read` handler, ported from pi's `tools.test.ts`.
 */
import { describe, expect, test } from 'bun:test'
import { Effect } from 'effect'
import { read } from '../internal/read.ts'
import { notesOf, run, textOf } from './support.ts'

const numbered = (count: number) =>
  Array.from({ length: count }, (_, index) => `Line ${index + 1}`).join('\n')

const PIXEL =
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGNgYGD4DwABBAEAX+XDSwAAAABJRU5ErkJggg=='

describe('read', () => {
  test('reads text with offsets and limits and reports continuation as a diagnostic', () =>
    run(({ workspace, config, write }) =>
      Effect.gen(function* () {
        yield* write('test.txt', numbered(100))
        const result = yield* read(workspace, { path: 'test.txt', offset: 41, limit: 20 }, config)
        const shown = textOf(result)
        expect(shown).not.toContain('Line 40')
        expect(shown).toContain('Line 41')
        expect(shown).toContain('Line 60')
        expect(shown).not.toContain('Line 61')
        expect(notesOf(result)).toBe('40 more lines in file. Use offset=61 to continue.')
      }),
    ))

  test('truncates large text by line count', () =>
    run(({ workspace, config, write }) =>
      Effect.gen(function* () {
        yield* write('large.txt', numbered(2500))
        const result = yield* read(workspace, { path: 'large.txt' }, config)
        expect(notesOf(result)).toBe('Showing lines 1-2000 of 2500. Use offset=2001 to continue.')
        expect(result.diagnostics?.[0]?.code).toBe('truncated')
        expect(result.details).toMatchObject({
          truncation: {
            truncated: true,
            truncatedBy: 'lines',
            totalLines: 2500,
            outputLines: 2000,
          },
        })
      }),
    ))

  test('does not count a trailing newline as an extra line at the truncation limit', () =>
    run(({ workspace, config, write }) =>
      Effect.gen(function* () {
        yield* write('exact.txt', `${Array.from({ length: 2000 }, () => 'x').join('\n')}\n`)
        const result = yield* read(workspace, { path: 'exact.txt' }, config)
        expect(result.details).toBeUndefined()
        expect(result.diagnostics).toBeUndefined()
      }),
    ))

  test('shows the start of a line longer than the byte limit', () =>
    run(({ workspace, config, write }) =>
      Effect.gen(function* () {
        yield* write('long.txt', `${'é'.repeat(40_000)}\nnext\n`)
        const result = yield* read(workspace, { path: 'long.txt' }, config)
        // Two-byte characters: the cut lands on a character boundary at or below the limit.
        expect(textOf(result)).toBe('é'.repeat(25_600))
        expect(notesOf(result)).toBe(
          "Line 1 is 78.1KB, exceeds the 50.0KB limit; showing its first 50.0KB. Use bash: sed -n '1p' long.txt | tail -c +51201",
        )
        expect(result.details).toMatchObject({
          truncation: { truncated: true, firstLineExceedsLimit: true, outputBytes: 51_200 },
        })
      }),
    ))

  test('rejects offsets beyond the file', () =>
    run(({ workspace, config, write }) =>
      Effect.gen(function* () {
        yield* write('short.txt', 'one\ntwo\nthree')
        const failure = yield* Effect.flip(
          read(workspace, { path: 'short.txt', offset: 100 }, config),
        )
        expect(textOf(failure)).toBe('Offset 100 is beyond end of file (3 lines total)')
      }),
    ))

  test('strips a byte order mark and repairs a leading @', () =>
    run(({ workspace, config, write }) =>
      Effect.gen(function* () {
        yield* write('bom.txt', '﻿hello')
        const result = yield* read(workspace, { path: '@bom.txt' }, config)
        expect(textOf(result)).toBe('hello')
      }),
    ))

  test('fails for a missing file', () =>
    run(({ workspace, config }) =>
      Effect.gen(function* () {
        const failure = yield* Effect.flip(read(workspace, { path: 'missing.txt' }, config))
        expect(notesOf(failure)).toContain('Could not read missing.txt')
      }),
    ))

  test('returns images by content as one image block', () =>
    run(({ workspace, config, write }) =>
      Effect.gen(function* () {
        yield* write('image.txt', Uint8Array.from(Buffer.from(PIXEL, 'base64')))
        const result = yield* read(workspace, { path: 'image.txt' }, config)
        expect(result.content).toEqual([{ type: 'image', data: PIXEL, mimeType: 'image/png' }])
        expect(notesOf(result)).toBe('Read image file [image/png].')
      }),
    ))
})
