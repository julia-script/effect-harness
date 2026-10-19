/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * The `write` and `edit` handlers, ported from pi's `tools.test.ts`.
 */
import { describe, expect, test } from 'bun:test'
import { applyPatch } from 'diff'
import { Effect, Schema } from 'effect'
import { CodingTools } from '../index.ts'
import { edit, write } from '../internal/mutate.ts'
import { notesOf, run, textOf } from './support.ts'

describe('write', () => {
  test('writes files and creates parent directories', () =>
    run(({ workspace, config, readBack }) =>
      Effect.gen(function* () {
        const result = yield* write(workspace, { path: 'a/b/c.txt', content: 'hi' }, config)
        expect(textOf(result)).toBe('Successfully wrote to a/b/c.txt')
        expect(yield* readBack('a/b/c.txt')).toBe('hi')
      }),
    ))
})

describe('edit', () => {
  test('applies disjoint edits and returns both diff formats', () =>
    run(({ workspace, config, write: put, readBack }) =>
      Effect.gen(function* () {
        const original = 'alpha\nbeta\ngamma\ndelta\n'
        yield* put('edit.txt', original)
        const result = yield* edit(
          workspace,
          {
            path: 'edit.txt',
            edits: [
              { oldText: 'alpha\n', newText: 'ALPHA\n' },
              { oldText: 'gamma\n', newText: 'GAMMA\n' },
            ],
          },
          config,
        )
        expect(textOf(result)).toBe('Successfully replaced 2 block(s) in edit.txt.')
        const details = yield* Effect.orDie(
          Schema.decodeUnknownEffect(CodingTools.EditDetails)(result.details),
        )
        expect(details.diff).toContain('ALPHA')
        expect(details.diff).toContain('GAMMA')
        expect(details.firstChangedLine).toBe(1)
        expect(applyPatch(original, details.patch)).toBe('ALPHA\nbeta\nGAMMA\ndelta\n')
        expect(yield* readBack('edit.txt')).toBe('ALPHA\nbeta\nGAMMA\ndelta\n')
      }),
    ))

  test('matches all edits against the original and rejects overlaps', () =>
    run(({ workspace, config, write: put, readBack }) =>
      Effect.gen(function* () {
        yield* put('edit.txt', 'one\ntwo\nthree\n')
        const failure = yield* Effect.flip(
          edit(
            workspace,
            {
              path: 'edit.txt',
              edits: [
                { oldText: 'one\ntwo\n', newText: 'ONE\nTWO\n' },
                { oldText: 'two\nthree\n', newText: 'TWO\nTHREE\n' },
              ],
            },
            config,
          ),
        )
        expect(textOf(failure)).toMatch(/overlap/u)
        expect(yield* readBack('edit.txt')).toBe('one\ntwo\nthree\n')
      }),
    ))

  test('rejects missing and duplicate target text, and an empty edit list', () =>
    run(({ workspace, config, write: put }) =>
      Effect.gen(function* () {
        yield* put('edit.txt', 'foo foo foo')
        const missing = yield* Effect.flip(
          edit(
            workspace,
            { path: 'edit.txt', edits: [{ oldText: 'bar', newText: 'baz' }] },
            config,
          ),
        )
        expect(textOf(missing)).toMatch(/Could not find the exact text/u)
        const duplicate = yield* Effect.flip(
          edit(
            workspace,
            { path: 'edit.txt', edits: [{ oldText: 'foo', newText: 'bar' }] },
            config,
          ),
        )
        expect(textOf(duplicate)).toMatch(/Found 3 occurrences/u)
        const empty = yield* Effect.flip(edit(workspace, { path: 'edit.txt', edits: [] }, config))
        expect(textOf(empty)).toMatch(/at least one replacement/u)
      }),
    ))

  test('fails for a missing file', () =>
    run(({ workspace, config }) =>
      Effect.gen(function* () {
        const failure = yield* Effect.flip(
          edit(workspace, { path: 'nope.txt', edits: [{ oldText: 'a', newText: 'b' }] }, config),
        )
        expect(textOf(failure)).toContain('Could not edit file: nope.txt.')
        expect(notesOf(failure)).toBe('')
      }),
    ))

  test('edits regular files through symlinks', () =>
    run(({ workspace, config, cwd, write: put, readBack }) =>
      Effect.gen(function* () {
        yield* put('target.txt', 'before\n')
        yield* Effect.orDie(
          workspace.fs.symlink('target.txt', workspace.path.join(cwd, 'link.txt')),
        )
        yield* edit(
          workspace,
          { path: 'link.txt', edits: [{ oldText: 'before', newText: 'after' }] },
          config,
        )
        expect(yield* readBack('target.txt')).toBe('after\n')
      }),
    ))

  test('preserves BOM and CRLF line endings', () =>
    run(({ workspace, config, write: put, readBack }) =>
      Effect.gen(function* () {
        yield* put('edit.txt', '﻿one\r\ntwo\r\n')
        yield* edit(
          workspace,
          { path: 'edit.txt', edits: [{ oldText: 'two', newText: 'TWO' }] },
          config,
        )
        expect(yield* readBack('edit.txt')).toBe('﻿one\r\nTWO\r\n')
      }),
    ))

  test('matches text that differs only in quotes and trailing spaces', () =>
    run(({ workspace, config, write: put, readBack }) =>
      Effect.gen(function* () {
        yield* put('edit.txt', 'const a = “x”   \nconst b = 1\n')
        yield* edit(
          workspace,
          { path: 'edit.txt', edits: [{ oldText: 'const a = "x"', newText: 'const a = "y"' }] },
          config,
        )
        expect(yield* readBack('edit.txt')).toBe('const a = "y"\nconst b = 1\n')
      }),
    ))

  test('serializes concurrent edits of one file', () =>
    run(({ workspace, config, write: put, readBack }) =>
      Effect.gen(function* () {
        yield* put('edit.txt', 'a\nb\nc\n')
        yield* Effect.forEach(
          ['a', 'b', 'c'],
          (line) =>
            edit(
              workspace,
              {
                path: 'edit.txt',
                edits: [{ oldText: `${line}\n`, newText: `${line.toUpperCase()}\n` }],
              },
              config,
            ),
          { concurrency: 'unbounded', discard: true },
        )
        expect(yield* readBack('edit.txt')).toBe('A\nB\nC\n')
      }),
    ))
})
