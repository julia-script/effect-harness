/**
 * Diffs of an edit, ported from pi's `tools/edit-diff.ts`: a unified patch, and a numbered diff for
 * display with a few lines of context around each change.
 */
import { createTwoFilesPatch, diffLines, FILE_HEADERS_ONLY } from 'diff'

const CONTEXT = 4

/** A standard unified patch of `path`. */
export const unifiedPatch = (path: string, before: string, after: string) =>
  createTwoFilesPatch(path, path, before, after, undefined, undefined, {
    context: CONTEXT,
    headerOptions: FILE_HEADERS_ONLY,
  })

interface Cursor {
  oldLine: number
  newLine: number
  readonly width: number
  readonly output: Array<string>
}

const numbered = (cursor: Cursor, mark: string, line: number, text: string) => {
  cursor.output.push(`${mark}${String(line).padStart(cursor.width, ' ')} ${text}`)
}

const context = (cursor: Cursor, lines: ReadonlyArray<string>) => {
  for (const line of lines) {
    numbered(cursor, ' ', cursor.oldLine, line)
    cursor.oldLine++
    cursor.newLine++
  }
}

/** Pass `count` lines, marked with `...` when `shown`. */
const skip = (cursor: Cursor, count: number, shown = true) => {
  if (count <= 0) return
  if (shown) cursor.output.push(` ${''.padStart(cursor.width, ' ')} ...`)
  cursor.oldLine += count
  cursor.newLine += count
}

/** Unchanged lines: up to `CONTEXT` after a change and before the next, the rest elided. */
const unchanged = (
  cursor: Cursor,
  lines: ReadonlyArray<string>,
  after: boolean,
  before: boolean,
) => {
  if (after && before && lines.length <= CONTEXT * 2) {
    context(cursor, lines)
  } else if (after && before) {
    context(cursor, lines.slice(0, CONTEXT))
    skip(cursor, lines.length - CONTEXT * 2)
    context(cursor, lines.slice(lines.length - CONTEXT))
  } else if (after) {
    context(cursor, lines.slice(0, CONTEXT))
    skip(cursor, lines.length - CONTEXT)
  } else if (before) {
    const skipped = Math.max(0, lines.length - CONTEXT)
    skip(cursor, skipped)
    context(cursor, lines.slice(skipped))
  } else {
    skip(cursor, lines.length, false)
  }
}

const isChange = (part: { readonly added: boolean; readonly removed: boolean }) =>
  part.added || part.removed

/** A numbered diff for display, and the first changed line in the new text. */
export const displayDiff = (before: string, after: string) => {
  const parts = diffLines(before, after)
  const width = String(Math.max(before.split('\n').length, after.split('\n').length)).length
  const cursor: Cursor = { oldLine: 1, newLine: 1, width, output: [] }
  let firstChangedLine: number | undefined
  let lastWasChange = false
  for (const [index, part] of parts.entries()) {
    const lines = part.value.split('\n')
    if (lines.at(-1) === '') lines.pop()
    if (isChange(part)) {
      firstChangedLine ??= cursor.newLine
      for (const line of lines) {
        if (part.added) numbered(cursor, '+', cursor.newLine++, line)
        else numbered(cursor, '-', cursor.oldLine++, line)
      }
      lastWasChange = true
      continue
    }
    const next = parts[index + 1]
    const nextIsChange = next !== undefined && isChange(next)
    unchanged(cursor, lines, lastWasChange, nextIsChange)
    lastWasChange = false
  }
  return { diff: cursor.output.join('\n'), firstChangedLine }
}
