/**
 * Exact-text replacements, ported from pi's `tools/edit-diff.ts`: matched exactly, or after a
 * fuzzy normalization of whitespace, quotes and dashes, and applied so that unchanged lines keep
 * their bytes. A replacement that cannot apply fails with the message the model sees.
 */
import { Result } from 'effect'

export interface Replacement {
  readonly oldText: string
  readonly newText: string
}

export const detectLineEnding = (content: string): '\r\n' | '\n' => {
  const crlf = content.indexOf('\r\n')
  const lf = content.indexOf('\n')
  return lf !== -1 && crlf !== -1 && crlf < lf ? '\r\n' : '\n'
}

export const normalizeToLF = (text: string) => text.replaceAll('\r\n', '\n').replaceAll('\r', '\n')

export const restoreLineEndings = (text: string, ending: '\r\n' | '\n') =>
  ending === '\r\n' ? text.replaceAll('\n', '\r\n') : text

/** Strip a byte order mark, and return it. */
export const stripBom = (content: string) =>
  content.startsWith('﻿') ? { bom: '﻿', text: content.slice(1) } : { bom: '', text: content }

/**
 * Text normalized for fuzzy matching: NFKC, trailing whitespace stripped from each line, smart
 * quotes, Unicode dashes and special spaces made ASCII.
 */
export const normalizeForFuzzyMatch = (text: string) =>
  text
    .normalize('NFKC')
    .split('\n')
    .map((line) => line.trimEnd())
    .join('\n')
    .replaceAll(/[‘’‚‛]/gu, "'")
    .replaceAll(/[“”„‟]/gu, '"')
    .replaceAll(/[‐‑‒–—―−]/gu, '-')
    .replaceAll(/[  -   　]/gu, ' ')

const splitLinesWithEndings = (content: string) => content.match(/[^\n]*\n|[^\n]+/gu) ?? []

interface Span {
  readonly start: number
  readonly end: number
}

interface Matched {
  readonly editIndex: number
  readonly matchIndex: number
  readonly matchLength: number
  readonly newText: string
}

const spansOf = (content: string): ReadonlyArray<Span> => {
  let offset = 0
  return splitLinesWithEndings(content).map((line) => {
    const span = { start: offset, end: offset + line.length }
    offset = span.end
    return span
  })
}

const OUTSIDE = 'Replacement range is outside the base content.'

/** The lines `[startLine, endLine)` a replacement touches. */
const lineRange = (
  lines: ReadonlyArray<Span>,
  replacement: Matched,
): Result.Result<{ readonly startLine: number; readonly endLine: number }, string> => {
  const start = replacement.matchIndex
  const end = start + replacement.matchLength
  const startLine = lines.findIndex((line) => start >= line.start && start < line.end)
  if (startLine === -1) return Result.fail(OUTSIDE)
  let endLine = startLine
  while (endLine < lines.length && (lines[endLine]?.end ?? 0) < end) endLine++
  return endLine >= lines.length
    ? Result.fail(OUTSIDE)
    : Result.succeed({ startLine, endLine: endLine + 1 })
}

const applyAll = (content: string, replacements: ReadonlyArray<Matched>, offset = 0) => {
  let result = content
  for (const replacement of replacements.toReversed()) {
    const at = replacement.matchIndex - offset
    result = result.slice(0, at) + replacement.newText + result.slice(at + replacement.matchLength)
  }
  return result
}

interface Group {
  startLine: number
  endLine: number
  readonly replacements: Array<Matched>
}

/**
 * Apply replacements matched against `base`, a normalized view of `original`, to `original`:
 * each touched line block is rewritten from `base`, and every other line copied from `original`.
 */
const applyPreservingUnchangedLines = (
  original: string,
  base: string,
  replacements: ReadonlyArray<Matched>,
): Result.Result<string, string> => {
  const originalLines = splitLinesWithEndings(original)
  const baseLines = spansOf(base)
  if (originalLines.length !== baseLines.length)
    return Result.fail('Cannot preserve unchanged lines: the normalized text has other lines')
  const groups: Array<Group> = []
  for (const replacement of replacements.toSorted((a, b) => a.matchIndex - b.matchIndex)) {
    const found = lineRange(baseLines, replacement)
    if (Result.isFailure(found)) return Result.fail(found.failure)
    const range = found.success
    const current = groups.at(-1)
    if (current !== undefined && range.startLine < current.endLine) {
      current.endLine = Math.max(current.endLine, range.endLine)
      current.replacements.push(replacement)
    } else groups.push({ ...range, replacements: [replacement] })
  }
  let next = 0
  let result = ''
  for (const group of groups) {
    result += originalLines.slice(next, group.startLine).join('')
    const from = baseLines[group.startLine]?.start ?? 0
    const to = baseLines[group.endLine - 1]?.end ?? from
    result += applyAll(base.slice(from, to), group.replacements, from)
    next = group.endLine
  }
  return Result.succeed(result + originalLines.slice(next).join(''))
}

/** Where `oldText` is in `content`: exactly, else in fuzzy-normalized space. */
const find = (content: string, oldText: string) => {
  const exact = content.indexOf(oldText)
  if (exact !== -1) return { index: exact, length: oldText.length, fuzzy: false }
  const fuzzyOld = normalizeForFuzzyMatch(oldText)
  const index = normalizeForFuzzyMatch(content).indexOf(fuzzyOld)
  return index === -1 ? undefined : { index, length: fuzzyOld.length, fuzzy: true }
}

const occurrences = (content: string, oldText: string) =>
  normalizeForFuzzyMatch(content).split(normalizeForFuzzyMatch(oldText)).length - 1

/** The edit's name in messages: "the text" for a single edit, `edits[i]` otherwise. */
const named = (index: number, total: number) => (total === 1 ? 'the text' : `edits[${index}]`)

const notFound = (path: string, index: number, total: number) =>
  total === 1
    ? `Could not find the exact text in ${path}. The old text must match exactly including all whitespace and newlines.`
    : `Could not find edits[${index}] in ${path}. The oldText must match exactly including all whitespace and newlines.`

const duplicate = (path: string, index: number, total: number, count: number) =>
  `Found ${count} occurrences of ${named(index, total)} in ${path}. ${total === 1 ? 'The text must be unique' : 'Each oldText must be unique'}. Please provide more context to make it unique.`

/** Match every edit against `base`, failing on a missing, repeated or empty one. */
const matchAll = (
  base: string,
  edits: ReadonlyArray<Replacement>,
  path: string,
): Result.Result<ReadonlyArray<Matched>, string> => {
  const matched: Array<Matched> = []
  for (const [index, edit] of edits.entries()) {
    if (edit.oldText.length === 0) {
      const which = edits.length === 1 ? 'oldText' : `edits[${index}].oldText`
      return Result.fail(`${which} must not be empty in ${path}.`)
    }
    const found = find(base, edit.oldText)
    if (found === undefined) return Result.fail(notFound(path, index, edits.length))
    const count = occurrences(base, edit.oldText)
    if (count > 1) return Result.fail(duplicate(path, index, edits.length, count))
    matched.push({
      editIndex: index,
      matchIndex: found.index,
      matchLength: found.length,
      newText: edit.newText,
    })
  }
  const sorted = matched.toSorted((a, b) => a.matchIndex - b.matchIndex)
  for (const [index, current] of sorted.entries()) {
    const previous = sorted[index - 1]
    if (previous !== undefined && previous.matchIndex + previous.matchLength > current.matchIndex)
      return Result.fail(
        `edits[${previous.editIndex}] and edits[${current.editIndex}] overlap in ${path}. Merge them into one edit or target disjoint regions.`,
      )
  }
  return Result.succeed(sorted)
}

/**
 * Apply `edits` to LF-normalized `content`, all matched against it. When any needs fuzzy
 * matching, they match in fuzzy-normalized space and only the lines they touch change.
 */
export const applyEdits = (
  content: string,
  edits: ReadonlyArray<Replacement>,
  path: string,
): Result.Result<string, string> => {
  const normalized = edits.map((edit) => ({
    oldText: normalizeToLF(edit.oldText),
    newText: normalizeToLF(edit.newText),
  }))
  const fuzzy = normalized.some((edit) => find(content, edit.oldText)?.fuzzy === true)
  const base = fuzzy ? normalizeForFuzzyMatch(content) : content
  return Result.flatMap(matchAll(base, normalized, path), (matched) => {
    const next = fuzzy
      ? applyPreservingUnchangedLines(content, base, matched)
      : Result.succeed(applyAll(base, matched))
    return Result.flatMap(next, (changed) =>
      changed === content
        ? Result.fail(
            edits.length === 1
              ? `No changes made to ${path}. The replacement produced identical content. This might indicate an issue with special characters or the text not existing as expected.`
              : `No changes made to ${path}. The replacements produced identical content.`,
          )
        : Result.succeed(changed),
    )
  })
}
