/**
 * Exact and tolerant text matching with unchanged-line preservation.
 */
import * as Serialization from '../Serialization.ts'
import { identity } from 'effect/Function'
import * as SchemaField from '../SchemaField.ts'
import * as Arr from 'effect/Array'
import * as Order from 'effect/Order'
import { dual } from 'effect/Function'
import * as Result from 'effect/Result'
// Adapted from pi-durable (MIT), pinned 636703a0; see package NOTICE.
/**
 * Shared diff computation utilities for the edit and similar tools.
 */

import * as diff from 'diff'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'

/**
 * Semantic edit error with its retained cause.
 *
 * @category errors
 */
export class EditError extends Schema.TaggedError<EditError>(
  '@effect-harness/harness/tools/EditDiff/EditError',
)('EditError', {
  code: Schema.Literals(['empty', 'not_found', 'duplicate', 'overlap', 'no_change', 'range']),
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
}) {}
const editError = (self: EditError['code'], message: string): EditError =>
  new EditError({ code: self, message })
function atUnsafe<A>(self: ReadonlyArray<A>, index: number): A {
  const value = self[index]
  // Every caller bounds the index to a dense array constructed in this module.
  if (value === undefined)
    throw new Error('BUG: Invalid internal diff position; please report an issue')
  return value
}

/**
 * Detects the first newline convention in text.
 *
 * @category combinators
 */
export function detectLineEnding(self: string): '\r\n' | '\n' {
  const crlfIdx = self.indexOf('\r\n')
  const lfIdx = self.indexOf('\n')
  if (lfIdx === -1) return '\n'
  if (crlfIdx === -1) return '\n'
  return crlfIdx < lfIdx ? '\r\n' : '\n'
}

/**
 * Normalizes CRLF and CR line endings to LF.
 *
 * @category combinators
 */
export function normalizeToLF(self: string): string {
  return self.replace(/\r\n/g, '\n').replace(/\r/g, '\n')
}

/**
 * Restores the selected newline convention in LF text.
 *
 * @category combinators
 */
function restoreLineEndingsImpl(self: string, ending: '\r\n' | '\n'): string {
  return ending === '\r\n' ? self.replace(/\n/g, '\r\n') : self
}

/**
 * Normalizes text for fuzzy matching.
 *
 * **Details**
 *
 * Applies progressive transformations:
 * - Strip trailing whitespace from each line
 * - Normalize smart quotes to ASCII equivalents
 * - Normalize Unicode dashes/hyphens to ASCII hyphen
 * - Normalize special Unicode spaces to regular space
 *
 * @category combinators
 */
export function normalizeForFuzzyMatch(self: string): string {
  return (
    self
      .normalize('NFKC')
      // Strip trailing whitespace per line
      .split('\n')
      .map((line) => line.trimEnd())
      .join('\n')
      // Smart single quotes → '
      .replace(/[\u2018\u2019\u201A\u201B]/g, "'")
      // Smart double quotes → "
      .replace(/[\u201C\u201D\u201E\u201F]/g, '"')
      // Various dashes/hyphens → -
      // U+2010 hyphen, U+2011 non-breaking hyphen, U+2012 figure dash,
      // U+2013 en-dash, U+2014 em-dash, U+2015 horizontal bar, U+2212 minus
      .replace(/[\u2010\u2011\u2012\u2013\u2014\u2015\u2212]/g, '-')
      // Special spaces → regular space
      // U+00A0 NBSP, U+2002-U+200A various spaces, U+202F narrow NBSP,
      // U+205F medium math space, U+3000 ideographic space
      .replace(/[\u00A0\u2002-\u200A\u202F\u205F\u3000]/g, ' ')
  )
}

function splitLinesWithEndings(self: string): Array<string> {
  return self.match(/[^\n]*\n|[^\n]+/g) ?? []
}

interface LineSpan {
  start: number
  end: number
}

interface MatchedEdit {
  editIndex: number
  matchIndex: number
  matchLength: number
  newText: string
}

type TextReplacement = Pick<MatchedEdit, 'matchIndex' | 'matchLength' | 'newText'>

function getLineSpans(self: string): Array<LineSpan> {
  let offset = 0
  return splitLinesWithEndings(self).map((line) => {
    const span = { start: offset, end: offset + line.length }
    offset = span.end
    return span
  })
}

function getReplacementLineRangeImpl(
  self: ReadonlyArray<LineSpan>,
  replacement: TextReplacement,
): Result.Result<{ startLine: number; endLine: number }, EditError> {
  if (
    !Number.isSafeInteger(replacement.matchIndex) ||
    !Number.isSafeInteger(replacement.matchLength) ||
    replacement.matchIndex < 0 ||
    replacement.matchLength <= 0
  )
    return Result.fail(editError('range', 'Replacement range is outside the base content.'))
  const replacementStart = replacement.matchIndex
  const replacementEnd = replacement.matchIndex + replacement.matchLength

  let startLine = -1
  for (let i = 0; i < self.length; i++) {
    const line = atUnsafe(self, i)
    if (replacementStart >= line.start && replacementStart < line.end) {
      startLine = i
      break
    }
  }
  if (startLine === -1) {
    return Result.fail(editError('range', 'Replacement range is outside the base content.'))
  }

  let endLine = startLine
  while (endLine < self.length && atUnsafe(self, endLine).end < replacementEnd) {
    endLine++
  }
  if (endLine >= self.length) {
    return Result.fail(editError('range', 'Replacement range is outside the base content.'))
  }

  return Result.succeed({ startLine, endLine: endLine + 1 })
}

function applyReplacements(
  self: string,
  replacements: ReadonlyArray<TextReplacement>,
  offset = 0,
): string {
  let result = self
  for (let i = replacements.length - 1; i >= 0; i--) {
    const replacement = atUnsafe(replacements, i)
    const matchIndex = replacement.matchIndex - offset
    result =
      result.substring(0, matchIndex) +
      replacement.newText +
      result.substring(matchIndex + replacement.matchLength)
  }
  return result
}

/**
 * Apply replacements matched against `baseContent` to `originalContent` while
 * preserving unchanged line blocks from the original.
 *
 * This is useful when `baseContent` is a normalized view of the original. Each
 * replacement is widened to the lines it actually touches, those touched lines
 * are rewritten from the normalized base, and all other lines are copied back
 * from `originalContent`. The actual replacement ranges drive preservation so
 * duplicate normalized lines cannot be aligned to the wrong occurrence.
 */
function applyReplacementsPreservingUnchangedLinesImpl(
  self: string,
  baseContent: string,
  replacements: ReadonlyArray<TextReplacement>,
): Result.Result<string, EditError> {
  return Result.gen(function* () {
    const originalLines = splitLinesWithEndings(self)
    const baseLines = getLineSpans(baseContent)
    if (originalLines.length !== baseLines.length) {
      return yield* Result.fail(
        editError(
          'range',
          'Cannot preserve unchanged lines because the base content has a different line count.',
        ),
      )
    }

    const groups: Array<{
      startLine: number
      endLine: number
      replacements: Array<TextReplacement>
    }> = []
    const sortedReplacements = Arr.sort(replacements, replacementOrder)
    let previousEnd = -1
    for (const replacement of sortedReplacements) {
      const range = yield* getReplacementLineRange(baseLines, replacement)
      if (replacement.matchIndex < previousEnd)
        return yield* Result.fail(editError('overlap', 'Replacement ranges overlap.'))
      previousEnd = replacement.matchIndex + replacement.matchLength
      const current = groups[groups.length - 1]
      if (current && range.startLine < current.endLine) {
        current.endLine = Math.max(current.endLine, range.endLine)
        current.replacements.push(replacement)
        continue
      }
      groups.push({ ...range, replacements: [replacement] })
    }

    let originalLineIndex = 0
    let result = ''
    for (const group of groups) {
      result += originalLines.slice(originalLineIndex, group.startLine).join('')

      const groupStartOffset = atUnsafe(baseLines, group.startLine).start
      const groupEndOffset = atUnsafe(baseLines, group.endLine - 1).end
      result += applyReplacements(
        baseContent.slice(groupStartOffset, groupEndOffset),
        group.replacements,
        groupStartOffset,
      )
      originalLineIndex = group.endLine
    }
    result += originalLines.slice(originalLineIndex).join('')

    return result
  })
}
/**
 * Applies normalized replacements while retaining unchanged original line bytes.
 *
 * @category combinators
 */
export const applyReplacementsPreservingUnchangedLines: {
  (
    baseContent: string,
    replacements: ReadonlyArray<TextReplacement>,
  ): (self: string) => Result.Result<string, EditError>
  (
    self: string,
    baseContent: string,
    replacements: ReadonlyArray<TextReplacement>,
  ): Result.Result<string, EditError>
} = dual(3, (self: string, baseContent: string, replacements: ReadonlyArray<TextReplacement>) =>
  Result.try({
    try: () => applyReplacementsPreservingUnchangedLinesImpl(self, baseContent, replacements),
    catch: diffFailure,
  }).pipe(Result.flatMap(identity)),
)

/**
 * Match location and exact or normalized content used for replacement.
 *
 * @category models
 */
export interface FuzzyMatchResult {
  /** The index where the match starts (in the content that should be used for replacement) */
  readonly index: number
  /** Length of the matched text */
  readonly matchLength: number
  /** Whether fuzzy matching was used (false = exact match) */
  readonly usedFuzzyMatch: boolean
  /**
   * The content to use for replacement operations.
   * When exact match: original content. When fuzzy match: normalized content.
   */
  readonly contentForReplacement: string
}

/**
 * Old/new text replacement requiring a unique match.
 *
 * @category models
 */
export interface Edit {
  readonly oldText: string
  readonly newText: string
}

/**
 * Normalized source and resulting text after validated edits.
 *
 * @category models
 */
export interface AppliedEditsResult {
  readonly baseContent: string
  readonly newContent: string
}

/**
 * Find oldText in content, trying exact match first, then fuzzy match.
 *
 * **Details**
 *
 * When fuzzy matching is used, the returned contentForReplacement is the
 * fuzzy-normalized version of the content (trailing whitespace stripped,
 * Unicode quotes/dashes normalized to ASCII).
 *
 * @category combinators
 */
export function fuzzyFindText(self: string, oldText: string): Option.Option<FuzzyMatchResult> {
  if (normalizeForFuzzyMatch(oldText).length === 0) return Option.none()
  // Try exact match first
  const exactIndex = self.indexOf(oldText)
  if (exactIndex !== -1) {
    return Option.some({
      index: exactIndex,
      matchLength: oldText.length,
      usedFuzzyMatch: false,
      contentForReplacement: self,
    })
  }

  // Try fuzzy match - work entirely in normalized space
  const fuzzyContent = normalizeForFuzzyMatch(self)
  const fuzzyOldText = normalizeForFuzzyMatch(oldText)
  const fuzzyIndex = fuzzyContent.indexOf(fuzzyOldText)

  if (fuzzyIndex === -1) return Option.none()

  // When fuzzy matching, return offsets in normalized space. Callers can use
  // the normalized content to compute replacements, then decide how much of
  // that normalized output should be written back.
  return Option.some({
    index: fuzzyIndex,
    matchLength: fuzzyOldText.length,
    usedFuzzyMatch: true,
    contentForReplacement: fuzzyContent,
  })
}

/**
 * Strip UTF-8 BOM if present, return both the BOM (if any) and the text without it
 *
 * @category combinators
 */
export function stripBom(self: string): StripBomResult {
  return self.startsWith('\uFEFF')
    ? { bom: '\uFEFF', text: self.slice(1) }
    : { bom: '', text: self }
}

function countOccurrences(self: string, oldText: string): number {
  const fuzzyContent = normalizeForFuzzyMatch(self)
  const fuzzyOldText = normalizeForFuzzyMatch(oldText)
  if (fuzzyOldText.length === 0) return 0
  let count = 0
  let offset = 0
  while (true) {
    const index = fuzzyContent.indexOf(fuzzyOldText, offset)
    if (index === -1) return count
    count++
    offset = index + 1
  }
}

function getNotFoundError(path: string, editIndex: number, totalEdits: number): EditError {
  if (totalEdits === 1) {
    return editError(
      'not_found',
      `Could not find the exact text in ${path}. The old text must match exactly including all whitespace and newlines.`,
    )
  }
  return editError(
    'not_found',
    `Could not find edits[${editIndex}] in ${path}. The oldText must match exactly including all whitespace and newlines.`,
  )
}

function getDuplicateError(
  path: string,
  editIndex: number,
  totalEdits: number,
  occurrences: number,
): EditError {
  if (totalEdits === 1) {
    return editError(
      'duplicate',
      `Found ${occurrences} occurrences of the text in ${path}. The text must be unique. Please provide more context to make it unique.`,
    )
  }
  return editError(
    'duplicate',
    `Found ${occurrences} occurrences of edits[${editIndex}] in ${path}. Each oldText must be unique. Please provide more context to make it unique.`,
  )
}

function getEmptyOldTextError(path: string, editIndex: number, totalEdits: number): EditError {
  if (totalEdits === 1) {
    return editError('empty', `oldText must not be empty in ${path}.`)
  }
  return editError('empty', `edits[${editIndex}].oldText must not be empty in ${path}.`)
}

function getNoChangeError(path: string, totalEdits: number): EditError {
  if (totalEdits === 1) {
    return editError(
      'no_change',
      `No changes made to ${path}. The replacement produced identical content. This might indicate an issue with special characters or the text not existing as expected.`,
    )
  }
  return editError(
    'no_change',
    `No changes made to ${path}. The replacements produced identical content.`,
  )
}

/**
 * Apply one or more exact-text replacements to LF-normalized content.
 *
 * All edits are matched against the same original content. Replacements are
 * then applied in reverse order so offsets remain stable. If any edit needs
 * fuzzy matching, the operation runs in fuzzy-normalized content space and then
 * overlays those line-level changes onto the original content so unchanged line
 * blocks keep their original bytes.
 */
function applyEditsToNormalizedContentImpl(
  self: string,
  edits: ReadonlyArray<Edit>,
  path: string,
): Result.Result<AppliedEditsResult, EditError> {
  return Result.gen(function* () {
    if (edits.length === 0)
      return yield* Result.fail(editError('empty', 'edits must contain at least one replacement'))
    const normalizedEdits = edits.map((edit) => ({
      oldText: normalizeToLF(edit.oldText),
      newText: normalizeToLF(edit.newText),
    }))

    for (let i = 0; i < normalizedEdits.length; i++) {
      if (normalizeForFuzzyMatch(atUnsafe(normalizedEdits, i).oldText).length === 0) {
        return yield* Result.fail(getEmptyOldTextError(path, i, normalizedEdits.length))
      }
    }

    const initialMatches = normalizedEdits.map((edit) => fuzzyFindText(self, edit.oldText))
    const usedFuzzyMatch = initialMatches.some(
      (match) => Option.isSome(match) && match.value.usedFuzzyMatch,
    )
    const replacementBaseContent = usedFuzzyMatch ? normalizeForFuzzyMatch(self) : self

    const matchedEdits: Array<MatchedEdit> = []
    for (let i = 0; i < normalizedEdits.length; i++) {
      const edit = atUnsafe(normalizedEdits, i)
      const matchResult = fuzzyFindText(replacementBaseContent, edit.oldText)
      if (Option.isNone(matchResult)) {
        return yield* Result.fail(getNotFoundError(path, i, normalizedEdits.length))
      }

      const occurrences = countOccurrences(replacementBaseContent, edit.oldText)
      if (occurrences > 1) {
        return yield* Result.fail(getDuplicateError(path, i, normalizedEdits.length, occurrences))
      }

      matchedEdits.push({
        editIndex: i,
        matchIndex: matchResult.value.index,
        matchLength: matchResult.value.matchLength,
        newText: edit.newText,
      })
    }

    // effect-review-allow P1-order-equivalence-params: matchedEdits is a locally owned buffer; in-place sorting retains ascending matchIndex and stable ties.
    matchedEdits.sort(replacementOrder)
    for (let i = 1; i < matchedEdits.length; i++) {
      const previous = atUnsafe(matchedEdits, i - 1)
      const current = atUnsafe(matchedEdits, i)
      if (previous.matchIndex + previous.matchLength > current.matchIndex) {
        return yield* Result.fail(
          editError(
            'overlap',
            `edits[${previous.editIndex}] and edits[${current.editIndex}] overlap in ${path}. Merge them into one edit or target disjoint regions.`,
          ),
        )
      }
    }

    const baseContent = self
    const newContent = usedFuzzyMatch
      ? yield* applyReplacementsPreservingUnchangedLines(self, replacementBaseContent, matchedEdits)
      : applyReplacements(replacementBaseContent, matchedEdits)

    if (baseContent === newContent) {
      return yield* Result.fail(getNoChangeError(path, normalizedEdits.length))
    }

    return { baseContent, newContent }
  })
}
/**
 * Validates disjoint original-content matches and applies replacements in reverse offset order.
 *
 * @category combinators
 */
export const applyEditsToNormalizedContent: {
  (
    edits: ReadonlyArray<Edit>,
    path: string,
  ): (self: string) => Result.Result<AppliedEditsResult, EditError>
  (
    self: string,
    edits: ReadonlyArray<Edit>,
    path: string,
  ): Result.Result<AppliedEditsResult, EditError>
} = dual(3, (self: string, edits: ReadonlyArray<Edit>, path: string) =>
  Result.try({
    try: () => applyEditsToNormalizedContentImpl(self, edits, path),
    catch: diffFailure,
  }).pipe(Result.flatMap(identity)),
)

/**
 * Generates a standard unified patch.
 *
 * @category combinators
 */
export function generateUnifiedPatch(
  path: string,
  oldContent: string,
  newContent: string,
  contextLines = 4,
): string {
  return diff.createTwoFilesPatch(path, path, oldContent, newContent, undefined, undefined, {
    context: contextLines,
    headerOptions: diff.FILE_HEADERS_ONLY,
  })
}

/**
 * Generate a display-oriented diff string with line numbers and context.
 * Returns both the diff string and the first changed line number (in the new file).
 */
function generateDiffStringImpl(
  self: string,
  newContent: string,
  contextLines = 4,
): DiffStringResult {
  const parts = diff.diffLines(self, newContent)
  const output: Array<string> = []

  const oldLines = self.split('\n')
  const newLines = newContent.split('\n')
  const maxLineNum = Math.max(oldLines.length, newLines.length)
  const lineNumWidth = String(maxLineNum).length

  let oldLineNum = 1
  let newLineNum = 1
  let lastWasChange = false
  let firstChangedLine: number | undefined

  for (let i = 0; i < parts.length; i++) {
    const part = atUnsafe(parts, i)
    const raw = part.value.split('\n')
    if (raw[raw.length - 1] === '') {
      raw.pop()
    }

    if (part.added || part.removed) {
      // Capture the first changed line (in the new file)
      if (firstChangedLine === undefined) {
        firstChangedLine = newLineNum
      }

      // Show the change
      for (const line of raw) {
        if (part.added) {
          const lineNum = String(newLineNum).padStart(lineNumWidth, ' ')
          output.push(`+${lineNum} ${line}`)
          newLineNum++
        } else {
          // removed
          const lineNum = String(oldLineNum).padStart(lineNumWidth, ' ')
          output.push(`-${lineNum} ${line}`)
          oldLineNum++
        }
      }
      lastWasChange = true
    } else {
      // Context lines - only show a few before/after changes
      const nextPartIsChange =
        i < parts.length - 1 && (atUnsafe(parts, i + 1).added || atUnsafe(parts, i + 1).removed)
      const hasLeadingChange = lastWasChange
      const hasTrailingChange = nextPartIsChange

      if (hasLeadingChange && hasTrailingChange) {
        if (raw.length <= contextLines * 2) {
          for (const line of raw) {
            const lineNum = String(oldLineNum).padStart(lineNumWidth, ' ')
            output.push(` ${lineNum} ${line}`)
            oldLineNum++
            newLineNum++
          }
        } else {
          const leadingLines = raw.slice(0, contextLines)
          const trailingLines = raw.slice(raw.length - contextLines)
          const skippedLines = raw.length - leadingLines.length - trailingLines.length

          for (const line of leadingLines) {
            const lineNum = String(oldLineNum).padStart(lineNumWidth, ' ')
            output.push(` ${lineNum} ${line}`)
            oldLineNum++
            newLineNum++
          }

          output.push(` ${''.padStart(lineNumWidth, ' ')} ...`)
          oldLineNum += skippedLines
          newLineNum += skippedLines

          for (const line of trailingLines) {
            const lineNum = String(oldLineNum).padStart(lineNumWidth, ' ')
            output.push(` ${lineNum} ${line}`)
            oldLineNum++
            newLineNum++
          }
        }
      } else if (hasLeadingChange) {
        const shownLines = raw.slice(0, contextLines)
        const skippedLines = raw.length - shownLines.length

        for (const line of shownLines) {
          const lineNum = String(oldLineNum).padStart(lineNumWidth, ' ')
          output.push(` ${lineNum} ${line}`)
          oldLineNum++
          newLineNum++
        }

        if (skippedLines > 0) {
          output.push(` ${''.padStart(lineNumWidth, ' ')} ...`)
          oldLineNum += skippedLines
          newLineNum += skippedLines
        }
      } else if (hasTrailingChange) {
        const skippedLines = Math.max(0, raw.length - contextLines)
        if (skippedLines > 0) {
          output.push(` ${''.padStart(lineNumWidth, ' ')} ...`)
          oldLineNum += skippedLines
          newLineNum += skippedLines
        }

        for (const line of raw.slice(skippedLines)) {
          const lineNum = String(oldLineNum).padStart(lineNumWidth, ' ')
          output.push(` ${lineNum} ${line}`)
          oldLineNum++
          newLineNum++
        }
      } else {
        // Skip these context lines entirely
        oldLineNum += raw.length
        newLineNum += raw.length
      }

      lastWasChange = false
    }
  }

  return { diff: output.join('\n'), firstChangedLine }
}
/**
 * Formats a display diff with line numbers and the first changed line.
 *
 * @category combinators
 */
export const generateDiffString: {
  (newContent: string, contextLines?: number): (self: string) => DiffStringResult
  (self: string, newContent: string, contextLines?: number): DiffStringResult
} = dual((args) => typeof args[1] === 'string', generateDiffStringImpl)

/**
 * Applies normalized replacements while retaining unchanged original line bytes. This synchronous operation can throw.
 *
 * @category unsafe
 */
export const applyReplacementsPreservingUnchangedLinesUnsafe: {
  (baseContent: string, replacements: ReadonlyArray<TextReplacement>): (self: string) => string
  (self: string, baseContent: string, replacements: ReadonlyArray<TextReplacement>): string
} = dual(
  3,
  (self: string, baseContent: string, replacements: ReadonlyArray<TextReplacement>): string =>
    Result.getOrThrow(applyReplacementsPreservingUnchangedLines(self, baseContent, replacements)),
)
/**
 * Validates disjoint original-content matches and applies replacements in reverse offset order. This synchronous operation can throw.
 *
 * @category unsafe
 */
export const applyEditsToNormalizedContentUnsafe: {
  (edits: ReadonlyArray<Edit>, path: string): (self: string) => AppliedEditsResult
  (self: string, edits: ReadonlyArray<Edit>, path: string): AppliedEditsResult
} = dual(3, (self: string, edits: ReadonlyArray<Edit>, path: string): AppliedEditsResult =>
  Result.getOrThrow(applyEditsToNormalizedContent(self, edits, path)),
)

const replacementOrder = Order.mapInput(Order.Number, (self: TextReplacement) => self.matchIndex)

const diffFailure = (cause: unknown): EditError =>
  cause instanceof EditError
    ? cause
    : new EditError({ code: 'range', message: Serialization.errorText(cause), cause })
function getReplacementLineRangeUnsafe(
  self: ReadonlyArray<LineSpan>,
  replacement: TextReplacement,
): { readonly startLine: number; readonly endLine: number } {
  return Result.getOrThrow(getReplacementLineRangeImpl(self, replacement))
}
const getReplacementLineRange = (
  self: ReadonlyArray<LineSpan>,
  replacement: TextReplacement,
): Result.Result<{ readonly startLine: number; readonly endLine: number }, EditError> =>
  Result.try({ try: () => getReplacementLineRangeUnsafe(self, replacement), catch: diffFailure })

/**
 * Initial byte-order mark and remaining text.
 *
 * @category models
 */
export type StripBomResult = stripBom.Result
/**
 * Display diff and the first modified source line when present.
 *
 * @category models
 */
export type DiffStringResult = generateDiffString.Result

/**
 * Restores LF text to the selected newline convention.
 *
 * @category combinators
 */
export const restoreLineEndings: {
  (ending: '\r\n' | '\n'): (self: string) => string
  (self: string, ending: '\r\n' | '\n'): string
} = dual(2, restoreLineEndingsImpl)

/**
 * Type-level contracts for `stripBom`.
 *
 * @category utility types
 */
export declare namespace stripBom {
  /**
   * Content without its initial BOM and whether that BOM was present.
   *
   * @category models
   */
  interface Result {
    readonly bom: string
    readonly text: string
  }
}

/**
 * Type-level contracts for `generateDiffString`.
 *
 * @category utility types
 */
export declare namespace generateDiffString {
  /**
   * Rendered unified diff and its first changed line.
   *
   * @category models
   */
  interface Result {
    readonly diff: string
    readonly firstChangedLine: number | undefined
  }
}
