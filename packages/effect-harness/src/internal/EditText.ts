import * as Result from 'effect/Result'

export interface Replacement {
  readonly oldText: string
  readonly newText: string
}
export interface Issue {
  readonly reason: 'emptyOldText' | 'missing' | 'ambiguous' | 'overlap' | 'noOp'
  readonly message: string
}
interface Line {
  readonly text: string
  readonly ending: string
}
interface Match {
  readonly index: number
  readonly end: number
  readonly editIndex: number
  readonly newText: string
}

export const normalizeLF = (text: string): string => text.replace(/\r\n|\r/g, '\n')

/** Pinned upstream edit-diff.ts normalization; never applied to unrelated lines. */
export const normalizeFuzzy = (text: string): string =>
  text
    .normalize('NFKC')
    .split('\n')
    .map((line) => line.trimEnd())
    .join('\n')
    .replace(/[\u2018\u2019\u201A\u201B]/g, "'")
    .replace(/[\u201C\u201D\u201E\u201F]/g, '"')
    .replace(/[\u2010\u2011\u2012\u2013\u2014\u2015\u2212]/g, '-')
    .replace(/[\u00A0\u2002-\u200A\u202F\u205F\u3000]/g, ' ')

const lines = (text: string): Line[] => {
  const output: Line[] = []
  let start = 0
  for (const match of text.matchAll(/\r\n|\r|\n/g)) {
    output.push({ text: text.slice(start, match.index), ending: match[0] })
    start = match.index + match[0].length
  }
  if (start < text.length) output.push({ text: text.slice(start), ending: '' })
  return output
}

/** Counts overlapping matches too (e.g. aa in aaa), unlike split-based counting. */
const occurrences = (content: string, text: string): number[] => {
  const output: number[] = []
  let start = 0
  while (start <= content.length - text.length) {
    const index = content.indexOf(text, start)
    if (index < 0) break
    output.push(index)
    if (output.length === 2) break
    start = index + 1
  }
  return output
}

const replace = (content: string, matches: ReadonlyArray<Match>, offset = 0): string => {
  let result = content
  for (const match of [...matches].reverse())
    result =
      result.slice(0, match.index - offset) + match.newText + result.slice(match.end - offset)
  return result
}

/** Match in one original normalized view; validate every region before assembling content. */
export const plan = (
  original: string,
  edits: ReadonlyArray<Replacement>,
): Result.Result<{ readonly content: string; readonly fuzzy: boolean }, Issue> => {
  const bom = original.startsWith('\uFEFF') ? '\uFEFF' : ''
  const source = original.slice(bom.length)
  const lf = normalizeLF(source)
  const fuzzyContent = normalizeFuzzy(lf)
  const normalized = edits.map((edit) => ({
    oldText: normalizeLF(edit.oldText),
    newText: normalizeLF(edit.newText),
  }))
  const fuzzy = normalized.some((edit) => !lf.includes(edit.oldText))
  const base = fuzzy ? fuzzyContent : lf
  const matches: Match[] = []
  for (const [editIndex, edit] of normalized.entries()) {
    const oldText = fuzzy ? normalizeFuzzy(edit.oldText) : edit.oldText
    const fuzzyOld = normalizeFuzzy(edit.oldText)
    if (oldText.length === 0 || fuzzyOld.length === 0)
      return Result.fail({
        reason: 'emptyOldText',
        message: `edits[${editIndex}].oldText is empty after normalization`,
      })
    const found = occurrences(base, oldText)
    const index = found[0]
    if (index === undefined)
      return Result.fail({
        reason: 'missing',
        message: `edits[${editIndex}].oldText was not found in the original file`,
      })
    if (found.length > 1 || occurrences(fuzzyContent, fuzzyOld).length > 1)
      return Result.fail({
        reason: 'ambiguous',
        message: `edits[${editIndex}].oldText is not unique; provide more context`,
      })
    if (lf.includes(edit.oldText) && edit.oldText === edit.newText)
      return Result.fail({ reason: 'noOp', message: `edits[${editIndex}] produces no change` })
    matches.push({ index, end: index + oldText.length, editIndex, newText: edit.newText })
  }
  matches.sort((a, b) => a.index - b.index)
  for (let i = 1; i < matches.length; i++) {
    const previous = matches[i - 1]
    const current = matches[i]
    if (previous && current && previous.end > current.index)
      return Result.fail({
        reason: 'overlap',
        message: `edits[${previous.editIndex}] and edits[${current.editIndex}] overlap`,
      })
  }
  const originalLines = lines(source)
  const defaultEnding = originalLines.find((line) => line.ending !== '')?.ending ?? '\n'
  let content: string
  if (!fuzzy) {
    // Map normalized LF boundaries back to original UTF-16 offsets. Copy all
    // bytes outside matches, even mixed endings within an otherwise touched line.
    const offsets = [0]
    for (let i = 0; i < source.length; i++) {
      if (source[i] === '\r' && source[i + 1] === '\n') i++
      offsets.push(i + 1)
    }
    const rawMatches = matches.map((match) => ({
      ...match,
      index: offsets[match.index] ?? source.length,
      end: offsets[match.end] ?? source.length,
      newText: match.newText.replace(/\n/g, defaultEnding),
    }))
    content = replace(source, rawMatches)
  } else {
    // Source-backed line overlay: normalization may expand characters or strip
    // trailing spaces. The matched ranges, not diff alignment, select touched
    // lines, so duplicate normalized lines cannot redirect an edit.
    const spans = lines(base).map((line) => line.text + line.ending)
    const starts: number[] = []
    let cursor = 0
    for (const line of spans) {
      starts.push(cursor)
      cursor += line.length
    }
    const groups: Array<{ start: number; end: number; matches: Match[] }> = []
    for (const match of matches) {
      const start = starts.findLastIndex((offset) => offset <= match.index)
      const end = starts.findLastIndex((offset) => offset < match.end) + 1
      const previous = groups.at(-1)
      if (previous && start < previous.end) {
        previous.end = Math.max(previous.end, end)
        previous.matches.push(match)
      } else groups.push({ start, end, matches: [match] })
    }
    const output: string[] = []
    let lineIndex = 0
    for (const group of groups) {
      for (const line of originalLines.slice(lineIndex, group.start))
        output.push(line.text + line.ending)
      const changed = replace(
        spans.slice(group.start, group.end).join(''),
        group.matches,
        starts[group.start] ?? 0,
      )
      for (const [index, line] of lines(changed).entries()) {
        const ending = originalLines[group.start + index]?.ending || defaultEnding
        output.push(line.text + (line.ending === '' ? '' : ending))
      }
      lineIndex = group.end
    }
    for (const line of originalLines.slice(lineIndex)) output.push(line.text + line.ending)
    content = output.join('')
  }
  content = bom + content
  if (content === original)
    return Result.fail({ reason: 'noOp', message: 'The replacements produced identical content' })
  return Result.succeed({ content, fuzzy })
}
