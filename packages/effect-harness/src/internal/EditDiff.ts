const lines = (content: string): string[] => content.match(/[^\n]*\n|[^\n]+/g) ?? []
const patchLine = (prefix: string, line: string): string =>
  line.endsWith('\n') ? prefix + line : `${prefix}${line}\n\\ No newline at end of file\n`
const range = (start: number, length: number): string =>
  `${length === 0 ? start : start + 1},${length}`

// Git unified patch headers use C-style byte escapes, not JSON unicode escapes.
const quotePath = (path: string): string => {
  let result = '"'
  for (const byte of new TextEncoder().encode(path)) {
    if (byte === 34 || byte === 92) result += '\\' + String.fromCharCode(byte)
    else if (byte < 32 || byte >= 127) result += '\\' + byte.toString(8).padStart(3, '0')
    else result += String.fromCharCode(byte)
  }
  return result + '"'
}

const physicalLines = (content: string): string[] =>
  content.match(/[^\r\n]*(?:\r\n|\r|\n)|[^\r\n]+/g) ?? []

interface Operation {
  readonly prefix: ' ' | '+' | '-'
  readonly line: string
}

/** Myers line diff. Bound search work; a large rewrite still gets a valid patch. */
const compare = (before: ReadonlyArray<string>, after: ReadonlyArray<string>): Operation[] => {
  const frontier = new Map<number, number>([[1, 0]])
  const trace: Array<Map<number, number>> = []
  let work = 0
  for (let distance = 0; distance <= before.length + after.length; distance++) {
    if (work > 1_000_000 || distance > 512) break
    trace.push(new Map(frontier))
    for (let diagonal = -distance; diagonal <= distance; diagonal += 2) {
      work++
      let x =
        diagonal === -distance ||
        (diagonal !== distance &&
          (frontier.get(diagonal - 1) ?? -1) < (frontier.get(diagonal + 1) ?? -1))
          ? (frontier.get(diagonal + 1) ?? 0)
          : (frontier.get(diagonal - 1) ?? 0) + 1
      let y = x - diagonal
      while (x < before.length && y < after.length && before[x] === after[y]) {
        x++
        y++
      }
      frontier.set(diagonal, x)
      if (x >= before.length && y >= after.length) {
        const output: Operation[] = []
        for (let d = trace.length - 1; d >= 0; d--) {
          const previous = trace[d]
          const k = x - y
          const previousK =
            k === -d || (k !== d && (previous?.get(k - 1) ?? -1) < (previous?.get(k + 1) ?? -1))
              ? k + 1
              : k - 1
          const previousX = previous?.get(previousK) ?? 0
          const previousY = previousX - previousK
          while (x > previousX && y > previousY) {
            output.push({ prefix: ' ', line: before[x - 1] ?? '' })
            x--
            y--
          }
          if (d === 0) break
          if (x === previousX) {
            output.push({ prefix: '+', line: after[y - 1] ?? '' })
            y--
          } else {
            output.push({ prefix: '-', line: before[x - 1] ?? '' })
            x--
          }
        }
        return output.reverse()
      }
    }
  }
  return [
    ...before.map((line): Operation => ({ prefix: '-', line })),
    ...after.map((line): Operation => ({ prefix: '+', line })),
  ]
}

const display = (before: string, after: string) => {
  const oldLines = physicalLines(before)
  const newLines = physicalLines(after)
  let start = 0
  while (start < Math.min(oldLines.length, newLines.length) && oldLines[start] === newLines[start])
    start++
  let end = 0
  while (
    end < Math.min(oldLines.length, newLines.length) - start &&
    oldLines[oldLines.length - end - 1] === newLines[newLines.length - end - 1]
  )
    end++
  const output: string[] = []
  const line = (prefix: string, index: number, value: string) =>
    `${prefix}${index + 1} ${value.replace(/(?:\r\n|\r|\n)$/, '')}`
  for (let i = Math.max(0, start - 4); i < start; i++) output.push(line(' ', i, oldLines[i] ?? ''))
  let oldLine = start
  let newLine = start
  for (const operation of compare(
    oldLines.slice(start, oldLines.length - end),
    newLines.slice(start, newLines.length - end),
  )) {
    output.push(
      line(operation.prefix, operation.prefix === '-' ? oldLine : newLine, operation.line),
    )
    if (operation.prefix !== '+') oldLine++
    if (operation.prefix !== '-') newLine++
  }
  for (let i = newLines.length - end; i < Math.min(newLines.length, newLines.length - end + 4); i++)
    output.push(line(' ', i, newLines[i] ?? ''))
  return { diff: output.join('\n'), firstChangedLine: start + 1 }
}

/** A valid single-hunk unified patch of actual bytes, including CRLF and EOF markers. */
export const describe = (path: string, before: string, after: string) => {
  const oldLines = lines(before)
  const newLines = lines(after)
  let prefix = 0
  while (
    prefix < Math.min(oldLines.length, newLines.length) &&
    oldLines[prefix] === newLines[prefix]
  )
    prefix++
  let suffix = 0
  while (
    suffix < Math.min(oldLines.length, newLines.length) - prefix &&
    oldLines[oldLines.length - suffix - 1] === newLines[newLines.length - suffix - 1]
  )
    suffix++
  const start = Math.max(0, prefix - 4)
  const oldEnd = Math.min(oldLines.length, oldLines.length - suffix + 4)
  const newEnd = Math.min(newLines.length, newLines.length - suffix + 4)
  const operations: Operation[] = []
  for (let i = start; i < prefix; i++) operations.push({ prefix: ' ', line: oldLines[i] ?? '' })
  for (const operation of compare(
    oldLines.slice(prefix, oldLines.length - suffix),
    newLines.slice(prefix, newLines.length - suffix),
  ))
    operations.push(operation)
  for (let i = oldLines.length - suffix; i < oldEnd; i++)
    operations.push({ prefix: ' ', line: oldLines[i] ?? '' })
  // Quote headers to keep path newlines/tabs from becoming patch control lines.
  const header = quotePath(path)
  const patch =
    `--- ${header}\n+++ ${header}\n@@ -${range(start, oldEnd - start)} +${range(start, newEnd - start)} @@\n` +
    operations.map((operation) => patchLine(operation.prefix, operation.line)).join('')
  return { ...display(before, after), patch }
}
