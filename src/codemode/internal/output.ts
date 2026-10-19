/**
 * A script's result as the model reads it, ported from pi's `codemode/output.ts`: a header with
 * the wall time, the script's text items (`==> text N/M <==` when there are several), its
 * `console` lines in one `<console_output>` block, the returned value, and on failure the error
 * with the calls already made. Text past the token budget is cut to its start and end, with the
 * complete text in a file; images are saved to files too, their path next to them.
 */
import type { CodemodeOutputItem, CodemodeResult } from '@earendil-works/pi-codemode/portable'
import { Effect } from 'effect'
import type { ImageContent, TextContent } from '../../model/Message.ts'
import type { Row } from './state.ts'

type Item = TextContent | ImageContent

/** Writes a file of the result under a fixed name: its path, or why it could not be written. */
export type Write = (
  name: string,
  data: string | Uint8Array,
) => Effect.Effect<{ readonly path: string } | { readonly error: string }>

/** The tool's output limits: results stay inside them, so nothing is cut without a file. */
export interface Limits {
  readonly maxBytes: number
  readonly maxLines: number
}
/** Outputs bounded by the script's own token budget; the tool's limits only stop a runaway. */
export const OUTPUT_LIMITS: Limits = { maxBytes: 4 * 1024 * 1024, maxLines: 100_000 }

/** Lines of the result besides the script's text: the header, markers, notes. */
const LINE_MARGIN = 100

export const DEFAULT_MAX_OUTPUT_TOKENS = 10_000
const CHARS_PER_TOKEN = 4

const IMAGE_EXTENSIONS: Readonly<Record<string, string>> = {
  'image/png': '.png',
  'image/jpeg': '.jpg',
  'image/gif': '.gif',
  'image/webp': '.webp',
}

const valueText = (value: unknown): string =>
  typeof value === 'string' ? value : (JSON.stringify(value) ?? String(value))

/** Text items marked when there are several; `console` lines last, in one block. */
const formatItems = (output: ReadonlyArray<CodemodeOutputItem>): Array<Item> => {
  const total = output.filter((item) => item.type === 'text' && item.console !== true).length
  const items: Array<Item> = []
  const lines: Array<string> = []
  let index = 0
  for (const item of output) {
    if (item.type === 'image') items.push(item)
    else if (item.console === true) lines.push(item.text)
    else {
      index += 1
      const marker = total > 1 ? `==> text ${index}/${total} <==\n` : ''
      items.push({ type: 'text', text: `${marker}${item.text}` })
    }
  }
  if (lines.length > 0)
    items.push({ type: 'text', text: `<console_output>\n${lines.join('\n')}\n</console_output>` })
  return items
}

/** Adjacent text items as one, each part on its own line. */
const joinText = (items: ReadonlyArray<Item>): Array<Item> => {
  const joined: Array<Item> = []
  let parts: Array<string> = []
  let last = ''
  const flush = () => {
    if (parts.length > 0) joined.push({ type: 'text', text: parts.join('') })
    parts = []
    last = ''
  }
  for (const item of items) {
    if (item.type !== 'text') {
      flush()
      joined.push(item)
      continue
    }
    if (last !== '' && !last.endsWith('\n')) {
      parts.push('\n')
      last = '\n'
    }
    parts.push(item.text)
    if (item.text !== '') last = item.text
  }
  flush()
  return joined
}

interface Cut {
  readonly items: Array<Item>
  readonly fullOutputPath?: string
}

const shorten = (combined: string, maxTokens: number, limits: Limits) => {
  // Within the limits too: a UTF-16 unit is at most 3 UTF-8 bytes.
  const budget = Math.min(maxTokens * CHARS_PER_TOKEN, Math.floor(limits.maxBytes / 4))
  const lineBudget = Math.floor((limits.maxLines - LINE_MARGIN) / 2)
  const lines = combined.split('\n').length
  if (combined.length <= budget && lines <= 2 * lineBudget) return null
  const headChars = Math.floor(budget / 2)
  const tailChars = budget - headChars
  const head = combined.slice(0, headChars).split('\n').slice(0, lineBudget).join('\n')
  const tail =
    tailChars > 0 ? combined.slice(-tailChars).split('\n').slice(-lineBudget).join('\n') : ''
  const removed = Math.ceil((combined.length - head.length - tail.length) / CHARS_PER_TOKEN)
  const original = Math.ceil(combined.length / CHARS_PER_TOKEN)
  return `Warning: truncated output (original token count: ${original})\nTotal output lines: ${lines}\n\n${head}…${removed} tokens truncated…${tail}`
}

/** Apply the token budget, with the complete text in a file. */
const truncate = (items: ReadonlyArray<Item>, maxTokens: number, limits: Limits, write: Write) =>
  Effect.gen(function* () {
    const texts = items.flatMap((item) => (item.type === 'text' ? [item.text] : []))
    const combined = texts.join('\n')
    const short = texts.length === 0 ? null : shorten(combined, maxTokens, limits)
    if (short === null) return { items: [...items] } satisfies Cut
    const spilled = yield* write('output.txt', combined)
    const note =
      'path' in spilled
        ? `\n\n[Full output: ${spilled.path} (read with offset/limit)]`
        : `\n\n[Could not save the full output: ${spilled.error}]`
    const images = items.filter((item) => item.type === 'image')
    const cut: Cut = {
      items: [{ type: 'text', text: `${short}${note}` }, ...images],
      ...('path' in spilled ? { fullOutputPath: spilled.path } : {}),
    }
    return cut
  })

const decode = (data: string) => Uint8Array.from(atob(data), (char) => char.codePointAt(0) ?? 0)

/** Each image saved to a file, with a text item naming its path before it; saved once each. */
const saveImages = (items: ReadonlyArray<Item>, write: Write) =>
  Effect.gen(function* () {
    const labels = new Map<string, string>()
    const saved: Array<Item> = []
    for (const item of items) {
      if (item.type !== 'image') {
        saved.push(item)
        continue
      }
      let label = labels.get(item.data)
      if (label === undefined) {
        const bytes = decode(item.data)
        const kind = `${item.mimeType}, ${bytes.length} bytes`
        const name = `image-${labels.size + 1}${IMAGE_EXTENSIONS[item.mimeType] ?? '.bin'}`
        const written = yield* write(name, bytes)
        label =
          'path' in written
            ? `[Image saved to ${written.path} (${kind})]`
            : `[Image (${kind}) could not be saved: ${written.error}]`
        labels.set(item.data, label)
      }
      saved.push({ type: 'text', text: label }, item)
    }
    return saved
  })

const errorHead = (error: Extract<CodemodeResult, { ok: false }>['error']) => {
  if (error.kind === 'script') return error.stack ?? `${error.name ?? 'Error'}: ${error.message}`
  if (error.kind === 'timeout') return `Script timed out: ${error.message}`
  if (error.kind === 'aborted') return `Script aborted: ${error.message}`
  return `Script sandbox failed: ${error.message}`
}

const formatError = (result: Extract<CodemodeResult, { ok: false }>, calls: ReadonlyArray<Row>) => {
  const made = calls.map((call) => `${call.name} (${call.status})`).join(', ')
  const summary =
    calls.length === 0
      ? 'No tool calls were made.'
      : `Tool calls made before the failure (they are not undone): ${made}`
  return `${errorHead(result.error)}\n\n${summary}`
}

export interface Options {
  readonly wallMs: number
  readonly maxOutputTokens: number
  readonly calls: ReadonlyArray<Row>
  readonly limits: Limits
  readonly write: Write
}

/** The result's content for a script's result, and the complete text's file when it was cut. */
export const format = (result: CodemodeResult, options: Options) =>
  Effect.gen(function* () {
    const output = [...result.output]
    if (result.ok && result.value !== undefined)
      output.push({ type: 'text', text: valueText(result.value) })
    const items = formatItems(output)
    if (!result.ok)
      items.push({ type: 'text', text: `Script error:\n${formatError(result, options.calls)}` })
    const { maxOutputTokens, limits, write } = options
    const cut = yield* truncate(joinText(items), maxOutputTokens, limits, write)
    // After truncation, which moves images after the text, so each path stays next to its image.
    const content = joinText(yield* saveImages(cut.items, write))
    const status = result.ok ? 'Script completed' : 'Script failed'
    const header = `${status}\nWall time ${(options.wallMs / 1000).toFixed(1)} seconds\nOutput:\n`
    return {
      content: [{ type: 'text', text: header } satisfies Item, ...content],
      ...(cut.fullOutputPath === undefined ? {} : { fullOutputPath: cut.fullOutputPath }),
    }
  })
