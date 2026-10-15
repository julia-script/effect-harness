/** Only protocol-owned content channels are visited; application JSON stays opaque. */
import type * as Schema from 'effect/Schema'

export type Path = ReadonlyArray<string | number>
type Visit = (path: Path) => void
const object = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {}
const field = (value: unknown, key: string): unknown => {
  const record = object(value)
  return Object.hasOwn(record, key) ? record[key] : undefined
}
const each = (value: unknown, path: Path, visit: (value: unknown, path: Path) => void) => {
  if (Array.isArray(value)) value.forEach((item, index) => visit(item, [...path, index]))
}
const part = (value: unknown, path: Path, visit: Visit) => {
  if (field(value, 'type') !== 'file') return
  const mime = field(value, 'mediaType')
  if (typeof mime !== 'string' || !mime.startsWith('image/')) return
  const data = field(value, 'data')
  if (typeof data === 'string' || data === null) visit([...path, 'data'])
  else if (field(data, '_tag') === '@effect-harness/Bytes') visit([...path, 'data', 'data'])
}
const content = (value: unknown, path: Path, visit: Visit) =>
  each(value, path, (item, at) => part(item, at, visit))
const envelope = (value: unknown, path: Path, visit: Visit) => {
  if (field(value, '_tag') === '@effect-harness/ToolContent')
    content(field(value, 'content'), [...path, 'content'], visit)
}
const messages = (value: unknown, path: Path, visit: Visit) =>
  each(value, path, (message, at) => {
    const role = field(message, 'role')
    if (role !== 'user' && role !== 'assistant' && role !== 'tool') return
    each(field(message, 'content'), [...at, 'content'], (item, location) => {
      if (role !== 'tool') part(item, location, visit)
      if (role !== 'user' && field(item, 'type') === 'tool-result')
        envelope(field(item, 'result'), [...location, 'result'], visit)
    })
  })
const entry = (value: unknown, path: Path, visit: Visit) => {
  messages(field(value, 'model'), [...path, 'model'], visit)
  each(field(value, 'edits'), [...path, 'edits'], (edit, at) => {
    if (field(edit, '_tag') === 'replace')
      messages(field(edit, 'messages'), [...at, 'messages'], visit)
  })
  if (field(value, 'kind') === 'tool.result')
    content(
      field(field(field(value, 'data'), 'result'), 'content'),
      [...path, 'data', 'result', 'content'],
      visit,
    )
}

export const record = (category: string, value: Schema.Json): ReadonlyArray<Path> => {
  const paths: Array<Path> = []
  const visit: Visit = (path) => paths.push(path)
  if (category === 'entry') entry(field(value, 'entry'), ['entry'], visit)
  if (category === 'task') {
    const kind = field(value, 'kind')
    const checkpoint = field(field(value, 'state'), 'checkpoint')
    if (kind === 'harness.run') {
      if (field(checkpoint, '_tag') === 'queued')
        content(
          field(field(checkpoint, 'draft'), 'content'),
          ['state', 'checkpoint', 'draft', 'content'],
          visit,
        )
      const partial = field(checkpoint, 'partial')
      messages(
        field(field(partial, 'message'), 'content'),
        ['state', 'checkpoint', 'partial', 'message', 'content'],
        visit,
      )
    }
    if (kind === 'harness.tool' || kind === 'harness.tool.nested')
      content(
        field(field(checkpoint, 'observedResult'), 'content'),
        ['state', 'checkpoint', 'observedResult', 'content'],
        visit,
      )
  }
  return paths
}

export const ownsDocumentMedia = (kind: string): boolean =>
  [
    'harness.request',
    'harness.inbox',
    'harness.tool.round',
    'harness.tool.nested.result',
    'harness.model.live',
  ].includes(kind)

export const document = (kind: string, value: Schema.Json): ReadonlyArray<Path> => {
  const paths: Array<Path> = []
  const visit: Visit = (path) => paths.push(path)
  if (kind === 'harness.request')
    messages(
      field(field(field(value, 'request'), 'prompt'), 'content'),
      ['request', 'prompt', 'content'],
      visit,
    )
  if (kind === 'harness.model.live')
    messages(
      field(field(field(field(value, 'generation'), 'partial'), 'message'), 'content'),
      ['generation', 'partial', 'message', 'content'],
      visit,
    )
  if (kind === 'harness.inbox')
    each(field(value, 'items'), ['items'], (item, at) =>
      entry(field(item, 'payload'), [...at, 'payload'], visit),
    )
  if (kind === 'harness.tool.round')
    each(field(field(value, 'round'), 'slots'), ['round', 'slots'], (slot, at) =>
      content(field(field(slot, 'result'), 'content'), [...at, 'result', 'content'], visit),
    )
  if (kind === 'harness.tool.nested.result') {
    const result = field(field(value, 'receipt'), 'result')
    if (field(result, 'valueKind') === 'content') {
      const data = field(result, 'structuredOutput')
      const at = ['receipt', 'result', 'structuredOutput']
      if (Array.isArray(data)) content(data, at, visit)
      else part(data, at, visit)
      if (field(result, 'valueFromDetails') === true) {
        const details = field(result, 'details')
        const location = ['receipt', 'result', 'details']
        if (Array.isArray(details)) content(details, location, visit)
        else part(details, location, visit)
      }
      // The alias protocol explicitly uses the tool-content details envelope here.
      if (field(result, 'valueFromContent') === true)
        envelope(field(result, 'details'), ['receipt', 'result', 'details'], visit)
    }
  }
  return paths
}

/** Own-property access only, including array indices. */
export const at = (value: Schema.Json, path: Path): Schema.Json | undefined => {
  let current: Schema.Json | undefined = value
  for (const key of path) {
    if (current === null || typeof current !== 'object' || !Object.hasOwn(current, key))
      return undefined
    current = (current as Record<string | number, Schema.Json>)[key]
  }
  return current
}
export const set = (value: Schema.Json, path: Path, data: Schema.Json): boolean => {
  const parent = at(value, path.slice(0, -1))
  const key = path.at(-1)
  if (
    key === undefined ||
    parent === null ||
    typeof parent !== 'object' ||
    !Object.hasOwn(parent, key)
  )
    return false
  Object.defineProperty(parent, key, {
    value: data,
    enumerable: true,
    configurable: true,
    writable: true,
  })
  return true
}
