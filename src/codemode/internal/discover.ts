/**
 * `searchTools()`, `describeTool()` and `describeNamespace()`, answered from the start data, so a
 * replay sees what the first run saw.
 */
import { toCodemodeIdentifier } from '@earendil-works/pi-codemode/declarations'
import { Predicate } from 'effect'
import type { Schema } from 'effect'
import { rank } from '../../harness/internal/search.ts'
import type { NamespaceInfo, Outcome, Start } from './state.ts'

export const GLOBALS: ReadonlyArray<string> = ['searchTools', 'describeTool', 'describeNamespace']
const DEFAULT_LIMIT = 8

const fail = (message: string): Outcome => ({ ok: false, message })
const ok = (value: Schema.Json | undefined): Outcome =>
  value === undefined ? { ok: true } : { ok: true, value }

const matches = (namespace: NamespaceInfo, query: string) =>
  namespace.name === query || toCodemodeIdentifier(namespace.name) === toCodemodeIdentifier(query)

const describeTool = (start: Start, [name]: ReadonlyArray<unknown>) => {
  if (typeof name !== 'string') return fail('describeTool() expects a tool name')
  return ok(start.tools.find((tool) => tool.name === name || tool.jsName === name)?.description)
}

const describeNamespace = (start: Start, [name]: ReadonlyArray<unknown>): Outcome => {
  if (typeof name !== 'string') return fail('describeNamespace() expects a namespace name')
  const namespace = start.namespaces.find((candidate) => matches(candidate, name))
  if (namespace === undefined) return { ok: true }
  return ok({
    name: namespace.name,
    ...(namespace.description === undefined ? {} : { description: namespace.description }),
    ...(namespace.instructions === undefined ? {} : { instructions: namespace.instructions }),
    tools: namespace.tools.map((tool) => toCodemodeIdentifier(tool)),
  })
}

const optionsOf = (value: unknown): { readonly limit?: unknown; readonly namespace?: unknown } =>
  Predicate.isObject(value) ? value : {}

const searchTools = (start: Start, [query, options]: ReadonlyArray<unknown>) => {
  if (typeof query !== 'string') return fail('searchTools() expects a query string')
  const { limit = DEFAULT_LIMIT, namespace } = optionsOf(options)
  if (typeof limit !== 'number' || !Number.isInteger(limit) || limit <= 0)
    return fail('searchTools() limit must be a positive integer')
  if (namespace !== undefined && namespace !== null && typeof namespace !== 'string')
    return fail('searchTools() namespace must be a string')
  const own = (name: string) => start.namespaces.find((each) => each.tools.includes(name))
  const documents = start.tools.flatMap((tool) => {
    const group = own(tool.name)
    if (typeof namespace === 'string' && (group === undefined || !matches(group, namespace)))
      return []
    const parts = [tool.name, tool.name.replaceAll('_', ' '), tool.description]
    const text = [...parts, group?.name ?? '', group?.description ?? ''].join(' ')
    return [{ name: tool.name, text }]
  })
  return ok(
    rank(query, documents, limit).flatMap((name) =>
      start.tools
        .filter((tool) => tool.name === name)
        .map((tool) => ({ name: tool.jsName, description: tool.description })),
    ),
  )
}

/** Answer discovery global `name` with the script's arguments. */
export const discover = (start: Start, name: string, args: unknown): Outcome => {
  const list = Array.isArray(args) ? args : []
  if (name === 'describeTool') return describeTool(start, list)
  if (name === 'describeNamespace') return describeNamespace(start, list)
  return searchTools(start, list)
}
