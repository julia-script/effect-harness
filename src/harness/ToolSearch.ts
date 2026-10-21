/**
 * The `tool_search` tool, ported from pi's `tool-search/`: the model searches the deferred tools
 * of its conversation that it is not offered yet, and the matches load with the `addTools`
 * control, so the next request offers them. Loading writes the conversation's `modelTools`, so
 * loaded tools stay offered on that branch, also after a compaction or a reset.
 *
 * Install `extension` with `layer`, and declare tools with `deferred: true` for it to find.
 */
import { Effect, Predicate, Schema } from 'effect'
import * as Tool from '../tool/Tool.ts'
import * as Toolkit from '../tool/Toolkit.ts'
import { Output, text } from '../tool/ToolResult.ts'
import * as Agent from './Agent.ts'
import * as Extension from './Extension.ts'
import { rank } from '../internal/search.ts'

export const NAME = 'tool_search'
export const DEFAULT_LIMIT = 8

/** Fixed, so it does not change while deferred tools come and go. */
const DESCRIPTION = `# Tool discovery\n\nSearches over deferred tool metadata with BM25 and exposes matching tools for the next model call.\n\nSome of the tools, such as tools of MCP servers, may not have been provided to you upfront, and you should use this tool (\`${NAME}\`) to search for the required tools. For MCP tool discovery, always use \`${NAME}\`.`

/** What a program or UI sees of a search: the tools it loaded. */
export const Details = Schema.Struct({ loaded: Schema.Array(Schema.String) })
export type Details = typeof Details.Type

export const ToolSearch = Tool.make(NAME, {
  description: DESCRIPTION,
  parameters: Schema.Struct({
    query: Schema.String.annotate({ description: 'Search query for deferred tools.' }),
    limit: Schema.optionalKey(
      Schema.Finite.annotate({
        description: `Maximum number of tools to return. Defaults to ${DEFAULT_LIMIT}.`,
      }),
    ),
  }),
  success: Output,
  failure: Output,
  replay: 'safe',
})

export const toolkit = Toolkit.make(ToolSearch)

/** The `tool-search` extension; installing it needs `layer`. */
export const extension = Extension.make('tool-search', { toolkit })

/** Descriptions and property names of a JSON Schema, recursively. */
const schemaText = (schema: unknown, parts: Array<string>): void => {
  if (!Predicate.isObject(schema)) return
  if (Predicate.isString(schema['description'])) parts.push(schema['description'])
  const properties = schema['properties']
  if (Predicate.isObject(properties)) {
    for (const [name, property] of Object.entries(properties)) {
      parts.push(name)
      schemaText(property, parts)
    }
  }
  schemaText(schema['items'], parts)
  for (const key of ['anyOf', 'oneOf', 'allOf']) {
    const variants = schema[key]
    if (Array.isArray(variants)) for (const variant of variants) schemaText(variant, parts)
  }
}

/** What a tool is found by: its name, with `_` as spaces too, its description, its parameters. */
const searchText = (tool: Tool.Any) => {
  const parts = [tool.name, tool.name.replaceAll('_', ' '), tool.description]
  schemaText(Tool.definition(tool).parameters, parts)
  return parts.filter((part) => part.trim() !== '').join(' ')
}

const summary = (loaded: ReadonlyArray<Tool.Any>) => {
  if (loaded.length === 0) return 'No matching tools found.'
  const lines = loaded.map(
    (tool) => `- ${tool.name}: ${tool.description.trim().split(/\r?\n/u)[0] ?? ''}`,
  )
  const plural = loaded.length === 1 ? '' : 's'
  return `Loaded ${loaded.length} tool${plural}. They are available from your next call:\n${lines.join('\n')}`
}

const search = Effect.fnUntraced(function* ({ query, limit }: Tool.Parameters<typeof ToolSearch>) {
  if (query.trim() === '') return yield* Effect.fail(text('query must not be empty'))
  const max = limit ?? DEFAULT_LIMIT
  if (!Number.isInteger(max) || max <= 0)
    return yield* Effect.fail(text('limit must be a positive integer'))
  const agent = yield* Agent.Current
  if (agent === undefined)
    return yield* Effect.fail(text(`${NAME} runs only for a conversation's calls`))
  const candidates = agent.deferred.map(({ tool }) => tool)
  const documents = candidates.map((tool) => ({ name: tool.name, text: searchText(tool) }))
  const names = rank(query, documents, max)
  const loaded = names.flatMap((name) => candidates.filter((tool) => tool.name === name))
  const details: Details = { loaded: [...names] }
  return text(summary(loaded), {
    details,
    ...(names.length === 0 ? {} : { control: { addTools: names } }),
  })
})

/** The implementation of `tool_search`. */
export const layer = toolkit.toLayer({ [NAME]: search })
