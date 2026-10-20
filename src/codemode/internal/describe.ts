/**
 * What the model reads about code mode, ported from pi's `codemode/describe.ts`: the tool's fixed
 * description, and the `codemode` section listing the tools scripts can call as TypeScript
 * declarations, grouped by namespace and limited to a token budget.
 */
import type { CodemodeJsonSchema } from '@earendil-works/pi-codemode'
import { renderToolSample, toCodemodeIdentifier } from '@earendil-works/pi-codemode/declarations'
import { Option, Schema } from 'effect'
import { JsonObject } from '../../storage/schema/Json.ts'
import * as Tool from '../../tool/Tool.ts'
import { Output } from '../../tool/ToolResult.ts'

/** A group of tools named by a common prefix, such as an MCP server's `mcp__github__`. */
export interface Namespace {
  readonly name: string
  readonly description?: string
  readonly instructions?: string
  /** Tool names starting with it belong to the namespace; the longest matching prefix wins. */
  readonly prefix: string
}

/** Default token budget of the declarations in the `codemode` section. */
export const DEFAULT_INLINE_BUDGET = 3000
const CHARS_PER_TOKEN = 4

const TEXT_OUTPUT: CodemodeJsonSchema = { type: 'string' }

/** The tool's description: the script environment and its globals, the same everywhere. */
export const DESCRIPTION = [
  `Run JavaScript that calls other tools. The input is raw JavaScript (not JSON, no code fence), run as an async function body in a QuickJS sandbox: top-level \`await\` and \`return\` work. No Node, file system, network, or timers.
- \`await tools.<name>({ ...args })\` resolves to a string, or a value if the tool's declaration says so, and rejects with an Error on failure. Calls still running when the script ends are cancelled.
- The script survives a restart: it resumes where it was, without running finished calls again.
- Optional first line: \`// @options: {"max_output_tokens": 10000, "timeout_ms": 60000}\``,
  [
    'Globals:',
    '- `text(value)`, `image(dataUrlOrImageBlock)`, `console.log(...)`, and top-level `return` add output; `exit()` ends the script. With several text items, each starts with a `==> text N/M <==` line, and `console` lines follow the other output in one `<console_output>` block. `image()` also saves the image to a temp file and the result names its path.',
    '- `store(key, value)` and `load(key)` keep JSON values across codemode calls.',
    '- `ALL_TOOLS`, `await searchTools(query, { limit?, namespace? })`, `await describeTool(name)`, `await describeNamespace(name)`: find tools the `codemode` section does not list.',
  ].join('\n'),
  'The `codemode` section of the system prompt lists the tools scripts can call, by namespace, as far as space allows.',
].join('\n\n')

/** Whether a call of `tool` resolves to its value: its success is not an `Output`. */
export const isStructured = (tool: Tool.Any) => tool.success !== Output

const jsonSchema = (schema: Tool.Codec): CodemodeJsonSchema => {
  const document = Schema.toJsonSchemaDocument(schema)
  const full =
    Object.keys(document.definitions).length === 0
      ? document.schema
      : { ...document.schema, $defs: document.definitions }
  return Option.getOrElse(Schema.decodeUnknownOption(JsonObject)(full), () => ({}))
}

/** A tool as a script sees it; one that is not structured resolves to its text output. */
export const declarationOf = (tool: Tool.Any) => ({
  name: tool.name,
  description: tool.description,
  inputSchema: Tool.definition(tool).parameters,
  outputSchema: isStructured(tool) ? jsonSchema(tool.success) : TEXT_OUTPUT,
})

/** The tool's description and TypeScript declaration. */
export const sampleOf = (tool: Tool.Any): string => renderToolSample(declarationOf(tool))

/** The namespace of a tool: the one with the longest prefix its name starts with. */
export const namespaceOf = (
  name: string,
  namespaces: ReadonlyArray<Namespace>,
): Namespace | undefined => {
  let found: Namespace | undefined
  for (const namespace of namespaces) {
    const longer = found === undefined || namespace.prefix.length > found.prefix.length
    if (name.startsWith(namespace.prefix) && longer) found = namespace
  }
  return found
}

const renderTool = (tool: Tool.Any) => {
  const id = toCodemodeIdentifier(tool.name)
  const heading = id === tool.name ? `### \`${id}\`` : `### \`${id}\` (\`${tool.name}\`)`
  return `${heading}\n${sampleOf(tool).trim()}`
}

interface Entry {
  readonly name: string
  readonly section: string
  readonly cost: number
}
interface Group {
  readonly namespace: Namespace | undefined
  readonly entries: Array<Entry>
}

/**
 * The sections that fit the budget: each round, every group (tools without a namespace first,
 * then namespaces by name) places its cheapest remaining tool; a group whose next tool does not
 * fit drops out. Every namespace is represented before any namespace is complete.
 */
const select = (groups: ReadonlyArray<Group>, budget: number): ReadonlySet<string> => {
  const shown = new Set<string>()
  let remaining = budget
  let active = groups
    .map((group) => group.entries.toSorted((a, b) => a.cost - b.cost))
    .filter((queue) => queue.length > 0)
  while (active.length > 0) {
    const still: typeof active = []
    for (const queue of active) {
      const [next] = queue
      if (next !== undefined && next.cost <= remaining) {
        remaining -= next.cost
        shown.add(next.name)
        queue.shift()
        if (queue.length > 0) still.push(queue)
      }
    }
    active = still
  }
  return shown
}

const groupsOf = (tools: ReadonlyArray<Tool.Any>, namespaces: ReadonlyArray<Namespace>) => {
  const groups = new Map<string, Group>([['', { namespace: undefined, entries: [] }]])
  for (const tool of tools) {
    const namespace = namespaceOf(tool.name, namespaces)
    const key = namespace === undefined ? '' : `ns:${namespace.name}`
    const group = groups.get(key) ?? { namespace, entries: [] }
    groups.set(key, group)
    const section = renderTool(tool)
    group.entries.push({
      name: tool.name,
      section,
      cost: Math.ceil(section.length / CHARS_PER_TOKEN),
    })
  }
  const rank = (group: Group) => (group.namespace === undefined ? '' : `~${group.namespace.name}`)
  return [...groups.values()].toSorted((a, b) => rank(a).localeCompare(rank(b)))
}

const heading = (namespace: Namespace, shown: number, total: number) => {
  const listing = shown === total ? '' : ' (some tools not listed)'
  const listed = shown === 0 ? ' (tools not listed)' : listing
  const description = namespace.description?.trim() ?? ''
  return `## ${namespace.name}${listed}${description === '' ? '' : `\n${description}`}`
}

/** The `codemode` section; `undefined` without callable tools. */
export const section = (
  tools: ReadonlyArray<Tool.Any>,
  namespaces: ReadonlyArray<Namespace>,
  budget: number,
): string | undefined => {
  if (tools.length === 0) return undefined
  const groups = groupsOf(tools, namespaces)
  const shown = select(groups, budget)
  const parts = ['Tools scripts can call (`await tools.<name>(args)`):']
  for (const { namespace, entries } of groups) {
    if (entries.length === 0) continue
    const visible = entries.filter((entry) => shown.has(entry.name))
    if (namespace !== undefined) parts.push(heading(namespace, visible.length, entries.length))
    else if (visible.length < entries.length)
      parts.push('Some tools are not listed: find them with `searchTools()`.')
    for (const entry of visible) parts.push(entry.section)
  }
  return parts.join('\n\n')
}
