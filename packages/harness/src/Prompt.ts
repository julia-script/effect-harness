// Positional prompt planning adapted from pi-durable (MIT), pinned 636703a0.
import * as Schema from 'effect/Schema'
import * as AiPrompt from 'effect/ai/Prompt'
import * as Context from './Context.ts'

export function replaySections(patches: ReadonlyArray<Context.SystemPatch>): Map<string, string> {
  const shown = new Map<string, string>()
  for (const patch of patches)
    for (const [key, value] of Object.entries(patch.sections ?? {})) {
      if (value === null) shown.delete(key)
      else shown.set(key, value)
    }
  return shown
}
export function replayTools(
  patches: ReadonlyArray<Context.SystemPatch>,
): ReadonlyArray<Context.ToolDeclaration> {
  const tools = new Map<string, Context.ToolDeclaration>()
  for (const patch of patches) {
    for (const name of patch.toolsRemoved ?? []) tools.delete(name)
    for (const tool of patch.toolsAdded ?? []) tools.set(tool.name, tool)
  }
  return [...tools.values()]
}
export function planSections(
  shown: ReadonlyMap<string, string>,
  desired: ReadonlyMap<string, string>,
): ReadonlyArray<Readonly<Record<string, string | null>>> {
  const patched = [
    ...[...shown.keys()].filter((key) => desired.has(key)),
    ...[...desired.keys()].filter((key) => !shown.has(key)),
  ]
  if (patched.some((key, index) => key !== [...desired.keys()][index]))
    return [
      Object.fromEntries([...shown.keys()].map((key) => [key, null])),
      Object.fromEntries(desired),
    ]
  const pairs: Array<readonly [string, string | null]> = []
  for (const [key, value] of shown)
    if (desired.get(key) !== value) pairs.push([key, desired.get(key) ?? null])
  for (const [key, value] of desired) if (!shown.has(key)) pairs.push([key, value])
  return pairs.length === 0 ? [] : [Object.fromEntries(pairs)]
}
const equalDeclaration = Schema.toEquivalence(Context.ToolDeclaration)
export function planTools(
  offered: ReadonlyArray<Context.ToolDeclaration>,
  desired: ReadonlyArray<Context.ToolDeclaration>,
): Pick<Context.SystemPatch, 'toolsRemoved' | 'toolsAdded'> {
  const wanted = new Map(desired.map((tool) => [tool.name, tool]))
  const kept = offered.filter((tool) => {
    const next = wanted.get(tool.name)
    return next !== undefined && equalDeclaration(tool, next)
  })
  const names = new Set(kept.map((tool) => tool.name))
  const added = desired.filter((tool) => !names.has(tool.name))
  if ([...kept, ...added].some((tool, index) => tool.name !== desired[index]?.name))
    return { toolsRemoved: offered.map((tool) => tool.name), toolsAdded: desired }
  return {
    toolsRemoved: offered.filter((tool) => !names.has(tool.name)).map((tool) => tool.name),
    toolsAdded: added,
  }
}
/** A head marker forces a full baseline once; prior retained systems are omitted even when values match. */
export function plan(
  view: Context.View,
  desired: ReadonlyMap<string, string>,
  tools: ReadonlyArray<Context.ToolDeclaration>,
): {
  readonly patches: ReadonlyArray<Context.SystemPatch>
  readonly edits: ReadonlyArray<Context.Edit>
} {
  const patches = Context.systemPatches(view)
  if (
    view.head !== undefined &&
    !view.entries.some(
      (entry) => entry.system !== undefined && entry.id > (view.head?.id ?? -Infinity),
    )
  )
    return {
      patches: [{ sections: Object.fromEntries(desired), toolsRemoved: [], toolsAdded: tools }],
      edits: view.entries
        .filter((entry) => entry.system !== undefined)
        .map((entry) => ({ target: entry.id, action: 'omit' })),
    }
  const sections = planSections(replaySections(patches), desired)
  const changes = planTools(replayTools(patches), tools)
  const hasTools = (changes.toolsRemoved?.length ?? 0) > 0 || (changes.toolsAdded?.length ?? 0) > 0
  if (!hasTools) return { patches: sections.map((value) => ({ sections: value })), edits: [] }
  if (sections.length === 0) return { patches: [changes], edits: [] }
  return {
    patches: sections.map((value, index) => ({
      sections: value,
      ...(index === sections.length - 1 ? changes : {}),
    })),
    edits: [],
  }
}
/** Metadata identifies encoded managed patches without discarding plain stored system instructions. */
export interface ProjectionOptions {
  readonly managedSystemMessages?: ReadonlyArray<AiPrompt.Message> | undefined
}
/** Projects plain system content in transcript order followed by the effective named sections once. */
export function toPrompt(
  messages: ReadonlyArray<AiPrompt.Message>,
  sections: ReadonlyMap<string, string>,
  options: ProjectionOptions = {},
): AiPrompt.Prompt {
  const managed = new Set(options.managedSystemMessages ?? [])
  const system = messages.filter((message) => message.role === 'system' && !managed.has(message))
  const content = [...sections.values()].filter((value) => value.length > 0).join('\n\n')
  return AiPrompt.fromMessages([
    ...system,
    ...(content === '' ? [] : [AiPrompt.systemMessage({ content })]),
    ...messages.filter((message) => message.role !== 'system'),
  ])
}
