/**
 * Managed section and tool deltas for native model prompts.
 */
import * as Record from 'effect/Record'
import { dual } from 'effect/Function'
import * as Arr from 'effect/Array'
import * as Option from 'effect/Option'
// Positional prompt planning adapted from pi-durable (MIT), pinned 636703a0.
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Context from './Context.ts'

/**
 * Replays ordered managed section patches into their effective dictionary.
 *
 * @category combinators
 */
export function replaySections(self: ReadonlyArray<Context.SystemPatch>): Map<string, string> {
  const shown = new Map<string, string>()
  for (const patch of self)
    for (const [key, value] of Object.entries(patch.sections ?? {})) {
      if (value === null) shown.delete(key)
      else shown.set(key, value)
    }
  return shown
}
/**
 * Replays ordered tool declarations and removals.
 *
 * @category combinators
 */
export function replayTools(
  self: ReadonlyArray<Context.SystemPatch>,
): Array<Context.ToolDeclaration> {
  const tools = new Map<string, Context.ToolDeclaration>()
  for (const patch of self) {
    for (const name of patch.toolsRemoved ?? []) tools.delete(name)
    for (const tool of patch.toolsAdded ?? []) tools.set(tool.name, tool)
  }
  return [...tools.values()]
}
function planSectionsImpl(
  self: ReadonlyMap<string, string>,
  that: ReadonlyMap<string, string>,
): Array<Readonly<Record<string, string | null>>> {
  const patched = [
    ...[...self.keys()].filter((key) => that.has(key)),
    ...[...that.keys()].filter((key) => !self.has(key)),
  ]
  if (patched.some((key, index) => !Option.contains(Arr.get([...that.keys()], index), key)))
    return [Record.fromIterableWith(self.keys(), (key) => [key, null]), Record.fromEntries(that)]
  const pairs: Array<readonly [string, string | null]> = []
  for (const [key, value] of self)
    if (!Option.contains(Option.fromUndefinedOr(that.get(key)), value))
      pairs.push([key, Option.getOrElse(Option.fromUndefinedOr(that.get(key)), () => null)])
  for (const [key, value] of that) if (!self.has(key)) pairs.push([key, value])
  return pairs.length === 0 ? [] : [Record.fromEntries(pairs)]
}
/**
 * Returns named section changes from the shown and desired dictionaries.
 *
 * @category combinators
 */
export const planSections: {
  (
    that: ReadonlyMap<string, string>,
  ): (self: ReadonlyMap<string, string>) => Array<Readonly<Record<string, string | null>>>
  (
    self: ReadonlyMap<string, string>,
    that: ReadonlyMap<string, string>,
  ): Array<Readonly<Record<string, string | null>>>
} = dual(2, planSectionsImpl)
const equalDeclaration = Schema.toEquivalence(Context.ToolDeclaration)
function planToolsImpl(
  self: ReadonlyArray<Context.ToolDeclaration>,
  that: ReadonlyArray<Context.ToolDeclaration>,
): Pick<Context.SystemPatch, 'toolsRemoved' | 'toolsAdded'> {
  const wanted = new Map(that.map((tool) => [tool.name, tool]))
  const kept = self.filter((tool) => {
    const next = Option.fromUndefinedOr(wanted.get(tool.name))
    return Option.exists(next, (value) => equalDeclaration(tool, value))
  })
  const names = new Set(kept.map((tool) => tool.name))
  const added = that.filter((tool) => !names.has(tool.name))
  if (
    [...kept, ...added].some(
      (tool, index) => !Option.exists(Arr.get(that, index), (value) => tool.name === value.name),
    )
  )
    return { toolsRemoved: self.map((tool) => tool.name), toolsAdded: that }
  return {
    toolsRemoved: self.filter((tool) => !names.has(tool.name)).map((tool) => tool.name),
    toolsAdded: added,
  }
}
/**
 * Returns ordered tool additions and removals for the desired declarations.
 *
 * @category combinators
 */
export const planTools: {
  (
    that: ReadonlyArray<Context.ToolDeclaration>,
  ): (
    self: ReadonlyArray<Context.ToolDeclaration>,
  ) => Pick<Context.SystemPatch, 'toolsRemoved' | 'toolsAdded'>
  (
    self: ReadonlyArray<Context.ToolDeclaration>,
    that: ReadonlyArray<Context.ToolDeclaration>,
  ): Pick<Context.SystemPatch, 'toolsRemoved' | 'toolsAdded'>
} = dual(2, planToolsImpl)
/** A head marker forces a full baseline once; prior retained systems are omitted even when values match. */
function planImpl(
  self: Context.View,
  desired: ReadonlyMap<string, string>,
  tools: ReadonlyArray<Context.ToolDeclaration>,
): {
  readonly patches: Array<Context.SystemPatch>
  readonly edits: Array<Context.Edit>
} {
  const patches = Context.systemPatches(self)
  if (
    self.head !== undefined &&
    !self.entries.some(
      (entry) => entry.system !== undefined && entry.id > (self.head?.id ?? -Infinity),
    )
  )
    return {
      patches: [{ sections: Record.fromEntries(desired), toolsRemoved: [], toolsAdded: tools }],
      edits: self.entries
        .filter((entry) => entry.system !== undefined)
        .map((entry) => ({ target: entry.id, _tag: 'omit' })),
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
/**
 * Returns managed system patches and context edits for the desired prompt state.
 *
 * @category combinators
 */
export const plan: {
  (
    desired: ReadonlyMap<string, string>,
    tools: ReadonlyArray<Context.ToolDeclaration>,
  ): (self: Context.View) => {
    readonly patches: Array<Context.SystemPatch>
    readonly edits: Array<Context.Edit>
  }
  (
    self: Context.View,
    desired: ReadonlyMap<string, string>,
    tools: ReadonlyArray<Context.ToolDeclaration>,
  ): {
    readonly patches: Array<Context.SystemPatch>
    readonly edits: Array<Context.Edit>
  }
} = dual(3, planImpl)
/**
 * Metadata identifies encoded managed patches without discarding plain stored system instructions.
 *
 * @category models
 */
export type ProjectionOptions = toPrompt.Options
/** Projects plain system content in transcript order followed by the effective named sections once. */
function toPromptImpl(
  self: ReadonlyArray<Prompt.Message>,
  sections: ReadonlyMap<string, string>,
  options: ProjectionOptions = {},
): Prompt.Prompt {
  const managed = new Set(options.managedSystemMessages ?? [])
  const system = self.filter((message) => message.role === 'system' && !managed.has(message))
  const content = [...sections.values()].filter((value) => value.length > 0).join('\n\n')
  return Prompt.fromMessages([
    ...system,
    ...(content === '' ? [] : [Prompt.systemMessage({ content })]),
    ...self.filter((message) => message.role !== 'system'),
  ])
}
/**
 * Projects unmanaged system messages and effective named sections into a native prompt.
 *
 * @category combinators
 */
export const toPrompt: {
  (
    sections: ReadonlyMap<string, string>,
    options?: ProjectionOptions,
  ): (self: ReadonlyArray<Prompt.Message>) => Prompt.Prompt
  (
    self: ReadonlyArray<Prompt.Message>,
    sections: ReadonlyMap<string, string>,
    options?: ProjectionOptions,
  ): Prompt.Prompt
} = dual((args) => Array.isArray(args[0]), toPromptImpl)

/**
 * Type-level contracts for `toPrompt`.
 *
 * @category utility types
 */
export declare namespace toPrompt {
  /**
   * Configuration accepted by toPrompt.
   *
   * @category models
   */
  interface Options {
    readonly managedSystemMessages?: ReadonlyArray<Prompt.Message> | undefined
  }
}
