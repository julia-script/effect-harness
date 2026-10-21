/* oxlint-disable effecttsgo/any-unknown-in-error-context -- extension code may fail with anything; a failure is reported or answers the call */
/**
 * The positional system prompt, ported from pi's `harness/prompt.ts`: which `$system` entries
 * make the sections and tools the model sees equal the ones the agent wants.
 */
import { Cause, Effect, Equal } from 'effect'
import type { Message, SystemMessage, ToolDefinition } from '../../model/Message.ts'
import * as Transcript from '../../model/Transcript.ts'
import type { EntryDraft } from '../../session/Session.ts'
import type { ContextEdit } from '../../storage/schema/Entry.ts'
import type { SectionInput } from '../Extension.ts'
import { SYSTEM_KIND } from './context.ts'
import type { View } from './context.ts'

/** Sections in effect after the system messages in order: set in place, `null` deletes. */
export const replaySections = (messages: ReadonlyArray<Message>): Map<string, string> => {
  const shown = new Map<string, string>()
  for (const message of messages) {
    if (message.role !== 'system' || message.sections === undefined) continue
    for (const [key, value] of Object.entries(message.sections)) {
      if (value === null) shown.delete(key)
      else shown.set(key, value)
    }
  }
  return shown
}

/**
 * Render the agent's sections in order. A section that fails keeps the text the model sees, and
 * the failure is reported as a warning.
 */
export const renderSections = Effect.fnUntraced(function* (
  input: Omit<SectionInput, 'shown'>,
  shown: ReadonlyMap<string, string>,
) {
  const desired = new Map<string, string>()
  const rendering: SectionInput = { ...input, shown: Object.fromEntries(shown) }
  for (const section of input.agent.sections) {
    const rendered = yield* Effect.catchCause(
      Effect.map(section.render(rendering), (text) =>
        text === undefined || section.tag === false
          ? text
          : `<${section.key}>\n${text}\n</${section.key}>`,
      ),
      (cause) =>
        Cause.hasInterrupts(cause)
          ? Effect.interrupt
          : Effect.as(
              Effect.logWarning(`Section ${section.key} failed to render`, cause),
              shown.get(section.key),
            ),
    )
    if (rendered !== undefined) desired.set(section.key, rendered)
  }
  return desired
})

interface ToolChanges {
  readonly toolsRemoved: ReadonlyArray<{ readonly name: string }>
  readonly toolsAdded: ReadonlyArray<ToolDefinition>
}

const sameDeclaration = (a: ToolDefinition, b: ToolDefinition): boolean => Equal.equals(a, b)

/**
 * Tool changes from `offered` to `desired`. A changed declaration is removed and re-added. Replay
 * keeps retained tools in place and appends additions; when that would not give the desired
 * order, every offered tool is removed and every desired one re-added in order.
 */
const planTools = (
  offered: ReadonlyArray<ToolDefinition>,
  desired: ReadonlyArray<ToolDefinition>,
): ToolChanges => {
  const wanted = new Map(desired.map((tool) => [tool.name, tool]))
  const kept = offered.filter((tool) => {
    const next = wanted.get(tool.name)
    return next !== undefined && sameDeclaration(tool, next)
  })
  const keptNames = new Set(kept.map((tool) => tool.name))
  const added = desired.filter((tool) => !keptNames.has(tool.name))
  const replayed = [...kept, ...added]
  if (replayed.some((tool, index) => tool.name !== desired[index]?.name))
    return { toolsRemoved: offered.map(({ name }) => ({ name })), toolsAdded: desired }
  return {
    toolsRemoved: offered.filter((tool) => !keptNames.has(tool.name)).map(({ name }) => ({ name })),
    toolsAdded: added,
  }
}

type Patch = Record<string, string | null>

/** Section patches: none, the minimal patch, or remove-all then re-add-all when order differs. */
const planSections = (
  shown: ReadonlyMap<string, string>,
  desired: ReadonlyMap<string, string>,
): ReadonlyArray<Patch> => {
  const patchedOrder = [
    ...[...shown.keys()].filter((key) => desired.has(key)),
    ...[...desired.keys()].filter((key) => !shown.has(key)),
  ]
  const desiredOrder = [...desired.keys()]
  if (patchedOrder.some((key, index) => key !== desiredOrder[index]))
    return [
      Object.fromEntries([...shown.keys()].map((key) => [key, null])),
      Object.fromEntries(desired),
    ]
  const patch: Patch = {}
  for (const [key, value] of shown) {
    const next = desired.get(key)
    if (next !== value) patch[key] = next ?? null
  }
  for (const [key, value] of desired) if (!shown.has(key)) patch[key] = value
  return Object.keys(patch).length === 0 ? [] : [patch]
}

const systemEntry = (
  sections: Patch | undefined,
  tools: ToolChanges | undefined,
  timestamp: number,
): EntryDraft => {
  const message: SystemMessage = {
    role: 'system',
    content: '',
    ...(sections === undefined ? {} : { sections }),
    ...(tools === undefined || tools.toolsRemoved.length === 0
      ? {}
      : { toolsRemoved: tools.toolsRemoved }),
    ...(tools === undefined || tools.toolsAdded.length === 0
      ? {}
      : { toolsAdded: tools.toolsAdded }),
    timestamp,
  }
  return { kind: SYSTEM_KIND, model: [message] }
}

const isSystem = (entry: { readonly kind: string }) => entry.kind === SYSTEM_KIND

/**
 * The `$system` entries that make the replayed sections and tools of `view` equal `desired` and
 * `tools`, in values and order:
 *
 * - After a head marker with no later `$system` entry: one complete baseline that omits every
 *   retained earlier `$system` entry, even when it restates what is shown.
 * - Otherwise the minimal section patch, or a remove-all/re-add-all pair when the patch would
 *   leave another order, or nothing.
 *
 * Tool changes ride on the last planned entry, or on one entry of their own.
 */
export const planSystemEntries = (
  view: View,
  desired: ReadonlyMap<string, string>,
  tools: ReadonlyArray<ToolDefinition>,
  timestamp: number,
): ReadonlyArray<EntryDraft> => {
  const { head } = view
  if (head !== undefined && !view.entries.some((entry) => isSystem(entry) && entry.id > head.id)) {
    const edits: Array<ContextEdit> = view.entries
      .filter((entry) => isSystem(entry))
      .map((entry) => ({ target: entry.id, action: 'omit' }))
    const baseline = systemEntry(
      Object.fromEntries(desired),
      { toolsRemoved: [], toolsAdded: tools },
      timestamp,
    )
    return [edits.length === 0 ? baseline : { ...baseline, edits }]
  }
  const sections = planSections(replaySections(view.messages), desired)
  const changes = planTools(Transcript.tools(view.messages), tools)
  if (changes.toolsRemoved.length === 0 && changes.toolsAdded.length === 0)
    return sections.map((patch) => systemEntry(patch, undefined, timestamp))
  if (sections.length === 0) return [systemEntry(undefined, changes, timestamp)]
  return sections.map((patch, index) =>
    systemEntry(patch, index === sections.length - 1 ? changes : undefined, timestamp),
  )
}
