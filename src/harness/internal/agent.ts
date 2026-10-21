/**
 * Agent resolution (spec §7.1): the selected extensions, their tools and sections with wrappers
 * applied, the tool filter, and the defaults of the settings.
 */
import { Effect, Option } from 'effect'
import type * as Session from '../../session/Session.ts'
import type { ConversationId } from '../../storage/schema/Ids.ts'
import type * as ToolTask from '../../tool/ToolTask.ts'
import { document } from '../Agent.ts'
import type { Resolved } from '../Agent.ts'
import type { ModelTools, Selection, State, ToolFilter } from '../AgentState.ts'
import type { Section } from '../Extension.ts'
import type { Installed, Service as Extensions, Snapshot } from '../Extensions.ts'
import type { Settings } from '../Settings.ts'
import { INSTRUCTIONS_KEY } from './extensions.ts'

const isList = (value: object): value is ReadonlyArray<string> => Array.isArray(value)

/** The selected installed extensions: the stored array, or the default selection edited. */
const select = (
  stored: Selection | undefined,
  snapshot: Snapshot,
  settings: Settings,
): ReadonlyArray<Installed> => {
  let names: ReadonlyArray<string>
  if (stored !== undefined && isList(stored)) names = stored
  else {
    const edit = stored ?? {}
    const base = settings.extensions ?? snapshot.map(({ extension }) => extension.name)
    const removed = new Set(edit.remove ?? [])
    names = [...base, ...(edit.add ?? [])].filter((name) => !removed.has(name))
  }
  return [...new Set(names)].flatMap((name) => {
    const installed = snapshot.find(({ extension }) => extension.name === name)
    return installed === undefined ? [] : [installed]
  })
}

/** `tools` through a stored filter. */
const filter = (
  tools: ReadonlyArray<ToolTask.Installed>,
  stored: ToolFilter | undefined,
): ReadonlyArray<ToolTask.Installed> => {
  if (stored === undefined) return tools
  if (isList(stored)) {
    return [...new Set(stored)].flatMap((name) => {
      const tool = tools.find((each) => each.tool.name === name)
      return tool === undefined ? [] : [tool]
    })
  }
  const removed = new Set(stored.remove)
  return tools.filter(({ tool }) => !removed.has(tool.name))
}

/** The enabled tools the model is offered, as `modelTools` selects them. */
const offer = (
  tools: ReadonlyArray<ToolTask.Installed>,
  stored: ModelTools | undefined,
): ReadonlyArray<ToolTask.Installed> => {
  if (stored !== undefined && isList(stored)) return filter(tools, stored)
  const removed = new Set(stored?.remove ?? [])
  const kept = tools.filter(({ tool }) => !tool.deferred && !removed.has(tool.name))
  // Loaded tools follow in load order, so loading one appends to the declared tools.
  const loaded = [...new Set(stored?.add ?? [])].flatMap((name) => {
    const found = tools.find(({ tool }) => tool.name === name)
    return found?.tool.deferred === true && !removed.has(name) ? [found] : []
  })
  return [...kept, ...loaded]
}

/** `names` appended to `list` where it lacks them. */
const appended = (list: ReadonlyArray<string>, names: ReadonlyArray<string>) => [
  ...list,
  ...names.filter((name, index) => !list.includes(name) && names.indexOf(name) === index),
]

/** Load `names` into the conversation's offered tools (see `Agent.addTools`). */
export const addTools = (
  tx: Session.Tx,
  conversationId: ConversationId,
  names: ReadonlyArray<string>,
) =>
  Effect.map(tx.doc(document, conversationId), (state) => {
    const { tools, modelTools } = state
    if (tools !== undefined && isList(tools)) state.tools = appended(tools, names)
    else if (tools?.remove.some((name) => names.includes(name)) === true)
      state.tools = { remove: tools.remove.filter((name) => !names.includes(name)) }
    if (modelTools !== undefined && isList(modelTools)) {
      state.modelTools = appended(modelTools, names)
      return
    }
    const remove = (modelTools?.remove ?? []).filter((name) => !names.includes(name))
    const add = appended(modelTools?.add ?? [], names)
    state.modelTools = remove.length === 0 ? { add } : { remove, add }
  })

const renamed = (target: string, name: string) =>
  Effect.logWarning(`A wrapper renamed ${target} to ${name}; ${target} is left out`)

/** Apply the selected extensions' wrappers to the composed tools and sections, in order. */
const wrapAll = Effect.fnUntraced(function* (
  extensions: ReadonlyArray<Installed>,
  tools: Map<string, ToolTask.Installed>,
  sections: Map<string, Section>,
) {
  for (const { extension } of extensions) {
    for (const wrap of extension.wraps) {
      if ('tool' in wrap) {
        const target = tools.get(wrap.tool)
        if (target === undefined || Option.isNone(target.runner)) continue
        const wrapped = wrap.wrap(target.runner.value)
        if (wrapped.tool.name === wrap.tool)
          tools.set(wrap.tool, { ...target, tool: wrapped.tool, runner: Option.some(wrapped) })
        else {
          tools.delete(wrap.tool)
          yield* renamed(wrap.tool, wrapped.tool.name)
        }
        continue
      }
      const target = sections.get(wrap.section)
      if (target === undefined) continue
      const wrapped = wrap.wrap(target)
      if (wrapped.key === wrap.section) sections.set(wrap.section, wrapped)
      else {
        sections.delete(wrap.section)
        yield* renamed(wrap.section, wrapped.key)
      }
    }
  }
})

/** The agent of `state` over `snapshot` and `settings`. */
export const resolve = Effect.fnUntraced(function* (
  state: State | undefined,
  snapshot: Snapshot,
  settings: Settings,
) {
  const extensions = select(state?.extensions, snapshot, settings)
  const tools = new Map<string, ToolTask.Installed>()
  const sections = new Map<string, Section>()
  for (const installed of extensions) {
    for (const tool of installed.tools) tools.set(tool.tool.name, tool)
    for (const section of installed.extension.sections) sections.set(section.key, section)
  }
  yield* wrapAll(extensions, tools, sections)
  const config = state?.config ?? {}
  const enabled: Array<ToolTask.Installed> = []
  for (const tool of filter([...tools.values()], state?.tools)) {
    const { name } = tool.tool
    enabled.push(Object.hasOwn(config, name) ? { ...tool, config: config[name] } : tool)
  }
  const byModel = enabled.filter(({ tool }) => tool.callers.includes('model'))
  const offered = offer(byModel, state?.modelTools)
  const deferred = byModel.filter((each) => each.tool.deferred && !offered.includes(each))
  const callable = enabled.filter(({ tool }) => tool.callers.includes('tools'))
  const instructions = state?.instructions
  const agentSections = [...sections.values()]
  if (instructions !== undefined)
    agentSections.push({ key: INSTRUCTIONS_KEY, render: () => Effect.succeed(instructions) })
  const model = state?.model ?? settings.model
  const resolved: Resolved = {
    ...(model === undefined ? {} : { model }),
    reasoning: state?.reasoning ?? settings.reasoning ?? 'off',
    extensions,
    tools: enabled,
    callable,
    offered,
    deferred,
    sections: agentSections,
    ...(instructions === undefined ? {} : { instructions }),
  }
  return resolved
})

/** The agent of conversation `conversationId` now: its committed `$agent`, resolved. */
export const current = Effect.fnUntraced(function* (
  read: Pick<Session.Service, 'snapshot'>,
  conversationId: ConversationId,
  extensions: Extensions,
  settings: Effect.Effect<Settings>,
) {
  const state = yield* read.snapshot(document, conversationId)
  return yield* resolve(state, yield* extensions.snapshot, yield* settings)
})
