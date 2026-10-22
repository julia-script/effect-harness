/**
 * The built-in `$agent` document, ported from pi's `harness/agent.ts`: what one conversation asks
 * with, and the agent resolved from it.
 *
 * A conversation stores its model, fallback models, reasoning level, extension selection, tool
 * filter, instructions and tool config; every field is optional. Each reader resolves the agent from the
 * stored state, an `Extensions` snapshot, and the settings, once per decision: the generation as it
 * prepares a request, the tool task as it runs a call, the compaction as it selects.
 *
 * Resolution (spec §7.1):
 *
 * - Extensions: an array selects exactly those names, in its order; `{ add, remove }` edits the
 *   default selection (`Settings.extensions`, or every installed extension). Names not installed
 *   are skipped.
 * - Tools: the selected extensions' tools in extension order, a later tool of one name replacing
 *   an earlier one in place; then the selected extensions' tool wrappers; then the `tools` filter:
 *   an array keeps exactly those names in its order, `{ remove }` drops names. A tool's config is
 *   the stored `config[name]` when set, else its extension's.
 * - Offered tools: the enabled tools through `modelTools`; deferred tools only once loaded.
 * - Sections: the same way, then `instructions` last, as the tagged section `instructions`.
 * - Model, fallbacks and reasoning: stored, else the settings' defaults.
 *
 * The document is rewindable: a fork starts from the agent at its fork entry, and a task-owned
 * conversation starts with a copy of its owner's.
 */
import { Context, Effect } from 'effect'
import type { ModelThinkingLevel } from '../model/Message.ts'
import type * as Model from '../model/Model.ts'
import * as Document from '../session/Document.ts'
import type { Tx } from '../session/Session.ts'
import type { TxDocumentError } from '../session/SessionError.ts'
import type { ConversationRecord } from '../storage/schema/Conversation.ts'
import type { ConversationId } from '../storage/schema/Ids.ts'
import * as Hook from '../task/Hook.ts'
import type * as ToolTask from '../tool/ToolTask.ts'
import type { Change, State, View } from '../protocol/AgentState.ts'
import type * as Extension from './Extension.ts'
import type * as Extensions from './Extensions.ts'
import * as internal from './internal/agent.ts'
import { strictJson } from '../internal/strictJson.ts'
import type { Settings } from './Settings.ts'

/** One per conversation; a fork starts from the agent at its fork entry. */
export const document = Document.make<State>({
  kind: '$agent',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  initial: () => Effect.succeed({}),
  checkpointWhen: () => Effect.succeed(true),
})

const FIELDS = [
  'model',
  'fallbacks',
  'reasoning',
  'extensions',
  'tools',
  'modelTools',
  'instructions',
  'config',
] as const

/** Apply `change` to the conversation's `$agent`. */
export const configure = (tx: Tx, conversationId: ConversationId, change: Change) =>
  Effect.map(tx.doc(document, conversationId), (state) => {
    for (const key of FIELDS) {
      const value = change[key]
      if (value === null) delete state[key]
      else if (value !== undefined) Object.assign(state, { [key]: strictJson(value) })
    }
  })

/**
 * Part of every commit that creates a conversation: a new task-owned conversation copies the
 * agent of its owner task's conversation; a fork keeps its as-of copy, and an ownerless one
 * starts empty.
 */
export const created = Effect.fnUntraced(function* (tx: Tx, record: ConversationRecord) {
  if (record.parent !== undefined) return
  const agent = yield* tx.doc(document, record.id)
  if (record.owner === undefined) return
  const owner = yield* tx.doc(document, record.owner.conversationId)
  Object.assign(agent, strictJson({ ...owner }))
})

/**
 * Load tools by name into the conversation's offered tools, as a call's `addTools` control does:
 * an array `tools` or `modelTools` gets each name it lacks appended, a `remove` loses them, and
 * an unset or object `modelTools` gets them in its `add`. A name not registered now is offered
 * once it is.
 */
export const addTools: (
  tx: Tx,
  conversationId: ConversationId,
  names: ReadonlyArray<string>,
) => Effect.Effect<void, TxDocumentError> = internal.addTools

/**
 * The agent of the call a tool handler runs, while the tool task runs it for a conversation;
 * `undefined` elsewhere. A handler reads it with `yield* Agent.Current`.
 */
export const Current: Context.Reference<Resolved | undefined> = Context.Reference(
  'effect-harness/harness/Agent/Current',
  { defaultValue: (): Resolved | undefined => undefined },
)

/** The agent of one decision. */
export interface Resolved {
  readonly model?: Model.Ref
  /** Where a run goes, in order, when `model` cannot answer: stored, else the settings'. */
  readonly fallbacks: ReadonlyArray<Model.Ref>
  readonly reasoning: ModelThinkingLevel
  readonly extensions: ReadonlyArray<Extensions.Installed>
  /** The enabled tools, in order. */
  readonly tools: ReadonlyArray<ToolTask.Installed>
  /** The enabled tools other tools may call, in order: what nested calls resolve among. */
  readonly callable: ReadonlyArray<ToolTask.Installed>
  /** The enabled tools the model may call that `modelTools` selects, in order: what it offers. */
  readonly offered: ReadonlyArray<ToolTask.Installed>
  /** Enabled deferred tools not offered yet, in order: what `tool_search` searches. */
  readonly deferred: ReadonlyArray<ToolTask.Installed>
  readonly sections: ReadonlyArray<Extension.Section>
  readonly instructions?: string
}

/**
 * Resolve the agent of `state` over `snapshot` and `settings`. A wrapper that renames its target
 * drops it, and is reported as a warning.
 */
export const resolve: (
  state: State | undefined,
  snapshot: Extensions.Snapshot,
  settings: Settings,
) => Effect.Effect<Resolved> = (state, snapshot, settings) =>
  internal.resolve(state, snapshot, settings)

/** The handlers of `kind` among the agent's selected extensions, in extension order. */
export const hooks = <H>(agent: Resolved, kind: Hook.Kind<H>): ReadonlyArray<Partial<H>> =>
  Hook.handlers(
    agent.extensions.flatMap(({ extension }) => extension.hooks),
    kind,
  )

/** The names in `agent`. */
export const view = (agent: Resolved): View => ({
  ...(agent.model === undefined ? {} : { model: agent.model }),
  ...(agent.fallbacks.length === 0 ? {} : { fallbacks: agent.fallbacks }),
  reasoning: agent.reasoning,
  extensions: agent.extensions.map(({ extension }) => extension.name),
  tools: agent.offered.map(({ tool }) => tool.name),
  ...(agent.deferred.length === 0 ? {} : { deferred: agent.deferred.map(({ tool }) => tool.name) }),
  sections: agent.sections.map(({ key }) => key),
  ...(agent.instructions === undefined ? {} : { instructions: agent.instructions }),
})
