/**
 * Extensions, ported from pi's `defineExtension` (spec §7.1): a named bundle of tools, system
 * prompt sections, hooks, wrappers of other extensions' tools and sections, and task definitions.
 * The `Extensions` registry installs them in the loop, and each conversation selects them by name
 * in its `pi.agent` document.
 *
 * An extension holds tool declarations, not handlers: installing it reads their implementations
 * from the Effect context, where `toolkit.toLayer(handlers)` provides them. Two extensions may
 * declare tools of one name; for a conversation that selects both, the later one's tool replaces
 * the earlier one's in place.
 */
import type { Effect } from 'effect'
import type * as Session from '../session/Session.ts'
import type { ConversationId } from '../storage/schema/Ids.ts'
import * as Hook from '../task/Hook.ts'
import type * as Task from '../task/Task.ts'
import type * as Tool from '../tool/Tool.ts'
import type * as Toolkit from '../tool/Toolkit.ts'
import type * as ToolTask from '../tool/ToolTask.ts'
import type * as Agent from './Agent.ts'

/** What a section renders from. */
export interface SectionInput {
  readonly conversationId: ConversationId
  /** The agent of this request; `agent.offered` are the tools it offers. */
  readonly agent: Agent.Resolved
  /** The sections the model sees now, by key, after replaying the transcript. */
  readonly shown: Readonly<Record<string, string>>
  /** Committed document reads. */
  readonly read: Pick<Session.Service, 'snapshot' | 'snapshotAsOf'>
}

/**
 * One named part of the system prompt. A section that fails keeps the text the model sees, and
 * the failure is reported; one that renders `undefined` is left out. Render deterministically:
 * any change in the text appends a system delta and invalidates provider prompt caches.
 */
export interface Section {
  /** Matches `^[a-z][a-z0-9_-]*$`; `instructions` is reserved for the agent's instructions. */
  readonly key: string
  /** Default: wrap the text as `<key>\n…\n</key>`; `false` leaves it as it is. */
  readonly tag?: boolean
  readonly render: (input: SectionInput) => Effect.Effect<string | undefined, unknown>
}

/** A section with key `key`. */
export const section = (
  key: string,
  render: Section['render'],
  options: { readonly tag?: boolean } = {},
): Section => (options.tag === undefined ? { key, render } : { key, render, tag: options.tag })

/**
 * Decorate a tool or a section of whichever selected extension provides it. A wrapper that
 * returns another name drops its target for that resolution; one with no target does nothing.
 */
export type Wrap =
  | { readonly tool: string; readonly wrap: (runner: Toolkit.Runner) => Toolkit.Runner }
  | { readonly section: string; readonly wrap: (section: Section) => Section }

/** Wrap the tool named `tool`: its declaration and its runner. */
export const wrapTool = (
  tool: string | Tool.Any,
  wrap: (runner: Toolkit.Runner) => Toolkit.Runner,
): Wrap => ({ tool: typeof tool === 'string' ? tool : tool.name, wrap })

/** Wrap the section with key `key`. */
export const wrapSection = (key: string, wrap: (section: Section) => Section): Wrap => ({
  section: key,
  wrap,
})

/** Answer the hooks of `kind`, such as `Generation.Hooks` or `ToolTask.Hooks`. */
export const hook: <H>(kind: Hook.Kind<H>, handlers: Partial<H>) => Hook.Registration = Hook.make

export const TypeId = '~effect-harness/harness/Extension'
export type TypeId = typeof TypeId

/** An extension whose tools have the names `Names`, for which installing needs implementations. */
export interface Extension<Names extends string = string> {
  readonly [TypeId]: { readonly names: Names | undefined }
  readonly name: string
  readonly tools: ReadonlyArray<Tool.Any>
  /** Each tool's config, encoded, by name; a conversation's `pi.agent` config overrides it. */
  readonly config: Readonly<Record<string, unknown>>
  readonly sections: ReadonlyArray<Section>
  readonly hooks: ReadonlyArray<Hook.Registration>
  /** Applied where this extension is selected, in order. */
  readonly wraps: ReadonlyArray<Wrap>
  /** Installed by name for every task, whichever conversations select this extension. */
  readonly tasks: ReadonlyArray<Task.Any>
}

export interface Options<Tools extends Record<string, Tool.Any>> {
  readonly toolkit?: Toolkit.Toolkit<Tools>
  readonly config?: ToolTask.Config<Tools>
  readonly sections?: ReadonlyArray<Section>
  readonly hooks?: ReadonlyArray<Hook.Registration>
  readonly wraps?: ReadonlyArray<Wrap>
  readonly tasks?: ReadonlyArray<Task.Any>
}

/** An extension named `name`. Nothing is checked until it is installed. */
export const make = <Tools extends Record<string, Tool.Any> = Record<string, never>>(
  name: string,
  options: Options<Tools> = {},
): Extension<Toolkit.Names<Tools>> => ({
  [TypeId]: { names: undefined },
  name,
  tools: options.toolkit === undefined ? [] : Object.values(options.toolkit.tools),
  config: options.config ?? {},
  sections: options.sections ?? [],
  hooks: options.hooks ?? [],
  wraps: options.wraps ?? [],
  tasks: options.tasks ?? [],
})
