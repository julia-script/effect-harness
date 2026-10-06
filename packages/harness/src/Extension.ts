import type { HookError } from './Error.ts'
import type * as Effect from 'effect/Effect'
import type * as Context from './Context.ts'
import type * as Hook from './Hook.ts'
import type { Invocation } from './Invocation.ts'
import type * as Tool from './Tool.ts'

export interface PromptInput {
  readonly view: Context.View
  readonly tools: ReadonlyArray<Tool.Registration>
  readonly cwd: string
}
export interface Section {
  readonly key: string
  readonly tag?: boolean | undefined
  readonly render: (input: PromptInput) => Effect.Effect<string | undefined, HookError, Invocation>
}
export interface ToolWrap {
  readonly name: string
  readonly wrap: (
    tool: Tool.Registration,
  ) => Effect.Effect<Tool.Registration, HookError, Invocation>
}
export interface SectionWrap {
  readonly key: string
  readonly wrap: (section: Section) => Effect.Effect<Section, HookError, Invocation>
}
/** Immutable executable code; install it again after restart. Build callbacks within Layers to capture services. */
export interface Extension {
  readonly name: string
  readonly tools?: ReadonlyArray<Tool.Registration> | undefined
  readonly sections?: ReadonlyArray<Section> | undefined
  readonly hooks?: ReadonlyArray<Hook.Registration> | undefined
  readonly toolWraps?: ReadonlyArray<ToolWrap> | undefined
  readonly sectionWraps?: ReadonlyArray<SectionWrap> | undefined
}
export const make = (extension: Extension): Extension => extension
