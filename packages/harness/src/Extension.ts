/**
 * Executable extension declarations and prompt-section callbacks.
 */
import { identity } from 'effect/Function'
import type { HookError } from './HookError.ts'
import type * as Effect from 'effect/Effect'
import type * as Context from './Context.ts'
import type * as Hook from './Hook.ts'
import type { Invocation } from './Invocation.ts'
import type * as Tool from './Tool.ts'

/**
 * Conversation and agent inputs supplied to an extension prompt section.
 *
 * @category models
 */
export type PromptInput = Extension.PromptInput
/**
 * Named prompt section with effectful rendering.
 *
 * @category models
 */
export type Section = Extension.Section
/**
 * Wrapper around a registered tool invocation.
 *
 * @category models
 */
export type ToolWrap = Extension.ToolWrap
/**
 * Wrapper around a prompt section renderer.
 *
 * @category models
 */
export type SectionWrap = Extension.SectionWrap
/**
 * Immutable executable code; install it again after restart.
 *
 * **Details**
 *
 * Build callbacks within Layers to capture services.
 *
 * @category models
 */
export interface Extension {
  readonly name: string
  readonly tools?: ReadonlyArray<Tool.Registration> | undefined
  readonly sections?: ReadonlyArray<Section> | undefined
  readonly hooks?: ReadonlyArray<Hook.Registration> | undefined
  readonly toolWraps?: ReadonlyArray<ToolWrap> | undefined
  readonly sectionWraps?: ReadonlyArray<SectionWrap> | undefined
}
/**
 * Returns the executable extension declaration with its inferred callback requirements.
 *
 * @category constructors
 */
export const make = identity<Extension>

/**
 * Type-level contracts for `Extension`.
 *
 * @category utility types
 */
export declare namespace Extension {
  /**
   * Conversation and agent inputs supplied to an extension prompt section.
   *
   * @category models
   */
  interface PromptInput {
    readonly view: Context.View
    readonly tools: ReadonlyArray<Tool.Registration>
    readonly cwd: string
  }
  /**
   * Named prompt section with effectful rendering.
   *
   * @category models
   */
  interface Section {
    readonly key: string
    readonly tag?: boolean | undefined
    readonly render: (
      input: PromptInput,
    ) => Effect.Effect<string | undefined, HookError, Invocation>
  }
  /**
   * Wrapper around a registered tool invocation.
   *
   * @category models
   */
  interface ToolWrap {
    readonly name: string
    readonly wrap: (
      tool: Tool.Registration,
    ) => Effect.Effect<Tool.Registration, HookError, Invocation>
  }
  /**
   * Wrapper around a prompt section renderer.
   *
   * @category models
   */
  interface SectionWrap {
    readonly key: string
    readonly wrap: (section: Section) => Effect.Effect<Section, HookError, Invocation>
  }
}
