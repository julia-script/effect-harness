/**
 * Executable extension declarations and prompt-section callbacks.
 *
 * @since 0.0.0
 */
import { identity } from 'effect/Function'
import type { HookError } from './HookError.ts'
import type * as Effect from 'effect/Effect'
import type * as Context from './Context.ts'
import type * as Hook from './Hook.ts'
import type { Invocation } from './Invocation.ts'
import type * as Tool from './Tool.ts'

/**
 * Extension prompt input contract.
 *
 * @category models
 * @since 0.0.0
 */
export type PromptInput = Extension.PromptInput
/**
 * Extension section contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Section = Extension.Section
/**
 * Extension tool wrap contract.
 *
 * @category models
 * @since 0.0.0
 */
export type ToolWrap = Extension.ToolWrap
/**
 * Extension section wrap contract.
 *
 * @category models
 * @since 0.0.0
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
 * @since 0.0.0
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
 * @since 0.0.0
 */
export const make = identity<Extension>

/**
 * Type contracts owned by `Extension`.
 *
 * @category utility types
 * @since 0.0.0
 */
export declare namespace Extension {
  /**
   * Extension prompt input type contract.
   *
   * @category models
   * @since 0.0.0
   */
  interface PromptInput {
    readonly view: Context.View
    readonly tools: ReadonlyArray<Tool.Registration>
    readonly cwd: string
  }
  /**
   * Extension section type contract.
   *
   * @category models
   * @since 0.0.0
   */
  interface Section {
    readonly key: string
    readonly tag?: boolean | undefined
    readonly render: (
      input: PromptInput,
    ) => Effect.Effect<string | undefined, HookError, Invocation>
  }
  /**
   * Extension tool wrap type contract.
   *
   * @category models
   * @since 0.0.0
   */
  interface ToolWrap {
    readonly name: string
    readonly wrap: (
      tool: Tool.Registration,
    ) => Effect.Effect<Tool.Registration, HookError, Invocation>
  }
  /**
   * Extension section wrap type contract.
   *
   * @category models
   * @since 0.0.0
   */
  interface SectionWrap {
    readonly key: string
    readonly wrap: (section: Section) => Effect.Effect<Section, HookError, Invocation>
  }
}
