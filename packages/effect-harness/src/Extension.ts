/**
 * Executable extension declarations and prompt-section callbacks.
 */
import { identity } from 'effect/Function'
import type { HookError } from './HookError.ts'
import type * as Effect from 'effect/Effect'
import type * as Transcript from './Transcript.ts'
import type * as Hook from './Hook.ts'
import type { Invocation } from './Invocation.ts'
import type * as ToolRegistration from './ToolRegistration.ts'
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
  readonly tools?: ReadonlyArray<ToolRegistration.Registration> | undefined
  readonly sections?: ReadonlyArray<Extension.Section> | undefined
  readonly hooks?: ReadonlyArray<Hook.Registration> | undefined
  readonly toolWraps?: ReadonlyArray<Extension.ToolWrap> | undefined
  readonly sectionWraps?: ReadonlyArray<Extension.SectionWrap> | undefined
}
/**
 * Returns the executable extension declaration with its inferred callback requirements.
 *
 * @category constructors
 */
export const make: (input: Extension) => Extension = identity

/**
 * Type-level contracts for `Extension`.
 *
 */
export declare namespace Extension {
  /**
   * Conversation and agent inputs supplied to an extension prompt section.
   *
   * @category models
   */
  interface PromptInput {
    readonly view: Transcript.View
    readonly tools: ReadonlyArray<ToolRegistration.Registration>
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
      tool: ToolRegistration.Registration,
    ) => Effect.Effect<ToolRegistration.Registration, HookError, Invocation>
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
