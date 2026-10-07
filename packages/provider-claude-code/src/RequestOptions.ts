/**
 * Request-local Claude Code model, session and inference controls.
 *
 * @since 0.0.0
 */
import * as Context from 'effect/Context'
import type * as Cli from './Cli.ts'

/**
 * Scoped per-request controls consumed by the ordinary native LanguageModel provider.
 *
 * @category types
 * @since 0.0.0
 */
export interface Values {
  readonly model?: string | undefined
  readonly sessionId?: string | undefined
  readonly effort?: Cli.Request['effort'] | undefined
  readonly thinkingEnabled?: boolean | undefined
  readonly maxTokens?: number | undefined
  readonly cache?: 'none' | 'short' | 'long' | undefined
  readonly autoCompact?: false | undefined
}
/**
 * Identifies the request option service used for per-call CLI overrides.
 *
 * @category services
 * @since 0.0.0
 */
export const Current = Context.Reference<Values>(
  '@effect-harness/provider-claude-code/RequestOptions',
  { defaultValue: () => ({}) },
)
