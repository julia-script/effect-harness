/**
 * Request-local Claude Code model, session and inference controls.
 */
import * as Context from 'effect/Context'
import type * as Cli from './Cli.ts'

/**
 * Scoped per-request controls consumed by the ordinary native LanguageModel provider.
 *
 * @category models
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
 */
export const Current = Context.Reference<Values>(
  '@effect-harness/provider-claude-code/RequestOptions',
  { defaultValue: () => ({}) },
)
