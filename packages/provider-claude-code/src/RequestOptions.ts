import * as Context from 'effect/Context'
import type * as Cli from './Cli.ts'

/** Scoped per-request controls consumed by the ordinary native LanguageModel provider. */
export interface Values {
  readonly model?: string | undefined
  readonly sessionId?: string | undefined
  readonly effort?: Cli.Request['effort']
  readonly thinkingEnabled?: boolean | undefined
  readonly maxTokens?: number | undefined
  readonly cache?: 'none' | 'short' | 'long' | undefined
  readonly autoCompact?: false | undefined
}
export const Current = Context.Reference<Values>(
  '@effect-harness/provider-claude-code/RequestOptions',
  { defaultValue: () => ({}) },
)
