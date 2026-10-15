/** Model-specific replay policy for history and top-level tool declaration changes. */
import * as Effect from 'effect/Effect'
import * as Http from '../internal/SubscriptionHttp.js'

export const beta = 'thinking-binding-controls-2026-08-01'
export type Policy = 'server-drop' | 'preserve'

/** Known replay capabilities; other models use the ordinary Messages path. */
export const forModel = (model: string): Policy =>
  /^claude-(?:fable-5-1|opus-5-5|sonnet-5-5|haiku-5-5)(?:-\d{8})?$/.test(model)
    ? 'server-drop'
    : 'preserve'

/** Never rewrite stored reasoning. The server checks old blocks and keeps newly valid thinking. */
export const configure = (policy: Policy, mode: string | undefined) => {
  if (policy === 'server-drop') {
    if (mode !== 'adaptive')
      return Effect.fail(
        Http.invalid('This model requires adaptive thinking for safe prefix changes'),
      )
    return Effect.succeed({ block_binding: { prefix_mismatch_behavior: 'drop_block' as const } })
  }
  return Effect.succeed({})
}
