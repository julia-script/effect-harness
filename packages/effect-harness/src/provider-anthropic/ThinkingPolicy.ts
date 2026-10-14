/** Model-specific replay policy for history and top-level tool declaration changes. */
import * as Effect from 'effect/Effect'
import * as Http from '../internal/SubscriptionHttp.js'

export const beta = 'thinking-binding-controls-2026-08-01'
/** Exact model families verified against the preserved-thinking protocol, not inferred from OAuth. */
export const forModel = (model: string): 'server-drop' | 'preserve' | 'unverified' => {
  if (/^claude-(?:fable-5-1|opus-5-5|sonnet-5-5|haiku-5-5)(?:-\d{8})?$/.test(model))
    return 'server-drop'
  if (
    /^claude-(?:(?:opus|sonnet|haiku)-4(?:-[0-8])?|(?:opus|sonnet)-5|mythos-5-1)(?:-\d{8})?$/.test(
      model,
    )
  )
    return 'preserve'
  return 'unverified'
}

/** Never rewrite stored reasoning. The server checks old blocks and keeps newly valid thinking. */
export const configure = (model: string, mode: string | undefined, hasThinking: boolean) => {
  const policy = forModel(model)
  if (policy === 'server-drop') {
    if (mode !== 'adaptive')
      return Effect.fail(
        Http.invalid('This model requires adaptive thinking for safe prefix changes'),
      )
    return Effect.succeed({ block_binding: { prefix_mismatch_behavior: 'drop_block' as const } })
  }
  if (policy === 'unverified' && (hasThinking || (mode !== undefined && mode !== 'disabled')))
    return Effect.fail(Http.invalid('Thinking replay policy is unverified for this model'))
  return Effect.succeed({})
}
