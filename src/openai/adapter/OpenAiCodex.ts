/**
 * The Codex adapter's declaration: an account signs in to a ChatGPT subscription with a device
 * code. It imports no SDK, so a client can use it; the implementation is
 * `OpenAiCodex.implementation` in `effect-harness/openai`.
 */
import * as Adapter from '../../accounts/Adapter.ts'

export const adapter = Adapter.make('openai-codex', {
  label: 'ChatGPT (Codex)',
  auth: Adapter.deviceCode,
})
