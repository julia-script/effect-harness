/** Claude subscription browser OAuth, implemented by `AnthropicOAuth.layerAdapter`. */
import * as Adapter from '../../accounts/Adapter.ts'

export const adapter = Adapter.make('anthropic-oauth', {
  label: 'Claude (subscription)',
  auth: Adapter.browserOAuth,
})
