/**
 * The Anthropic adapter's declaration: an account holds an API key, and may point at a gateway. It
 * imports no SDK, so a client can use it; the implementation is `Anthropic.implementation` in
 * `effect-harness/anthropic`.
 */
import { Schema } from 'effect'
import * as Adapter from '../../accounts/Adapter.ts'

export const adapter = Adapter.make('anthropic', {
  label: 'Anthropic',
  auth: Adapter.apiKey,
  settings: Schema.Struct({
    /** Default `https://api.anthropic.com`; set it for a gateway. */
    apiUrl: Schema.optionalKey(Schema.String),
  }),
})
