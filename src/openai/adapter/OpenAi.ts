/**
 * The OpenAI adapter's declaration: an account holds an API key, and may point at any
 * OpenAI-compatible Responses API. It imports no SDK, so a client can use it; the implementation
 * is `OpenAi.implementation` in `effect-harness/openai`.
 */
import { Schema } from 'effect'
import * as Adapter from '../../accounts/Adapter.ts'

export const adapter = Adapter.make('openai', {
  label: 'OpenAI',
  auth: Adapter.apiKey,
  settings: Schema.Struct({
    /** Default `https://api.openai.com/v1`; set it for an OpenAI-compatible server. */
    apiUrl: Schema.optionalKey(Schema.String),
  }),
})
