/** Ready-to-use OpenRouter integration with public metadata and credential-managed inference. */
import * as Schema from 'effect/Schema'
import * as Compatible from '../provider-openai-compatible/Provider.js'
import * as Catalog from './Catalog.js'

/** OpenRouter supplies the endpoint and catalog; instances need no public configuration. */
export const Config = Schema.Struct({})

/** Register once, then add any number of API-key-backed or unauthenticated instances. */
export const definition = Compatible.define({
  version: 1,
  config: Config,
  connection: () => ({ baseUrl: 'https://openrouter.ai/api/v1', reasoningFormat: 'openrouter' }),
  models: {
    list: (_context, input) => Catalog.list(input),
    get: (_context, modelId) => Catalog.get(modelId),
  },
})
