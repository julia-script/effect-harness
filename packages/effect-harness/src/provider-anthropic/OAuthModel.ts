/** Harness model descriptor with pinned OAuth protocol and credential bindings. */
import * as Effect from 'effect/Effect'
import type * as Model from '../Model.js'
import * as SubscriptionModel from '../internal/SubscriptionModel.js'
import * as OAuthLanguageModel from './OAuthLanguageModel.js'
import * as ThinkingPolicy from './ThinkingPolicy.js'
import * as Http from '../internal/SubscriptionHttp.js'

export const make = Effect.fnUntraced(function* (
  options: OAuthLanguageModel.Options & { readonly definition: Model.Definition },
) {
  if (
    options.definition.ref.provider !== 'anthropic' ||
    options.definition.ref.modelId !== options.model ||
    options.definition.capabilities.structuredOutput
  )
    return yield* Http.invalid(
      'OAuth definition must match the selected model and declare structuredOutput false',
    )
  const languageModel = yield* OAuthLanguageModel.make(options)
  return yield* SubscriptionModel.make({
    settings: OAuthLanguageModel.Settings,
    tag: OAuthLanguageModel.Config,
    defaults: options.config ?? {},
    binding: options.binding,
    protocol: `effect-harness/anthropic/oauth/api/${options.apiVersion ?? '2023-06-01'}`,
    definition: options.definition,
    languageModel,
    capabilities: {
      thinkingPolicy: ThinkingPolicy.forModel(options.model),
      thinkingModes: [...(options.thinkingModes ?? [])],
      efforts: [...(options.efforts ?? [])],
      supportsLongCache: options.supportsLongCache ?? false,
      supportsTemperature: options.supportsTemperature ?? false,
    },
  })
})
