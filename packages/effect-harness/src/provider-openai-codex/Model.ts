/** Constructs a harness model with a serializable, account-bound configuration codec. */
import * as Effect from 'effect/Effect'
import type * as Model from '../Model.js'
import * as SubscriptionModel from '../internal/SubscriptionModel.js'
import * as CodexLanguageModel from './CodexLanguageModel.js'
import * as Http from '../internal/SubscriptionHttp.js'

export const make = Effect.fnUntraced(function* (
  options: CodexLanguageModel.Options & { readonly definition: Model.Definition },
) {
  if (
    options.definition.ref.provider !== 'openai-codex' ||
    options.definition.ref.modelId !== options.model ||
    options.definition.capabilities.structuredOutput
  )
    return yield* Http.invalid(
      'Codex definition must match the selected model and declare structuredOutput false',
    )
  const languageModel = yield* CodexLanguageModel.make(options)
  return yield* SubscriptionModel.make({
    settings: CodexLanguageModel.Settings,
    tag: CodexLanguageModel.Config,
    defaults: options.config ?? {},
    binding: options.binding,
    protocol: 'effect-harness/openai-codex/sse-v1',
    definition: options.definition,
    languageModel,
  })
})
