/**
 * Anthropic account model construction sharing its captured native client.
 */
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as Config from 'effect/Config'
import type * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as AnthropicLanguageModel from './AnthropicLanguageModel.ts'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import type * as AiError from 'effect/ai/AiError'
import type * as HttpClient from 'effect/http/HttpClient'
import type { OAuth } from './OAuth.ts'

import * as AnthropicAccountClient from './AnthropicAccountClient.ts'
/**
 * Native direct Messages LanguageModel, preserving structured Prompt history.
 *
 * @category layers
 */
export const layer = (
  options: AnthropicAccountClient.AnthropicAccountClient.ClientOptions & {
    readonly model: string
    readonly config?: Omit<typeof AnthropicLanguageModel.Config.Service, 'model'> | undefined
  },
): Layer.Layer<
  LanguageModel.LanguageModel | AnthropicClient.AnthropicClient,
  AiError.AiError,
  HttpClient.HttpClient | OAuth
> =>
  AnthropicLanguageModel.layer({ model: options.model, config: options.config }).pipe(
    Layer.provideMerge(AnthropicAccountClient.layer(options)),
  )

/**
 * Resolves all layer options through the caller's ConfigProvider.
 *
 * @category layers
 */
export const layerConfig = (
  config: Config.Wrap<NonNullable<Parameters<typeof layer>[0]>>,
): Layer.Layer<
  LanguageModel.LanguageModel | AnthropicClient.AnthropicClient,
  AiError.AiError | Config.ConfigError,
  HttpClient.HttpClient | OAuth
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layer(yield* Config.unwrap(config))
    }),
  )
