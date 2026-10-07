/**
 * ChatGPT account model construction sharing its captured Responses client.
 */
import * as Config from 'effect/Config'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as OpenAiLanguageModel from './OpenAiLanguageModel.ts'
import * as ChatGptClient from './ChatGptClient.ts'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as HttpClient from 'effect/http/HttpClient'
import { ChatGpt } from './ChatGpt.ts'

/**
 * Provides a native LanguageModel using authorized ChatGPT account transport.
 *
 * **Details**
 *
 * Preserves structured Prompt history and disables remote response storage. Consumes ChatGpt
 * and HttpClient and exposes the captured native OpenAI client.
 *
 * @category layers
 */
export const layer = (options: {
  readonly account: string
  readonly model: string
  readonly config?: Omit<typeof OpenAiLanguageModel.Config.Service, 'model'> | undefined
}): Layer.Layer<
  LanguageModel.LanguageModel | OpenAiClient.OpenAiClient,
  never,
  ChatGpt | HttpClient.HttpClient
> =>
  Layer.effect(
    LanguageModel.LanguageModel,
    OpenAiLanguageModel.make({
      model: options.model,
      config: { ...options.config, store: false },
    }),
  ).pipe(Layer.provideMerge(ChatGptClient.layer({ account: options.account })))

/**
 * Resolves all layer options through the caller's ConfigProvider.
 *
 * @category layers
 */
export const layerConfig = (
  config: Config.Wrap<NonNullable<Parameters<typeof layer>[0]>>,
): Layer.Layer<
  LanguageModel.LanguageModel | OpenAiClient.OpenAiClient,
  Config.ConfigError,
  ChatGpt | HttpClient.HttpClient
> => Layer.unwrap(Effect.map(Config.unwrap(config), layer))
