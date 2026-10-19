/**
 * Captured native OpenAI models and API-key transport composition.
 */
import * as config from 'effect/Config'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as OpenAiLanguageModel from '@effect/ai-openai/OpenAiLanguageModel'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import type * as Redacted from 'effect/Redacted'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as HttpClient from 'effect/http/HttpClient'
import * as ToolResult from './ToolResult.ts'

/**
 * Constructs the native model with canonical tool-media translation at its captured client boundary.
 *
 * @category constructors
 */
export const make = Effect.fnUntraced(function* (
  options: Parameters<typeof OpenAiLanguageModel.make>[0],
): Effect.fn.Return<typeof LanguageModel.LanguageModel.Service, never, OpenAiClient.OpenAiClient> {
  const native = yield* OpenAiClient.OpenAiClient
  return yield* OpenAiLanguageModel.make(options).pipe(
    Effect.provideService(OpenAiClient.OpenAiClient, ToolResult.client(native, options.config)),
  )
})

/**
 * Provides the exact selected client with a single native model construction.
 *
 * @category layers
 */
export const layer = (
  options: Parameters<typeof OpenAiLanguageModel.make>[0],
): Layer.Layer<
  LanguageModel.LanguageModel | OpenAiClient.OpenAiClient,
  never,
  OpenAiClient.OpenAiClient
> =>
  Layer.effectContext(
    Effect.gen(function* () {
      const client = yield* OpenAiClient.OpenAiClient
      const model = yield* make(options)
      return Context.make(LanguageModel.LanguageModel, model).pipe(
        Context.add(OpenAiClient.OpenAiClient, client),
      )
    }),
  )

/**
 * Provides a native OpenAI LanguageModel and client using a Redacted API key.
 *
 * **Details**
 *
 * Consumes HttpClient. model selects the Responses model; config supplies native request
 * defaults and apiUrl can override the API endpoint.
 *
 * @category layers
 */
export const layerApiKey = (options: {
  readonly apiKey: Redacted.Redacted<string>
  readonly model: string
  readonly config?: Omit<typeof OpenAiLanguageModel.Config.Service, 'model'> | undefined
  readonly apiUrl?: string | undefined
}): Layer.Layer<
  LanguageModel.LanguageModel | OpenAiClient.OpenAiClient,
  never,
  HttpClient.HttpClient
> =>
  Layer.effect(
    LanguageModel.LanguageModel,
    make({ model: options.model, config: options.config }),
  ).pipe(Layer.provideMerge(OpenAiClient.layer({ apiKey: options.apiKey, apiUrl: options.apiUrl })))

/**
 * Resolves all layerApiKey options through the caller's ConfigProvider.
 *
 * @category layers
 */
export const layerApiKeyConfig = (
  configuration: config.Wrap<NonNullable<Parameters<typeof layerApiKey>[0]>>,
): Layer.Layer<
  LanguageModel.LanguageModel | OpenAiClient.OpenAiClient,
  config.ConfigError,
  HttpClient.HttpClient
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layerApiKey(yield* config.unwrap(configuration))
    }),
  )

/**
 * Resolves the model options while retaining the caller's exact native client.
 *
 * @category layers
 */
export const layerConfig = (
  configuration: config.Wrap<Parameters<typeof layer>[0]>,
): Layer.Layer<
  LanguageModel.LanguageModel | OpenAiClient.OpenAiClient,
  config.ConfigError,
  OpenAiClient.OpenAiClient
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layer(yield* config.unwrap(configuration))
    }),
  )
/**
 * Public APIs from `@effect/ai-openai/OpenAiLanguageModel`.
 *
 * @category re-exports
 */
export { Config } from '@effect/ai-openai/OpenAiLanguageModel'
/**
 * Public APIs from `@effect/ai-openai/OpenAiLanguageModel`.
 *
 * @category re-exports
 */
export type { Model } from '@effect/ai-openai/OpenAiLanguageModel'
