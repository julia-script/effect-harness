import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as AnthropicLanguageModel from '@effect/ai-anthropic/AnthropicLanguageModel'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as Config from 'effect/Config'
import * as Effect from 'effect/Effect'
import type * as HttpClient from 'effect/http/HttpClient'
import * as Layer from 'effect/Layer'
import type * as Redacted from 'effect/Redacted'
import * as Prompt from './Prompt.ts'

/** Anthropic model and transport settings, preserving native provider options. */
export interface Options extends AnthropicClient.Options {
  readonly apiKey: Redacted.Redacted<string>
  readonly model: AnthropicLanguageModel.Model | (string & {})
  readonly config?: Omit<typeof AnthropicLanguageModel.Config.Service, 'model'> | undefined
}

/** Provides the native Effect AI model and client. Supply an HttpClient Layer at the app boundary. */
export const layer = (
  options: Options,
): Layer.Layer<
  LanguageModel.LanguageModel | AnthropicClient.AnthropicClient,
  never,
  HttpClient.HttpClient
> =>
  Prompt.layer({ model: options.model, config: options.config }).pipe(
    Layer.provideMerge(AnthropicClient.layer(options)),
  )

/** Resolves every model and transport option through the caller's ConfigProvider. */
export const layerConfig = (
  config: Config.Wrap<Options>,
): Layer.Layer<
  LanguageModel.LanguageModel | AnthropicClient.AnthropicClient,
  Config.ConfigError,
  HttpClient.HttpClient
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layer(yield* Config.unwrap(config))
    }),
  )

/** Reads ANTHROPIC_API_KEY by default; all other options use the caller's ConfigProvider. */
export const layerDefaultConfig = (
  config: Config.Wrap<Omit<Options, 'apiKey'>>,
): Layer.Layer<
  LanguageModel.LanguageModel | AnthropicClient.AnthropicClient,
  Config.ConfigError,
  HttpClient.HttpClient
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      const options = yield* Config.unwrap(config)
      const apiKey = yield* Config.Redacted('ANTHROPIC_API_KEY')
      return layer({ ...options, apiKey })
    }),
  )

/** Applies native Anthropic options to a single Effect or Stream request. */
export const withConfigOverride = AnthropicLanguageModel.withConfigOverride
