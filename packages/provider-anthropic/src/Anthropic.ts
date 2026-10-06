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

/** Loads an API key from Config and provides the ordinary Effect AI services. */
export const layerConfig = (options: {
  readonly model: AnthropicLanguageModel.Model | (string & {})
  readonly apiKey?: Config.Config<Redacted.Redacted<string>> | undefined
  readonly apiUrl?: Config.Config<string> | undefined
  readonly apiVersion?: Config.Config<string> | undefined
  readonly config?: Omit<typeof AnthropicLanguageModel.Config.Service, 'model'> | undefined
  readonly transformClient?: AnthropicClient.Options['transformClient']
}): Layer.Layer<
  LanguageModel.LanguageModel | AnthropicClient.AnthropicClient,
  Config.ConfigError,
  HttpClient.HttpClient
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      const apiKey = yield* options.apiKey ?? Config.Redacted('ANTHROPIC_API_KEY')
      const apiUrl = options.apiUrl === undefined ? undefined : yield* options.apiUrl
      const apiVersion = options.apiVersion === undefined ? undefined : yield* options.apiVersion
      return layer({
        apiKey,
        apiUrl,
        apiVersion,
        model: options.model,
        config: options.config,
        transformClient: options.transformClient,
      })
    }),
  )

/** Applies native Anthropic options to a single Effect or Stream request. */
export const withConfigOverride = AnthropicLanguageModel.withConfigOverride
