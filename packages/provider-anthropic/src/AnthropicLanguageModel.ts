/**
 * Captured native Anthropic models and API-key transport composition.
 *
 * @since 0.0.0
 */
import * as Config from 'effect/Config'
// effect-review-allow P9-namespace-alias-equals-module: @effect/ai-anthropic/AnthropicLanguageModel and packages/provider-anthropic/src/AnthropicLanguageModel.ts both bind AnthropicLanguageModel; anthropicLanguageModel distinguishes the concepts.
import * as anthropicLanguageModel from '@effect/ai-anthropic/AnthropicLanguageModel'
import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as ToolResult from './ToolResult.ts'

import type * as HttpClient from 'effect/http/HttpClient'
import type * as Redacted from 'effect/Redacted'
/**
 * Constructs the native model with provider-owned system history handling.
 *
 * @category constructors
 * @since 0.0.0
 */
export const make = Effect.fnUntraced(function* (
  options: Parameters<typeof anthropicLanguageModel.make>[0],
): Effect.fn.Return<
  typeof LanguageModel.LanguageModel.Service,
  never,
  AnthropicClient.AnthropicClient
> {
  const native = yield* AnthropicClient.AnthropicClient
  return yield* anthropicLanguageModel
    .make(options)
    .pipe(Effect.provideService(AnthropicClient.AnthropicClient, ToolResult.client(native)))
})

/**
 * Provides the standard Effect AI LanguageModel service; the caller supplies its native AnthropicClient.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer = (
  options: Parameters<typeof anthropicLanguageModel.make>[0],
): Layer.Layer<
  LanguageModel.LanguageModel | AnthropicClient.AnthropicClient,
  never,
  AnthropicClient.AnthropicClient
> =>
  Layer.effectContext(
    Effect.gen(function* () {
      const client = yield* AnthropicClient.AnthropicClient
      const model = yield* make(options)
      return Context.make(LanguageModel.LanguageModel, model).pipe(
        Context.add(AnthropicClient.AnthropicClient, client),
      )
    }),
  )

/**
 * Resolves all layer options through the caller's ConfigProvider.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerConfig = (
  config: Config.Wrap<NonNullable<Parameters<typeof layer>[0]>>,
): Layer.Layer<
  LanguageModel.LanguageModel | AnthropicClient.AnthropicClient,
  Config.ConfigError,
  AnthropicClient.AnthropicClient
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layer(yield* Config.unwrap(config))
    }),
  )
/**
 * Anthropic model and transport settings, preserving native provider options.
 *
 * @category types
 * @since 0.0.0
 */
export declare namespace AnthropicLanguageModel {
  /**
   * Describes the Options contract.
   *
   * @category types
   * @since 0.0.0
   */
  export interface Options extends AnthropicClient.Options {
    readonly apiKey: Redacted.Redacted<string>
    readonly model: anthropicLanguageModel.Model | (string & {})
    readonly config?: Omit<typeof anthropicLanguageModel.Config.Service, 'model'> | undefined
  }
}
/**
 * Describes the Options contract.
 *
 * @category types
 * @since 0.0.0
 */
export type Options = AnthropicLanguageModel.Options

/**
 * Provides the native Effect AI model and client. Supply an HttpClient Layer at the app boundary.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerApiKey = (
  options: Options,
): Layer.Layer<
  LanguageModel.LanguageModel | AnthropicClient.AnthropicClient,
  never,
  HttpClient.HttpClient
> =>
  layer({ model: options.model, config: options.config }).pipe(
    Layer.provideMerge(AnthropicClient.layer(options)),
  )

/**
 * Resolves every model and transport option through the caller's ConfigProvider.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerApiKeyConfig = (
  config: Config.Wrap<Options>,
): Layer.Layer<
  LanguageModel.LanguageModel | AnthropicClient.AnthropicClient,
  Config.ConfigError,
  HttpClient.HttpClient
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layerApiKey(yield* Config.unwrap(config))
    }),
  )

/**
 * Reads ANTHROPIC_API_KEY by default; all other options use the caller's ConfigProvider.
 *
 * @category layers
 * @since 0.0.0
 */
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
      return layerApiKey({ ...options, apiKey })
    }),
  )

/**
 * Applies native Anthropic options to a single request Effect.
 *
 * @category models
 * @since 0.0.0
 */
export const withConfigOverride: typeof anthropicLanguageModel.withConfigOverride =
  anthropicLanguageModel.withConfigOverride

/**
 * Forwards the supported public declarations from their owning concept.
 *
 * @category exports
 * @since 0.0.0
 */
export { Config } from '@effect/ai-anthropic/AnthropicLanguageModel'
export type { Model } from '@effect/ai-anthropic/AnthropicLanguageModel'

/**
 * Forwards the supported public declarations from their owning concept.
 *
 * @category exports
 * @since 0.0.0
 */
export { model } from '@effect/ai-anthropic/AnthropicLanguageModel'
export type {
  AnthropicUserDefinedTool,
  AnthropicProviderDefinedTool,
} from '@effect/ai-anthropic/AnthropicLanguageModel'
