/**
 * Captured native Anthropic models and API-key transport composition.
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
 * Type-level contracts for `AnthropicLanguageModel`.
 *
 * @category utility types
 */
export declare namespace AnthropicLanguageModel {
  /**
   * Redacted API key, selected native model and Anthropic client/request configuration.
   *
   * @category models
   */
  export interface Options extends AnthropicClient.Options {
    readonly apiKey: Redacted.Redacted<string>
    readonly model: anthropicLanguageModel.Model | (string & {})
    readonly config?: Omit<typeof anthropicLanguageModel.Config.Service, 'model'> | undefined
  }
}
/**
 * Redacted API key, selected native model and Anthropic client/request configuration.
 *
 * @category models
 */
export type Options = AnthropicLanguageModel.Options

/**
 * Provides a native Anthropic LanguageModel and client using a Redacted API key.
 *
 * **Details**
 *
 * Consumes HttpClient and applies native model configuration. Harness tool-content envelopes
 * are expanded at the captured client boundary.
 *
 * @category layers
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
 */
export const withConfigOverride: typeof anthropicLanguageModel.withConfigOverride =
  anthropicLanguageModel.withConfigOverride

/**
 * Public APIs from `@effect/ai-anthropic/AnthropicLanguageModel`.
 *
 * @category re-exports
 */
export { Config } from '@effect/ai-anthropic/AnthropicLanguageModel'
/**
 * Public APIs from `@effect/ai-anthropic/AnthropicLanguageModel`.
 *
 * @category re-exports
 */
export type { Model } from '@effect/ai-anthropic/AnthropicLanguageModel'

/**
 * Public APIs from `@effect/ai-anthropic/AnthropicLanguageModel`.
 *
 * @category re-exports
 */
export { model } from '@effect/ai-anthropic/AnthropicLanguageModel'
/**
 * Public APIs from `@effect/ai-anthropic/AnthropicLanguageModel`.
 *
 * @category re-exports
 */
export type {
  AnthropicUserDefinedTool,
  AnthropicProviderDefinedTool,
} from '@effect/ai-anthropic/AnthropicLanguageModel'
