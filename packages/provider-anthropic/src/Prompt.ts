import * as AnthropicLanguageModel from '@effect/ai-anthropic/AnthropicLanguageModel'
import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as NativePrompt from 'effect/ai/Prompt'
import * as ToolResult from './ToolResult.ts'

/**
 * Collects system messages into one leading group in transcript order.
 *
 * Use this explicit projection when an application wants leading instructions.
 * Native models handle their own history capabilities. Keeping each original
 * block preserves provider options, including cache breakpoints, while
 * all user, assistant and tool messages retain their original data and ordering.
 */
export function normalize(prompt: NativePrompt.Prompt): NativePrompt.Prompt {
  return NativePrompt.fromMessages([
    ...prompt.content.filter((message) => message.role === 'system'),
    ...prompt.content.filter((message) => message.role !== 'system'),
  ])
}

/** Constructs the native model with provider-owned system history handling. */
export const make = Effect.fnUntraced(function* (
  options: Parameters<typeof AnthropicLanguageModel.make>[0],
) {
  const native = yield* AnthropicClient.AnthropicClient
  return yield* AnthropicLanguageModel.make(options).pipe(
    Effect.provideService(AnthropicClient.AnthropicClient, ToolResult.client(native)),
  )
})

/** Provides the standard Effect AI LanguageModel service; the caller supplies its native AnthropicClient. */
export const layer = (options: Parameters<typeof AnthropicLanguageModel.make>[0]) =>
  Layer.effect(LanguageModel.LanguageModel, make(options))
