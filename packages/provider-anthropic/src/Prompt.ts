import * as AnthropicLanguageModel from '@effect/ai-anthropic/AnthropicLanguageModel'
import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as NativePrompt from 'effect/ai/Prompt'
import type * as Tool from 'effect/ai/Tool'
import type { NoExcessProperties } from 'effect/Types'
import * as ToolResult from './ToolResult.ts'

/**
 * Collects system messages into one leading group in transcript order.
 *
 * The native Anthropic converter replaces preceding system groups. Keeping each
 * original block preserves provider options, including cache breakpoints, while
 * all user, assistant and tool messages retain their original data and ordering.
 */
export function normalize(prompt: NativePrompt.Prompt): NativePrompt.Prompt {
  return NativePrompt.fromMessages([
    ...prompt.content.filter((message) => message.role === 'system'),
    ...prompt.content.filter((message) => message.role !== 'system'),
  ])
}

/** Applies normalization at the native LanguageModel input boundary, retaining its generic method types. */
export function normalizeModel(model: LanguageModel.LanguageModel): LanguageModel.LanguageModel {
  // TypeScript reduces overloaded methods to their final signature when a
  // delegate receives transformed arguments. Only the prompt is changed; the
  // native method still owns every toolkit/error/service and parameter-mode
  // generic. Restore those exact overloads at this narrowly bounded boundary.
  const generateText = (<
    Tools extends Record<string, Tool.Any>,
    Options extends NoExcessProperties<
      LanguageModel.GenerateTextOptions<Tools> & {
        readonly toolkit: LanguageModel.ToolkitInput<Tools>
      },
      Options
    >,
  >(
    options: Options &
      LanguageModel.GenerateTextOptions<Tools> & {
        readonly toolkit: LanguageModel.ToolkitInput<Tools>
      },
  ) =>
    model.generateText({
      ...options,
      prompt: normalize(NativePrompt.make(options.prompt)),
    })) as LanguageModel.LanguageModel['generateText']
  const streamText = (<
    Tools extends Record<string, Tool.Any>,
    Options extends NoExcessProperties<
      LanguageModel.GenerateTextOptions<Tools> & {
        readonly toolkit: LanguageModel.ToolkitInput<Tools>
      },
      Options
    >,
  >(
    options: Options &
      LanguageModel.GenerateTextOptions<Tools> & {
        readonly toolkit: LanguageModel.ToolkitInput<Tools>
      },
  ) =>
    model.streamText({
      ...options,
      prompt: normalize(NativePrompt.make(options.prompt)),
    })) as LanguageModel.LanguageModel['streamText']
  return LanguageModel.LanguageModel.of({
    ...model,
    generateText,
    generateObject: (options) =>
      model.generateObject({ ...options, prompt: normalize(NativePrompt.make(options.prompt)) }),
    streamText,
  })
}

/** Constructs the ordinary native model with faithful default system normalization. */
export const make = Effect.fnUntraced(function* (
  options: Parameters<typeof AnthropicLanguageModel.make>[0],
) {
  const native = yield* AnthropicClient.AnthropicClient
  return normalizeModel(
    yield* AnthropicLanguageModel.make(options).pipe(
      Effect.provideService(AnthropicClient.AnthropicClient, ToolResult.client(native)),
    ),
  )
})

/** Provides the standard Effect AI LanguageModel service; the caller supplies its native AnthropicClient. */
export const layer = (options: Parameters<typeof AnthropicLanguageModel.make>[0]) =>
  Layer.effect(LanguageModel.LanguageModel, make(options))
