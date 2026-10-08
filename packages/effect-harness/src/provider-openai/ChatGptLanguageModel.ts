/**
 * ChatGPT account model construction sharing its captured Responses client.
 */
import * as Config from 'effect/Config'
import * as Context from 'effect/Context'
import type * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as OpenAiLanguageModel from './OpenAiLanguageModel.ts'
import * as ChatGptClient from './ChatGptClient.ts'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as HttpClient from 'effect/http/HttpClient'
import * as Stream from 'effect/Stream'
import type { ChatGpt } from './ChatGpt.ts'

const accountContext = <R>(context: Context.Context<R>): Context.Context<R> =>
  Context.add(
    context,
    OpenAiLanguageModel.Config,
    OpenAiLanguageModel.Config.of({
      ...Context.getOrUndefined(context, OpenAiLanguageModel.Config),
      store: false,
      useItemReferences: false,
    }),
  )

const accountEffect = <A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.updateContext(self, accountContext<R>)

/**
 * Constructs an account model that retains full history and disables item references for every request.
 *
 * @category constructors
 */
export const make = Effect.fnUntraced(function* (
  options: Parameters<typeof OpenAiLanguageModel.make>[0],
): Effect.fn.Return<LanguageModel.LanguageModel, never, OpenAiClient.OpenAiClient> {
  const native = yield* OpenAiLanguageModel.make(options)
  // The SDK methods have higher-rank toolkit/codec overloads. These decorators only
  // transform the request context; preserve those exact signatures, pinned by typetests.
  // effect-nit-allow P2-no-unknown-any-in-channels: Parameters projects the SDK's last higher-rank toolkit overload and erases its private inferred R; this identity decorator forwards the same request and changes only Config through generic accountEffect<A, E, R>. Its public callable is exactly native.generateText, with concrete service/handler-error/toolkit-mode witnesses in typetest/provider-openai/ChatGptLanguageModel.tst.ts.
  const generateText = ((request: Parameters<typeof native.generateText>[0]) =>
    accountEffect(
      // @effect-diagnostics-next-line anyUnknownInErrorContext:off -- Private SDK overload projection; the exact public native callable retains concrete request-service channels.
      // oxlint-disable-next-line effecttsgo/any-unknown-in-error-context -- Private SDK overload projection only; concrete public request-service channels remain exact.
      native.generateText(request),
    )) as typeof native.generateText
  // effect-nit-allow P2-no-unknown-any-in-channels: Parameters instantiates the SDK generic Encoder DecodingServices as unknown only inside this passthrough; generic accountEffect leaves A/E/R unchanged and the exact native callable is restored. typetest/provider-openai/ChatGptLanguageModel.tst.ts checks distinct schema/handler services and rejected toolkit input.
  const generateObject = ((request: Parameters<typeof native.generateObject>[0]) =>
    accountEffect(
      // @effect-diagnostics-next-line anyUnknownInErrorContext:off -- Private SDK Encoder projection; the exact public callable retains concrete schema and toolkit services.
      // oxlint-disable-next-line effecttsgo/any-unknown-in-error-context -- Private SDK Encoder projection only; public schema and toolkit service requirements remain exact.
      native.generateObject(request),
    )) as typeof native.generateObject
  const streamText = ((request: Parameters<typeof native.streamText>[0]) =>
    Stream.updateContext(native.streamText(request), accountContext)) as typeof native.streamText
  return LanguageModel.LanguageModel.of({
    ...native,
    generateText,
    generateObject,
    streamText,
  })
})

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
    make({
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
