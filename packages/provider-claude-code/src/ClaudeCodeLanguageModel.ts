/**
 * Claude Code language models with scoped transport and intent-session ownership.
 *
 * @since 0.0.0
 */
import * as Config from 'effect/Config'
import * as Context from 'effect/Context'
import type * as AiError from 'effect/ai/AiError'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Cli from './Cli.ts'
import * as IntentServer from './IntentServer.ts'
import * as Prompt from './Prompt.ts'
import * as Turn from './Turn.ts'
import * as RequestOptions from './RequestOptions.ts'
import { unsupported } from './ClaudeCodeError.ts'

/**
 * Describes the Options contract.
 *
 * @category types
 * @since 0.0.0
 */
export declare namespace ClaudeCodeLanguageModel {
  /**
   * Describes the Options contract.
   *
   * @category types
   * @since 0.0.0
   */
  export interface Options {
    readonly model: string
    readonly cwd?: string | undefined
    readonly effort?: Cli.Request['effort'] | undefined
    readonly historyMode?: Prompt.HistoryMode | undefined
  }
}
/**
 * Describes the Options contract.
 *
 * @category types
 * @since 0.0.0
 */
export type Options = ClaudeCodeLanguageModel.Options

/**
 * Native Effect AI provider. The caller supplies CLI transport and a scoped intent-only MCP server.
 *
 * @category constructors
 * @since 0.0.0
 */
export const make = Effect.fnUntraced(function* (
  options: Options,
): Effect.fn.Return<
  typeof LanguageModel.LanguageModel.Service,
  AiError.AiError,
  Cli.Cli | IntentServer.IntentServer
> {
  if (options.model.length === 0) return yield* unsupported('an empty model name')
  const cli = yield* Cli.Cli
  const server = yield* IntentServer.IntentServer
  const stream = (request: LanguageModel.ProviderOptions) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const dynamic = yield* RequestOptions.Current
        if (dynamic.model !== undefined && dynamic.model !== options.model)
          return yield* unsupported('redirecting a pinned CLI model')
        const input = yield* Prompt.prepare(request, options.historyMode)
        const session = input.tools.length === 0 ? undefined : yield* server.open(input.tools)
        return Turn.translate(
          cli.run({
            model: options.model,
            cwd: options.cwd,
            effort: dynamic.effort ?? options.effort,
            sessionId: dynamic.sessionId,
            thinkingEnabled: dynamic.thinkingEnabled,
            maxTokens: dynamic.maxTokens,
            cache: dynamic.cache,
            autoCompact: dynamic.autoCompact,
            system: input.system,
            content: input.content,
            ...(session === undefined
              ? {}
              : { mcp: { url: session.url, aliases: [...session.aliases.keys()] } }),
          }),
          session?.aliases ?? new Map(),
        )
      }),
    )
  return yield* LanguageModel.make({
    generateText: (request) => Turn.collect(stream(request)),
    streamText: stream,
  })
})

/**
 * Provides ClaudeCodeLanguageModel services with the declared native dependencies.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer = (
  options: Options,
): Layer.Layer<
  LanguageModel.LanguageModel | Cli.Cli,
  AiError.AiError,
  Cli.Cli | IntentServer.IntentServer
> =>
  Layer.effectContext(
    Effect.gen(function* () {
      const cli = yield* Cli.Cli
      const model = yield* make(options)
      return Context.make(LanguageModel.LanguageModel, model).pipe(Context.add(Cli.Cli, cli))
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
  LanguageModel.LanguageModel | Cli.Cli,
  AiError.AiError | Config.ConfigError,
  Cli.Cli | IntentServer.IntentServer
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layer(yield* Config.unwrap(config))
    }),
  )
