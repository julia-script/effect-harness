import * as NativeLanguageModel from '../internal/NativeLanguageModel.ts'
/**
 * Claude Code language models with scoped transport and intent-session ownership.
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
 * Type-level contracts for `ClaudeCodeLanguageModel`.
 *
 */
export declare namespace ClaudeCodeLanguageModel {
  /**
   * Selected CLI model, cwd, effort and canonical-history import policy.
   *
   * @category models
   */
  export interface Options {
    readonly model: string
    readonly cwd?: string | undefined
    readonly effort?: Cli.Request['effort'] | undefined
    readonly historyMode?: Prompt.HistoryMode | undefined
  }
}

/**
 * Creates a native LanguageModel backed by the installed CLI and intent server.
 *
 * **Details**
 *
 * Consumes Cli and IntentServer. model, cwd and effort configure each request. historyMode
 * defaults to rejecting canonical history the CLI cannot faithfully import.
 *
 * **Gotchas**
 *
 * transcript mode renders saved messages as input data; it does not resume a native CLI
 * session. Unsupported media, options and structured-object generation fail with native
 * AiError.
 *
 * @category constructors
 */
export const make = Effect.fnUntraced(function* (
  options: ClaudeCodeLanguageModel.Options,
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
  const native = yield* NativeLanguageModel.factory
  return yield* native.make({
    generateText: (request) => Turn.collect(stream(request)),
    streamText: stream,
  })
})

/**
 * Provides the CLI-backed native LanguageModel and captured Cli service.
 *
 * **Details**
 *
 * Consumes Cli and IntentServer. Their owning scopes must remain alive for requests.
 *
 * @see {@link make} for history and capability constraints.
 * @category layers
 */
export const layer = (
  options: ClaudeCodeLanguageModel.Options,
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
