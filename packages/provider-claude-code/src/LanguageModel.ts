import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Stream from 'effect/Stream'
import * as NativeLanguageModel from 'effect/ai/LanguageModel'
import * as Cli from './Cli.ts'
import * as IntentServer from './IntentServer.ts'
import * as Prompt from './Prompt.ts'
import * as Turn from './Turn.ts'
import * as RequestOptions from './RequestOptions.ts'
import { unsupported } from './Error.ts'

export interface Options {
  readonly model: string
  readonly cwd?: string | undefined
  readonly effort?: Cli.Request['effort']
  readonly historyMode?: Prompt.HistoryMode | undefined
}

/** Native Effect AI provider. The caller supplies CLI transport and a scoped intent-only MCP server. */
export const make = (options: Options) =>
  Effect.gen(function* () {
    if (options.model.length === 0) return yield* unsupported('an empty model name')
    const cli = yield* Cli.Cli
    const server = yield* IntentServer.IntentServer
    const stream = (request: NativeLanguageModel.ProviderOptions) =>
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
    return yield* NativeLanguageModel.make({
      generateText: (request) => Turn.collect(stream(request)),
      streamText: stream,
    })
  })

export const layer = (options: Options) =>
  Layer.effect(NativeLanguageModel.LanguageModel)(make(options))
