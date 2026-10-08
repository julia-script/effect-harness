/** Deterministic native AI boundary shared by embedded harness and restart tests. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Prompt from 'effect/ai/Prompt'
import type * as Response from 'effect/ai/Response'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Executor from '../../src/Executor.ts'
import * as Invocation from '../../src/Invocation.ts'
import * as Model from '../../src/Model.ts'
import * as Registry from '../../src/Registry.ts'
import * as ToolError from '../../src/ToolError.ts'
import * as ToolRegistration from '../../src/ToolRegistration.ts'

export const ref = { provider: 'test', modelId: 'offline' }
export const Uppercase = Tool.make('uppercase', {
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.String,
  failure: ToolError.ToolError,
}).addDependency(Invocation.ToolCall)
const toolkit = Toolkit.make(Uppercase)
export const finish = (reason: Response.FinishReason): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 5, text: 5, reasoning: undefined },
  },
  response: undefined,
})
export const answer = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: 'text-start', id: 'answer' },
  { type: 'text-delta', id: 'answer', delta: text },
  { type: 'text-end', id: 'answer' },
  finish('stop'),
]
export const toolCall = (): ReadonlyArray<Response.StreamPartEncoded> => [
  {
    type: 'tool-call',
    id: 'uppercase-call',
    name: 'uppercase',
    params: { text: 'hello' },
    providerExecuted: false,
  },
  finish('tool-calls'),
]
export const hasToolResult = (prompt: Prompt.Prompt): boolean =>
  prompt.content.some(
    (message) =>
      message.role === 'tool' &&
      message.content.some((part) => part.type === 'tool-result' && part.name === 'uppercase'),
  )
export const provider: Parameters<typeof LanguageModel.make>[0] = {
  generateText: () => Effect.succeed([{ type: 'text', text: 'summary' }, finish('stop')]),
  streamText: ({ prompt }) =>
    Stream.fromIterable(hasToolResult(prompt) ? answer('HELLO') : toolCall()),
}
export interface Options {
  readonly provider?: Parameters<typeof LanguageModel.make>[0]
  readonly handle?: (
    input: typeof Uppercase.parametersSchema.Type,
  ) => Effect.Effect<string, ToolError.ToolError, Invocation.ToolCall>
  readonly replay?: 'safe' | 'unsafe'
}
/** Test handlers own no external resources; all live orchestration belongs to Harness Scope. */
export const makeExecutor = Effect.fn('test.makeExecutor')(function* (options: Options = {}) {
  const model = yield* LanguageModel.make(options.provider ?? provider)
  const handle =
    options.handle ??
    (({ text }: typeof Uppercase.parametersSchema.Type) => Effect.succeed(text.toUpperCase()))
  const tools = yield* ToolRegistration.bind(toolkit, {
    uppercase: { replay: options.replay ?? 'safe' },
  }).pipe(Effect.provide(toolkit.toLayer({ uppercase: handle })))
  const registry = yield* Registry.make([{ name: 'test', tools }])
  const descriptor: Model.Descriptor = {
    ref,
    model,
    contextWindow: 100000,
    maxOutputTokens: 1000,
    configure: () => Effect.succeed(Context.empty()),
  }
  const executor = yield* Executor.Executor.pipe(
    Effect.provide(
      Executor.layer.pipe(
        Layer.provide(Model.layer([descriptor])),
        Layer.provide(Layer.succeed(Registry.Registry, registry)),
      ),
    ),
  )
  return { executor, registry }
})
