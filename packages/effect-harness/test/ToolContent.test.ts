import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Hook from 'effect-harness/Hook'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Tool from 'effect-harness/Tool'
import * as ToolResult from 'effect-harness/ToolResult'
import * as Toolkit from 'effect-harness/Toolkit'
import * as OpenAiResult from 'effect-harness/provider-openai/ToolResult'
import * as AnthropicResult from 'effect-harness/provider-anthropic/ToolResult'

const png =
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aZkwAAAAASUVORK5CYII='
const image = Prompt.filePart({
  mediaType: 'image/png',
  data: png,
  options: {
    openai: { imageDetail: 'high' },
    anthropic: { cacheControl: { type: 'ephemeral' } },
  },
})
const finish = {
  type: 'finish' as const,
  reason: 'stop' as const,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
}
const run = <A, E, R>(program: Effect.Effect<A, E, R>) =>
  Effect.scoped(program).pipe(Effect.provide(Storage.layerMemory))
const execute = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.Toolkit<Tools>,
  hooks: ReadonlyArray<Hook.Hook<'afterTool', unknown, never>>,
) =>
  Effect.gen(function* () {
    let observed: unknown
    const model = yield* LanguageModel.make({
      generateText: ({ prompt }) => {
        const result = prompt.content
          .flatMap((message) => (message.role === 'tool' ? message.content : []))
          .find((part) => part.type === 'tool-result')
        if (result !== undefined) {
          observed = result.result
          return Effect.succeed([{ type: 'text', text: 'done' }, finish])
        }
        return Effect.succeed([
          { type: 'tool-call', id: 'call-1', name: 'work', params: {}, providerExecuted: false },
          { ...finish, reason: 'tool-calls' as const },
        ])
      },
      streamText: () => Stream.empty,
    })
    const runtime = yield* HarnessRuntime.make({ tools: toolkit, hooks }).pipe(
      Effect.provideService(LanguageModel.LanguageModel, model),
    )
    const root = yield* runtime.backend.root
    const job = yield* runtime.backend.submit({
      conversationId: root,
      draft: { type: 'input', content: 'go' },
    })
    const status = (yield* runtime.backend.wait(job.id)).status
    const entries = yield* Stream.runCollect(
      Session.scanEntries(runtime.session, { conversationId: root }),
    )
    const persisted = entries.find((entry) => entry.kind === 'tool.result')
    return { observed, persisted, status }
  })

describe('model-visible tool content', () => {
  it.effect('post-hook text and PNG content survives history and both provider boundaries', () =>
    run(
      Effect.gen(function* () {
        const toolkit = Toolkit.make(Tool.make('work', { success: Schema.String }))
        const hook = Hook.make({
          event: 'afterTool',
          execute: () =>
            Schema.decodeEffect(ToolResult.ResultSchema)({
              content: [Prompt.textPart({ text: 'caption' }), image],
              isError: false,
              diagnostics: [],
            }),
        })
        const { observed, persisted, status } = yield* execute(toolkit, [hook]).pipe(
          Effect.provide(toolkit.toLayer({ work: () => Effect.succeed('original') })),
        )
        assert.strictEqual(status, 'done')
        const envelope = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(ToolResult.Envelope))(
          observed,
        )
        assert.deepEqual(envelope.content, [Prompt.textPart({ text: 'caption' }), image])
        const persistedResult = yield* Schema.decodeUnknownEffect(
          Schema.toCodecJson(ToolResult.ResultSchema),
        )(persisted?.data)
        assert.deepEqual(persistedResult.content, envelope.content)
        const persistedMessages = yield* Schema.decodeUnknownEffect(
          Schema.toCodecJson(Schema.Array(Prompt.Message)),
        )(persisted?.model)
        const persistedPart = persistedMessages
          .flatMap((message) => (message.role === 'tool' ? message.content : []))
          .find((part) => part.type === 'tool-result')
        assert.deepEqual(persistedPart?.result, observed)
        const openai = yield* OpenAiResult.request({
          model: 'test',
          input: [
            { type: 'function_call_output', call_id: 'call-1', output: JSON.stringify(observed) },
          ],
        })
        if (
          openai.input == null ||
          typeof openai.input === 'string' ||
          openai.input[0]?.type !== 'function_call_output'
        )
          throw new Error('Missing OpenAI result')
        assert.deepEqual(openai.input[0].output, [
          { type: 'input_text', text: 'caption' },
          { type: 'input_image', detail: 'high', image_url: `data:image/png;base64,${png}` },
        ])
        const anthropic = yield* AnthropicResult.request({
          payload: {
            model: 'test',
            max_tokens: 1,
            messages: [
              {
                role: 'user',
                content: [
                  { type: 'tool_result', tool_use_id: 'call-1', content: JSON.stringify(observed) },
                ],
              },
            ],
          },
        })
        const message = anthropic.payload.messages[0]
        if (
          message === undefined ||
          typeof message.content === 'string' ||
          message.content[0]?.type !== 'tool_result'
        )
          throw new Error('Missing Anthropic result')
        assert.deepEqual(message.content[0].content, [
          { type: 'text', text: 'caption', cache_control: null },
          {
            type: 'image',
            cache_control: { type: 'ephemeral' },
            source: { type: 'base64', media_type: 'image/png', data: png },
          },
        ])
        assert.include(JSON.stringify(persisted?.data), png)
      }),
    ),
  )

  it.effect('a content redaction is authoritative while typed details remain durable', () =>
    run(
      Effect.gen(function* () {
        const toolkit = Toolkit.make(Tool.make('work', { success: Schema.String }))
        const hook = Hook.make({
          event: 'afterTool',
          execute: ({ result }) =>
            Schema.decodeEffect(ToolResult.ResultSchema)({
              ...result,
              content: [Prompt.textPart({ text: 'redacted' })],
            }),
        })
        const { observed, persisted } = yield* execute(toolkit, [hook]).pipe(
          Effect.provide(toolkit.toLayer({ work: () => Effect.succeed('original text') })),
        )
        assert.strictEqual(observed, 'redacted')
        assert.notInclude(JSON.stringify(observed), 'original text')
        const persistedResult = yield* Schema.decodeUnknownEffect(
          Schema.toCodecJson(ToolResult.ResultSchema),
        )(persisted?.data)
        assert.strictEqual(persistedResult.details, 'original text')
        assert.include(JSON.stringify(persisted?.data), 'redacted')
      }),
    ),
  )

  it.effect('explicit canonical envelope success exposes media to hooks and the model', () =>
    run(
      Effect.gen(function* () {
        const toolkit = Toolkit.make(Tool.make('work', { success: ToolResult.Envelope }))
        let hookContent: ToolResult.Content = []
        const hook = Hook.make({
          event: 'afterTool',
          execute: ({ result }) =>
            Effect.sync(() => {
              hookContent = result.content
            }),
        })
        const { observed } = yield* execute(toolkit, [hook]).pipe(
          Effect.provide(
            toolkit.toLayer({
              work: () =>
                Effect.succeed({ _tag: '@effect-harness/ToolContent' as const, content: [image] }),
            }),
          ),
        )
        assert.include(JSON.stringify(observed), png)
        assert.include(JSON.stringify(observed), '@effect-harness/ToolContent')
        assert.deepEqual(hookContent, [image])
        const envelope = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(ToolResult.Envelope))(
          observed,
        )
        assert.deepEqual(envelope.content, [image])
      }),
    ),
  )

  it.effect('ordinary string success remains directly visible to a custom native model', () =>
    run(
      Effect.gen(function* () {
        const toolkit = Toolkit.make(Tool.make('work', { success: Schema.String }))
        const { observed } = yield* execute(toolkit, []).pipe(
          Effect.provide(toolkit.toLayer({ work: () => Effect.succeed('HELLO') })),
        )
        assert.strictEqual(observed, 'HELLO')
      }),
    ),
  )

  it.effect('plain text hook parts preserve the newline convention', () =>
    run(
      Effect.gen(function* () {
        const toolkit = Toolkit.make(Tool.make('work', { success: Schema.String }))
        const hook = Hook.make({
          event: 'afterTool',
          execute: ({ result }) =>
            Effect.succeed({
              ...result,
              content: [Prompt.textPart({ text: 'first' }), Prompt.textPart({ text: 'second' })],
            }),
        })
        const { observed } = yield* execute(toolkit, [hook]).pipe(
          Effect.provide(toolkit.toLayer({ work: () => Effect.succeed('original') })),
        )
        assert.strictEqual(observed, 'first\nsecond')
      }),
    ),
  )

  it.effect('text provider options select the envelope without losing their metadata', () =>
    run(
      Effect.gen(function* () {
        const toolkit = Toolkit.make(Tool.make('work', { success: Schema.String }))
        const part = Prompt.textPart({
          text: 'cached',
          options: { anthropic: { cacheControl: { type: 'ephemeral' } } },
        })
        const hook = Hook.make({
          event: 'afterTool',
          execute: ({ result }) => Effect.succeed({ ...result, content: [part] }),
        })
        const { observed } = yield* execute(toolkit, [hook]).pipe(
          Effect.provide(toolkit.toLayer({ work: () => Effect.succeed('original') })),
        )
        const envelope = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(ToolResult.Envelope))(
          observed,
        )
        assert.deepEqual(envelope.content, [part])
        assert.deepEqual(yield* AnthropicResult.content(envelope.content), [
          { type: 'text', text: 'cached', cache_control: { type: 'ephemeral' } },
        ])
      }),
    ),
  )

  it.effect('explicit all-text envelopes retain their tagged result protocol', () =>
    run(
      Effect.gen(function* () {
        const toolkit = Toolkit.make(Tool.make('work', { success: ToolResult.Envelope }))
        const content = [Prompt.textPart({ text: 'explicit' })]
        const { observed } = yield* execute(toolkit, []).pipe(
          Effect.provide(
            toolkit.toLayer({
              work: () => Effect.succeed({ _tag: '@effect-harness/ToolContent' as const, content }),
            }),
          ),
        )
        const envelope = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(ToolResult.Envelope))(
          observed,
        )
        assert.deepEqual(envelope.content, content)
      }),
    ),
  )
})
