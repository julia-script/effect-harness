import { assert, describe, it } from '@effect/vitest'
import * as Arr from 'effect/Array'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as Response from 'effect/ai/Response'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import { finish } from '../dist/DemoModel.js'

const call: Response.ToolCallPartEncoded = {
  type: 'tool-call',
  id: 'unknown-call',
  name: 'unregistered',
  params: { text: 'preserved' },
  providerExecuted: false,
}

const model = LanguageModel.make({
  generateText: () => Effect.succeed([call, finish('tool-calls')]),
  streamText: () => Stream.fromIterable([call, finish('tool-calls')]),
})

const prompt = Prompt.make('Check caller-owned tool settlement')
// This probe admits dynamic names and unknown payloads; the application's
// Uppercase tool keeps its precise parameter and result schemas.
const declaredName: string = 'declared'
const toolkit = Toolkit.make(
  Tool.make(declaredName, { parameters: Schema.Unknown, success: Schema.String }),
)
const handlers = toolkit.toLayer({ declared: () => Effect.succeed('unused') })

describe('native unknown-tool boundary', () => {
  for (const stream of [false, true]) {
    it.effect(`rejects unknown calls by default (${stream ? 'stream' : 'generate'})`, () =>
      Effect.gen(function* () {
        const native = yield* model
        const options = { prompt, toolkit, disableToolCallResolution: true } as const
        const response = stream
          ? Stream.runDrain(native.streamText(options))
          : Effect.asVoid(native.generateText(options))
        assert.isTrue(yield* Effect.isFailure(response))
      }),
    )

    it.effect(`requires caller-owned settlement (${stream ? 'stream' : 'generate'})`, () =>
      Effect.gen(function* () {
        const native = yield* model
        const options = { prompt, toolkit, allowUnknownToolCalls: true } as const
        const response = stream
          ? Stream.runDrain(native.streamText(options))
          : Effect.asVoid(native.generateText(options))
        assert.isTrue(yield* Effect.isFailure(response.pipe(Effect.provide(handlers))))
      }),
    )
  }

  it.effect('preserves unknown names and parameters when both flags are enabled', () =>
    Effect.gen(function* () {
      const native = yield* model
      const options = {
        prompt,
        toolkit,
        disableToolCallResolution: true,
        allowUnknownToolCalls: true,
      } as const
      const generated = yield* native.generateText(options)
      const streamed = yield* Stream.runCollect(native.streamText(options))
      const schema = Response.ToolCallPart('unregistered', Schema.Struct({ text: Schema.String }))
      const generatedCall = yield* Effect.fromOption(Arr.head(generated.toolCalls))
      const streamedPart = yield* Effect.fromOption(Arr.findFirst(streamed, Schema.is(schema)))
      const decoded = yield* Schema.decodeUnknownEffect(schema)(generatedCall)
      const streamedCall = yield* Schema.decodeEffect(schema)(streamedPart)
      assert.strictEqual(decoded.params.text, 'preserved')
      assert.strictEqual(streamedCall.params.text, 'preserved')
    }),
  )
})
