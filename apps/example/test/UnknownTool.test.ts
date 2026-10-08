import { assert, describe, it } from '@effect/vitest'
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

const prompt = Prompt.make('Check strict tool validation')
const declaredName: string = 'declared'
const toolkit = Toolkit.make(
  Tool.make(declaredName, { parameters: Schema.Unknown, success: Schema.String }),
)

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
  }
})
