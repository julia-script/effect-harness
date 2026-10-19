import { Effect, Layer, Schema } from 'effect'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as Harness from 'effect-harness/Harness'
import * as Storage from 'effect-harness/Storage'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import { ToolExecution } from 'effect-harness/ToolExecution'

export const tools = Toolkit.make(
  Tool.make('uppercase', {
    description: 'Convert text to uppercase',
    parameters: Schema.Struct({ text: Schema.String }),
    success: Schema.String,
    replay: 'safe',
  }),
)
export const ToolsLive = tools.toLayer({
  uppercase: Effect.fn('uppercase')(function* ({ text }) {
    const execution = yield* ToolExecution
    yield* execution.output('Converting text\n')
    return text.toUpperCase()
  }),
})
export const withModel = <E, R>(model: Layer.Layer<LanguageModel.LanguageModel, E, R>) =>
  Harness.layerLocal({ tools }).pipe(
    Layer.provide(ToolsLive),
    Layer.provide(model),
    Layer.provide(Storage.layerMemory),
  )
