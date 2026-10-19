import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Tool from 'effect-harness/Tool'
import { ToolExecution } from 'effect-harness/ToolExecution'
import * as Toolkit from 'effect-harness/Toolkit'

export const tools = Toolkit.make(
  Tool.make('uppercase', {
    description: 'Convert text to uppercase',
    parameters: Schema.Struct({ text: Schema.String }),
    success: Schema.String,
    replay: 'safe',
  }),
)

export const layer = tools.toLayer({
  uppercase: Effect.fn('uppercase')(function* ({ text }) {
    const execution = yield* ToolExecution
    yield* execution.output('Converting text\n')
    return text.toUpperCase()
  }),
})
