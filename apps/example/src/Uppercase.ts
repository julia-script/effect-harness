import { ToolCall } from '@effect-harness/harness/Invocation'
import * as Registry from '@effect-harness/harness/Registry'
import { bind } from '@effect-harness/harness/Tool'
import { ToolError } from '@effect-harness/harness/ToolError'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'

export const Uppercase = Tool.make('uppercase', {
  description: 'Convert text to uppercase without external side effects.',
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.String,
  failure: ToolError,
}).addDependency(ToolCall)

export const toolkit = Toolkit.make(Uppercase)

export const handle = Effect.fn('Uppercase.handle')(function* ({
  text,
}: typeof Uppercase.parametersSchema.Type) {
  const call = yield* ToolCall
  const result = text.toUpperCase()
  yield* call.output(result)
  return result
})

export const layerHandlers = toolkit.toLayer({ uppercase: handle })

export const layerRegistry = Layer.unwrap(
  Effect.gen(function* () {
    // Pure uppercasing is safe to repeat if recovery occurs before its receipt.
    const tools = yield* bind(toolkit, { uppercase: { replay: 'safe' } })
    return Registry.layer([{ name: 'example', tools }])
  }),
)
