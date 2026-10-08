import { Invocation, Registry, ToolRegistration, ToolError } from 'effect-harness'
import { Effect, Layer, Schema } from 'effect'
import { Tool, Toolkit } from 'effect/ai'

export const Uppercase = Tool.make('uppercase', {
  description: 'Convert text to uppercase without external side effects.',
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.String,
  failure: ToolError.ToolError,
}).addDependency(Invocation.ToolCall)

export const toolkit = Toolkit.make(Uppercase)

export const handle = Effect.fn('Uppercase.handle')(function* ({
  text,
}: typeof Uppercase.parametersSchema.Type) {
  const call = yield* Invocation.ToolCall
  const result = text.toUpperCase()
  yield* call.output(result)
  return result
})

export const layerHandlers: Layer.Layer<Tool.Handler<'uppercase'>> = toolkit.toLayer({
  uppercase: handle,
})

export const layerRegistry: Layer.Layer<
  Registry.Registry,
  Registry.RegistryError,
  Tool.Handler<'uppercase'>
> = Layer.unwrap(
  Effect.gen(function* () {
    // Pure uppercasing is safe to repeat if recovery occurs before its result is committed.
    const tools = yield* ToolRegistration.bind(toolkit, { uppercase: { replay: 'safe' } })
    return Registry.layer([{ name: 'example', tools }])
  }),
)
