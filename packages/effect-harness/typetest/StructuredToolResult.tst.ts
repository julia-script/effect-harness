import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Extension from 'effect-harness/Extension'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Hook from 'effect-harness/Hook'
import type * as Storage from 'effect-harness/Storage'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as Tool from 'effect-harness/Tool'
import type * as ToolResult from 'effect-harness/ToolResult'
import * as Toolkit from 'effect-harness/Toolkit'

const tool = Tool.makeResult('lookup', {
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Struct({ id: Schema.Finite }),
  failure: Schema.Struct({ reason: Schema.String }),
})
const tools = Toolkit.make(tool)
test('independent output channels retain their success and failure types', () => {
  expect<Tool.Parameters<typeof tool>>().type.toBe<{ readonly query: string }>()
  expect<Tool.Success<typeof tool>>().type.toBe<ToolResult.Output<{ readonly id: number }>>()
  expect<Tool.Failure<typeof tool>>().type.toBe<ToolResult.Output<{ readonly reason: string }>>()
  expect(tools.toLayer).type.not.toBeCallableWith({
    lookup: () => Effect.succeed({ content: [], structuredOutput: { id: 'bad' } }),
  })
  expect(tools.toLayer).type.not.toBeCallableWith({
    lookup: () => Effect.fail({ content: [], structuredOutput: { reason: 1 } }),
  })
  expect(tools.toLayer).type.not.toBeCallableWith({
    lookup: () => Effect.succeed({ structuredOutput: { id: 1 } }),
  })
  expect(
    tools.toLayer({
      lookup: ({ query }) =>
        Effect.succeed({
          content: [Prompt.textPart({ text: query })],
          structuredOutput: { id: 1 },
          details: { panel: 'ui' },
        }),
    }),
  ).type.toBe<Layer.Layer<Toolkit.Handler<'lookup'>>>()
  expect(
    tools.toLayer({
      lookup: () => Effect.fail({ content: [], structuredOutput: { reason: 'unavailable' } }),
    }),
  ).type.toBe<Layer.Layer<Toolkit.Handler<'lookup'>>>()
  expect(tools.toLayer({ lookup: () => Effect.succeed({ content: [] }) })).type.toBe<
    Layer.Layer<Toolkit.Handler<'lookup'>>
  >()
})

test('no dedicated schema means optional JSON, and ordinary handlers remain value-style', () => {
  const fallback = Tool.makeResult('json')
  expect<Tool.Success<typeof fallback>>().type.toBe<ToolResult.Output<Schema.Json>>()
  expect<Tool.Failure<typeof fallback>>().type.toBe<ToolResult.Output<Schema.Json>>()
  const ordinary = Toolkit.make(Tool.make('value', { success: Schema.String }))
  expect(ordinary.toLayer).type.not.toBeCallableWith({
    value: () => Effect.succeed({ content: [] }),
  })
  expect(ordinary.toLayer({ value: () => Effect.succeed('text') })).type.toBe<
    Layer.Layer<Toolkit.Handler<'value'>>
  >()
  const combined = Toolkit.merge(tools, ordinary)
  expect(combined.tools.lookup).type.toBe<typeof tool>()
  expect(combined.toLayer).type.not.toBeCallableWith({
    lookup: () => Effect.succeed('text'),
    value: () => Effect.succeed('text'),
  })
})

class Codec extends Context.Service<Codec, string>()('typetest/StructuredToolResult/Codec') {}
class Client extends Context.Service<Client, string>()('typetest/StructuredToolResult/Client') {}
test('schema services, native dependencies and extension handler requirements remain captured', () => {
  const schema = Schema.DateFromString.pipe(
    Schema.middlewareEncoding((effect) => Effect.flatMap(Codec, () => effect)),
    Schema.middlewareDecoding((effect) => Effect.flatMap(Codec, () => effect)),
  )
  const clock = Tool.makeResult('clock', { success: schema }).addDependency(Client)
  const clocks = Toolkit.make(clock)
  const layer = clocks.toLayer({
    clock: () => Effect.succeed({ content: [], structuredOutput: new Date() }),
  })
  expect(layer).type.toBe<Layer.Layer<Toolkit.Handler<'clock'>, never, Codec | Client>>()
  expect(HarnessRuntime.layer({ tools: clocks })).type.toBe<
    Layer.Layer<
      HarnessRuntime.HarnessRuntime | import('effect-harness/HarnessBackend').HarnessBackend,
      import('effect-harness/Harness').HarnessError,
      Storage.Storage | LanguageModel.LanguageModel | Toolkit.Handler<'clock'> | Codec | Client
    >
  >()
  const extension = Extension.make({ name: 'clock', tools: clocks }).pipe(Extension.provide(layer))
  expect<Extension.Requirements<typeof extension>>().type.toBe<Codec | Client>()
})

test('hooks expose the durable JSON channel independently of visible content and UI details', () => {
  Hook.make({
    event: 'afterTool',
    execute: ({ result }) => {
      expect(result.structuredOutput).type.toBe<Schema.Json | undefined>()
      expect(result.details).type.toBe<Schema.Json | undefined>()
      expect(result.content).type.toBe<ToolResult.Content>()
      return Effect.succeed({ ...result, content: [] })
    },
  })
})
