import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Result from 'effect/Result'
import * as Layer from 'effect/Layer'
import type * as Scope from 'effect/Scope'
import * as Schema from 'effect/Schema'
import * as Tool from 'effect-harness/Tool'
import type * as ToolResult from 'effect-harness/ToolResult'
import * as Toolkit from 'effect-harness/Toolkit'
import * as ToolInventory from 'effect-harness/ToolInventory'
import * as ToolControl from 'effect-harness/ToolControl'
import * as Registry from 'effect-harness/Registry'
import * as ToolWrapper from 'effect-harness/ToolWrapper'
import type * as RegistryScope from 'effect-harness/RegistryScope'
import type { ToolExecution } from 'effect-harness/ToolExecution'
import type { Failure } from 'effect-harness/ExecutionError'
import type { RegistryError } from 'effect-harness/RegistryError'
import type * as Agent from 'effect-harness/Agent'

class Codec extends Context.Service<Codec, string>()('typetest/caller-controls/Codec') {}
class Client extends Context.Service<Client, string>()('typetest/caller-controls/Client') {}
class BuildError extends Schema.TaggedError<BuildError>()('CallerControlBuildError', {}) {}

const strict = Tool.makeResult('strict', {
  success: Schema.DateFromString.pipe(
    Schema.middlewareEncoding((effect) => Effect.flatMap(Codec, () => effect)),
  ),
  resultValidation: 'strict',
  callers: ['tools'],
}).addDependency(Client)
const toolkit = Toolkit.make(strict)

test('typed transformed wrapper captures outer codecs before registry erasure', () => {
  const outer = Tool.makeResult('strict', {
    parameters: Schema.Struct({ value: Schema.String }).pipe(
      Schema.middlewareDecoding((effect) => Effect.flatMap(Client, () => effect)),
    ),
    success: Schema.DateFromString.pipe(
      Schema.middlewareEncoding((effect) => Effect.flatMap(Codec, () => effect)),
    ),
    resultValidation: 'strict',
  })
  expect(Toolkit.compileCodec(outer)).type.toBe<
    Effect.Effect<Toolkit.BoundCodec, never, Codec | Client>
  >()
  const wrapper = ToolWrapper.bind(outer, (item) =>
    Effect.succeed({ ...item, invoke: item.invoke }),
  )
  expect(wrapper).type.toBe<ToolWrapper.Wrapper<Codec | Client>>()
  const registration = Registry.toolWrapper({ id: 'wrapper', version: 1 }, wrapper)
  expect(registration).type.toBe<Registry.Registration<Codec | Client>>()
  expect(registration.acquire).type.not.toBeAssignableTo<
    Effect.Effect<unknown, never, Scope.Scope>
  >()
  const partial = ToolWrapper.provide(wrapper, Layer.succeed(Codec, 'outer'))
  expect(partial).type.toBe<ToolWrapper.Wrapper<Client>>()
  const bound = ToolWrapper.provide(partial, Layer.succeed(Client, 'outer'))
  expect(bound).type.toBe<ToolWrapper.Wrapper>()
})

test('strict success is required, errors may omit, codecs and request services remain visible', () => {
  expect<Tool.Success<typeof strict>>().type.toBe<ToolResult.StrictOutput<Date>>()
  expect<Tool.Failure<typeof strict>>().type.toBe<ToolResult.Output<Schema.Json>>()
  expect(toolkit.toLayer).type.not.toBeCallableWith({
    strict: () => Effect.succeed({ content: [] }),
  })
  const layer = toolkit.toLayer({
    strict: () =>
      Effect.succeed({ content: [], structuredOutput: new Date(), control: { terminate: true } }),
  })
  expect(layer).type.toBe<Layer.Layer<Toolkit.Handler<'strict'>, never, Codec | Client>>()
  expect(toolkit.toLayer({ strict: () => Effect.fail({ content: [] }) })).type.toBe<
    Layer.Layer<Toolkit.Handler<'strict'>, never, Codec | Client>
  >()
  expect(
    Toolkit.validateResult(strict, {
      content: [],
      isError: false,
      structuredOutput: '2026-01-01',
      diagnostics: [],
    }),
  ).type.toBe<
    Effect.Effect<ToolResult.Result, Schema.SchemaError | ToolControl.InvalidControl, Codec>
  >()
  const build = Effect.gen(function* () {
    yield* Client
    return yield* new BuildError()
  })
  expect(toolkit.toLayer(build)).type.toBe<
    Layer.Layer<Toolkit.Handler<'strict'>, BuildError, Client | Codec>
  >()
  expect(Registry.toolkit({ id: 'owner', version: 1 }, toolkit)).type.toBe<
    Registry.Registration<Toolkit.Handler<'strict'> | Codec | Client>
  >()
})

test('optional default and legacy value tools retain their public types', () => {
  const optional = Tool.makeResult('optional', { success: Schema.String })
  expect<Tool.Success<typeof optional>>().type.toBe<ToolResult.Output<string>>()
  expect(
    Toolkit.make(optional).toLayer({ optional: () => Effect.succeed({ content: [] }) }),
  ).type.toBe<Layer.Layer<Toolkit.Handler<'optional'>>>()
  const ordinary = Tool.make('ordinary', { success: Schema.String })
  expect<Tool.Success<typeof ordinary>>().type.toBe<string>()
  expect(Tool.make).type.not.toBeCallableWith('invalid', { callers: ['human'] })
  expect(ToolControl.reduceRound([], [], [])).type.toBe<
    Result.Result<ToolControl.Barrier, ToolControl.InvalidControl>
  >()
  expect<ToolControl.Control>().type.toBe<{
    readonly terminate?: true
    readonly handoff?: string
    readonly addTools?: ReadonlyArray<string>
  }>()
})

test('public admission exposes failures and only the fresh invocation service', () => {
  const check = (lease: RegistryScope.Lease) => {
    expect(ToolInventory.admit(lease, 'strict', 'tools')).type.toBe<
      Effect.Effect<RegistryScope.BoundTool, ToolInventory.Denial | RegistryError>
    >()
    expect(
      ToolInventory.invoke(lease, { id: 'one', name: 'strict', arguments: {} }, 'model'),
    ).type.toBe<
      Effect.Effect<
        ToolResult.Result,
        | ToolInventory.Denial
        | RegistryError
        | Failure
        | Schema.SchemaError
        | ToolControl.InvalidControl,
        ToolExecution
      >
    >()
    expect(
      ToolInventory.invokeNested(lease, { id: 'one', name: 'strict', arguments: {} }),
    ).type.toBe<
      Effect.Effect<
        ToolResult.Nested,
        | ToolInventory.Denial
        | RegistryError
        | Failure
        | Schema.SchemaError
        | ToolControl.InvalidControl,
        ToolExecution
      >
    >()
  }
  expect(check).type.toBe<(lease: RegistryScope.Lease) => void>()
  expect<ToolResult.Nested>().type.not.toHaveProperty('control')
  expect<ToolResult.Nested>().type.not.toHaveProperty('content')
  expect<Agent.State>().type.toHaveProperty('modelTools')
  expect<Agent.State>().type.toHaveProperty('toolkits')
  expect<Agent.Change['modelTools']>().type.toBe<Agent.ModelTools | null | undefined>()
})
