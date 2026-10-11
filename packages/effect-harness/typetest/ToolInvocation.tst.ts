import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import * as Tool from 'effect-harness/Tool'
import * as ToolInvocation from 'effect-harness/ToolInvocation'
import type * as TaskExecution from 'effect-harness/TaskExecution'
import type * as TaskScheduler from 'effect-harness/TaskScheduler'
import type * as RegistryScope from 'effect-harness/RegistryScope'

class Parameters extends Context.Service<Parameters, string>()('types/nested/Parameters') {}
class Success extends Context.Service<Success, string>()('types/nested/Success') {}
class Failure extends Context.Service<Failure, string>()('types/nested/Failure') {}
class Use extends Context.Service<Use, string>()('types/nested/Use') {}
class Denied extends Context.Service<Denied, string>()('types/nested/Denied') {}
class BuildError extends Schema.TaggedError<BuildError>()('NestedBuildError', {}) {}
const tool = Tool.makeResult('typed', {
  parameters: Schema.Struct({
    value: Schema.FiniteFromString.pipe(
      Schema.middlewareEncoding((effect) => Effect.flatMap(Parameters, () => effect)),
    ),
  }),
  success: Schema.DateFromString.pipe(
    Schema.middlewareDecoding((effect) => Effect.flatMap(Success, () => effect)),
  ),
  failure: Schema.String.pipe(
    Schema.middlewareDecoding((effect) => Effect.flatMap(Failure, () => effect)),
  ),
  resultValidation: 'strict',
})
declare const access: ToolInvocation.Access
declare const native: TaskExecution.Access
declare const handle: ToolInvocation.Handle
declare const services: Layer.Layer<Parameters | Success, BuildError, Use>
declare const factory: NonNullable<TaskScheduler.Options<Use>['invocation']>
declare const lease: RegistryScope.Lease

test('concrete public parameter encoding and success/failure decoding services remain in R', () => {
  const result = ToolInvocation.invoke(native, tool, { value: 42 })
  expect(result).type.toBe<
    Effect.Effect<
      ToolInvocation.DecodedReceipt<typeof tool>,
      ToolInvocation.Failure,
      Parameters | Success | Failure
    >
  >()
  expect(result).type.not.toBeAssignableTo<
    Effect.Effect<ToolInvocation.DecodedReceipt<typeof tool>, ToolInvocation.Failure>
  >()
  expect(result.pipe(Effect.provideService(Parameters, 'parameter'))).type.toBe<
    Effect.Effect<
      ToolInvocation.DecodedReceipt<typeof tool>,
      ToolInvocation.Failure,
      Success | Failure
    >
  >()
  expect(result.pipe(Effect.provide(services))).type.toBe<
    Effect.Effect<
      ToolInvocation.DecodedReceipt<typeof tool>,
      ToolInvocation.Failure | BuildError,
      Use | Failure
    >
  >()
  expect(ToolInvocation.decode(native, tool, handle)).type.toBe<
    Effect.Effect<
      ToolInvocation.DecodedReceipt<typeof tool>,
      ToolInvocation.Failure,
      Success | Failure
    >
  >()
  expect(ToolInvocation.invoke<typeof tool>).type.not.toBeCallableWith(native, tool, {
    value: 'wire',
  })
})

test('use and denial callbacks preserve their own services and typed failures', () => {
  const result = access.withTool(
    tool,
    () => Effect.flatMap(Use, () => Effect.fail('use' as const)),
    {
      options: { key: 'stable', binding: { id: 'owner', version: 2 } },
      onDenied: () => Effect.flatMap(Denied, () => Effect.fail('denied' as const)),
    },
  )
  expect(result).type.toBe<
    Effect.Effect<never, ToolInvocation.Failure | 'use' | 'denied', Use | Denied>
  >()
  expect(result).type.not.toBeAssignableTo<Effect.Effect<never, ToolInvocation.Failure>>()
  expect(
    access.withTool(tool, () => Effect.as(Use, 1 as const), {
      onDenied: () => Effect.as(Denied, 'denied' as const),
    }),
  ).type.toBe<Effect.Effect<1 | 'denied', ToolInvocation.Failure, Use | Denied>>()
  expect(access.withTool(tool, () => Effect.as(Use, 1 as const))).type.toBe<
    Effect.Effect<1, ToolInvocation.Failure, Use>
  >()
})

test('strict, optional, null and ordinary value channels stay distinct from unavailable receipts', () => {
  const optional = Tool.makeResult('optional', { success: Schema.Finite })
  const nullable = Tool.makeResult('null', { success: Schema.Null, resultValidation: 'strict' })
  const ordinary = Tool.make('ordinary', { success: Schema.Finite, failure: Schema.String })
  type Strict = Extract<
    ToolInvocation.DecodedReceipt<typeof tool>,
    { valueKind: 'decoded'; status: 'succeeded' }
  >
  type Optional = Extract<
    ToolInvocation.DecodedReceipt<typeof optional>,
    { valueKind: 'decoded'; status: 'succeeded' }
  >
  type Null = Extract<
    ToolInvocation.DecodedReceipt<typeof nullable>,
    { valueKind: 'decoded'; status: 'succeeded' }
  >
  type Ordinary = Extract<
    ToolInvocation.DecodedReceipt<typeof ordinary>,
    { valueKind: 'decoded'; status: 'succeeded' }
  >
  type Error = Extract<
    ToolInvocation.DecodedReceipt<typeof ordinary>,
    { valueKind: 'decoded'; status: 'failed' }
  >
  expect<Strict['value']>().type.toBe<Date>()
  expect<Optional['value']>().type.toBe<number | undefined>()
  expect<Null['value']>().type.toBe<null>()
  expect<Ordinary['value']>().type.toBe<number>()
  expect<Error['error']>().type.toBe<string>()
})

test('native factory requires the actual access and fresh Scope and retains captured host services', () => {
  expect(factory.open(native, lease, 'run')).type.toBe<
    Effect.Effect<ToolInvocation.Access, ToolInvocation.Failure, Use | Scope.Scope>
  >()
  expect(access.invokeTool('dynamic', {})).type.toBe<
    Effect.Effect<ToolInvocation.WireReceipt, ToolInvocation.Failure>
  >()
})
