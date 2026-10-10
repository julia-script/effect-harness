import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import type * as Scope from 'effect/Scope'
import type * as Stream from 'effect/Stream'
import * as Extension from 'effect-harness/Extension'
import * as Registry from 'effect-harness/Registry'
import type { RegistryError } from 'effect-harness/RegistryError'
import type * as RegistryScope from 'effect-harness/RegistryScope'
import * as Task from 'effect-harness/Task'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import type { Failure } from 'effect-harness/ExecutionError'
import type * as ToolResult from 'effect-harness/ToolResult'
import type { ToolExecution } from 'effect-harness/ToolExecution'

class Backend extends Context.Service<Backend, string>()('typetest/registry/Backend') {}
class Credentials extends Context.Service<Credentials, string>()('typetest/registry/Credentials') {}
class Unrelated extends Context.Service<Unrelated, string>()('typetest/registry/Unrelated') {}
class Decoder extends Context.Service<Decoder, string>()('typetest/registry/Decoder') {}
class Encoder extends Context.Service<Encoder, string>()('typetest/registry/Encoder') {}
class BuildError extends Schema.TaggedError<BuildError>()('RegistryBuildError', {}) {}
declare const backend: Layer.Layer<Backend, BuildError, Credentials>
declare const service: Registry.Service
declare const lease: RegistryScope.Lease

const tool = Tool.make('read', { success: Schema.String }).addDependency(Backend)
const toolkit = Toolkit.make(tool)
const handlers = toolkit.toLayer({ read: () => Backend })
const definition = { id: 'files', version: 1 }
const unprovided = Registry.toolkit(definition, toolkit)
const bound = Registry.provide(unprovided, handlers.pipe(Layer.provideMerge(backend)))

test('toolkit capture preserves missing handlers, backend requirements and Layer build errors', () => {
  expect(unprovided).type.toBe<Registry.Registration<Toolkit.Handler<'read'> | Backend>>()
  expect(bound).type.toBe<Registry.Registration<Credentials, BuildError>>()
  expect(service.install(bound)).type.toBe<
    Effect.Effect<RegistryScope.Snapshot, BuildError | RegistryError, Credentials>
  >()
  expect(unprovided.acquire).type.toBeAssignableTo<
    Effect.Effect<unknown, never, Toolkit.Handler<'read'> | Backend | Scope.Scope>
  >()
  expect(service.install(unprovided)).type.not.toBeAssignableTo<
    Effect.Effect<RegistryScope.Snapshot, RegistryError>
  >()
  expect(Registry.provide(unprovided, Layer.succeed(Unrelated, 'unrelated'))).type.toBe<
    Registry.Registration<Toolkit.Handler<'read'> | Backend>
  >()
  expect(Effect.provideService(service.install(bound), Unrelated, 'x')).type.not.toBeAssignableTo<
    Effect.Effect<RegistryScope.Snapshot, RegistryError | BuildError>
  >()
})

test('extension construction channels remain visible at install', () => {
  const extension = Extension.make({ name: 'extension', tools: toolkit }).pipe(
    Extension.provide(handlers.pipe(Layer.provideMerge(backend))),
  )
  const registration = Registry.extension(definition, extension)
  expect(registration).type.toBe<Registry.Registration<Credentials, BuildError>>()
  expect(service.install(registration)).type.toBe<
    Effect.Effect<RegistryScope.Snapshot, RegistryError | BuildError, Credentials>
  >()
})

test('codec services must be satisfied independently of handler acquisition', () => {
  const text = Schema.String.pipe(
    Schema.decodeTo(Schema.String, {
      decode: SchemaGetter.transformEffect((value: string) => Decoder.pipe(Effect.as(value))),
      encode: SchemaGetter.transformEffect((value: string) => Encoder.pipe(Effect.as(value))),
    }),
  )
  const tool = Tool.make('coded', { parameters: Schema.Struct({ text }), success: text })
  const toolkit = Toolkit.make(tool)
  const registration = Registry.toolkit(definition, toolkit)
  expect(registration).type.toBe<
    Registry.Registration<Toolkit.Handler<'coded'> | Decoder | Encoder>
  >()
  const handler = toolkit.toLayer({ coded: ({ text }) => Effect.succeed(text) })
  const withoutHandler = Registry.provide(registration, handler)
  expect(withoutHandler).type.toBe<Registry.Registration<Decoder | Encoder>>()
  expect(Registry.provide(withoutHandler, Layer.succeed(Decoder, 'decoder'))).type.toBe<
    Registry.Registration<Encoder>
  >()
  expect(service.install(withoutHandler)).type.not.toBeAssignableTo<
    Effect.Effect<RegistryScope.Snapshot, RegistryError>
  >()
})

const task = Task.make({
  kind: 'custom',
  version: 1,
  input: Schema.String,
  checkpoint: Schema.Struct({ phase: Schema.Literal('ready') }),
  success: Schema.String,
  failure: Schema.String,
  initial: () => ({ phase: 'ready' as const }),
  phases: { ready: () => Backend.pipe(Effect.asVoid) },
})
const tasks = Registry.withTask(Registry.toolkit(definition, Toolkit.make()), task)
test('task acquisition retains construction requirements, and inspection needs no codec execution', () => {
  expect(tasks).type.toBe<Registry.Registration<Backend>>()
  expect(Registry.provide(tasks, backend)).type.toBe<
    Registry.Registration<Credentials, BuildError>
  >()
  const providedTask = task.pipe(Task.provide(backend))
  expect(Registry.withTask(Registry.toolkit(definition, Toolkit.make()), providedTask)).type.toBe<
    Registry.Registration<Credentials, BuildError>
  >()
  expect(service.snapshot).type.toBe<Effect.Effect<RegistryScope.Snapshot>>()
  expect(service.changes).type.toBe<Stream.Stream<RegistryScope.Snapshot>>()
})

test('leases require an invocation Scope and keep invocation access explicit', () => {
  expect(Registry.make).type.toBe<Effect.Effect<Registry.Service, never, Scope.Scope>>()
  expect(Registry.layer).type.toBe<Layer.Layer<Registry.Registry>>()
  expect(service.acquire).type.toBe<
    Effect.Effect<RegistryScope.Lease, RegistryError, Scope.Scope>
  >()
  expect(lease.invokeTool({ id: 'call', name: 'read', arguments: {} })).type.toBe<
    Effect.Effect<ToolResult.Result, RegistryError | Failure, ToolExecution>
  >()
  expect(lease.resolveTool('read', { binding: 'files', version: 1 })).type.toBe<
    Effect.Effect<RegistryScope.BoundTool, RegistryError>
  >()
  expect(lease.resolveTool).type.not.toBeCallableWith('read', { version: '1' })
  expect(service.listen(() => Credentials.pipe(Effect.asVoid))).type.toBe<
    Effect.Effect<void, never, Credentials | Scope.Scope>
  >()
})
