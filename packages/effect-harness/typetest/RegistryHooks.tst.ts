import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import type * as Scope from 'effect/Scope'
import * as Hook from 'effect-harness/Hook'
import type * as HookReducer from 'effect-harness/HookReducer'
import type { HookExecution } from 'effect-harness/HookExecution'
import * as PromptSectionWrapper from 'effect-harness/PromptSectionWrapper'
import * as Registry from 'effect-harness/Registry'
import type * as RegistryScope from 'effect-harness/RegistryScope'
import type { RegistryError } from 'effect-harness/RegistryError'
import * as Task from 'effect-harness/Task'
import * as TaskHook from 'effect-harness/TaskHook'
import * as Tool from 'effect-harness/Tool'
import { ToolExecution } from 'effect-harness/ToolExecution'
import * as ToolWrapper from 'effect-harness/ToolWrapper'

class Backend extends Context.Service<Backend, string>()('types/RegistryHooks/Backend') {}
class Host extends Context.Service<Host, string>()('types/RegistryHooks/Host') {}
class Decoder extends Context.Service<Decoder, number>()('types/RegistryHooks/Decoder') {}
class BuildError extends Schema.TaggedError<BuildError>()('RegistryHooksBuildError', {}) {}
const backend = Layer.effect(Backend, Host.pipe(Effect.andThen(Effect.fail(new BuildError()))))
const wrapper = ToolWrapper.make({
  name: 'wrapped',
  wrap: (item) =>
    Effect.succeed({
      ...item,
      invoke: (call) =>
        Effect.gen(function* () {
          yield* Backend
          yield* ToolExecution
          yield* Effect.addFinalizer(() => Effect.void)
          return yield* item.invoke(call)
        }),
    }),
})
const codec = Schema.Finite.pipe(
  Schema.decodeTo(Schema.Finite, {
    decode: SchemaGetter.transformEffect((value: number) => Decoder.pipe(Effect.as(value))),
    encode: SchemaGetter.passthrough(),
  }),
)
const outer = Tool.make('wrapped', { parameters: Schema.Struct({ count: codec }), success: codec })
const bound = ToolWrapper.bind(outer, (item) => Effect.succeed(item))
const section = PromptSectionWrapper.make({
  key: 'context',
  wrap: (item) =>
    Effect.succeed({
      ...item,
      render: (prompt) => Backend.pipe(Effect.andThen(item.render(prompt))),
    }),
})
const hook = Hook.make({ event: 'beforeRequest', execute: () => Backend.pipe(Effect.asVoid) }).pipe(
  Hook.provide(backend),
)
const task = Task.make({
  kind: 'types.task-hooks',
  version: 1,
  input: codec,
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal('ready'), count: Schema.Finite }),
    Schema.Struct({ phase: Schema.Literal('done'), message: Schema.String }),
  ]),
  success: Schema.String,
  failure: Schema.String,
  initial: () => ({ phase: 'ready' as const, count: 1 }),
  phases: { ready: () => Effect.void, done: () => Effect.void },
})
const phase = TaskHook.make(task, {
  phase: 'ready',
  stage: 'beforePhase',
  execute: (event) => {
    expect(event.input).type.toBe<number>()
    expect(event.checkpoint.count).type.toBe<number>()
    expect(event.checkpoint.message).type.toRaiseError()
    return Backend.pipe(Effect.asVoid)
  },
})
declare const registry: Registry.Service
declare const lease: RegistryScope.Lease
declare const event: TaskHook.Event

test('wrapper acquisition exposes services and Layer failures without invocation access', () => {
  expect(wrapper).type.toBe<ToolWrapper.Wrapper<Backend>>()
  expect(bound).type.toBe<ToolWrapper.Wrapper<Decoder>>()
  expect(Registry.toolWrapper({ id: 'outer', version: 1 }, bound)).type.toBe<
    Registry.Registration<Decoder>
  >()
  expect(ToolWrapper.provide(wrapper, backend)).type.toBe<ToolWrapper.Wrapper<Host, BuildError>>()
  expect(section).type.toBe<PromptSectionWrapper.Wrapper<Backend>>()
  expect(Registry.hook({ id: 'hook', version: 1 }, hook)).type.toBe<
    Registry.Registration<Host, BuildError>
  >()
  expect(
    Effect.runPromise(registry.install(Registry.toolWrapper({ id: 'missing', version: 1 }, bound))),
  ).type.toRaiseError()
})

test('task observers narrow phase and retain concrete decoder and callback requirements', () => {
  expect(phase).type.toBe<TaskHook.Registration<Backend | Decoder>>()
  expect(TaskHook.provide(phase, backend)).type.toBe<
    TaskHook.Registration<Host | Decoder, BuildError>
  >()
  expect(
    TaskHook.make(task, { phase: 'missing', stage: 'beforePhase', execute: () => Effect.void }),
  ).type.toRaiseError()
  expect(lease.dispatchTaskHook(event)).type.toBe<
    Effect.Effect<void, RegistryError | HookReducer.HookFailure, HookExecution>
  >()
  expect(
    lease.dispatchTaskHook(event, {
      report: () => Host.pipe(Effect.andThen(Effect.fail(new BuildError()))),
    }),
  ).type.toBe<
    Effect.Effect<void, RegistryError | HookReducer.HookFailure | BuildError, HookExecution | Host>
  >()
  expect(Effect.scoped(phase.acquire)).type.toBe<
    Effect.Effect<TaskHook.Compiled, never, Backend | Decoder>
  >()
  expect(phase.acquire).type.toBe<
    Effect.Effect<TaskHook.Compiled, never, Backend | Decoder | Scope.Scope>
  >()
})
