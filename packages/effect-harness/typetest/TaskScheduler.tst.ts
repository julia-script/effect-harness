import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import type * as Scope from 'effect/Scope'
import type * as Session from 'effect-harness/Session'
import * as Record from 'effect-harness/Record'
import * as Registry from 'effect-harness/Registry'
import type { RegistryError } from 'effect-harness/RegistryError'
import * as Task from 'effect-harness/Task'
import { TaskExecution } from 'effect-harness/TaskExecution'
import * as TaskScheduler from 'effect-harness/TaskScheduler'
import * as Toolkit from 'effect-harness/Toolkit'

class Backend extends Context.Service<Backend, number>()('types/TaskScheduler/Backend') {}
class Credentials extends Context.Service<Credentials, number>()(
  'types/TaskScheduler/Credentials',
) {}
class Decoder extends Context.Service<Decoder, number>()('types/TaskScheduler/Decoder') {}
class Encoder extends Context.Service<Encoder, number>()('types/TaskScheduler/Encoder') {}
class BuildError extends Schema.TaggedError<BuildError>()('TaskSchedulerBuildError', {}) {}
const number = Schema.String.pipe(
  Schema.decodeTo(Schema.Natural, {
    decode: SchemaGetter.transformEffect((value: string) => Decoder.pipe(Effect.as(Number(value)))),
    encode: SchemaGetter.transformEffect((value: number) => Encoder.pipe(Effect.as(String(value)))),
  }),
)
const contract = {
  kind: 'types.scheduler',
  version: 1,
  input: number,
  checkpoint: Schema.Struct({ phase: Schema.Literal('run') }),
  success: number,
  failure: Schema.String,
}
const definition = Task.make({
  ...contract,
  initial: () => ({ phase: 'run' as const }),
  phases: {
    run: () =>
      Effect.gen(function* () {
        const value = yield* Backend
        const execution = yield* TaskExecution
        yield* execution.commit((tx) =>
          execution.progress(tx, contract, {
            status: 'completing',
            outcome: { status: 'completed', result: value },
          }),
        )
      }),
  },
})
declare const backend: Layer.Layer<Backend, BuildError, Credentials>
declare const registry: Registry.Service
declare const session: Session.Session
declare const scheduler: TaskScheduler.Service
declare const ref: Task.Ref<typeof definition>
const registration = Registry.withTask(
  Registry.toolkit({ id: 'scheduler', version: 1 }, Toolkit.make()),
  definition.pipe(Task.provide(backend)),
)

test('compiled registration preserves acquisition requirements and errors', () => {
  expect(registration).type.toBe<
    Registry.Registration<Credentials | Decoder | Encoder, BuildError>
  >()
  expect(registry.install(registration)).type.toBe<
    Effect.Effect<
      import('effect-harness/RegistryScope').Snapshot,
      RegistryError | BuildError,
      Credentials | Decoder | Encoder
    >
  >()
  expect(Effect.runPromise(registry.install(registration))).type.toRaiseError()
  expect(Registry.provide(registration, Layer.succeed(Decoder, 1))).type.toBe<
    Registry.Registration<Credentials | Encoder, BuildError>
  >()
})

test('scheduler construction captures only its declared host services and requires a Scope', () => {
  const host: TaskScheduler.Host<Backend> = {
    open: () => Backend.pipe(Effect.as({})),
  }
  expect(TaskScheduler.make(session)).type.toBe<
    Effect.Effect<TaskScheduler.Service, TaskScheduler.Failure, Registry.Registry | Scope.Scope>
  >()
  expect(TaskScheduler.make(session, { host })).type.toBe<
    Effect.Effect<
      TaskScheduler.Service,
      TaskScheduler.Failure,
      Registry.Registry | Scope.Scope | Backend
    >
  >()
  expect(TaskScheduler.layer(session, { host })).type.toBe<
    Layer.Layer<TaskScheduler.TaskScheduler, TaskScheduler.Failure, Registry.Registry | Backend>
  >()
  expect(Effect.runPromise(TaskScheduler.make(session, { host }))).type.toRaiseError()
  expect(
    Effect.runPromise(
      TaskScheduler.make(session).pipe(Effect.provideService(Registry.Registry, registry)),
    ),
  ).type.toRaiseError()
})

test('start uses captured codecs, outcome keeps explicit decoder services and input type', () => {
  expect(scheduler.start(definition, 2, { conversationId: Record.ROOT_CONVERSATION_ID })).type.toBe<
    Effect.Effect<Task.Ref<typeof definition>, TaskScheduler.Failure>
  >()
  expect(scheduler.start).type.not.toBeCallableWith(definition, '2', {
    conversationId: Record.ROOT_CONVERSATION_ID,
  })
  expect(scheduler.wait(ref)).type.toBe<
    Effect.Effect<Task.Outcome<typeof definition>, TaskScheduler.Failure, Decoder>
  >()
  expect(Effect.runPromise(scheduler.wait(ref))).type.toRaiseError()
  expect(scheduler.resume).type.toBe<Effect.Effect<void, TaskScheduler.Failure>>()
})
