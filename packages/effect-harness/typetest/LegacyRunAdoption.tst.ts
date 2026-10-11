import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import type { HarnessError } from 'effect-harness/HarnessError'
import * as Registry from 'effect-harness/Registry'
import type { RegistryError } from 'effect-harness/RegistryError'
import type * as RegistryScope from 'effect-harness/RegistryScope'
import type * as Session from 'effect-harness/Session'
import type { Storage } from 'effect-harness/Storage'
import type { StorageFormat } from 'effect-harness/StorageFormat'
import * as Task from 'effect-harness/Task'
import * as TaskScheduler from 'effect-harness/TaskScheduler'
import * as Toolkit from 'effect-harness/Toolkit'

class Owner extends Context.Service<Owner, string>()('types/LegacyRunAdoption/Owner') {}
class Credentials extends Context.Service<Credentials, string>()(
  'types/LegacyRunAdoption/Credentials',
) {}
class AcquisitionError extends Schema.TaggedError<AcquisitionError>()(
  'LegacyAcquisitionError',
  {},
) {}
declare const ownedLayer: Layer.Layer<Owner, AcquisitionError, Credentials>
declare const runtime: HarnessRuntime.HarnessRuntimeService
declare const session: Session.Session
declare const adapter: TaskScheduler.NativeRunAdapter<Owner>
const definition = Task.make({
  kind: 'types.adoption',
  version: 1,
  input: Schema.String,
  checkpoint: Schema.Struct({ phase: Schema.Literal('run') }),
  success: Schema.String,
  failure: Schema.String,
  initial: () => ({ phase: 'run' as const }),
  phases: { run: () => Owner.pipe(Effect.asVoid) },
})
const registration = Registry.withTask(
  Registry.toolkit({ id: 'custom', version: 1 }, Toolkit.make()),
  definition.pipe(Task.provide(ownedLayer)),
)

test('runtime construction remains compatible; actual supplied registry requires no ambient replacement', () => {
  expect(HarnessRuntime.make()).type.toBe<
    Effect.Effect<
      HarnessRuntime.HarnessRuntimeService,
      HarnessError,
      Scope.Scope | Storage | StorageFormat | LanguageModel.LanguageModel
    >
  >()
  expect(HarnessRuntime.make({ registry: runtime.registry })).type.toBe<
    Effect.Effect<
      HarnessRuntime.HarnessRuntimeService,
      HarnessError,
      Scope.Scope | Storage | StorageFormat | LanguageModel.LanguageModel
    >
  >()
  expect(runtime.registry).type.toBe<Registry.Service>()
  expect(runtime.scheduler).type.toBe<TaskScheduler.Service>()
  expect(Effect.runPromise(HarnessRuntime.make())).type.toRaiseError()
})

test('custom installation retains missing construction services and failures on the real runtime', () => {
  expect(runtime.registry.install(registration)).type.toBe<
    Effect.Effect<RegistryScope.Snapshot, RegistryError | AcquisitionError, Credentials>
  >()
  expect(Effect.runPromise(runtime.registry.install(registration))).type.toRaiseError()
  expect(
    runtime.registry.install(Registry.provide(registration, Layer.succeed(Credentials, 'bound'))),
  ).type.toBe<Effect.Effect<RegistryScope.Snapshot, RegistryError | AcquisitionError>>()
  expect(TaskScheduler.make(session, { nativeRun: adapter })).type.toBe<
    Effect.Effect<
      TaskScheduler.Service,
      TaskScheduler.Failure,
      Registry.Registry | Scope.Scope | Owner
    >
  >()
  expect(TaskScheduler.layer(session, { nativeRun: adapter })).type.toBe<
    Layer.Layer<TaskScheduler.TaskScheduler, TaskScheduler.Failure, Registry.Registry | Owner>
  >()
  expect(Effect.runPromise(TaskScheduler.make(session, { nativeRun: adapter }))).type.toRaiseError()
})
