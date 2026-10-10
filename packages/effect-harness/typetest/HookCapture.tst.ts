import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import type * as Prompt from 'effect/ai/Prompt'
import * as Hook from 'effect-harness/Hook'
import { HookExecution } from 'effect-harness/HookExecution'
import * as Registry from 'effect-harness/Registry'

class Backend extends Context.Service<Backend, string>()('types/HookCapture/Backend') {}
class Credentials extends Context.Service<Credentials, string>()('types/HookCapture/Credentials') {}
class CallbackError extends Schema.TaggedError<CallbackError>()('HookCaptureCallbackError', {}) {}
class BuildError extends Schema.TaggedError<BuildError>()('HookCaptureBuildError', {}) {}
declare const backend: Layer.Layer<Backend, BuildError, Credentials>

test('captured callbacks retain narrowed inputs, callback errors and scoped service channels', () => {
  const definition = {
    event: 'beforeRequest' as const,
    execute: (event: Hook.Input<'beforeRequest'>) =>
      Backend.pipe(
        Effect.andThen(HookExecution),
        Effect.andThen(Effect.addFinalizer(() => Effect.void)),
        Effect.andThen(Effect.fail(new CallbackError())),
        Effect.as(event.prompt),
      ),
  }
  const hook = Hook.make(definition)
  expect(hook.execute).type.toBe<
    (
      event: Hook.Input<'beforeRequest'>,
    ) => Effect.Effect<Prompt.Prompt | void, CallbackError, Backend | HookExecution | Scope.Scope>
  >()
  expect<Hook.Requirements<typeof hook>>().type.toBe<Backend>()
  expect(Registry.hook({ id: 'capture', version: 1 }, hook)).type.toBe<
    Registry.Registration<Backend>
  >()
  const provided = hook.pipe(Hook.provide(backend))
  expect(provided).type.toBe<
    Hook.Hook<'beforeRequest', CallbackError, Credentials | HookExecution | Scope.Scope, BuildError>
  >()
  expect(Registry.hook({ id: 'capture', version: 1 }, provided)).type.toBe<
    Registry.Registration<Credentials, BuildError>
  >()
  expect(hook.execute).type.not.toBeCallableWith({ _tag: 'afterTools', results: [] })
})
