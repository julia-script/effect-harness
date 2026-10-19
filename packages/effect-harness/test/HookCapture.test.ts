import * as TemporaryStorage from './fixtures/Storage.js'
import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Scope from 'effect/Scope'
import * as Prompt from 'effect/ai/Prompt'
import * as Hook from '../src/Hook.js'
import { HookExecution } from '../src/HookExecution.js'
import * as Registry from '../src/Registry.js'
import * as Session from '../src/Session.js'

import * as Transaction from '../src/Transaction.js'

class Backend extends Context.Service<Backend, string>()('test/HookCapture/Backend') {}

const access = Effect.gen(function* () {
  const session = yield* Session.make()
  const root = yield* Session.commit(session, Transaction.ensureRoot)
  const execution: Context.Service.Shape<typeof HookExecution> = {
    conversationId: root.id,
    snapshot: (d, t) => Session.snapshot(session, d, t),
    watch: (d, t) => Session.watch(session, d, t),
    commit: (f) => Session.commit(session, f),
  }
  return execution
})

describe('Hook callback capture', () => {
  it.effect('keeps existing leases on the original callback until an explicit reload', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const execution = yield* access
        const registry = yield* Registry.make
        const seen: string[] = []
        const callback =
          (label: string) =>
          ({ prompt }: Hook.Input<'beforeRequest'>) =>
            Effect.sync(() => {
              seen.push(label)
              return prompt
            })
        const original = callback('original')
        const definition = { event: 'beforeRequest' as const, execute: original }
        const hook = Hook.make(definition)
        yield* registry.install(Registry.hook({ id: 'captured', version: 1 }, hook))
        const oldScope = yield* Scope.make()
        const old = yield* Scope.provide(registry.acquire, oldScope)
        const event = { _tag: 'beforeRequest' as const, prompt: Prompt.make('input') }
        const dispatch = old
          .dispatchHook(event)
          .pipe(Effect.provideService(HookExecution, execution))
        assert.strictEqual(yield* dispatch, event.prompt)
        definition.execute = callback('replacement')
        assert.strictEqual(hook.execute, original)
        assert.strictEqual(yield* dispatch, event.prompt)
        const reloaded = Hook.make(definition)
        assert.strictEqual(reloaded.execute, definition.execute)
        yield* registry.install(Registry.hook({ id: 'captured', version: 2 }, reloaded))
        const current = yield* registry.acquire
        yield* current.dispatchHook(event).pipe(Effect.provideService(HookExecution, execution))
        yield* registry.uninstall('captured')
        yield* dispatch
        assert.deepStrictEqual(seen, ['original', 'original', 'replacement', 'original'])
        yield* Scope.close(oldScope, Exit.void)
        const failure = yield* dispatch.pipe(Effect.flip)
        assert.strictEqual(failure._tag, 'RegistryError')
        if (failure._tag === 'RegistryError') assert.strictEqual(failure.reason, 'revoked')
      }),
    ).pipe(Effect.provide(TemporaryStorage.layer)),
  )

  it.effect('captures before asynchronous installation and retains its own scoped backend', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const execution = yield* access
        const registry = yield* Registry.make
        const started = yield* Deferred.make<void>()
        const resume = yield* Deferred.make<void>()
        const seen: string[] = []
        let released = 0
        const backend = Layer.effect(
          Backend,
          Effect.gen(function* () {
            yield* Effect.acquireRelease(Effect.void, () =>
              Effect.sync(() => {
                released++
              }),
            )
            yield* Deferred.succeed(started, undefined)
            yield* Deferred.await(resume)
            return 'owned'
          }),
        )
        const callback =
          (label: string) =>
          ({ prompt }: Hook.Input<'beforeRequest'>) =>
            Effect.gen(function* () {
              const backend = yield* Backend
              const current = yield* HookExecution
              assert.strictEqual(current, execution)
              assert.strictEqual(released, 0)
              yield* Effect.addFinalizer(() => Effect.void)
              seen.push(`${label}/${backend}`)
              return prompt
            })
        const original = callback('original')
        const definition = { event: 'beforeRequest' as const, execute: original }
        const hook = Hook.make(definition).pipe(Hook.provide(backend))
        const install = yield* registry
          .install(Registry.hook({ id: 'async', version: 1 }, hook))
          .pipe(Effect.forkChild)
        yield* Deferred.await(started)
        definition.execute = callback('replacement')
        yield* Deferred.succeed(resume, undefined)
        yield* Fiber.join(install)
        assert.isTrue(Object.is(hook.execute, original))
        const scope = yield* Scope.make()
        const lease = yield* Scope.provide(registry.acquire, scope)
        yield* registry.uninstall('async')
        const event = { _tag: 'beforeRequest' as const, prompt: Prompt.make('input') }
        assert.strictEqual(
          yield* lease
            .dispatchHook(event)
            .pipe(
              Effect.provideService(HookExecution, execution),
              Effect.provideService(Backend, 'ambient'),
            ),
          event.prompt,
        )
        assert.deepStrictEqual(seen, ['original/owned'])
        assert.strictEqual(released, 0)
        yield* Scope.close(scope, Exit.void)
        assert.strictEqual(released, 1)
      }),
    ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
})
