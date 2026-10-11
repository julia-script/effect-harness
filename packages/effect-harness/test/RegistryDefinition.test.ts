import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Scope from 'effect/Scope'
import * as PromptSection from '../src/PromptSection.js'
import * as Registry from '../src/Registry.js'
import { call, execution, registration } from './fixtures/Registry.js'

class Acquisition extends Context.Service<Acquisition, void>()('test/registry/Acquisition') {}

describe('Registry definition ownership', () => {
  for (const reload of [false, true])
    for (const enabled of [false, true])
      it.effect(
        `captures ${enabled ? 'enabled' : 'disabled'} metadata before blocked ${reload ? 'reload' : 'install'}`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const registry = yield* Registry.make
              const releases: string[] = []
              const started = yield* Deferred.make<void>()
              const resume = yield* Deferred.make<void>()
              const resource = (label: string) =>
                Layer.effect(
                  Acquisition,
                  Effect.acquireRelease(Effect.void, () =>
                    Effect.sync(() => {
                      releases.push(label)
                    }),
                  ),
                )
              if (reload)
                yield* registry.install(
                  Registry.provide(registration('valid', 'old'), resource('old')),
                )
              const oldScope = yield* Scope.make()
              const oldLease = yield* Scope.provide(registry.acquire, oldScope)
              const before = yield* registry.snapshot
              const definition = { id: 'valid', version: 2, enabled }
              const candidate = {
                definition,
                acquire: registration('unused', 'new').acquire,
              } satisfies Registry.Registration<never, never>
              const blocked = resource('new').pipe(
                Layer.tap(() =>
                  Effect.gen(function* () {
                    yield* Deferred.succeed(started, undefined)
                    yield* Deferred.await(resume)
                  }),
                ),
              )
              const delayed = {
                definition,
                acquire: Registry.provide(candidate, blocked).acquire,
              } satisfies Registry.Registration<never, never>
              const fiber = yield* registry.install(delayed).pipe(Effect.forkChild)
              yield* Deferred.await(started)
              definition.id = 'effect-harness/reserved'
              definition.version = 0
              definition.enabled = !enabled
              delayed.definition = { id: 'constructor', version: 0, enabled: !enabled }
              assert.strictEqual(yield* registry.snapshot, before)
              yield* Deferred.succeed(resume, undefined)
              const published = yield* Fiber.join(fiber)
              assert.deepStrictEqual(published.registrations, [
                { id: 'valid', version: 2, enabled },
              ])
              assert.strictEqual(published.generation, before.generation + 1)
              assert.isTrue(Object.isFrozen(published))
              assert.isTrue(Object.isFrozen(published.registrations))
              assert.isTrue(Object.isFrozen(published.registrations[0]))
              assert.deepStrictEqual(releases, [])
              if (reload) {
                assert.strictEqual((yield* oldLease.resolveTool('same')).owner.version, 1)
                assert.strictEqual((yield* oldLease.invokeTool(call)).details, 'old')
              } else assert.deepStrictEqual(oldLease.snapshot.registrations, [])
              yield* Scope.close(oldScope, Exit.void)
              assert.deepStrictEqual(releases, reload ? ['old'] : [])
              const leaseScope = yield* Scope.make()
              const lease = yield* Scope.provide(registry.acquire, leaseScope)
              if (enabled) {
                const item = yield* lease.resolveTool('same', { binding: 'valid', version: 2 })
                assert.strictEqual(item.owner, published.registrations[0])
                assert.strictEqual(
                  (yield* lease.resolveTool('same', { version: 0 }).pipe(Effect.flip)).reason,
                  'incompatibleVersion',
                )
                assert.strictEqual((yield* lease.invokeTool(call)).details, 'new')
              } else
                assert.strictEqual(
                  (yield* lease.resolveTool('same').pipe(Effect.flip)).reason,
                  'disabled',
                )
              yield* registry.uninstall('valid')
              assert.deepStrictEqual((yield* registry.snapshot).registrations, [])
              assert.deepStrictEqual(releases, reload ? ['old'] : [])
              yield* Scope.close(leaseScope, Exit.void)
              assert.deepStrictEqual(releases, reload ? ['old', 'new'] : ['new'])
            }),
          ).pipe(Effect.provide(execution)),
      )

  it.effect(
    'validates acquired contents against the captured ID and cleans failed candidates',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          const before = yield* registry.snapshot
          const started = yield* Deferred.make<void>()
          const resume = yield* Deferred.make<void>()
          const definition = { id: 'valid', version: 1 }
          let released = 0
          const candidate: Registry.Registration<never, never> = {
            definition,
            acquire: Effect.gen(function* () {
              yield* Effect.acquireRelease(Effect.void, () =>
                Effect.sync(() => {
                  released++
                }),
              )
              yield* Deferred.succeed(started, undefined)
              yield* Deferred.await(resume)
              return {
                tools: [],
                tasks: [],
                hooks: [],
                sections: [
                  {
                    declaration: PromptSection.make({
                      key: 'instructions',
                      render: () => Effect.succeed(''),
                    }),
                    context: Context.empty(),
                  },
                ],
              }
            }),
          }
          const fiber = yield* registry.install(candidate).pipe(Effect.flip, Effect.forkChild)
          yield* Deferred.await(started)
          definition.id = 'effect-harness/reserved'
          definition.version = 0
          yield* Deferred.succeed(resume, undefined)
          const failed = yield* Fiber.join(fiber)
          assert.strictEqual(failed.reason, 'invalidSection')
          assert.strictEqual(failed.registration, 'valid')
          assert.strictEqual(released, 1)
          assert.strictEqual(yield* registry.snapshot, before)
        }),
      ),
  )
})
