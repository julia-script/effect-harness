import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Registry from '../src/Registry.js'
import * as Task from '../src/Task.js'
import { call, execution, Label, registration, toolkit } from './fixtures/Registry.js'

class BuildError extends Schema.TaggedError<BuildError>()('RegistryTestBuildError', {}) {}

describe('Registry scoped lifetimes', () => {
  it.effect('retains an existing provided task Layer until its final definition lease ends', () =>
    Effect.scoped(
      Effect.gen(function* () {
        let released = 0
        const backend = Layer.effect(
          Label,
          Effect.acquireRelease(Effect.succeed('task-backend'), () =>
            Effect.sync(() => {
              released++
            }),
          ),
        )
        const definition = Task.make({
          kind: 'custom',
          version: 1,
          input: Schema.String,
          checkpoint: Schema.Struct({ phase: Schema.Literal('ready') }),
          success: Schema.String,
          failure: Schema.String,
          initial: () => ({ phase: 'ready' as const }),
          phases: { ready: () => Label.pipe(Effect.asVoid) },
        }).pipe(Task.provide(backend))
        const registry = yield* Registry.make
        yield* registry.install(
          Registry.provide(
            Registry.withTask(Registry.toolkit({ id: 'tasks', version: 1 }, toolkit), definition),
            toolkit
              .toLayer({ same: () => Label })
              .pipe(Layer.provideMerge(Layer.succeed(Label, 'tool-backend'))),
          ),
        )
        assert.strictEqual(released, 0)
        const leaseScope = yield* Scope.make()
        const lease = yield* Scope.provide(registry.acquire, leaseScope)
        const item = yield* lease.resolveTask('custom')
        assert.strictEqual(Context.get(item.context, Label), 'task-backend')
        yield* registry.uninstall('tasks')
        assert.strictEqual(released, 0)
        yield* Scope.close(leaseScope, Exit.void)
        assert.strictEqual(released, 1)
      }),
    ),
  )

  it.effect(
    'keeps a blocked old invocation live across reload, then closes only its old resources',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          const started = yield* Deferred.make<void>()
          const resume = yield* Deferred.make<void>()
          const releases: string[] = []
          const services = (label: string) =>
            Layer.effect(
              Label,
              Effect.acquireRelease(Effect.succeed(label), (label) =>
                Effect.sync(() => {
                  releases.push(label)
                }),
              ),
            )
          const handler = toolkit.toLayer({
            same: () =>
              Effect.gen(function* () {
                const label = yield* Label
                yield* Deferred.succeed(started, undefined)
                yield* Deferred.await(resume)
                assert.isFalse(releases.includes(label))
                return label
              }),
          })
          const old = Registry.provide(
            Registry.toolkit({ id: 'owner', version: 1 }, toolkit),
            handler.pipe(Layer.provideMerge(services('old'))),
          )
          yield* registry.install(old)
          assert.deepStrictEqual(releases, [])
          const invoke = Effect.scoped(
            Effect.gen(function* () {
              const lease = yield* registry.acquire
              return yield* lease.invokeTool(call)
            }),
          )
          const fiber = yield* invoke.pipe(Effect.forkChild)
          yield* Deferred.await(started)
          yield* registry.install(registration('owner', 'new', 2))
          assert.deepStrictEqual(releases, [])
          yield* Effect.scoped(
            Effect.gen(function* () {
              const lease = yield* registry.acquire
              assert.strictEqual((yield* lease.invokeTool(call)).details, 'new')
              assert.strictEqual((yield* lease.resolveTool('same')).owner.version, 2)
            }),
          )
          yield* Deferred.succeed(resume, undefined)
          assert.strictEqual((yield* Fiber.join(fiber)).details, 'old')
          assert.deepStrictEqual(releases, ['old'])
        }),
      ).pipe(Effect.provide(execution)),
  )

  it.effect(
    'pins resources until an invocation settles even if its lease Scope closes concurrently',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          const started = yield* Deferred.make<void>()
          const resume = yield* Deferred.make<void>()
          let released = 0
          const service = Layer.effect(
            Label,
            Effect.acquireRelease(Effect.succeed('resource'), () =>
              Effect.sync(() => {
                released++
              }),
            ),
          )
          const handler = toolkit.toLayer({
            same: () =>
              Effect.gen(function* () {
                yield* Deferred.succeed(started, undefined)
                yield* Deferred.await(resume)
                assert.strictEqual(released, 0)
                return yield* Label
              }),
          })
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: 'owner', version: 1 }, toolkit),
              handler.pipe(Layer.provideMerge(service)),
            ),
          )
          const scope = yield* Scope.make()
          const lease = yield* Scope.provide(registry.acquire, scope)
          const fiber = yield* lease.invokeTool(call).pipe(Effect.forkChild)
          yield* Deferred.await(started)
          yield* Scope.close(scope, Exit.void)
          yield* registry.uninstall('owner')
          assert.strictEqual(released, 0)
          assert.strictEqual(
            (yield* lease.invokeTool(call).pipe(Effect.flip))._tag,
            'RegistryError',
          )
          yield* Deferred.succeed(resume, undefined)
          assert.strictEqual((yield* Fiber.join(fiber)).details, 'resource')
          assert.strictEqual(released, 1)
        }),
      ).pipe(Effect.provide(execution)),
  )

  it.effect('acquisition racing uninstall either pins the old snapshot or sees the removal', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* Registry.make
        for (let i = 0; i < 30; i++) {
          let releases = 0
          const resource = Layer.effect(
            Label,
            Effect.acquireRelease(Effect.succeed(`iteration-${i}`), () =>
              Effect.sync(() => {
                releases++
              }),
            ),
          )
          const handler = toolkit.toLayer({ same: () => Label })
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: 'owner', version: 1 }, toolkit),
              handler.pipe(Layer.provideMerge(resource)),
            ),
          )
          const leaseScope = yield* Scope.make()
          const [lease] = yield* Effect.all(
            [Scope.provide(registry.acquire, leaseScope), registry.uninstall('owner')],
            { concurrency: 'unbounded' },
          )
          if (lease.snapshot.tools.length > 0) {
            assert.strictEqual(releases, 0)
            assert.strictEqual((yield* lease.invokeTool(call)).details, `iteration-${i}`)
          } else assert.strictEqual(releases, 1)
          yield* Scope.close(leaseScope, Exit.void)
          assert.strictEqual(releases, 1)
        }
      }),
    ).pipe(Effect.provide(execution)),
  )

  it.effect(
    'failed Layer construction and interrupted builds close candidates and publish nothing',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          yield* registry.install(registration('owner', 'valid'))
          const before = yield* registry.snapshot
          let released = 0
          const acquired = yield* Deferred.make<void>()
          const broken = Layer.effect(
            Label,
            Effect.gen(function* () {
              yield* Effect.acquireRelease(Effect.void, () =>
                Effect.sync(() => {
                  released++
                }),
              )
              return yield* new BuildError()
            }),
          )
          const failed = Registry.provide(
            Registry.toolkit({ id: 'owner', version: 2 }, toolkit),
            toolkit.toLayer({ same: () => Label }).pipe(Layer.provideMerge(broken)),
          )
          assert.strictEqual(
            (yield* registry.install(failed).pipe(Effect.flip))._tag,
            'RegistryTestBuildError',
          )
          assert.strictEqual(released, 1)
          assert.strictEqual(yield* registry.snapshot, before)
          const blocked = Layer.effect(
            Label,
            Effect.gen(function* () {
              yield* Effect.acquireRelease(Effect.void, () =>
                Effect.sync(() => {
                  released++
                }),
              )
              yield* Deferred.succeed(acquired, undefined)
              return yield* Effect.never
            }),
          )
          const fiber = yield* registry
            .install(
              Registry.provide(
                Registry.toolkit({ id: 'owner', version: 3 }, toolkit),
                toolkit.toLayer({ same: () => Label }).pipe(Layer.provideMerge(blocked)),
              ),
            )
            .pipe(Effect.forkChild)
          yield* Deferred.await(acquired)
          const lease = yield* registry.acquire
          assert.strictEqual((yield* lease.resolveTool('same')).owner.version, 1)
          yield* Fiber.interrupt(fiber)
          assert.strictEqual(released, 2)
          assert.strictEqual(yield* registry.snapshot, before)
        }),
      ),
  )

  it.effect(
    'closes retired resources after lease cancellation and leaves another registration intact',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          const acquired = yield* Deferred.make<void>()
          let released = 0
          const resource = Layer.effect(
            Label,
            Effect.acquireRelease(Effect.succeed('retired'), () =>
              Effect.sync(() => {
                released++
              }),
            ),
          )
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: 'retired', version: 1 }, toolkit),
              toolkit.toLayer({ same: () => Label }).pipe(Layer.provideMerge(resource)),
            ),
          )
          yield* registry.install(registration('remaining', 'live'))
          const fiber = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* registry.acquire
              yield* Deferred.succeed(acquired, undefined)
              return yield* Effect.never
            }),
          ).pipe(Effect.forkChild)
          yield* Deferred.await(acquired)
          yield* registry.uninstall('retired')
          assert.strictEqual(released, 0)
          yield* Fiber.interrupt(fiber)
          assert.strictEqual(released, 1)
          const lease = yield* registry.acquire
          assert.strictEqual((yield* lease.invokeTool(call)).details, 'live')
        }),
      ).pipe(Effect.provide(execution)),
  )

  it.effect(
    'listener Scope closes independently and a stalled listener cannot backpressure publication',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          const started = yield* Deferred.make<void>()
          let ended = false
          let notifications = 0
          const scope = yield* Scope.make()
          yield* Scope.provide(
            registry.listen(() =>
              Effect.gen(function* () {
                notifications++
                yield* Deferred.succeed(started, undefined)
                return yield* Effect.never
              }).pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    ended = true
                  }),
                ),
              ),
            ),
            scope,
          )
          yield* registry.install(registration('owner', 'first'))
          yield* Deferred.await(started)
          for (let i = 0; i < 20; i++) yield* registry.install(registration('owner', `reload-${i}`))
          assert.strictEqual((yield* registry.snapshot).generation, 21)
          yield* Scope.close(scope, Exit.void)
          assert.isTrue(ended)
          yield* registry.uninstall('owner')
          assert.strictEqual(notifications, 1)
        }),
      ),
  )

  it.effect(
    'shuts the registry while existing leases retain resources, denying further operations',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* Scope.make()
          const registry = yield* Scope.provide(Registry.make, scope)
          let released = 0
          const resource = Layer.effect(
            Label,
            Effect.acquireRelease(Effect.succeed('old'), () =>
              Effect.sync(() => {
                released++
              }),
            ),
          )
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: 'owner', version: 1 }, toolkit),
              toolkit.toLayer({ same: () => Label }).pipe(Layer.provideMerge(resource)),
            ),
          )
          const leaseScope = yield* Scope.make()
          const lease = yield* Scope.provide(registry.acquire, leaseScope)
          yield* Scope.close(scope, Exit.void)
          assert.strictEqual(released, 0)
          assert.strictEqual((yield* lease.invokeTool(call)).details, 'old')
          assert.strictEqual(
            (yield* registry.uninstall('owner').pipe(Effect.flip)).reason,
            'revoked',
          )
          assert.strictEqual((yield* registry.acquire.pipe(Effect.flip)).reason, 'revoked')
          yield* Scope.close(leaseScope, Exit.void)
          assert.strictEqual(released, 1)
        }),
      ).pipe(Effect.provide(execution)),
  )

  it.effect('rejects publication when a slow build finishes after the registry Scope closes', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const scope = yield* Scope.make()
        const registry = yield* Scope.provide(Registry.make, scope)
        const started = yield* Deferred.make<void>()
        const resume = yield* Deferred.make<void>()
        let released = 0
        const backend = Layer.effect(
          Label,
          Effect.gen(function* () {
            yield* Effect.acquireRelease(Effect.void, () =>
              Effect.sync(() => {
                released++
              }),
            )
            yield* Deferred.succeed(started, undefined)
            yield* Deferred.await(resume)
            return 'candidate'
          }),
        )
        const candidate = Registry.provide(
          Registry.toolkit({ id: 'slow', version: 1 }, toolkit),
          toolkit.toLayer({ same: () => Label }).pipe(Layer.provideMerge(backend)),
        )
        const fiber = yield* registry.install(candidate).pipe(Effect.flip, Effect.forkChild)
        yield* Deferred.await(started)
        yield* Scope.close(scope, Exit.void)
        yield* Deferred.succeed(resume, undefined)
        assert.strictEqual((yield* Fiber.join(fiber)).reason, 'revoked')
        assert.strictEqual(released, 1)
        assert.strictEqual((yield* registry.snapshot).generation, 0)
      }),
    ),
  )
})
