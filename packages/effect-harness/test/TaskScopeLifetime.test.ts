import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Task from '../src/Task.js'

class Backend extends Context.Service<Backend, { readonly use: Effect.Effect<void> }>()(
  'test/TaskScopeLifetime/Backend',
) {}
class Seed extends Context.Service<Seed, number>()('test/TaskScopeLifetime/Seed') {}
class BuildError extends Schema.TaggedError<BuildError>()('TaskScopeLifetimeBuildError', {}) {}
const definition = Task.make({
  kind: 'scope-lifetime',
  version: 1,
  input: Schema.String,
  checkpoint: Schema.Struct({ phase: Schema.Literal('ready') }),
  success: Schema.String,
  failure: Schema.String,
  initial: () => ({ phase: 'ready' as const }),
  phases: { ready: () => Backend.pipe(Effect.flatMap((backend) => backend.use)) },
})

describe('Task provided Layer lifetimes', () => {
  it.effect('retains chained scoped dependencies and closes them after handler failure', () =>
    Effect.gen(function* () {
      const events: string[] = []
      const seed = Layer.effect(
        Seed,
        Effect.acquireRelease(Effect.succeed(7), () =>
          Effect.sync(() => {
            events.push('seed released')
          }),
        ),
      )
      const backend = Layer.effect(
        Backend,
        Effect.gen(function* () {
          const value = yield* Seed
          return yield* Effect.acquireRelease(
            Effect.succeed({
              use: Effect.sync(() => {
                assert.strictEqual(value, 7)
                assert.deepStrictEqual(events, [])
              }),
            }),
            () =>
              Effect.sync(() => {
                events.push('backend released')
              }),
          )
        }),
      )
      const provided = definition.pipe(Task.provide(backend), Task.provide(seed))
      const failure = new BuildError()
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const captured = yield* provided.context
          yield* definition.phases.ready().pipe(Effect.provideContext(captured))
          return yield* failure
        }),
      ).pipe(Effect.flip)
      assert.strictEqual(result, failure)
      assert.deepStrictEqual(events, ['backend released', 'seed released'])
    }),
  )

  it.effect('orders acquisition, context capture, handler use and exactly one owner release', () =>
    Effect.gen(function* () {
      const events: string[] = []
      const layer = Layer.effect(
        Backend,
        Effect.acquireRelease(
          Effect.sync(() => {
            events.push('acquired')
            return {
              use: Effect.sync(() => {
                assert.isFalse(events.includes('released'))
                events.push('handler')
              }),
            }
          }),
          () =>
            Effect.sync(() => {
              events.push('released')
            }),
        ),
      )
      const provided = definition.pipe(Task.provide(layer))
      const owner = yield* Scope.make()
      const captured = yield* Scope.provide(provided.context, owner)
      events.push('context')
      assert.deepStrictEqual(events, ['acquired', 'context'])
      yield* definition.phases.ready().pipe(Effect.provideContext(captured))
      assert.deepStrictEqual(events, ['acquired', 'context', 'handler'])
      yield* Scope.close(owner, Exit.void)
      yield* Scope.close(owner, Exit.void)
      assert.deepStrictEqual(events, ['acquired', 'context', 'handler', 'released'])
    }),
  )

  it.effect('keeps the backend alive after an invocation Scope closes', () =>
    Effect.gen(function* () {
      let released = 0
      const layer = Layer.effect(
        Backend,
        Effect.acquireRelease(Effect.succeed({ use: Effect.void }), () =>
          Effect.sync(() => {
            released++
          }),
        ),
      )
      yield* Effect.scoped(
        Effect.gen(function* () {
          const captured = yield* Task.provide(definition, layer).context
          yield* Effect.scoped(
            Effect.gen(function* () {
              const invocation = yield* Scope.Scope
              yield* definition.phases
                .ready()
                .pipe(
                  Effect.provideContext(
                    Context.merge(captured, Context.make(Scope.Scope, invocation)),
                  ),
                )
            }),
          )
          assert.strictEqual(released, 0)
          yield* definition.phases.ready().pipe(Effect.provideContext(captured))
        }),
      )
      assert.strictEqual(released, 1)
    }),
  )

  it.effect('releases a partially acquired Layer on failed registration Scope closure', () =>
    Effect.gen(function* () {
      let released = 0
      const failure = new BuildError()
      const layer = Layer.effect(
        Backend,
        Effect.gen(function* () {
          yield* Effect.acquireRelease(Effect.void, () =>
            Effect.sync(() => {
              released++
            }),
          )
          return yield* failure
        }),
      )
      const result = yield* Effect.scoped(Task.provide(definition, layer).context).pipe(Effect.flip)
      assert.strictEqual(result, failure)
      assert.strictEqual(released, 1)
    }),
  )

  it.effect('releases a partial acquisition when its scoped caller is interrupted', () =>
    Effect.gen(function* () {
      let released = 0
      const acquired = yield* Deferred.make<void>()
      const layer = Layer.effect(
        Backend,
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
      const fiber = yield* Effect.scoped(Task.provide(definition, layer).context).pipe(
        Effect.forkChild,
      )
      yield* Deferred.await(acquired)
      assert.strictEqual(released, 0)
      yield* Fiber.interrupt(fiber)
      assert.strictEqual(released, 1)
    }),
  )

  it.effect('releases a captured backend on outer caller interruption', () =>
    Effect.gen(function* () {
      let released = 0
      const started = yield* Deferred.make<void>()
      const layer = Layer.effect(
        Backend,
        Effect.acquireRelease(
          Effect.succeed({
            use: Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
          }),
          () =>
            Effect.sync(() => {
              released++
            }),
        ),
      )
      const fiber = yield* Effect.scoped(
        Effect.gen(function* () {
          const captured = yield* Task.provide(definition, layer).context
          yield* definition.phases.ready().pipe(Effect.provideContext(captured))
        }),
      ).pipe(Effect.forkChild)
      yield* Deferred.await(started)
      assert.strictEqual(released, 0)
      yield* Fiber.interrupt(fiber)
      assert.strictEqual(released, 1)
    }),
  )

  it.effect('keeps separate registration acquisitions independent and retains Layer inputs', () =>
    Effect.gen(function* () {
      const acquired: number[] = []
      const released: number[] = []
      const layer = Layer.effect(
        Backend,
        Effect.gen(function* () {
          const seed = yield* Seed
          return yield* Effect.acquireRelease(
            Effect.sync(() => {
              acquired.push(seed)
              return {
                use: Effect.sync(() => {
                  assert.isFalse(released.includes(seed))
                }),
              }
            }),
            () =>
              Effect.sync(() => {
                released.push(seed)
              }),
          )
        }),
      )
      const provided = Task.provide(definition, layer)
      const first = yield* Scope.make()
      const second = yield* Scope.make()
      yield* Scope.provide(provided.context, first).pipe(Effect.provideService(Seed, 1))
      const captured = yield* Scope.provide(provided.context, second).pipe(
        Effect.provideService(Seed, 2),
      )
      assert.deepStrictEqual(acquired, [1, 2])
      yield* Scope.close(first, Exit.void)
      assert.deepStrictEqual(released, [1])
      yield* definition.phases.ready().pipe(Effect.provideContext(captured))
      yield* Scope.close(second, Exit.void)
      assert.deepStrictEqual(released, [1, 2])
    }),
  )
})
