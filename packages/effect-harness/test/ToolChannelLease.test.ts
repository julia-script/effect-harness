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
import * as Tool from '../src/Tool.js'
import * as Toolkit from '../src/Toolkit.js'
import * as ToolInventory from '../src/ToolInventory.js'
import { execution } from './fixtures/Registry.js'

class Codec extends Context.Service<Codec, string>()('test/channel-lease/Codec') {}

describe('final validation holds the owner resource pin', () => {
  for (const interrupted of [false, true]) {
    it.effect(
      `closing the lease during async decoding ${interrupted ? 'and interruption' : 'then success'} releases only after validation ends`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            let released = 0
            let calls = 0
            const started = yield* Deferred.make<void>()
            const resume = yield* Deferred.make<void>()
            const codec = Layer.effect(
              Codec,
              Effect.acquireRelease(Effect.succeed('owner'), () =>
                Effect.sync(() => {
                  released++
                }),
              ),
            )
            const schema = Schema.String.pipe(
              Schema.middlewareDecoding((effect) =>
                Effect.gen(function* () {
                  assert.strictEqual(yield* Codec, 'owner')
                  assert.strictEqual(released, 0)
                  yield* Deferred.succeed(started, undefined)
                  yield* Deferred.await(resume)
                  assert.strictEqual(released, 0)
                  return yield* effect
                }),
              ),
            )
            const toolkit = Toolkit.make(
              Tool.makeResult('work', { success: schema, resultValidation: 'strict' }),
            )
            const registry = yield* Registry.make
            yield* registry.install(
              Registry.provide(
                Registry.toolkit({ id: 'owner', version: 1 }, toolkit),
                toolkit
                  .toLayer({
                    work: () =>
                      Effect.sync(() => {
                        calls++
                        return { content: [], structuredOutput: 'typed' }
                      }),
                  })
                  .pipe(Layer.provideMerge(codec)),
              ),
            )
            const scope = yield* Scope.make()
            const lease = yield* Scope.provide(registry.acquire, scope)
            const fiber = yield* ToolInventory.invoke(
              lease,
              { id: 'one', name: 'work', arguments: {} },
              'model',
            ).pipe(Effect.forkChild)
            yield* Deferred.await(started)
            yield* Scope.close(scope, Exit.void)
            yield* registry.uninstall('owner')
            assert.strictEqual(released, 0)
            assert.strictEqual(calls, 1)
            assert.strictEqual(
              (yield* lease.resolveTool('work').pipe(Effect.flip)).reason,
              'revoked',
            )
            if (interrupted) yield* Fiber.interrupt(fiber)
            else {
              yield* Deferred.succeed(resume, undefined)
              assert.strictEqual((yield* Fiber.join(fiber)).structuredOutput, 'typed')
            }
            assert.strictEqual(released, 1)
          }),
        ).pipe(Effect.provide(execution)),
    )
  }
  it.effect('mutable caller selections affect future calls, not a call already admitted', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>()
        const resume = yield* Deferred.make<void>()
        let calls = 0
        const toolkit = Toolkit.make(
          Tool.makeResult('work', { success: Schema.String, resultValidation: 'strict' }),
        )
        const registry = yield* Registry.make
        yield* registry.install(
          Registry.provide(
            Registry.toolkit({ id: 'owner', version: 1 }, toolkit),
            toolkit.toLayer({
              work: () =>
                Effect.gen(function* () {
                  calls++
                  yield* Deferred.succeed(entered, undefined)
                  yield* Deferred.await(resume)
                  return { content: [], structuredOutput: 'valid', control: { addTools: ['work'] } }
                }),
            }),
          ),
        )
        const lease = yield* registry.acquire
        const tools = ['work']
        const options = { agent: { tools } }
        const fiber = yield* ToolInventory.invoke(
          lease,
          { id: 'one', name: 'work', arguments: {} },
          'model',
          options,
        ).pipe(Effect.forkChild)
        yield* Deferred.await(entered)
        tools.length = 0
        yield* Deferred.succeed(resume, undefined)
        const result = yield* Fiber.join(fiber)
        assert.deepStrictEqual(result.control, { addTools: ['work'] })
        assert.strictEqual(calls, 1)
        assert.strictEqual(
          (yield* ToolInventory.invoke(
            lease,
            { id: 'two', name: 'work', arguments: {} },
            'model',
            options,
          ).pipe(Effect.flip))._tag,
          'ToolAdmissionDenied',
        )
        assert.strictEqual(calls, 1)
      }),
    ).pipe(Effect.provide(execution)),
  )
})
