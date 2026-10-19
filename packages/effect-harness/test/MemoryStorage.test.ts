import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Storage from 'effect-harness/Storage'
import * as Format from 'effect-harness/StorageFormat'
import * as Testing from 'effect-harness/Testing'
import * as Memory from '../src/internal/Memory.js'
import * as Store from '../src/internal/Store.js'

// Reopen the engine over one live backing allocation to prove the adapter records
// complete commits. The public Layer intentionally allocates fresh backing state.
const fixture = Memory.make.pipe(Effect.map((persistence) => ({ open: Store.make(persistence) })))

describe('memory shared-store conformance', () => {
  for (const test of Testing.storageConformance({
    make: fixture,
    capabilities: { history: true, reopen: true },
  }))
    it.effect(test.name, () => test.run)
})

it.effect('public memory layers isolate state and reject access after their scope closes', () =>
  Effect.gen(function* () {
    const closed = yield* Effect.scoped(
      Effect.gen(function* () {
        const context = yield* Layer.build(Layer.fresh(Storage.layerMemory))
        const storage = Context.get(context, Storage.Storage)
        yield* storage.commit([
          { _tag: 'conversation', value: { id: Storage.ROOT_CONVERSATION_ID } },
        ])
        yield* Format.requireSupportedFormat({ formatVersion: 2, minimumReaderVersion: 2 }).pipe(
          Effect.provide(context),
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            const other = yield* Layer.build(Layer.fresh(Storage.layerMemory))
            const isolated = Context.get(other, Storage.Storage)
            assert.isTrue(Option.isNone(yield* isolated.conversation(Storage.ROOT_CONVERSATION_ID)))
            assert.strictEqual((yield* Format.inspect.pipe(Effect.provide(other))).formatVersion, 1)
          }),
        )
        assert.isTrue(Option.isSome(yield* storage.conversation(Storage.ROOT_CONVERSATION_ID)))
        return storage
      }),
    )
    assert.strictEqual(
      (yield* closed.conversation(Storage.ROOT_CONVERSATION_ID).pipe(Effect.flip)).reason,
      'closed',
    )
    assert.strictEqual((yield* closed.commit([]).pipe(Effect.flip)).reason, 'closed')
    yield* Effect.scoped(
      Effect.gen(function* () {
        const context = yield* Layer.build(Layer.fresh(Storage.layerMemory))
        assert.isTrue(
          Option.isNone(
            yield* Context.get(context, Storage.Storage).conversation(Storage.ROOT_CONVERSATION_ID),
          ),
        )
      }),
    )
  }),
)
