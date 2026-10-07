import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as SchemaIssue from 'effect/SchemaIssue'
import * as Record from '@effect-harness/durable/Record'
import * as Store from '@effect-harness/durable/Store'
import type { StorageError } from '@effect-harness/durable/StorageError'
import * as Memory from '@effect-harness/durable/storage/Memory'

class Policy extends Context.Service<Policy, { calls: number; rejectAt?: number }>()(
  'test/durable/StoreMint/Policy',
) {}
const checkedId = Record.TaskId.pipe(
  Schema.middlewareDecoding((effect) =>
    Effect.gen(function* () {
      const policy = yield* Policy
      policy.calls++
      const id = yield* effect
      if (policy.calls === policy.rejectAt)
        return yield* Effect.fail(
          new SchemaIssue.InvalidValue({ message: 'Policy rejects this allocation' }),
        )
      yield* Effect.yieldNow
      return id
    }),
  ),
)
const allocate: Effect.Effect<Record.TaskId, StorageError, Store.Store | Policy> =
  Store.mintId(checkedId)

describe('Store.mintId accessor', () => {
  it.effect(
    'preserves service-dependent precommit and postallocation validation and exhaustion',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* Store.Store
          const before = Policy.of({ calls: 0, rejectAt: 1 })
          assert.strictEqual(
            (yield* allocate.pipe(Effect.provideService(Policy, before), Effect.flip)).reason._tag,
            'Invalid',
          )
          assert.strictEqual(before.calls, 1)
          assert.strictEqual((yield* store.read).nextId, 2)
          const after = Policy.of({ calls: 0, rejectAt: 2 })
          assert.strictEqual(
            (yield* allocate.pipe(Effect.provideService(Policy, after), Effect.flip)).reason._tag,
            'Invalid',
          )
          assert.strictEqual(after.calls, 2)
          assert.strictEqual((yield* store.read).nextId, 3)
          const allowed = Policy.of({ calls: 0 })
          const ids = yield* Effect.forEach([1, 2, 3], () => allocate, {
            concurrency: 'unbounded',
          }).pipe(Effect.provideService(Policy, allowed))
          assert.deepStrictEqual(
            [...ids].sort((a, b) => a - b),
            [3, 4, 5],
          )
          assert.strictEqual(allowed.calls, 6)
          yield* store.transact((state) =>
            Effect.succeed(
              Store.makeCandidate({
                state: { ...state, nextId: Number.MAX_SAFE_INTEGER },
                writes: [],
                result: null,
              }),
            ),
          )
          assert.strictEqual(
            yield* allocate.pipe(Effect.provideService(Policy, allowed)),
            Number.MAX_SAFE_INTEGER,
          )
          assert.strictEqual(
            (yield* allocate.pipe(Effect.provideService(Policy, allowed), Effect.flip)).reason._tag,
            'Invalid',
          )
          assert.strictEqual(allowed.calls, 8)
        }).pipe(Effect.provide(Memory.layer)),
      ),
  )
})
