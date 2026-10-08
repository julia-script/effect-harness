import { assertFailure } from '@effect/vitest/utils'
import { rejected } from 'effect-harness/StorageError'
import { assert, describe, it } from '@effect/vitest'

import * as Deferred from 'effect/Deferred'

import * as Effect from 'effect/Effect'

import * as Fiber from 'effect/Fiber'

import * as Schema from 'effect/Schema'

import * as SchemaGetter from 'effect/SchemaGetter'

import * as Document from 'effect-harness/Document'

import * as Record from 'effect-harness/Record'

const old = (id: number, count: number): Document.Document.Snapshot =>
  Document.makeSnapshot({
    record: {
      id: Record.DocumentId.make(id),
      kind: 'migrated',
      scope: { _tag: 'session' as const },
      createdAt: Record.Seq.make(1),
    },
    version: 1,
    value: { count },
    deltasSinceBase: 0,
  })

describe('DocumentMigration', () => {
  it.effect(
    'shares concurrent successful migrations and detaches each return with the exact incarnation key',
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const shape = Schema.Struct({ count: Schema.Finite })
        const codec = shape.pipe(
          Schema.decodeTo(shape, {
            decode: SchemaGetter.passthrough(),
            encode: SchemaGetter.transformEffect((value) =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.as(value),
              ),
            ),
          }),
        )
        let calls = 0
        const shared = Document.defineUnsafe({
          kind: 'migrated',
          version: 2,
          scope: 'session',
          schema: codec,
          initial: () => ({ count: 0 }),
          migrate: (value) => {
            calls++
            return { count: Number(value.count) + 1 }
          },
        })
        const cache = yield* Document.makeMigrationCache
        const a = yield* Document.typed(shared, old(2, 10), cache).pipe(Effect.forkScoped)
        yield* Deferred.await(entered)
        const b = yield* Document.typed(shared, old(2, 10), cache).pipe(Effect.forkScoped)
        yield* Effect.yieldNow
        yield* Deferred.succeed(release, undefined)
        const first = yield* Fiber.join(a)
        const second = yield* Fiber.join(b)
        assert.strictEqual(calls, 1)
        assert.notStrictEqual(first.value, second.value)
        Object.defineProperty(first.value, 'count', { value: 999, enumerable: true })
        assert.strictEqual(second.value.count, 11)
        assert.strictEqual((yield* Document.typed(shared, old(2, 10), cache)).value.count, 11)
        assert.strictEqual((yield* Document.typed(shared, old(3, 10), cache)).value.count, 11)
        assert.strictEqual(calls, 2)
        assert.strictEqual((yield* Document.typed(shared, old(2, 20), cache)).value.count, 21)
        assert.strictEqual(calls, 3)
      }),
  )

  it.effect(
    'migration failures expire immediately and equivalent token objects never share identity',
    () =>
      Effect.gen(function* () {
        let calls = 0
        const migrationFailure = new Error('first migration fails')
        const token = Document.defineUnsafe({
          kind: 'migrated',
          version: 2,
          scope: 'session',
          schema: Schema.Struct({ count: Schema.Finite }),
          initial: () => ({ count: 0 }),
          migrate: (value) => {
            if (++calls === 1) throw migrationFailure
            return { count: Number(value.count) + 1 }
          },
        })
        const cache = yield* Document.makeMigrationCache
        assertFailure(
          yield* Document.typed(token, old(2, 1), cache).pipe(Effect.result),
          rejected('Document migration failed', undefined, migrationFailure),
        )
        assert.strictEqual((yield* Document.typed(token, old(2, 1), cache)).value.count, 2)
        assert.strictEqual(calls, 2)
        yield* Document.typed(Document.defineUnsafe({ ...token.definition }), old(2, 1), cache)
        assert.strictEqual(calls, 3)
      }),
  )

  it.effect('detaches decoded codec aliases before returning migrated snapshots', () =>
    Effect.gen(function* () {
      const shared = { count: 11 }
      const shape = Schema.Struct({ count: Schema.Finite })
      const codec = shape.pipe(
        Schema.decodeTo(Schema.declare<{ count: number }>(Schema.is(shape)), {
          decode: SchemaGetter.transform(() => shared),
          encode: SchemaGetter.transform((value) => ({ count: value.count })),
        }),
      )
      const token = Document.defineUnsafe({
        kind: 'migrated',
        version: 2,
        scope: 'session',
        schema: codec,
        initial: () => ({ count: 0 }),
        migrate: (value) => ({ count: Number(value.count) + 1 }),
      })
      const cache = yield* Document.makeMigrationCache
      const first = yield* Document.typed(token, old(2, 10), cache)
      const second = yield* Document.typed(token, old(2, 10), cache)
      assert.notStrictEqual(first.value, shared)
      assert.notStrictEqual(first.value, second.value)
      Object.defineProperty(first.value, 'count', { value: 999, enumerable: true })
      assert.strictEqual(shared.count, 11)
      assert.strictEqual(second.value.count, 11)
      assert.strictEqual((yield* Document.typed(token, old(2, 10), cache)).value.count, 11)
    }),
  )
})
