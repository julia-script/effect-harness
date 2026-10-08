import { assertExitFailure } from '@effect/vitest/utils'

import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Cause from 'effect/Cause'

import * as Schema from 'effect/Schema'

import * as Document from 'effect-harness/durable/Document'

import * as Session from 'effect-harness/durable/Session'

import * as Store from 'effect-harness/durable/Store'

import { sessionLayer } from 'effect-harness/durable/testing/Storage'

const definition: Document.Document.DefinitionInput<{ count: number }> = {
  kind: 'counter',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ count: Schema.Finite }),
  initial: () => ({ count: 0 }),
}

describe('SessionDraftSentinel', () => {
  it.effect('translates only the private draft sentinel and leaves genuine defects intact', () =>
    Effect.gen(function* () {
      const session = yield* Session.Session
      const token = Document.defineUnsafe(definition)
      const foreign = new Error('foreign getter failure')
      const bad = new Proxy(
        {},
        {
          // effect-nit-allow P7-v4-data-type-naming: ProxyHandler requires the exact ownKeys trap name; this hostile fixture must throw the original foreign cause to exercise the guarded boundary.
          ownKeys() {
            throw foreign
          },
        },
      )
      for (const operation of [
        (draft: Document.Document.Draft<{ count: number }>) => Reflect.set(draft, 'count', bad),
        (draft: Document.Document.Draft<{ count: number }>) =>
          Object.defineProperty(draft, 'count', {
            value: bad,
            enumerable: true,
            writable: true,
            configurable: true,
          }),
      ]) {
        const error = yield* session
          .transaction((tx) =>
            Effect.gen(function* () {
              const draft = yield* tx.doc(token)
              operation(draft)
            }),
          )
          .pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, 'InvalidError')
        assert.strictEqual(error.cause, foreign)
      }
      const invalid = yield* session
        .transaction((tx) =>
          Effect.gen(function* () {
            const draft = yield* tx.doc(token)
            Reflect.set(draft, 'count', 1n)
          }),
        )
        .pipe(Effect.flip)
      assert.strictEqual(invalid.reason._tag, 'InvalidError')
      assert.ok(invalid.cause instanceof Schema.SchemaError)
      const defect = new Error('genuine callback defect')
      const exit = yield* session.transaction(() => Effect.die(defect)).pipe(Effect.exit)
      assertExitFailure(exit, Cause.die(defect))
      assert.strictEqual((yield* session.committed).nextId, 2)
    }).pipe(Effect.provide(sessionLayer(Store.layerMemory))),
  )
})
