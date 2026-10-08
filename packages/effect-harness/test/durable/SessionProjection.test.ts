import { assertFailure, assertSuccess } from '@effect/vitest/utils'
import * as Result from 'effect/Result'
import { rejected } from 'effect-harness/durable/StorageError'
import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Ref from 'effect/Ref'

import * as Schema from 'effect/Schema'

import * as SchemaGetter from 'effect/SchemaGetter'

import * as Option from 'effect/Option'

import * as Context from 'effect/Context'

import * as Document from 'effect-harness/durable/Document'

import * as Session from 'effect-harness/durable/Session'

import * as Store from 'effect-harness/durable/Store'

const counter = Document.defineUnsafe({
  kind: 'counter',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ count: Schema.Finite }),
  initial: () => ({ count: 0 }),
})

describe('SessionProjection', () => {
  it.effect('batches projections, completes each failed Exit and resamples after writes', () =>
    Effect.gen(function* () {
      const original = yield* Store.makeMemory
      const reads = yield* Ref.make(0)
      const sameLease = {}
      const store = Store.Store.of({
        ...original,
        readContext: Effect.succeed(sameLease),
        read: Ref.update(reads, (n) => n + 1).pipe(Effect.andThen(original.read)),
      })
      const session = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          yield* tx.doc(counter)
        }),
      )
      const incompatible = Document.defineUnsafe({ ...counter.definition, version: 2 })
      const results = yield* Effect.all(
        [
          session.snapshot(counter).pipe(Effect.map(Option.getOrUndefined), Effect.result),
          session.snapshot(incompatible).pipe(Effect.map(Option.getOrUndefined), Effect.result),
        ],
        { concurrency: 2 },
      )
      assert.strictEqual(yield* Ref.get(reads), 1)
      assertSuccess(
        Result.map(results[0], (snapshot) => snapshot?.value.count),
        0,
      )
      assertFailure(results[1], rejected('Document stored version is incompatible'))
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          ;(yield* tx.doc(counter)).count = 7
        }),
      )
      assert.strictEqual(
        (yield* session.snapshot(counter).pipe(Effect.map(Option.getOrUndefined)))?.value.count,
        7,
      )
      assert.strictEqual(yield* Ref.get(reads), 2)
    }),
  )

  it.effect('restores each entry caller Context for optional document decoding services', () =>
    Effect.gen(function* () {
      class Label extends Context.Service<Label, string>()('test/StateTime/Label') {}
      const encoded = Schema.Struct({ count: Schema.Finite })
      const decoded = Schema.Struct({ count: Schema.Finite, label: Schema.String })
      const codec = encoded.pipe(
        Schema.decodeTo(decoded, {
          decode: SchemaGetter.transformEffect((value) =>
            Effect.serviceOption(Label).pipe(
              Effect.map((label) => ({ ...value, label: Option.getOrElse(label, () => 'absent') })),
            ),
          ),
          encode: SchemaGetter.transform((value) => ({ count: value.count })),
        }),
      )
      const token = Document.defineUnsafe({
        kind: 'context',
        version: 1,
        scope: 'session',
        schema: codec,
        initial: () => ({ count: 0, label: 'initial' }),
      })
      const original = yield* Store.makeMemory
      const lease = {}
      const reads = yield* Ref.make(0)
      const store = Store.Store.of({
        ...original,
        readContext: Effect.succeed(lease),
        read: Ref.update(reads, (count) => count + 1).pipe(Effect.andThen(original.read)),
      })
      const session = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          yield* tx.doc(token)
        }),
      )
      const values = yield* Effect.all(
        [
          session
            .snapshot(token)
            .pipe(Effect.map(Option.getOrUndefined), Effect.provideService(Label, 'first')),
          session
            .snapshot(token)
            .pipe(Effect.map(Option.getOrUndefined), Effect.provideService(Label, 'second')),
        ],
        { concurrency: 2 },
      )
      assert.deepStrictEqual(
        values.map((value) => value?.value.label),
        ['first', 'second'],
      )
      assert.strictEqual(yield* Ref.get(reads), 1)
    }),
  )
})
