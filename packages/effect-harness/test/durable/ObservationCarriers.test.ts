import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Stream from 'effect/Stream'

import * as Record from 'effect-harness/durable/Record'

import * as Observation from 'effect-harness/durable/Observation'

import * as View from 'effect-harness/durable/View'

const record: Record.Document = {
  id: Record.DocumentId.make(2),
  kind: 'counter',
  scope: { _tag: 'session' as const },
  createdAt: Record.Seq.make(1),
}

describe('ObservationCarriers', () => {
  it.effect('forwards lazy watch and state getters without any construction sampling', () =>
    Effect.sync(() => {
      let reads = 0
      let count = 0
      const watchInput = Object.freeze({
        get value() {
          reads++
          return { count }
        },
        record,
        changes: Stream.empty,
        closed: Effect.succeed('stopped' as const),
        stop: Effect.void,
        listen: () => Effect.void,
      })
      const watch = Observation.makeWatch(watchInput)
      const state = Observation.makeState(
        Object.freeze({
          get value() {
            reads++
            return { count }
          },
          record,
          get cursor() {
            return count
          },
          closed: Effect.succeed('stopped' as const),
        }),
      )
      const projected = View.makeProjectionWatch(
        Object.freeze({
          get value() {
            reads++
            return count
          },
          changes: Stream.empty,
          closed: Effect.succeed('stopped' as const),
          stop: Effect.void,
          listen: () => Effect.void,
        }),
      )
      assert.strictEqual(reads, 0)
      assert.isTrue(Observation.isWatch(watch))
      assert.isTrue(Observation.isState(state))
      assert.isTrue(View.isProjectionWatch(projected))
      assert.isFalse(Observation.isWatch(watchInput))
      count = 9
      assert.strictEqual(watch.value?.count, 9)
      assert.strictEqual(state.value?.count, 9)
      assert.strictEqual(state.cursor, 9)
      assert.strictEqual(projected.value, 9)
      assert.strictEqual(reads, 3)
    }),
  )
})
