import { assert, describe, it } from '@effect/vitest'

import * as Time from 'effect-harness/Time'

import * as DateTime from 'effect/DateTime'

import * as Duration from 'effect/Duration'

import * as Effect from 'effect/Effect'

import * as TestClock from 'effect/testing/TestClock'

import { remaining } from 'effect-harness/durable/workflow/ModelRetry'

describe('ModelRetryTime', () => {
  it.effect('waits only the remaining span of an unchanged fractional cached deadline', () =>
    Effect.gen(function* () {
      const deadline = Time.fromEpochMillis(1000.5)
      assert.strictEqual(Duration.toMillis(yield* remaining(deadline)), 1000.5)
      yield* TestClock.adjust('500 millis')
      assert.strictEqual(Duration.toMillis(yield* remaining(deadline)), 500.5)
      yield* TestClock.adjust('1 second')
      assert.strictEqual(Duration.toMillis(yield* remaining(deadline)), 0)
      assert.strictEqual(DateTime.toEpochMillis(deadline), 1000.5)
    }),
  )
})
