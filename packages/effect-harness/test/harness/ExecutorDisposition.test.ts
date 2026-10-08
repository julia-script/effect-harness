import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Duration from 'effect/Duration'
import * as Schema from 'effect/Schema'
import * as Executor from 'effect-harness/Executor'

describe('ExecutorDisposition', () => {
  it.effect(
    'legacy deferred disposition keeps fractional duration encoding and opaque native handle fields',
    () =>
      Effect.gen(function* () {
        const wire = {
          type: 'deferred' as const,
          decision: { handle: { providerOpaque: ['a', null, 2] }, pollAfterMs: 2.5 },
        }
        const decoded = yield* Schema.decodeEffect(Executor.Disposition)(wire)
        assert.strictEqual(decoded._tag, 'deferred')
        if (decoded._tag !== 'deferred') return assert.fail('Expected deferred disposition')
        assert.strictEqual(Duration.toMillis(decoded.decision.pollAfterMs ?? Duration.zero), 2.5)
        assert.deepStrictEqual(yield* Schema.encodeEffect(Executor.Disposition)(decoded), wire)
      }),
  )
})
