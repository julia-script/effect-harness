// effect-review-allow P9-namespace-alias-equals-module: effect-harness/auth/Duration and effect/Duration both bind Duration; AuthDuration distinguishes the concepts.
import * as AuthDuration from 'effect-harness/auth/Duration'
import * as Duration from 'effect/Duration'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'

describe('Duration', () => {
  it.effect(
    'duration normalization retains exact bigint nanos and detaches foreign native values',
    () =>
      Effect.gen(function* () {
        const nanos = 9007199254740993123456789n
        const input = {
          '~effect/Duration': '~effect/Duration',
          value: { _tag: 'Nanos', nanos },
        } as const
        const normalized = yield* AuthDuration.fromInput(
          input as unknown as Duration.Input,
          'Invalid duration',
        )
        assert.isFalse(Object.is(normalized, input))
        assert.deepStrictEqual(normalized.value, { _tag: 'Nanos', nanos })
        assert.deepStrictEqual(
          (yield* AuthDuration.fromInput(Duration.nanos(nanos), 'Invalid duration')).value,
          { _tag: 'Nanos', nanos },
        )
        assert.strictEqual(
          Duration.toMillis(
            yield* AuthDuration.fromInput({ milliseconds: 0.25 }, 'Invalid duration'),
          ),
          0.25,
        )
      }),
  )
})
