import { assert, describe, it } from '@effect/vitest'

import * as Outcome from 'effect-harness/durable/workflow/Outcome'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'

describe('OutcomeSchema', () => {
  it.effect(
    'owned failure guards refine decoded tags while the classifier handles opaque extensions',
    () =>
      Effect.gen(function* () {
        const decoded = yield* Schema.decodeEffect(Outcome.Failed)({
          _tag: 'Failed',
          status: 'failed',
          error: { message: 'failed work' },
        })
        assert.isTrue(Outcome.isFailedOutcome(decoded))
        assert.isTrue(Outcome.isFailed(decoded))
        assert.isFalse(Outcome.isFailedOutcome({ status: 'failed', arbitrary: true }))
        assert.isTrue(Outcome.isFailed({ status: 'failed', arbitrary: true }))
        assert.isFalse(Outcome.isFailedOutcome({ ...decoded, error: { message: 1 } }))
        assert.isFalse(Outcome.isFailedOutcome(undefined))
      }),
  )

  it('classifies explicit extension envelopes without interpreting arbitrary custom results', () => {
    assert.isTrue(
      Outcome.isFailed({ receipt: { status: 'failed' }, execution: { arbitrary: true } }),
    )
    assert.isFalse(Outcome.isFailed({ status: 'completed', receipt: { status: 'failed' } }))
    assert.isFalse(Outcome.isFailed({ status: 1, receipt: { status: 'failed' } }))
    assert.isTrue(Outcome.isFailed({ status: null, receipt: { status: 'aborted' } }))
    assert.isFalse(Outcome.isFailed({ arbitrary: { status: 'failed' } }))
    assert.strictEqual(
      Outcome.classifyOrUndefined({
        status: 'faulted',
        detail: 42,
        error: { message: 'ignored' },
      })?.message,
      undefined,
    )
  })
})
