import { describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Duration from 'effect/Duration'

import * as TestSchema from 'effect/testing/TestSchema'

import * as Executor from 'effect-harness/Executor'

describe('ExecutorDisposition', () => {
  it.effect(
    'tagged deferred disposition keeps fractional duration encoding and opaque native handle fields',
    () =>
      Effect.gen(function* () {
        const wire = {
          _tag: 'deferred' as const,
          decision: { handle: { providerOpaque: ['a', null, 2] }, pollAfterMs: 2.5 },
        }
        const expected = {
          _tag: 'deferred' as const,
          decision: {
            handle: { providerOpaque: ['a', null, 2] },
            pollAfterMs: Duration.millis(2.5),
          },
        }
        const codec = new TestSchema.Asserts(Executor.Disposition)
        yield* codec.decoding().succeedEffect(wire, expected)
        yield* codec.encoding().succeedEffect(expected, wire)
      }),
  )
})
