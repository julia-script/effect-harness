import { assertFailure } from '@effect/vitest/utils'

import { describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Record from 'effect-harness/durable/Record'

import * as View from 'effect-harness/durable/View'

const view: View.Value = {
  conversation: { id: Record.ROOT_CONVERSATION_ID },
  entries: [],
  docs: {},
}

describe('ViewReplay', () => {
  it.effect('returns typed replay failures for paths, deletions and splice targets', () =>
    Effect.sync(() => {
      const cases = [
        {
          op: ['set', ['docs', 'absent', 'field'], 1],
          cause: new TypeError('Invalid view operation path'),
        },
        { op: ['delete', []], cause: new TypeError('Invalid view operation') },
        {
          op: ['splice', ['conversation'], 0, 1, []],
          cause: new TypeError('View splice requires an array'),
        },
      ] as const
      for (const sample of cases)
        assertFailure(
          View.apply(view, [sample.op] as unknown as ReadonlyArray<View.Op>),
          new View.ViewOperationError({ message: 'Invalid view operation', cause: sample.cause }),
        )
    }),
  )
})
