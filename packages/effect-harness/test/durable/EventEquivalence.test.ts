import { assert, describe, it } from '@effect/vitest'

import * as Prompt from 'effect/ai/Prompt'

import * as Event from 'effect-harness/durable/Event'

describe('EventEquivalence', () => {
  it('uses native part equivalence for content replacements with reordered option keys', () => {
    const before = Prompt.assistantMessage({
      content: [Prompt.textPart({ text: 'same', options: { a: 1, b: 2 } })],
    })
    const next = Prompt.assistantMessage({
      content: [Prompt.textPart({ text: 'same', options: { b: 2, a: 1 } })],
    })
    assert.deepStrictEqual(
      Event.messageChanges(
        [['set', ['docs', 'harness.live', 'generation', 'message', 'content'], next.content]],
        before,
        next,
      ),
      [],
    )
  })
})
