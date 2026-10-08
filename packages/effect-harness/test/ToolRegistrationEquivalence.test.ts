import { assert, describe, it } from '@effect/vitest'

import * as Prompt from 'effect/ai/Prompt'

import * as Json from 'effect-harness/Json'

import * as ResponseAccumulator from 'effect-harness/ResponseAccumulator'

describe('ToolRegistrationEquivalence', () => {
  it('schema equivalence preserves JSON key order independence and native opaque parameters without encoding', () => {
    assert.strictEqual(
      Json.equals({ nested: { a: 1, b: [2, 3] } }, { nested: { b: [2, 3], a: 1 } }),
      true,
    )
    let encoded = 0
    const params = {
      toJSON: () => {
        encoded++
        throw new Error('must remain opaque')
      },
      payload: new Uint8Array([1, 2]),
    }
    const before = Prompt.assistantMessage({
      content: [Prompt.toolCallPart({ id: 'c', name: 'native', params, providerExecuted: true })],
      options: { native: { b: 2, a: 1 } },
    })
    const after = Prompt.assistantMessage({
      content: [Prompt.toolCallPart({ id: 'c', name: 'native', params, providerExecuted: true })],
      options: { native: { a: 1, b: 2 } },
    })
    assert.deepStrictEqual(ResponseAccumulator.delta(before, after), [])
    assert.strictEqual(encoded, 0)
    assert.strictEqual(
      after.content[0]?.type === 'tool-call' ? after.content[0].params : undefined,
      params,
    )
    const changed = Prompt.assistantMessage({
      content: [
        Prompt.toolCallPart({
          id: 'c',
          name: 'native',
          params: new Date(0),
          providerExecuted: true,
        }),
      ],
    })
    assert.strictEqual(ResponseAccumulator.delta(undefined, changed)[0]?.value, changed)
  })
})
