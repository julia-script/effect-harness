import { assert, describe, it } from '@effect/vitest'
import * as Prompt from 'effect/ai/Prompt'

import * as ResponseAccumulator from 'effect-harness/ResponseAccumulator'

describe('ResponseAccumulatorDelta', () => {
  it('preserves ordered text/reasoning/options changes and exact newly added native parts', () => {
    const previous = Prompt.assistantMessage({
      content: [Prompt.textPart({ text: 'a' }), Prompt.reasoningPart({ text: 'r' })],
    })
    const options = { harness: { partial: true } }
    const added = Prompt.toolCallPart({
      id: 'call',
      name: 'tool',
      params: { value: 1 },
      providerExecuted: false,
    })
    const next = Prompt.assistantMessage({
      content: [
        Prompt.textPart({ text: 'ab', options }),
        Prompt.reasoningPart({ text: 'rs' }),
        added,
      ],
    })
    const changes = ResponseAccumulator.delta(previous, next)
    assert.deepStrictEqual(changes, [
      { type: 'append', path: ['content', 0, 'text'], value: 'b' },
      { type: 'set', path: ['content', 0, 'options'], value: options },
      { type: 'append', path: ['content', 1, 'text'], value: 's' },
      { type: 'set', path: ['content', 2], value: added },
    ])
    assert.strictEqual(changes[1]?.value, options)
    assert.strictEqual(changes[3]?.value, added)
  })

  it('tool parameters append only with matching identifiers, names and execution ownership', () => {
    const call = Prompt.toolCallPart({
      id: 'call',
      name: 'tool',
      params: '{',
      providerExecuted: false,
    })
    const fields = {
      id: call.id,
      name: call.name,
      params: call.params,
      providerExecuted: call.providerExecuted,
    }
    const previous = Prompt.assistantMessage({ content: [call] })
    const params = { value: 1 }
    const object = Prompt.assistantMessage({
      content: [Prompt.toolCallPart({ ...fields, params })],
    })
    const set = ResponseAccumulator.delta(previous, object)
    assert.deepStrictEqual(set, [{ type: 'set', path: ['content', 0, 'params'], value: params }])
    assert.strictEqual(set[0]?.value, params)
    const text = Prompt.assistantMessage({
      content: [Prompt.toolCallPart({ ...fields, params: '{"value":' })],
    })
    assert.deepStrictEqual(ResponseAccumulator.delta(previous, text), [
      { type: 'append', path: ['content', 0, 'params'], value: '"value":' },
    ])
    for (const changed of [
      Prompt.toolCallPart({ ...fields, id: 'other' }),
      Prompt.toolCallPart({ ...fields, name: 'other' }),
      Prompt.toolCallPart({ ...fields, providerExecuted: true }),
    ]) {
      const next = Prompt.assistantMessage({ content: [changed] })
      assert.deepStrictEqual(ResponseAccumulator.delta(previous, next), [
        { type: 'set', path: [], value: next },
      ])
    }
  })

  it('first incompatible part discards accumulated deltas and does not read later array getters', () => {
    let reads = 0
    const content = [Prompt.textPart({ text: 'ab' }), Prompt.textPart({ text: 'replacement' })]
    Object.defineProperty(content, '2', {
      enumerable: true,
      get: () => {
        reads++
        return Prompt.textPart({ text: 'later' })
      },
    })
    const next = Prompt.assistantMessage({ content })
    const previous = Prompt.assistantMessage({
      content: [Prompt.textPart({ text: 'a' }), Prompt.reasoningPart({ text: 'r' })],
    })
    const changes = ResponseAccumulator.delta(previous, next)
    assert.strictEqual(reads, 0)
    assert.deepStrictEqual(changes, [{ type: 'set', path: [], value: next }])
    assert.strictEqual(changes[0]?.value, next)
  })

  it('preserves sparse public native-array failures rather than silently treating in-bounds holes as absence', () => {
    const sparse: Array<Prompt.AssistantMessagePart> = []
    sparse.length = 1
    const previous = Prompt.assistantMessage({ content: sparse })
    const next = Prompt.assistantMessage({ content: [Prompt.textPart({ text: 'a' })] })
    assert.throws(() => ResponseAccumulator.delta(previous, next), TypeError)
    assert.strictEqual(0 in sparse, false)
  })
})
