import { assert, describe, it } from '@effect/vitest'
import * as Protocol from 'effect-harness/provider-claude-code/Protocol'

describe('ProtocolGuard', () => {
  it('native CLI schema guards validate canonical data without brands or opaque input coercion', () => {
    const usage = { input_tokens: 0, output_tokens: 1 }
    assert.isTrue(Protocol.isUsage(usage))
    assert.isFalse(Protocol.isUsage({ input_tokens: -1 }))
    assert.isFalse(Protocol.isUsage({ output_tokens: undefined }))
    const input = { type: 'not-a-native-block', nested: [null, { signature: 42 }] }
    const block = { type: 'tool_use', id: 'call', name: 'tool', input }
    assert.isTrue(Protocol.isBlock(block))
    assert.strictEqual(block.input, input)
    assert.isFalse(Protocol.isBlock({ ...block, id: '' }))
    assert.isFalse(Protocol.isBlock({ type: 'thinking', thinking: 'text', signature: undefined }))
    const event = { type: 'system', subtype: 'init', tools: [] }
    assert.isTrue(Protocol.isEvent(event))
    assert.isFalse(Protocol.isEvent({ ...event, tools: [1] }))
    assert.isFalse(Protocol.isEvent({ type: 'invented-event' }))
    assert.deepStrictEqual(Object.keys(block), ['type', 'id', 'name', 'input'])
    assert.strictEqual(JSON.stringify(event), '{"type":"system","subtype":"init","tools":[]}')
  })
})
