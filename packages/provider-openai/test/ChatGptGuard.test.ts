import { assert, describe, it } from '@effect/vitest'
import * as ChatGpt from '@effect-harness/provider-openai/ChatGpt'

describe('ChatGptGuard', () => {
  it('account model guard validates the native model DTO without adding metadata', () => {
    const model = { slug: 'model', display_name: 'Model', visibility: 'public' }
    assert.isTrue(ChatGpt.isModel(model))
    assert.isFalse(ChatGpt.isModel({ ...model, slug: '' }))
    assert.isFalse(ChatGpt.isModel({ ...model, display_name: undefined }))
    assert.isFalse(ChatGpt.isModel({ ...model, visibility: null }))
    assert.deepStrictEqual(Object.keys(model), ['slug', 'display_name', 'visibility'])
    assert.strictEqual(
      JSON.stringify(model),
      '{"slug":"model","display_name":"Model","visibility":"public"}',
    )
  })
})
