import { assert, describe, it } from '@effect/vitest'

import * as PromptPreparation from 'effect-harness/PromptPreparation'

describe('PromptPreparationPatch', () => {
  it('section dictionaries preserve own prototype-like keys and first-seen order', () => {
    const desired = new Map([
      ['__proto__', 'alpha'],
      ['constructor', 'beta'],
    ])
    const patches = PromptPreparation.planSections(new Map(), desired)
    assert.deepStrictEqual(PromptPreparation.planSections(desired)(new Map()), patches)
    assert.strictEqual(patches.length, 1)
    const first = patches[0]
    assert.ok(first)
    assert.strictEqual(Object.getPrototypeOf(first), Object.prototype)
    assert.isTrue(Object.hasOwn(first, '__proto__'))
    assert.strictEqual(first['__proto__'], 'alpha')
    assert.strictEqual(JSON.stringify(first), '{"__proto__":"alpha","constructor":"beta"}')
    assert.deepStrictEqual(
      [...PromptPreparation.replaySections(patches.map((sections) => ({ sections })))],
      [...desired],
    )
  })
})
