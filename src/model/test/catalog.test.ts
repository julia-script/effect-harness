/**
 * Catalog thinking levels: which levels a model takes, as pi-ai's `getSupportedThinkingLevels`,
 * and the effort a requested level sends.
 */
import { describe, expect, test } from 'bun:test'
import { effort, thinkingLevels } from '../internal/catalog.ts'

/** Codex's gpt-5.5: only the levels that differ from their own names are mapped. */
const sparse = { id: 'gpt-5.5', reasoning: true, efforts: { xhigh: 'xhigh', minimal: 'low' } }
/** An adaptive Anthropic model: no `off`, and `xhigh` and `max` mapped. */
const adaptive = {
  id: 'claude',
  reasoning: true,
  efforts: { off: null, xhigh: 'xhigh', max: 'max' },
}

describe('catalog thinking levels', () => {
  test('a model takes its unmapped levels by name, and xhigh and max only where mapped', () => {
    expect(thinkingLevels(sparse)).toEqual(['off', 'minimal', 'low', 'medium', 'high', 'xhigh'])
    expect(thinkingLevels(adaptive)).toEqual(['minimal', 'low', 'medium', 'high', 'xhigh', 'max'])
    expect(thinkingLevels({ id: 'plain' })).toEqual(['off'])
  })

  test('a level the model takes sends its own name unless mapped', () => {
    expect(effort(sparse, 'medium')).toBe('medium')
    expect(effort(sparse, 'minimal')).toBe('low')
    expect(effort(adaptive, 'medium')).toBe('medium')
  })

  test('a level the model lacks falls to the nearest lower one it takes', () => {
    expect(effort(sparse, 'max')).toBe('xhigh')
    expect(effort({ id: 'm', reasoning: true }, 'xhigh')).toBe('high')
    expect(effort(undefined, 'high')).toBeUndefined()
  })
})
