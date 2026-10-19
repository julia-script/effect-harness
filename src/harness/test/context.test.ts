/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * Context derivation and system prompt planning, ported from pi's `harness-context.test.ts` and
 * `harness-prompt.test.ts` cases that need no model.
 */
import { describe, expect, test } from 'bun:test'
import type { EntryRecord } from '../../storage/schema/Entry.ts'
import { ConversationId, EntryId } from '../../storage/schema/Ids.ts'
import { derive } from '../internal/context.ts'
import { planSystemEntries } from '../internal/prompt.ts'

const conversationId = ConversationId.make(1)

const entry = (id: number, fields: Partial<EntryRecord> = {}): EntryRecord => ({
  id: EntryId.make(id),
  conversationId,
  kind: 'note',
  ...fields,
})

const user = (text: string) => ({ role: 'user', content: text, timestamp: 0 })

const system = (sections: Record<string, string | null>, tools: ReadonlyArray<string> = []) => ({
  role: 'system',
  content: '',
  sections,
  ...(tools.length === 0
    ? {}
    : { toolsAdded: tools.map((name) => ({ name, description: name, parameters: {} })) }),
  timestamp: 0,
})

const assistant = (calls: ReadonlyArray<string>, stopReason = 'toolUse') => ({
  role: 'assistant',
  content: calls.map((id) => ({ type: 'toolCall', id, name: 'read', arguments: {} })),
  api: 'faux',
  provider: 'faux',
  model: 'faux-1',
  usage: {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  },
  stopReason,
  timestamp: 0,
})

const result = (id: string) => ({
  role: 'toolResult',
  toolCallId: id,
  toolName: 'read',
  content: [],
  isError: false,
  timestamp: 0,
})

describe('context', () => {
  test('leads with the system prompt written after the input', () => {
    const view = derive(undefined, [
      entry(1, { model: [user('hi')] }),
      entry(2, { kind: 'pi.system', model: [system({ a: 'A' })] }),
    ])
    expect(view.messages.map((message) => message.role)).toEqual(['system', 'user'])
  })

  test('orders results after their calls, synthesizes missing ones, drops strays', () => {
    const view = derive(undefined, [
      entry(1, { model: [assistant(['c1', 'c2'])] }),
      entry(2, { model: [result('c2')] }),
      entry(3, { model: [result('stray')] }),
    ])
    expect(view.messages).toMatchObject([
      { role: 'assistant' },
      {
        role: 'toolResult',
        toolCallId: 'c1',
        isError: true,
        details: { reason: 'missing_result' },
      },
      { role: 'toolResult', toolCallId: 'c2', isError: false },
    ])
  })

  test('applies the newest edit per target and leaves out failed responses', () => {
    const view = derive(undefined, [
      entry(1, { model: [user('a')] }),
      entry(2, { model: [user('b')] }),
      entry(3, { model: [assistant([], 'error')] }),
      entry(4, { edits: [{ target: EntryId.make(1), action: 'omit' }] }),
      entry(5, { edits: [{ target: EntryId.make(2), action: 'replace', messages: [user('B')] }] }),
    ])
    expect(view.messages).toMatchObject([{ content: 'B' }])
  })

  test('starts at the head marker', () => {
    const marker = { ...entry(3, { model: [user('summary')] }), head: EntryId.make(2) }
    const view = derive(marker, [entry(2, { model: [user('kept')] }), marker])
    expect(view.entries.map((each) => Number(each.id))).toEqual([3, 2])
    expect(view.messages).toMatchObject([{ content: 'summary' }, { content: 'kept' }])
  })
})

describe('system prompt planning', () => {
  const tool = { name: 'read', description: 'read', parameters: {} }

  test('writes a baseline with every section and tool first, then nothing while unchanged', () => {
    const desired = new Map([['a', 'A']])
    const empty = derive(undefined, [entry(1, { model: [user('hi')] })])
    expect(planSystemEntries(empty, desired, [tool], 0)).toMatchObject([
      { kind: 'pi.system', model: [{ sections: { a: 'A' }, toolsAdded: [{ name: 'read' }] }] },
    ])
    const shown = derive(undefined, [
      entry(1, { model: [user('hi')] }),
      entry(2, { kind: 'pi.system', model: [system({ a: 'A' }, ['read'])] }),
    ])
    expect(planSystemEntries(shown, desired, [tool], 0)).toEqual([])
  })

  test('patches changed sections, and re-adds them all when the order changes', () => {
    const shown = derive(undefined, [
      entry(1, { kind: 'pi.system', model: [system({ a: 'A', b: 'B' })] }),
    ])
    const patch = new Map([
      ['a', 'A2'],
      ['b', 'B'],
    ])
    expect(planSystemEntries(shown, patch, [], 0)).toMatchObject([
      { model: [{ sections: { a: 'A2' } }] },
    ])
    const reordered = new Map([
      ['b', 'B'],
      ['a', 'A'],
    ])
    expect(planSystemEntries(shown, reordered, [], 0)).toMatchObject([
      { model: [{ sections: { a: null, b: null } }] },
      { model: [{ sections: { b: 'B', a: 'A' } }] },
    ])
  })

  test('after a head marker, writes a complete baseline omitting older system entries', () => {
    const marker = { ...entry(3, { model: [user('summary')] }), head: EntryId.make(1) }
    const view = derive(marker, [
      entry(1, { kind: 'pi.system', model: [system({ a: 'A' })] }),
      entry(2, { model: [user('kept')] }),
      marker,
    ])
    expect(planSystemEntries(view, new Map([['a', 'A']]), [], 0)).toMatchObject([
      { model: [{ sections: { a: 'A' } }], edits: [{ target: EntryId.make(1), action: 'omit' }] },
    ])
  })
})
