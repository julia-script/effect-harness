import { assert, describe, it } from '@effect/vitest'
import * as NativePrompt from 'effect/ai/Prompt'
import * as Context from '../../src/Context.ts'
import * as Prompt from '../../src/Prompt.ts'

const system = (content: string) => NativePrompt.systemMessage({ content })
const user = NativePrompt.userMessage({ content: [NativePrompt.textPart({ text: 'question' })] })

describe('native prompt projection', () => {
  it('retains plain system content in source order and appends current sections once', () => {
    const base = system('base')
    const later = system('additional instructions')
    const original = [base, user, later]
    const result = Prompt.toPrompt(original, new Map([['current', 'effective section']]))
    assert.deepEqual(
      result.content.map((message) => message.role),
      ['system', 'system', 'system', 'user'],
    )
    assert.deepEqual(
      result.content
        .filter((message) => message.role === 'system')
        .map((message) => message.content),
      ['base', 'additional instructions', 'effective section'],
    )
    assert.strictEqual(result.content[0], base)
    assert.strictEqual(result.content[1], later)
    assert.strictEqual(result.content[3], user)
    assert.deepEqual(original, [base, user, later])
  })

  it('identifies managed patches by identity, retaining identical plain content and native options', () => {
    const plain = NativePrompt.systemMessage({
      content: 'same text',
      options: { anthropic: { cacheControl: { type: 'ephemeral', ttl: '1h' } } },
    })
    const encodedPatch = system('same text')
    const result = Prompt.toPrompt([plain, user, encodedPatch], new Map(), {
      managedSystemMessages: [encodedPatch, user],
    })
    assert.deepEqual(result.content, [plain, user])
    assert.strictEqual(result.content[0]?.options, plain.options)
  })

  it('replays updates, removals and delete/reinsert section order without old encoded patch text', () => {
    const view = Context.derive([
      { id: 1, messages: [system('base')] },
      {
        id: 2,
        system: { sections: { first: 'old first', removed: 'obsolete', last: 'last' } },
        messages: [system('encoded first patch')],
      },
      { id: 3, messages: [user] },
      {
        id: 4,
        system: { sections: { first: 'updated first', removed: null } },
        messages: [system('encoded update patch')],
      },
      {
        id: 5,
        system: { sections: { first: null } },
        messages: [system('encoded remove patch')],
      },
      {
        id: 6,
        system: { sections: { first: 'reinserted first' } },
        messages: [system('encoded reinsert patch')],
      },
    ])
    const patches = view.entries.flatMap((entry) =>
      entry.system === undefined ? [] : [entry.system],
    )
    const sections = Prompt.replaySections(patches)
    assert.deepEqual(
      [...sections],
      [
        ['last', 'last'],
        ['first', 'reinserted first'],
      ],
    )
    const managedSystemMessages = view.entries.flatMap((entry, index) =>
      entry.system === undefined ? [] : (view.contributions[index] ?? []),
    )
    const result = Prompt.toPrompt(view.messages, sections, { managedSystemMessages })
    assert.deepEqual(result.content, [system('base'), system('last\n\nreinserted first'), user])
  })

  it('uses the edited managed contributions instead of stale entry messages', () => {
    const replacement = system('replacement encoded patch')
    const view = Context.derive([
      { id: 1, messages: [system('plain')] },
      { id: 2, messages: [system('old patch')], system: { sections: { current: 'current' } } },
      { id: 3, edits: [{ target: 2, action: 'replace', messages: [replacement] }] },
      { id: 4, messages: [user] },
    ])
    const result = Prompt.toPrompt(view.messages, new Map([['current', 'current']]), {
      managedSystemMessages: view.entries.flatMap((entry, index) =>
        entry.system === undefined ? [] : (view.contributions[index] ?? []),
      ),
    })
    assert.deepEqual(result.content, [system('plain'), system('current'), user])
  })

  it('preserves a reordered desired section baseline after a head marker', () => {
    const view = Context.derive([
      {
        id: 1,
        system: { sections: { first: 'old', second: 'old second' } },
        messages: [system('old encoded')],
      },
      {
        id: 2,
        head: 1,
        messages: [
          NativePrompt.userMessage({ content: [NativePrompt.textPart({ text: 'summary' })] }),
        ],
      },
      { id: 3, messages: [user] },
    ])
    const desired = new Map([
      ['second', 'new second'],
      ['first', 'new first'],
    ])
    const planned = Prompt.plan(view, desired, [])
    assert.deepEqual(planned.edits, [{ target: 1, action: 'omit' }])
    assert.deepEqual(Prompt.replaySections(planned.patches), desired)
    const result = Prompt.toPrompt(view.messages, desired, {
      managedSystemMessages: view.contributions[1],
    })
    assert.deepEqual(
      result.content
        .filter((message) => message.role === 'system')
        .map((message) => message.content),
      ['new second\n\nnew first'],
    )
  })

  it('retains native assistant and tool result structures, bytes and message options', () => {
    const image = NativePrompt.userMessage({
      content: [NativePrompt.filePart({ mediaType: 'image/png', data: new Uint8Array([1, 2]) })],
    })
    const assistant = NativePrompt.assistantMessage({
      content: [
        NativePrompt.toolCallPart({
          id: 'call',
          name: 'read',
          params: {},
          providerExecuted: false,
        }),
      ],
    })
    const tool = NativePrompt.toolMessage({
      content: [
        NativePrompt.toolResultPart({
          id: 'call',
          name: 'read',
          result: 'answer',
          isFailure: false,
          providerExecuted: false,
        }),
      ],
    })
    const result = Prompt.toPrompt([image, assistant, system('late'), tool], new Map())
    assert.deepEqual(
      result.content.map((message) => message.role),
      ['system', 'user', 'assistant', 'tool'],
    )
    assert.strictEqual(result.content[1], image)
    assert.strictEqual(result.content[2], assistant)
    assert.strictEqual(result.content[3], tool)
    assert.deepEqual(Prompt.toPrompt([], new Map()).content, [])
  })

  it('empty named section values do not add delimiters or an empty system block', () => {
    assert.deepEqual(Prompt.toPrompt([], new Map([['empty', '']])).content, [])
    assert.deepEqual(
      Prompt.toPrompt(
        [],
        new Map([
          ['empty', ''],
          ['value', 'present'],
          ['also-empty', ''],
        ]),
      ).content,
      [system('present')],
    )
  })
})
