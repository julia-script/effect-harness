import * as Schema from 'effect/Schema'

import * as Identity from 'effect-harness/Identity'

import { assert, describe, it } from '@effect/vitest'

import * as Prompt from 'effect/ai/Prompt'

import * as Transcript from 'effect-harness/Transcript'

import * as PromptPreparation from 'effect-harness/PromptPreparation'

describe('PromptPreparationNormalization', () => {
  const entryIdUnsafe = Schema.decodeSync(Identity.EntryId)
  const system = (content: string) => Prompt.systemMessage({ content })
  const user = Prompt.userMessage({ content: [Prompt.textPart({ text: 'question' })] })

  describe('native prompt projection', () => {
    it('retains plain system content in source order and appends current sections once', () => {
      const base = system('base')
      const later = system('additional instructions')
      const original = [base, user, later]
      const result = PromptPreparation.toPrompt(
        original,
        new Map([['current', 'effective section']]),
      )
      assert.deepStrictEqual(
        result.content.map((message) => message.role),
        ['system', 'system', 'system', 'user'],
      )
      assert.deepStrictEqual(
        result.content
          .filter((message) => message.role === 'system')
          .map((message) => message.content),
        ['base', 'additional instructions', 'effective section'],
      )
      assert.strictEqual(result.content[0], base)
      assert.strictEqual(result.content[1], later)
      assert.strictEqual(result.content[3], user)
      assert.deepStrictEqual(original, [base, user, later])
    })

    it('identifies managed patches by identity, retaining identical plain content and native options', () => {
      const plain = Prompt.systemMessage({
        content: 'same text',
        options: { anthropic: { cacheControl: { type: 'ephemeral', ttl: '1h' } } },
      })
      const encodedPatch = system('same text')
      const result = PromptPreparation.toPrompt([plain, user, encodedPatch], new Map(), {
        managedSystemMessages: [encodedPatch, user],
      })
      assert.deepStrictEqual(result.content, [plain, user])
      assert.strictEqual(result.content[0]?.options, plain.options)
    })

    it('replays updates, removals and delete/reinsert section order without old encoded patch text', () => {
      const view = Transcript.derive([
        { id: entryIdUnsafe(1), messages: [system('base')] },
        {
          id: entryIdUnsafe(2),
          system: { sections: { first: 'old first', removed: 'obsolete', last: 'last' } },
          messages: [system('encoded first patch')],
        },
        { id: entryIdUnsafe(3), messages: [user] },
        {
          id: entryIdUnsafe(4),
          system: { sections: { first: 'updated first', removed: null } },
          messages: [system('encoded update patch')],
        },
        {
          id: entryIdUnsafe(5),
          system: { sections: { first: null } },
          messages: [system('encoded remove patch')],
        },
        {
          id: entryIdUnsafe(6),
          system: { sections: { first: 'reinserted first' } },
          messages: [system('encoded reinsert patch')],
        },
      ])
      const patches = view.entries.flatMap((entry) =>
        entry.system === undefined ? [] : [entry.system],
      )
      const sections = PromptPreparation.replaySections(patches)
      assert.deepStrictEqual(
        [...sections],
        [
          ['last', 'last'],
          ['first', 'reinserted first'],
        ],
      )
      const managedSystemMessages = view.entries.flatMap((entry, index) =>
        entry.system === undefined ? [] : (view.contributions[index] ?? []),
      )
      const result = PromptPreparation.toPrompt(view.messages, sections, { managedSystemMessages })
      assert.deepStrictEqual(result.content, [
        system('base'),
        system('last\n\nreinserted first'),
        user,
      ])
    })

    it('uses the edited managed contributions instead of stale entry messages', () => {
      const replacement = system('replacement encoded patch')
      const view = Transcript.derive([
        { id: entryIdUnsafe(1), messages: [system('plain')] },
        {
          id: entryIdUnsafe(2),
          messages: [system('old patch')],
          system: { sections: { current: 'current' } },
        },
        {
          id: entryIdUnsafe(3),
          edits: [{ target: entryIdUnsafe(2), _tag: 'replace', messages: [replacement] }],
        },
        { id: entryIdUnsafe(4), messages: [user] },
      ])
      const result = PromptPreparation.toPrompt(view.messages, new Map([['current', 'current']]), {
        managedSystemMessages: view.entries.flatMap((entry, index) =>
          entry.system === undefined ? [] : (view.contributions[index] ?? []),
        ),
      })
      assert.deepStrictEqual(result.content, [system('plain'), system('current'), user])
    })

    it('preserves a reordered desired section baseline after a head marker', () => {
      const view = Transcript.derive([
        {
          id: entryIdUnsafe(1),
          system: { sections: { first: 'old', second: 'old second' } },
          messages: [system('old encoded')],
        },
        {
          id: entryIdUnsafe(2),
          head: entryIdUnsafe(1),
          messages: [Prompt.userMessage({ content: [Prompt.textPart({ text: 'summary' })] })],
        },
        { id: entryIdUnsafe(3), messages: [user] },
      ])
      const desired = new Map([
        ['second', 'new second'],
        ['first', 'new first'],
      ])
      const planned = PromptPreparation.plan(view, desired, [])
      assert.deepStrictEqual(planned.edits, [{ target: entryIdUnsafe(1), _tag: 'omit' }])
      assert.deepStrictEqual(PromptPreparation.replaySections(planned.patches), desired)
      const result = PromptPreparation.toPrompt(view.messages, desired, {
        managedSystemMessages: view.contributions[1],
      })
      assert.deepStrictEqual(
        result.content
          .filter((message) => message.role === 'system')
          .map((message) => message.content),
        ['new second\n\nnew first'],
      )
    })

    it('retains native assistant and tool result structures, bytes and message options', () => {
      const image = Prompt.userMessage({
        content: [Prompt.filePart({ mediaType: 'image/png', data: new Uint8Array([1, 2]) })],
      })
      const assistant = Prompt.assistantMessage({
        content: [
          Prompt.toolCallPart({
            id: 'call',
            name: 'read',
            params: {},
            providerExecuted: false,
          }),
        ],
      })
      const tool = Prompt.toolMessage({
        content: [
          Prompt.toolResultPart({
            id: 'call',
            name: 'read',
            result: 'answer',
            isFailure: false,
            providerExecuted: false,
          }),
        ],
      })
      const result = PromptPreparation.toPrompt([image, assistant, system('late'), tool], new Map())
      assert.deepStrictEqual(
        result.content.map((message) => message.role),
        ['system', 'user', 'assistant', 'tool'],
      )
      assert.strictEqual(result.content[1], image)
      assert.strictEqual(result.content[2], assistant)
      assert.strictEqual(result.content[3], tool)
      assert.deepStrictEqual(PromptPreparation.toPrompt([], new Map()).content, [])
    })

    it('empty named section values do not add delimiters or an empty system block', () => {
      assert.deepStrictEqual(PromptPreparation.toPrompt([], new Map([['empty', '']])).content, [])
      assert.deepStrictEqual(
        PromptPreparation.toPrompt(
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
})
