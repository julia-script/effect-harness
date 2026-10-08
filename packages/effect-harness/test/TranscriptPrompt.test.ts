import * as Option from 'effect/Option'

import * as Schema from 'effect/Schema'

import * as Identity from 'effect-harness/Identity'

import * as Layer from 'effect/Layer'

import * as Context from 'effect/Context'

import * as LanguageModel from 'effect/ai/LanguageModel'

import * as Stream from 'effect/Stream'

import * as Executor from 'effect-harness/Executor'

import * as Model from 'effect-harness/Model'

import * as Registry from 'effect-harness/Registry'

import * as Invocation from 'effect-harness/Invocation'

import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Prompt from 'effect/ai/Prompt'

import * as AiError from 'effect/ai/AiError'

import * as Transcript from 'effect-harness/Transcript'

import * as PromptPreparation from 'effect-harness/PromptPreparation'

import * as Compaction from 'effect-harness/Compaction'

import * as Agent from 'effect-harness/Agent'

import * as Usage from 'effect-harness/Usage'

import { HookError, HookFailureError } from 'effect-harness/HookError'

import * as ToolResult from 'effect-harness/ToolResult'

describe('TranscriptPrompt', () => {
  const entryIdUnsafe = Schema.decodeSync(Identity.EntryId)
  const user = (text: string) => Prompt.userMessage({ content: [Prompt.textPart({ text })] })
  const assistant = (text: string) =>
    Prompt.assistantMessage({ content: [Prompt.textPart({ text })] })
  const call = (id: string) =>
    Prompt.toolCallPart({ id, name: 'test', params: { x: 1 }, providerExecuted: false })
  const result = (id: string, value: string) =>
    Prompt.toolMessage({
      content: [
        Prompt.toolResultPart({
          id,
          name: 'test',
          result: value,
          isFailure: false,
          providerExecuted: false,
        }),
      ],
    })

  describe('canonical context, prompt protocol and cuts', () => {
    it('invalid output contributes generic feedback while edits and other failures retain their semantics', () => {
      const failed = {
        id: entryIdUnsafe(1),
        messages: [assistant('incomplete response')],
        status: 'error' as const,
        error: new AiError.AiError({
          module: 'LanguageModel',
          method: 'streamText',
          reason: new AiError.InvalidOutputError({ description: 'Unavailable rejected response' }),
        }),
      }
      const view = Transcript.derive([failed])
      assert.strictEqual(view.entries[0], failed)
      assert.strictEqual(view.messages.length, 1)
      const feedback = view.messages[0]
      assert.ok(feedback?.role === 'user')
      const text = feedback.content[0]
      assert.ok(text?.type === 'text')
      assert.include(text.text, 'The rejected output is unavailable')
      assert.deepStrictEqual(
        Transcript.derive([
          failed,
          { id: entryIdUnsafe(2), edits: [{ target: entryIdUnsafe(1), _tag: 'omit' }] },
        ]).messages,
        [],
      )
      assert.deepStrictEqual(
        Transcript.derive([
          failed,
          {
            id: entryIdUnsafe(2),
            edits: [{ target: entryIdUnsafe(1), _tag: 'replace', messages: [user('replacement')] }],
          },
        ]).messages,
        [user('replacement')],
      )
      assert.deepStrictEqual(
        Transcript.derive([
          {
            ...failed,
            error: new AiError.AiError({
              module: 'provider',
              method: 'request',
              reason: new AiError.InvalidRequestError({ description: 'Invalid request' }),
            }),
          },
        ]).messages,
        [],
      )
    })
    it.effect(
      'actual preparation respects omitted/deleted managed patches and plain edited replacements',
      () =>
        Effect.gen(function* () {
          const native = yield* LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () => Stream.empty,
          })
          const model = { provider: 'test', modelId: 'model' }
          const catalog = Model.layer([
            {
              ref: model,
              model: native,
              contextWindow: 10000,
              maxOutputTokens: 1000,
              configure: () => Effect.succeed(Context.empty()),
            },
          ])
          const registry = Registry.layer([
            {
              name: 'section',
              sections: [
                {
                  key: 's',
                  render: () =>
                    Effect.fail(
                      new HookError({
                        reason: new HookFailureError({ message: 'section unavailable' }),
                      }),
                    ),
                },
              ],
            },
          ])
          yield* Effect.gen(function* () {
            const executor = yield* Executor.Executor
            const original = {
              id: entryIdUnsafe(1),
              system: { sections: { s: 'old omitted instruction' } },
            }
            for (const edits of [
              [{ target: entryIdUnsafe(1), _tag: 'omit' as const }],
              [
                {
                  target: entryIdUnsafe(1),
                  _tag: 'replace' as const,
                  messages: [Prompt.systemMessage({ content: 'plain replacement' })],
                },
              ],
            ]) {
              const view = Transcript.derive([original, { id: entryIdUnsafe(2), edits }])
              const prepared = yield* executor.prepare({
                state: { model },
                settings: yield* Agent.settings(),
                view,
              })
              const systems = prepared.request.prompt.content
                .filter((message) => message.role === 'system')
                .map((message) => message.content)
              assert.deepStrictEqual(
                systems,
                edits[0]?._tag === 'omit' ? [] : ['plain replacement'],
              )
              assert.deepStrictEqual(Transcript.systemPatches(view), [])
            }
            const deleted = Transcript.derive([
              original,
              { id: entryIdUnsafe(2), system: { sections: { s: null } } },
            ])
            const prepared = yield* executor.prepare({
              state: { model },
              settings: yield* Agent.settings(),
              view: deleted,
            })
            assert.deepStrictEqual(prepared.request.prompt.content, [])
            assert.strictEqual(Transcript.systemPatches(deleted).length, 2)
          }).pipe(
            Effect.provide(Executor.layer.pipe(Layer.provide(Layer.mergeAll(registry, catalog)))),
            Effect.provideService(
              Invocation.Invocation,
              Invocation.Invocation.of({
                cwd: '.',
                report: () => Effect.void,
                progress: () => Effect.void,
              }),
            ),
          )
        }),
    )

    it('system delta estimates exclude deletion/object overhead and count tool declarations separately', () => {
      const toolsAdded = [{ name: 'read', parameters: { type: 'object' } }]
      const patch = {
        sections: { removed: null, first: 'abc', second: 'def' },
        toolsAdded,
        toolsRemoved: ['old'],
      }
      const entry = { id: entryIdUnsafe(1), system: patch }
      const expected =
        Math.ceil('abc\n\ndef'.length / 3.5) +
        Math.ceil(JSON.stringify(toolsAdded).length / 3.5) +
        Math.ceil(JSON.stringify(['old']).length / 3.5)
      assert.strictEqual(Transcript.estimate(Transcript.derive([entry])), expected)
      assert.strictEqual(Transcript.systemMessages({ sections: { gone: null } }).length, 0)
      assert.strictEqual(
        Transcript.estimate(Transcript.derive([entry]), [], () => 7),
        21,
      )
      assert.strictEqual(
        Transcript.estimate(
          Transcript.derive([
            entry,
            { id: entryIdUnsafe(2), edits: [{ target: entryIdUnsafe(1), _tag: 'omit' as const }] },
          ]),
        ),
        0,
      )
      assert.strictEqual(
        Transcript.estimate(
          Transcript.derive([
            entry,
            {
              id: entryIdUnsafe(2),
              edits: [
                {
                  target: entryIdUnsafe(1),
                  _tag: 'replace' as const,
                  messages: [user('replacement')],
                },
              ],
            },
          ]),
        ),
        Math.ceil('replacement'.length / 3.5),
      )
      const measured = {
        id: entryIdUnsafe(2),
        messages: [assistant('answer')],
        usage: { ...Usage.make(), input: 100, output: 20 },
      }
      assert.strictEqual(
        Transcript.estimate(
          Transcript.derive([entry, measured, { id: entryIdUnsafe(3), system: patch }]),
        ),
        120 + expected,
      )
    })

    it.effect(
      'pinned token fallback counts visible text and fixed images independent of encoded bytes',
      () =>
        Effect.gen(function* () {
          assert.strictEqual(Transcript.estimateMessage(user('12345678')), 3)
          for (const bytes of [new Uint8Array([1]), new Uint8Array(40000)]) {
            const encoded = yield* ToolResult.encode({
              content: [
                Prompt.textPart({ text: 'visible' }),
                Prompt.filePart({ mediaType: 'image/png', data: bytes }),
              ],
              details: { private: 'x'.repeat(10000) },
            })
            const tool = Prompt.toolMessage({
              content: [
                Prompt.toolResultPart({
                  id: 'image',
                  name: 'image',
                  result: encoded,
                  isFailure: false,
                  providerExecuted: false,
                }),
              ],
            })
            assert.strictEqual(Transcript.estimateMessage(tool), Math.ceil((7 + 4800) / 3.5))
          }
        }),
    )

    it('newest head first, older head edits count, stopped assistant omitted from model only', () => {
      const view = Transcript.derive([
        { id: entryIdUnsafe(1), messages: [user('old')] },
        {
          id: entryIdUnsafe(2),
          head: entryIdUnsafe(1),
          edits: [{ target: entryIdUnsafe(1), _tag: 'omit' }],
        },
        { id: entryIdUnsafe(3), messages: [assistant('broken')], status: 'error' },
        { id: entryIdUnsafe(4), head: entryIdUnsafe(1), messages: [user('summary')] },
        { id: entryIdUnsafe(5), messages: [user('new')] },
      ])
      assert.deepStrictEqual(
        view.entries.map((entry) => entry.id),
        [4, 1, 3, 5],
      )
      assert.deepStrictEqual(
        view.contributions.map((messages) => messages.length),
        [1, 0, 0, 1],
      )
      assert.strictEqual(Transcript.derive(view.entries, entryIdUnsafe(3)).head, undefined)
    })
    it('orders results before intervening users, chooses first duplicate, drops orphans and synthesizes missing', () => {
      const owner = Prompt.assistantMessage({ content: [call('a'), call('b'), call('c')] })
      const ordered = Transcript.orderToolResults([
        result('orphan', '?'),
        owner,
        user('steer'),
        result('b', 'B'),
        result('a', 'A'),
        result('a', 'duplicate'),
        assistant('next'),
        result('c', 'late'),
      ])
      assert.deepStrictEqual(
        ordered.map((message) => message.role),
        ['assistant', 'tool', 'tool', 'tool', 'user', 'assistant'],
      )
      const results = ordered.flatMap((message) => (message.role === 'tool' ? message.content : []))
      assert.deepStrictEqual(
        results.map((part) => (part.type === 'tool-result' ? part.id : '')),
        ['a', 'b', 'c'],
      )
      assert.deepStrictEqual(results[2]?.type === 'tool-result' ? results[2].result : null, {
        reason: 'missing_result',
        message: 'Tool result unavailable: history ends before this call completed.',
      })
    })
    it('section replacement keeps position and delete/readd moves to end', () => {
      assert.deepStrictEqual(
        [
          ...PromptPreparation.replaySections([
            { sections: { a: '1', b: '2' } },
            { sections: { a: '3' } },
          ]),
        ],
        [
          ['a', '3'],
          ['b', '2'],
        ],
      )
      assert.deepStrictEqual(
        [
          ...PromptPreparation.replaySections([
            { sections: { a: '1', b: '2' } },
            { sections: { a: null } },
            { sections: { a: '3' } },
          ]),
        ],
        [
          ['b', '2'],
          ['a', '3'],
        ],
      )
      assert.deepStrictEqual(
        PromptPreparation.planSections(
          new Map([
            ['a', '1'],
            ['b', '2'],
          ]),
          new Map([
            ['b', '2'],
            ['a', '1'],
          ]),
        ),
        [
          { a: null, b: null },
          { b: '2', a: '1' },
        ],
      )
    })
    it('tools compare structurally, changed declaration removed/readded and order changes full redeclare', () => {
      const a = { name: 'a', parameters: { type: 'object', properties: { n: { type: 'number' } } } }
      const b = { name: 'b', parameters: {} }
      assert.deepStrictEqual(
        PromptPreparation.planTools(
          [a],
          [{ ...a, parameters: { properties: { n: { type: 'number' } }, type: 'object' } }],
        ),
        { toolsRemoved: [], toolsAdded: [] },
      )
      assert.deepStrictEqual(PromptPreparation.planTools([a, b], [b, a]), {
        toolsRemoved: ['a', 'b'],
        toolsAdded: [b, a],
      })
    })
    it('head forces a baseline once even when values match and omits retained prior systems', () => {
      const entries: Array<Transcript.Entry> = [
        { id: entryIdUnsafe(1), system: { sections: { a: 'x' } } },
        { id: entryIdUnsafe(2), messages: [user('u')] },
        { id: entryIdUnsafe(3), head: entryIdUnsafe(1) },
      ]
      const view = Transcript.derive(entries)
      assert.deepStrictEqual(PromptPreparation.plan(view, new Map([['a', 'x']]), []).edits, [
        { target: entryIdUnsafe(1), _tag: 'omit' },
      ])
      assert.strictEqual(
        PromptPreparation.plan(
          Transcript.derive([
            ...entries,
            { id: entryIdUnsafe(4), system: { sections: { a: 'x' } } },
          ]),
          new Map([['a', 'x']]),
          [],
        ).patches.length,
        0,
      )
    })
    it('native system string preserves plain instructions before ordered sections', () => {
      const prompt = PromptPreparation.toPrompt(
        [Prompt.systemMessage({ content: 'legacy' }), user('u')],
        new Map([
          ['a', 'A'],
          ['b', 'B'],
        ]),
      )
      assert.strictEqual(
        prompt.content[0]?.role === 'system' ? prompt.content[0].content : '',
        'legacy',
      )
      assert.strictEqual(
        prompt.content[1]?.role === 'system' ? prompt.content[1].content : '',
        'A\n\nB',
      )
      assert.strictEqual(prompt.content[2]?.role, 'user')
    })
    it('measured context uses appended entry order and estimates only following messages', () => {
      const view = Transcript.derive([
        { id: entryIdUnsafe(1), messages: [user('a')] },
        {
          id: entryIdUnsafe(2),
          messages: [assistant('b')],
          usage: { ...Usage.make(), input: 100, output: 20 },
        },
        { id: entryIdUnsafe(3), messages: [user('abcdefgh')] },
      ])
      assert.strictEqual(Transcript.estimate(view), 123)
      assert.strictEqual(Transcript.estimate(view, [user('1234')]), 125)
    })
    it('cuts cannot strand a late tool result after an intervening user', () => {
      const view = Transcript.derive([
        { id: entryIdUnsafe(1), messages: [user('first')] },
        { id: entryIdUnsafe(2), messages: [Prompt.assistantMessage({ content: [call('a')] })] },
        { id: entryIdUnsafe(3), messages: [user('intervenes')] },
        { id: entryIdUnsafe(4), messages: [result('a', 'done')] },
        { id: entryIdUnsafe(5), messages: [assistant('last')] },
      ])
      assert.isFalse(
        Option.contains(
          Compaction.selectCut(view, 3, () => 1),
          2,
        ),
      )
      assert.deepStrictEqual(
        Compaction.selectCut(
          Transcript.derive([
            { id: entryIdUnsafe(1), head: entryIdUnsafe(1), messages: [user('summary')] },
          ]),
          0,
        ),
        Option.none(),
      )
    })
    it('thresholds are strict, background zero disables, manual selection does not inspect enabled', () => {
      const policy = Object.assign({}, Agent.defaultCompaction, {
        reserveTokens: 10,
        backgroundTokens: 20,
      })
      assert.deepStrictEqual(Compaction.threshold(90, 100, policy), Option.some('background'))
      assert.deepStrictEqual(Compaction.threshold(91, 100, policy), Option.some('blocking'))
      assert.deepStrictEqual(Compaction.threshold(70, 100, policy), Option.none())
      assert.deepStrictEqual(
        Compaction.threshold(89, 100, Object.assign({}, policy, { backgroundTokens: 0 })),
        Option.none(),
      )
      assert.deepStrictEqual(
        Compaction.threshold(100, 100, Object.assign({}, policy, { enabled: false })),
        Option.none(),
      )
    })
    it('summary source omits systems/files, preserves reasoning/calls, bounds tool text', () => {
      const text = Compaction.serializeConversation([
        Prompt.systemMessage({ content: 'secret' }),
        user('goal'),
        Prompt.assistantMessage({
          content: [Prompt.reasoningPart({ text: 'why' }), call('a')],
        }),
        result('a', 'x'.repeat(2100)),
      ])
      assert.strictEqual(text.includes('secret'), false)
      assert.strictEqual(text.includes('[Assistant thinking]: why'), true)
      assert.strictEqual(text.includes('test(x=1)'), true)
      assert.strictEqual(text.includes('100 more characters truncated'), true)
    })
  })
})
