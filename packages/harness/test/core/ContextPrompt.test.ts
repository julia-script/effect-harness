import * as Schema from 'effect/Schema'
import * as Identity from '../../src/Identity.ts'
import * as Layer from 'effect/Layer'
import * as NativeContext from 'effect/Context'
import * as NativeModel from 'effect/ai/LanguageModel'
import * as Stream from 'effect/Stream'
import * as Executor from '../../src/Executor.ts'
import * as Model from '../../src/Model.ts'
import * as Registry from '../../src/Registry.ts'
import * as Invocation from '../../src/Invocation.ts'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as AiPrompt from 'effect/ai/Prompt'
import * as Context from '../../src/Context.ts'
import * as Prompt from '../../src/Prompt.ts'
import * as Compaction from '../../src/Compaction.ts'
import * as Agent from '../../src/Agent.ts'
import * as Usage from '../../src/Usage.ts'
import * as ToolContent from '../../src/ToolResult.ts'
const entryId = Schema.decodeSync(Identity.EntryId)
const user = (text: string) => AiPrompt.userMessage({ content: [AiPrompt.textPart({ text })] })
const assistant = (text: string) =>
  AiPrompt.assistantMessage({ content: [AiPrompt.textPart({ text })] })
const call = (id: string) =>
  AiPrompt.toolCallPart({ id, name: 'test', params: { x: 1 }, providerExecuted: false })
const result = (id: string, value: string) =>
  AiPrompt.toolMessage({
    content: [
      AiPrompt.toolResultPart({
        id,
        name: 'test',
        result: value,
        isFailure: false,
        providerExecuted: false,
      }),
    ],
  })

describe('canonical context, prompt protocol and cuts', () => {
  it.effect(
    'actual preparation respects omitted/deleted managed patches and plain edited replacements',
    () =>
      Effect.gen(function* () {
        const native = yield* NativeModel.make({
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
            configure: () => Effect.succeed(NativeContext.empty()),
          },
        ])
        const registry = Registry.layer([
          {
            name: 'section',
            sections: [{ key: 's', render: () => Effect.die('section unavailable') }],
          },
        ])
        yield* Effect.gen(function* () {
          const executor = yield* Executor.Executor
          const original = {
            id: entryId(1),
            system: { sections: { s: 'old omitted instruction' } },
          }
          for (const edits of [
            [{ target: entryId(1), action: 'omit' as const }],
            [
              {
                target: entryId(1),
                action: 'replace' as const,
                messages: [AiPrompt.systemMessage({ content: 'plain replacement' })],
              },
            ],
          ]) {
            const view = Context.derive([original, { id: entryId(2), edits }])
            const prepared = yield* executor.prepare({
              state: { model },
              settings: Agent.settings(),
              view,
            })
            const systems = prepared.request.prompt.content
              .filter((message) => message.role === 'system')
              .map((message) => message.content)
            assert.deepStrictEqual(
              systems,
              edits[0]?.action === 'omit' ? [] : ['plain replacement'],
            )
            assert.deepStrictEqual(Context.systemPatches(view), [])
          }
          const deleted = Context.derive([
            original,
            { id: entryId(2), system: { sections: { s: null } } },
          ])
          const prepared = yield* executor.prepare({
            state: { model },
            settings: Agent.settings(),
            view: deleted,
          })
          assert.deepStrictEqual(prepared.request.prompt.content, [])
          assert.strictEqual(Context.systemPatches(deleted).length, 2)
        }).pipe(
          Effect.provide(Executor.layer.pipe(Layer.provide(Layer.mergeAll(registry, catalog)))),
          Effect.provideService(Invocation.Invocation, {
            cwd: '.',
            report: () => Effect.void,
            progress: () => Effect.void,
          }),
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
    const entry = { id: entryId(1), system: patch }
    const expected =
      Math.ceil('abc\n\ndef'.length / 3.5) +
      Math.ceil(JSON.stringify(toolsAdded).length / 3.5) +
      Math.ceil(JSON.stringify(['old']).length / 3.5)
    assert.strictEqual(Context.estimate(Context.derive([entry])), expected)
    assert.strictEqual(Context.systemMessages({ sections: { gone: null } }).length, 0)
    assert.strictEqual(
      Context.estimate(Context.derive([entry]), [], () => 7),
      21,
    )
    assert.strictEqual(
      Context.estimate(
        Context.derive([
          entry,
          { id: entryId(2), edits: [{ target: entryId(1), action: 'omit' as const }] },
        ]),
      ),
      0,
    )
    assert.strictEqual(
      Context.estimate(
        Context.derive([
          entry,
          {
            id: entryId(2),
            edits: [
              { target: entryId(1), action: 'replace' as const, messages: [user('replacement')] },
            ],
          },
        ]),
      ),
      Math.ceil('replacement'.length / 3.5),
    )
    const measured = {
      id: entryId(2),
      messages: [assistant('answer')],
      usage: { ...Usage.zero(), input: 100, output: 20 },
    }
    assert.strictEqual(
      Context.estimate(Context.derive([entry, measured, { id: entryId(3), system: patch }])),
      120 + expected,
    )
  })

  it.effect(
    'pinned token fallback counts visible text and fixed images independent of encoded bytes',
    () =>
      Effect.gen(function* () {
        assert.strictEqual(Context.estimateMessage(user('12345678')), 3)
        for (const bytes of [new Uint8Array([1]), new Uint8Array(40000)]) {
          const encoded = yield* ToolContent.encode({
            content: [
              AiPrompt.textPart({ text: 'visible' }),
              AiPrompt.filePart({ mediaType: 'image/png', data: bytes }),
            ],
            details: { private: 'x'.repeat(10000) },
          })
          const tool = AiPrompt.toolMessage({
            content: [
              AiPrompt.toolResultPart({
                id: 'image',
                name: 'image',
                result: encoded,
                isFailure: false,
                providerExecuted: false,
              }),
            ],
          })
          assert.strictEqual(Context.estimateMessage(tool), Math.ceil((7 + 4800) / 3.5))
        }
      }),
  )

  it('newest head first, older head edits count, stopped assistant omitted from model only', () => {
    const view = Context.derive([
      { id: entryId(1), messages: [user('old')] },
      { id: entryId(2), head: entryId(1), edits: [{ target: entryId(1), action: 'omit' }] },
      { id: entryId(3), messages: [assistant('broken')], status: 'error' },
      { id: entryId(4), head: entryId(1), messages: [user('summary')] },
      { id: entryId(5), messages: [user('new')] },
    ])
    assert.deepStrictEqual(
      view.entries.map((entry) => entry.id),
      [4, 1, 3, 5],
    )
    assert.deepStrictEqual(
      view.contributions.map((messages) => messages.length),
      [1, 0, 0, 1],
    )
    assert.strictEqual(Context.derive(view.entries, entryId(3)).head, undefined)
  })
  it('orders results before intervening users, chooses first duplicate, drops orphans and synthesizes missing', () => {
    const owner = AiPrompt.assistantMessage({ content: [call('a'), call('b'), call('c')] })
    const ordered = Context.orderToolResults([
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
      [...Prompt.replaySections([{ sections: { a: '1', b: '2' } }, { sections: { a: '3' } }])],
      [
        ['a', '3'],
        ['b', '2'],
      ],
    )
    assert.deepStrictEqual(
      [
        ...Prompt.replaySections([
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
      Prompt.planSections(
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
      Prompt.planTools(
        [a],
        [{ ...a, parameters: { properties: { n: { type: 'number' } }, type: 'object' } }],
      ),
      { toolsRemoved: [], toolsAdded: [] },
    )
    assert.deepStrictEqual(Prompt.planTools([a, b], [b, a]), {
      toolsRemoved: ['a', 'b'],
      toolsAdded: [b, a],
    })
  })
  it('head forces a baseline once even when values match and omits retained prior systems', () => {
    const entries: Context.Entry[] = [
      { id: entryId(1), system: { sections: { a: 'x' } } },
      { id: entryId(2), messages: [user('u')] },
      { id: entryId(3), head: entryId(1) },
    ]
    const view = Context.derive(entries)
    assert.deepStrictEqual(Prompt.plan(view, new Map([['a', 'x']]), []).edits, [
      { target: entryId(1), action: 'omit' },
    ])
    assert.strictEqual(
      Prompt.plan(
        Context.derive([...entries, { id: entryId(4), system: { sections: { a: 'x' } } }]),
        new Map([['a', 'x']]),
        [],
      ).patches.length,
      0,
    )
  })
  it('native system string preserves plain instructions before ordered sections', () => {
    const prompt = Prompt.toPrompt(
      [AiPrompt.systemMessage({ content: 'legacy' }), user('u')],
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
    const view = Context.derive([
      { id: entryId(1), messages: [user('a')] },
      {
        id: entryId(2),
        messages: [assistant('b')],
        usage: { ...Usage.zero(), input: 100, output: 20 },
      },
      { id: entryId(3), messages: [user('abcdefgh')] },
    ])
    assert.strictEqual(Context.estimate(view), 123)
    assert.strictEqual(Context.estimate(view, [user('1234')]), 125)
  })
  it('cuts cannot strand a late tool result after an intervening user', () => {
    const view = Context.derive([
      { id: entryId(1), messages: [user('first')] },
      { id: entryId(2), messages: [AiPrompt.assistantMessage({ content: [call('a')] })] },
      { id: entryId(3), messages: [user('intervenes')] },
      { id: entryId(4), messages: [result('a', 'done')] },
      { id: entryId(5), messages: [assistant('last')] },
    ])
    assert.notStrictEqual(
      Compaction.selectCut(view, 3, () => 1),
      2,
    )
    assert.strictEqual(
      Compaction.selectCut(
        Context.derive([{ id: entryId(1), head: entryId(1), messages: [user('summary')] }]),
        0,
      ),
      undefined,
    )
  })
  it('thresholds are strict, background zero disables, manual selection does not inspect enabled', () => {
    const policy = Object.assign({}, Agent.defaultCompaction, {
      reserveTokens: 10,
      backgroundTokens: 20,
    })
    assert.strictEqual(Compaction.threshold(90, 100, policy), 'background')
    assert.strictEqual(Compaction.threshold(91, 100, policy), 'blocking')
    assert.strictEqual(Compaction.threshold(70, 100, policy), undefined)
    assert.strictEqual(
      Compaction.threshold(89, 100, Object.assign({}, policy, { backgroundTokens: 0 })),
      undefined,
    )
    assert.strictEqual(
      Compaction.threshold(100, 100, Object.assign({}, policy, { enabled: false })),
      undefined,
    )
  })
  it('summary source omits systems/files, preserves reasoning/calls, bounds tool text', () => {
    const text = Compaction.serializeConversation([
      AiPrompt.systemMessage({ content: 'secret' }),
      user('goal'),
      AiPrompt.assistantMessage({ content: [AiPrompt.reasoningPart({ text: 'why' }), call('a')] }),
      result('a', 'x'.repeat(2100)),
    ])
    assert.strictEqual(text.includes('secret'), false)
    assert.strictEqual(text.includes('[Assistant thinking]: why'), true)
    assert.strictEqual(text.includes('test(x=1)'), true)
    assert.strictEqual(text.includes('100 more characters truncated'), true)
  })
})
