import * as TestClock from 'effect/testing/TestClock'
import type * as Scope from 'effect/Scope'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Document from 'effect-harness/durable/Document'
import * as Event from 'effect-harness/durable/Event'
import * as Inbox from 'effect-harness/durable/Inbox'
import * as Record from 'effect-harness/durable/Record'
import * as Session from 'effect-harness/durable/Session'
import * as View from 'effect-harness/durable/View'
import * as Store from 'effect-harness/durable/Store'

const base: readonly [string, ...string[]] = [
  'docs',
  'harness.live',
  'generation',
  'message',
  'content',
]
const message = (text: string) =>
  Prompt.assistantMessage({
    content: [
      Prompt.textPart({ text }),
      Prompt.reasoningPart({ text }),
      Prompt.toolCallPart({
        id: 'c',
        name: 'tool',
        params: { path: text, other: text },
        providerExecuted: false,
      }),
    ],
  })
const layers = Event.layer.pipe(
  Layer.provideMerge(View.layer),
  Layer.provideMerge(Session.layer),
  Layer.provideMerge(Store.layerMemory),
)

// Advance modeled journal polling only after its wait/consumer fiber has been admitted.
const awaitObserved = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R | Scope.Scope> =>
  Effect.gen(function* () {
    const fiber = yield* effect.pipe(Effect.forkScoped)
    for (let attempt = 0; attempt < 100; attempt++) {
      if (fiber.pollUnsafe() !== undefined) return yield* Fiber.join(fiber)
      yield* TestClock.adjust('20 millis')
    }
    return yield* Effect.die('Admitted observation did not settle')
  })
describe('EventDeltas', () => {
  it.effect('preserves block starts, block replacements and text type-change fallbacks', () =>
    Effect.sync(() => {
      const before = Prompt.assistantMessage({ content: [] })
      const after = message('new')
      const blocks = after.content
      assert.ok(blocks[0])
      assert.ok(blocks[1])
      assert.ok(blocks[2])
      assert.deepStrictEqual(Event.messageChanges([['set', base, []]], before, after), [
        { _tag: 'text_start' as const, contentIndex: 0, block: blocks[0] },
        {
          _tag: 'thinking_start' as const,
          contentIndex: 1,
          block: blocks[1],
        },
        {
          _tag: 'toolcall_start' as const,
          contentIndex: 2,
          block: blocks[2],
        },
      ])
      assert.deepStrictEqual(Event.messageChanges([['set', [...base, 0], {}]], before, after), [
        { _tag: 'block' as const, contentIndex: 0, block: blocks[0] },
      ])
      const text = Prompt.assistantMessage({ content: [Prompt.textPart({ text: 'a' })] })
      const reasoning = Prompt.assistantMessage({
        content: [Prompt.reasoningPart({ text: 'abc' })],
      })
      const block = reasoning.content[0]
      assert.ok(block)
      assert.deepStrictEqual(
        Event.messageChanges([['set', [...base, 0, 'text'], 'abc']], text, reasoning),
        [{ _tag: 'block' as const, contentIndex: 0, block }],
      )
    }),
  )

  it.effect('coalesces repeated text, reasoning and nested tool argument suffixes', () =>
    Effect.sync(() => {
      const before = message('a')
      const after = message('abc')
      const ops: View.Op[] = [
        ['set', [...base, 0, 'text'], 'ab'],
        ['set', [...base, 0, 'text'], 'abc'],
        ['set', [...base, 1, 'text'], 'ab'],
        ['set', [...base, 1, 'text'], 'abc'],
        ['set', [...base, 2, 'params', 'path'], 'ab'],
        ['set', [...base, 2, 'params', 'path'], 'abc'],
        ['set', [...base, 2, 'params', 'other'], 'abc'],
      ]
      const changes = Event.messageChanges(ops, before, after)
      assert.deepStrictEqual(changes, [
        { _tag: 'text_delta' as const, contentIndex: 0, delta: 'bc' },
        { _tag: 'thinking_delta' as const, contentIndex: 1, delta: 'bc' },
        {
          _tag: 'toolcall_delta' as const,
          contentIndex: 2,
          path: ['path'],
          delta: 'bc',
        },
        {
          _tag: 'toolcall_delta' as const,
          contentIndex: 2,
          path: ['other'],
          delta: 'bc',
        },
      ])
      for (const type of ['text_delta', 'thinking_delta', 'toolcall_delta']) {
        const suffix = changes
          .filter(
            (change) =>
              change._tag === type &&
              (change._tag !== 'toolcall_delta' || change.path[0] === 'path'),
          )
          .map((change) => ('delta' in change ? change.delta : ''))
          .join('')
        assert.strictEqual(`a${suffix}`, 'abc')
      }
    }),
  )

  it.effect('suppresses narrower deltas for block and content replacements in either order', () =>
    Effect.sync(() => {
      const before = message('a')
      const after = message('abc')
      const narrower: View.Op = ['set', [...base, 0, 'text'], 'abc']
      for (const ancestor of [
        ['set', [...base, 0], { type: 'text', text: 'abc' }],
        ['set', base, []],
        ['set', [...base, 0, 'options'], {}],
      ] satisfies View.Op[]) {
        for (const ops of [
          [narrower, ancestor],
          [ancestor, narrower],
        ]) {
          const changes = Event.messageChanges(ops, before, after)
          assert.strictEqual(
            changes.filter((change) => 'contentIndex' in change && change.contentIndex === 0)
              .length,
            1,
          )
          const block = after.content[0]
          assert.ok(block)
          assert.deepStrictEqual(changes[0], {
            _tag: 'block' as const,
            contentIndex: 0,
            block,
          })
        }
      }
      for (const ancestor of [
        ['set', ['docs', 'harness.live', 'generation', 'message'], {}],
        ['replace', { conversation: { id: Record.ROOT_CONVERSATION_ID }, entries: [], docs: {} }],
      ] satisfies View.Op[]) {
        assert.deepStrictEqual(Event.messageChanges([narrower, ancestor], before, after), [
          { _tag: 'message' as const, message: after },
        ])
      }
    }),
  )

  it.effect('falls back once for incompatible text, reasoning and tool values', () =>
    Effect.sync(() => {
      const before = message('abc')
      const after = message('z')
      for (const [index, tail] of [
        [0, ['text']],
        [1, ['text']],
        [2, ['params', 'path']],
      ] as const) {
        const block = after.content[index]
        assert.ok(block)
        assert.deepStrictEqual(
          Event.messageChanges(
            [
              ['set', [...base, index, ...tail], 'abcd'],
              ['set', [...base, index, ...tail], 'z'],
            ],
            before,
            after,
          ),
          [{ _tag: 'block' as const, contentIndex: index, block }],
        )
      }
      const first: View.Op = ['set', [...base, 2, 'params', 'path'], 'abcd']
      const fallback: View.Op = ['set', [...base, 2, 'params'], {}]
      for (const ops of [
        [first, fallback],
        [fallback, first],
      ]) {
        const block = message('abcd').content[2]
        assert.ok(block)
        assert.deepStrictEqual(Event.messageChanges(ops, before, message('abcd')), [
          { _tag: 'block' as const, contentIndex: 2, block },
        ])
      }
      assert.deepStrictEqual(
        Event.messageChanges(
          [['delete', [...base, 2]]],
          before,
          Prompt.assistantMessage({ content: [] }),
        ),
        [
          {
            _tag: 'message' as const,
            message: Prompt.assistantMessage({ content: [] }),
          },
        ],
      )
    }),
  )

  it.effect('publishes one suffix per final changed path through Event.watch', () =>
    Effect.gen(function* () {
      const session = yield* Session.Session
      const events = yield* Event.Event
      const root = yield* session.root()
      const codec = Schema.toCodecJson(Prompt.AssistantMessage)
      const initial = yield* Schema.encodeEffect(codec)(message('a'))
      yield* session.transaction((tx) =>
        Effect.gen(function* () {
          const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
          live.generation = { attempt: 1, message: Document.copyUnsafe(initial) }
        }),
      )
      const watch = yield* events.watch(root.id)
      yield* session.transaction((tx) =>
        Effect.gen(function* () {
          const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
          assert.ok(live.generation?.message)
          const partial = live.generation.message
          assert.ok(typeof partial === 'object' && partial !== null && !Array.isArray(partial))
          assert.ok(Array.isArray(partial.content))
          for (const block of partial.content) {
            assert.ok(typeof block === 'object' && block !== null && !Array.isArray(block))
            if (block.type === 'text' || block.type === 'reasoning') {
              block.text = 'ab'
              block.text = 'abc'
            } else if (block.type === 'tool-call') {
              assert.ok(
                block.params !== null &&
                  typeof block.params === 'object' &&
                  !Array.isArray(block.params),
              )
              block.params.path = 'ab'
              block.params.path = 'abc'
              block.params.other = 'abc'
            }
          }
        }),
      )
      const batches = yield* awaitObserved(
        Stream.runCollect(watch.changes.pipe(Stream.take(1), Stream.timeout('3 seconds'))),
      )
      const update = batches[0]?.find((event) => event._tag === 'message_update')
      assert.ok(update)
      assert.deepStrictEqual(update.changes, [
        { _tag: 'text_delta' as const, contentIndex: 0, delta: 'bc' },
        { _tag: 'thinking_delta' as const, contentIndex: 1, delta: 'bc' },
        {
          _tag: 'toolcall_delta' as const,
          contentIndex: 2,
          path: ['path'],
          delta: 'bc',
        },
        {
          _tag: 'toolcall_delta' as const,
          contentIndex: 2,
          path: ['other'],
          delta: 'bc',
        },
      ])
      const current = yield* session
        .snapshot(Inbox.LiveDoc, { owner: root.id })
        .pipe(Effect.map(Option.getOrUndefined))
      const final = current?.value.generation?.message
      assert.ok(final !== undefined)
      assert.deepStrictEqual(yield* Schema.decodeEffect(codec)(final), message('abc'))
    }).pipe(Effect.provide(layers)),
  )
})
