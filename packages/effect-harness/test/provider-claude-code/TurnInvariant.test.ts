import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Protocol from 'effect-harness/provider-claude-code/Protocol'
import * as Turn from 'effect-harness/provider-claude-code/Turn'

describe('TurnInvariant', () => {
  it.effect(
    'overflow validation runs before tool-turn termination and before pulling later events',
    () =>
      Effect.gen(function* () {
        const events = yield* Schema.decodeEffect(Schema.Array(Protocol.Event))([
          { type: 'system', subtype: 'init', tools: ['intent'] },
          {
            type: 'assistant',
            message: {
              id: 'message',
              model: 'model',
              stop_reason: 'tool_use',
              content: [{ type: 'tool_use', id: 'call', name: 'intent', input: {} }],
              usage: { input_tokens: Number.MAX_SAFE_INTEGER, cache_read_input_tokens: 1 },
            },
          },
          { type: 'result', subtype: 'success', is_error: false, usage: {} },
        ])
        let pulled = 0
        const error = yield* Stream.fromIterable(events).pipe(
          Stream.tap(() =>
            Effect.sync(() => {
              pulled++
            }),
          ),
          Turn.translate(new Map([['intent', 'public-tool']])),
          Stream.runCollect,
          Effect.flip,
        )
        assert.strictEqual(error._tag, 'AiError')
        assert.strictEqual(error.method, 'protocol')
        assert.strictEqual(error.reason._tag, 'InvalidOutputError')
        if (error.reason._tag === 'InvalidOutputError')
          assert.strictEqual(
            error.reason.description,
            'Claude Code input token total exceeds safe integer accounting',
          )
        assert.strictEqual(pulled, 2)
      }),
  )

  it.effect(
    'one translated stream can be rerun concurrently without sharing accounting or block state',
    () =>
      Effect.gen(function* () {
        const events = yield* Schema.decodeEffect(Schema.Array(Protocol.Event))([
          { type: 'system', subtype: 'init', tools: [] },
          {
            type: 'assistant',
            message: {
              id: 'message',
              model: 'model',
              content: [{ type: 'text', text: 'hello' }],
              usage: { input_tokens: 2 },
              stop_reason: 'end_turn',
            },
          },
          {
            type: 'result',
            subtype: 'success',
            is_error: false,
            usage: { input_tokens: 3, cache_read_input_tokens: 4, output_tokens: 5 },
            stop_reason: 'end_turn',
          },
        ])
        const translated = Turn.translate(Stream.fromIterable(events), new Map())
        const run = Stream.runCollect(translated)
        const first = yield* run
        const repeated = yield* run
        const concurrent = yield* Effect.all([run, run], { concurrency: 2 })
        assert.deepStrictEqual(repeated, first)
        assert.deepStrictEqual(concurrent, [first, first])
        const finish = first.find((part) => part.type === 'finish')
        assert.strictEqual(finish?.type, 'finish')
        if (finish?.type === 'finish') {
          assert.strictEqual(finish.reason, 'stop')
          assert.strictEqual(finish.usage.inputTokens.total, 7)
          assert.strictEqual(finish.usage.outputTokens.total, 5)
        }
      }),
  )

  it.effect('upstream typed failures retain identity before terminal validation', () =>
    Effect.gen(function* () {
      const initial = yield* Schema.decodeEffect(Protocol.Event)({
        type: 'system',
        subtype: 'init',
        tools: [],
      })
      const failure = { _tag: 'ForeignFailure' as const, detail: 'same-instance' }
      const error = yield* Stream.make(initial).pipe(
        Stream.concat(Stream.fail(failure)),
        Turn.translate(new Map()),
        Stream.runCollect,
        Effect.flip,
      )
      assert.strictEqual(error, failure)
    }),
  )
})
