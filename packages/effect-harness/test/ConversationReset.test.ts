import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Conversation from '../src/Conversation.ts'
import * as Executor from '../src/Executor.ts'
import * as Harness from '../src/Harness.ts'
import { Persistence } from '../src/Persistence.ts'
import * as Submission from '../src/Submission.ts'
import * as Memory from '../src/storage/Memory.ts'
import * as Native from './embedded/NativeFixture.ts'

describe('conversation reset', () => {
  it.effect(
    'clears the next model context while preserving historical entries and fork cutoffs',
    () =>
      Effect.gen(function* () {
        const prompts = yield* Ref.make<ReadonlyArray<ReadonlyArray<string>>>([])
        const { executor } = yield* Native.makeExecutor({
          provider: {
            ...Native.provider,
            streamText: (input) =>
              Stream.unwrap(
                Ref.update(prompts, (values) => [
                  ...values,
                  input.prompt.content.flatMap((message) =>
                    message.role === 'user'
                      ? message.content.flatMap((part) => (part.type === 'text' ? [part.text] : []))
                      : [],
                  ),
                ]).pipe(Effect.as(Native.provider.streamText(input))),
              ),
          },
        })
        const store = yield* Memory.make
        const harness = yield* Harness.make({
          agent: { model: Native.ref },
          settings: { retry: { enabled: false }, compaction: { enabled: false } },
        }).pipe(
          Effect.provideService(Persistence, store),
          Effect.provideService(Executor.Executor, executor),
        )
        const conversation = yield* harness.root
        const first = yield* Submission.wait(
          yield* Conversation.submit(conversation, 'old context'),
        )
        assert.strictEqual(first.status, 'done')
        assert.strictEqual(first.type, 'input')
        if (first.status !== 'done' || first.type !== 'input') return
        const before = yield* Conversation.snapshot(conversation)
        const reset = yield* Conversation.reset(conversation)
        assert.strictEqual(reset.kind, 'harness.reset')
        assert.strictEqual(reset.head, reset.id)
        assert.deepEqual(reset.model ?? [], [])
        const after = yield* Conversation.snapshot(conversation)
        assert.deepEqual(after.entries.slice(0, -1), before.entries)
        assert.strictEqual(after.entries.at(-1)?.id, reset.id)
        assert.strictEqual(
          (yield* Submission.wait(yield* Conversation.submit(conversation, 'fresh context')))
            .status,
          'done',
        )
        assert.deepEqual(yield* Ref.get(prompts), [
          ['old context'],
          ['old context'],
          ['fresh context'],
          ['fresh context'],
        ])
        const archived = yield* Conversation.fork(conversation, first.answer)
        const historical = yield* Conversation.snapshot(archived)
        assert.deepEqual(historical.entries, before.entries)
        assert.isFalse(historical.entries.some((entry) => entry.kind === 'harness.reset'))
      }),
  )

  it.effect('exports reusable metadata and data Schemas for conversation entry payloads', () =>
    Effect.gen(function* () {
      const value = { harness: { status: 'stop' as const }, message: 'done' }
      const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(Conversation.Data))(value)
      assert.deepEqual(
        yield* Schema.decodeEffect(Schema.toCodecJson(Conversation.Data))(encoded),
        value,
      )
      assert.deepEqual(yield* Schema.decodeEffect(Conversation.Metadata)({ status: 'stop' }), {
        status: 'stop',
      })
    }),
  )
})
