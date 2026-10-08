import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'

import * as Prompt from 'effect/ai/Prompt'

import * as Invocation from 'effect-harness/Invocation'

import * as Compaction from 'effect-harness/Compaction'

const serializedTool = (result: unknown) =>
  Compaction.serializeConversation([
    Prompt.toolMessage({
      content: [
        Prompt.toolResultPart({
          id: 'c',
          name: 't',
          result,
          isFailure: false,
          providerExecuted: false,
        }),
      ],
    }),
  ])

describe('CompactionSerialization', () => {
  it.effect(
    'compaction recognizes canonical and generic mixed blocks and retains unrelated serialized fallback',
    () =>
      Effect.gen(function* () {
        const canonical = yield* Schema.encodeEffect(Schema.toCodecJson(Invocation.Result))({
          content: [
            Prompt.textPart({ text: 'one' }),
            Prompt.filePart({ mediaType: 'image/png', data: new Uint8Array([1, 2]) }),
          ],
        })
        assert.strictEqual(
          serializedTool({
            _tag: '@effect-harness/ToolContent',
            ...(yield* Schema.decodeUnknownEffect(Schema.Record(Schema.String, Schema.Unknown))(
              canonical,
            )),
          }),
          '[Tool result]: one',
        )
        assert.strictEqual(
          serializedTool({
            content: [
              { type: 'text', text: 'a' },
              { type: 'unknown', value: 2 },
              null,
              { type: 'text', text: 7 },
              { type: 'text', text: 'b' },
            ],
          }),
          '[Tool result]: a\nb',
        )
        assert.strictEqual(serializedTool({ content: [{ type: 'image', bytes: [1, 2] }] }), '')
        assert.strictEqual(
          serializedTool({ native: { arbitrary: [true, null] } }),
          '[Tool result]: {"native":{"arbitrary":[true,null]}}',
        )
        assert.strictEqual(serializedTool('literal'), '[Tool result]: literal')
      }),
  )
})
