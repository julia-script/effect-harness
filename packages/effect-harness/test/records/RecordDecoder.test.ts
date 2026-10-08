import { assert, describe, it } from '@effect/vitest'

import * as Context from 'effect/Context'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'

import * as SchemaGetter from 'effect/SchemaGetter'

import * as Result from 'effect/Result'

import * as Record from 'effect-harness/Record'

class DecoderValue extends Context.Service<DecoderValue, { readonly prefix: string }>()(
  'test/DecoderValue',
) {}

const data = Schema.String.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transformEffect((input) =>
      DecoderValue.pipe(Effect.map(({ prefix }) => prefix + input)),
    ),
    encode: SchemaGetter.transform((value) => value),
  }),
)

describe('RecordDecoder', () => {
  it('task guards enforce physical identity bounds without recovering a result type', () => {
    assert.isTrue(Record.isTaskId(1))
    assert.isTrue(Record.isTaskId(Number.MAX_SAFE_INTEGER))
    for (const u of [
      undefined,
      null,
      '1',
      0,
      -1,
      1.5,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      Number.MAX_SAFE_INTEGER + 1,
    ])
      assert.isFalse(Record.isTaskId(u))
  })

  it.effect(
    'kind identity never validates data; decoder preserves schema services and failure',
    () =>
      Effect.gen(function* () {
        const schema = Schema.Struct({
          ...Record.Entry.fields,
          kind: Schema.Literal('custom'),
          data,
        })
        const token = Result.getOrThrow(Record.defineEntry('custom', schema))
        const invalid: Record.Entry = {
          id: Record.EntryId.make(2),
          conversationId: Record.ROOT_CONVERSATION_ID,
          kind: 'custom',
          data: 42,
        }
        assert.isTrue(token.is(invalid))
        const decoding = token.decode(invalid)
        assert.strictEqual(
          (yield* decoding.pipe(
            Effect.provideService(DecoderValue, DecoderValue.of({ prefix: 'decoded:' })),
            Effect.flip,
          ))._tag,
          'SchemaError',
        )
        const valid = yield* token
          .decode({ ...invalid, data: 'value' })
          .pipe(Effect.provideService(DecoderValue, DecoderValue.of({ prefix: 'decoded:' })))
        assert.strictEqual(valid.data, 'decoded:value')
      }),
  )
})
