import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'
import * as TestSchema from 'effect/testing/TestSchema'

import * as Transcript from 'effect-harness/Transcript'

import * as Identity from 'effect-harness/Identity'

import * as Compaction from 'effect-harness/Compaction'

import * as Prompt from 'effect/ai/Prompt'

describe('TranscriptEdit', () => {
  it.effect(
    'tagged edit codecs preserve replacement messages and control transcript projection',
    () =>
      Effect.gen(function* () {
        const wire: typeof Transcript.Edit.Encoded = {
          target: 2,
          _tag: 'replace' as const,
          messages: [{ options: {}, role: 'user', content: 'replacement' }],
        }
        const edit = {
          _tag: 'replace' as const,
          target: Identity.EntryId.make(2),
          messages: [Prompt.userMessage({ content: [Prompt.textPart({ text: 'replacement' })] })],
        }
        const codec = new TestSchema.Asserts(Transcript.Edit)
        yield* codec.decoding().succeedEffect(wire, edit)
        yield* codec.encoding().succeedEffect(edit, wire)
        assert.strictEqual(edit._tag, 'replace')
        const id = yield* Schema.decodeEffect(Identity.EntryId)(2)
        const next = yield* Schema.decodeEffect(Identity.EntryId)(3)
        const view = Transcript.derive(undefined)([
          { id, messages: [Prompt.userMessage({ content: [Prompt.textPart({ text: 'old' })] })] },
          { id: next, edits: [edit] },
        ])
        assert.deepStrictEqual(view.messages, [
          Prompt.userMessage({ content: [Prompt.textPart({ text: 'replacement' })] }),
        ])
        const tokenize = () => 1
        assert.deepStrictEqual(
          Compaction.selectCut(view, 1, tokenize),
          Compaction.selectCut(1, tokenize)(view),
        )
      }),
  )
})
