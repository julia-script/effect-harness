import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Context from '@effect-harness/harness/Context'
import * as Identity from '@effect-harness/harness/Identity'
import * as Compaction from '@effect-harness/harness/Compaction'
import * as Prompt from 'effect/ai/Prompt'

describe('ContextEdit', () => {
  it.effect(
    'legacy edit bytes survive tagged decoding and still control transcript projection',
    () =>
      Effect.gen(function* () {
        const wire: typeof Context.Edit.Encoded = {
          target: 2,
          action: 'replace' as const,
          messages: [{ options: {}, role: 'user', content: 'replacement' }],
        }
        const edit = yield* Schema.decodeEffect(Context.Edit)(wire)
        assert.strictEqual(edit._tag, 'replace')
        const encoded = yield* Schema.encodeEffect(Context.Edit)(edit)
        assert.deepStrictEqual(encoded, wire)
        assert.strictEqual(JSON.stringify(encoded), JSON.stringify(wire))
        const id = yield* Schema.decodeEffect(Identity.EntryId)(2)
        const next = yield* Schema.decodeEffect(Identity.EntryId)(3)
        const view = Context.derive(undefined)([
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
