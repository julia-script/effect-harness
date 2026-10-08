import * as TestSchema from 'effect/testing/TestSchema'

import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Identity from 'effect-harness/Identity'

import * as Transcript from 'effect-harness/Transcript'

import * as Executor from 'effect-harness/Executor'

describe('IdentitySchema', () => {
  it.effect(
    'canonical IDs validate exact numeric bounds, remain wire numbers and preserve nominal separation',
    () =>
      Effect.gen(function* () {
        const entry = Identity.EntryId.make(Number.MAX_SAFE_INTEGER)
        const conversation = Identity.ConversationId.make(1)
        const entryAsserts = new TestSchema.Asserts(Identity.EntryId)
        const conversationAsserts = new TestSchema.Asserts(Identity.ConversationId)
        yield* entryAsserts.decoding().succeedEffect(Number.MAX_SAFE_INTEGER, entry)
        yield* conversationAsserts.decoding().succeedEffect(1, conversation)
        yield* entryAsserts.encoding().succeedEffect(entry, Number.MAX_SAFE_INTEGER)
        yield* conversationAsserts.encoding().succeedEffect(conversation, 1)
        for (const [input, issue] of [
          [0, 'Expected a value between 1 and 9007199254740991'],
          [-1, 'Expected a value between 1 and 9007199254740991'],
          [1.2, 'Expected an integer'],
          [Number.MAX_SAFE_INTEGER + 1, 'Expected an integer'],
          [Infinity, 'Expected an integer'],
          ['1', 'Expected number'],
        ] as const) {
          yield* entryAsserts.decoding().failEffect(input, issue)
          yield* conversationAsserts.decoding().failEffect(input, issue)
        }
        yield* new TestSchema.Asserts(Transcript.Edit)
          .decoding()
          .succeedEffect(
            { _tag: 'omit', target: 3 },
            { _tag: 'omit', target: Identity.EntryId.make(3) },
          )
        assert.strictEqual(Executor.SummaryRequest.fields.firstKept, Identity.EntryId)
        assert.strictEqual(Executor.SummaryRequest.fields.tail, Identity.EntryId)
      }),
  )
})
