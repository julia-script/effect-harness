import { expect, test } from 'tstyche'
import * as Effect from 'effect/Effect'
import { Harness, Submission } from 'effect-harness'
import type * as Record from 'effect-harness/Record'

declare const harness: Harness.HarnessService
declare const id: Record.SubmissionId

test('saved branded IDs acquire handles with the existing typed client error', () => {
  expect(harness.submission(id)).type.toBe<
    Effect.Effect<Submission.Submission, Harness.HarnessError>
  >()
  expect(harness.submission).type.not.toBeCallableWith(1)
  expect(harness.submission).type.not.toBeCallableWith('saved-id')
  const program = Effect.gen(function* () {
    const client = yield* Harness.Harness
    const saved = yield* client.submission(id)
    expect(saved.conversationId).type.toBe<Record.ConversationId>()
    expect(saved.pipe(Submission.read())).type.toBe<
      Effect.Effect<Submission.Record, Harness.HarnessError>
    >()
    expect(saved.pipe(Submission.withdraw())).type.toBe<
      Effect.Effect<Submission.Withdrawal, Harness.HarnessError>
    >()
    return yield* saved.pipe(Submission.wait())
  })
  expect(program).type.toBe<
    Effect.Effect<Submission.Settled, Harness.HarnessError, Harness.Harness>
  >()
})
