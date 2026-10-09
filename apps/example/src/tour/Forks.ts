import * as Effect from 'effect/Effect'
import * as Conversation from 'effect-harness/Conversation'
import * as Harness from 'effect-harness/Harness'
import * as Submission from 'effect-harness/Submission'

export const program = Effect.gen(function* () {
  const harness = yield* Harness.Harness
  const root = yield* harness.root
  const settled = yield* root.pipe(
    Conversation.submit({ type: 'input', content: 'Investigate the login test' }),
    Effect.flatMap(Submission.wait),
  )
  if (settled._tag !== 'InputDone') return [settled]

  const fork = yield* root.pipe(Conversation.fork({ at: settled.answer }))
  yield* fork.pipe(Conversation.configure({ instructions: 'Explore another approach.' }))
  return yield* Effect.all(
    [
      root.pipe(
        Conversation.submit({ type: 'input', content: 'Check the test setup' }),
        Effect.flatMap(Submission.wait),
      ),
      fork.pipe(
        Conversation.submit({ type: 'input', content: 'Check the timeout' }),
        Effect.flatMap(Submission.wait),
      ),
    ],
    { concurrency: 'unbounded' },
  )
})
