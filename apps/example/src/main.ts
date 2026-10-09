import { NodeRuntime } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as Conversation from 'effect-harness/Conversation'
import * as Harness from 'effect-harness/Harness'
import * as Submission from 'effect-harness/Submission'
import * as Application from './Application.js'

export const program = Effect.gen(function* () {
  const harness = yield* Harness.Harness
  const root = yield* harness.root
  const submission = yield* root.pipe(Conversation.submit({ type: 'input', content: 'hello' }))
  return yield* Submission.wait(submission)
})

NodeRuntime.runMain(
  program.pipe(Effect.provide(Application.layerMemory), Effect.flatMap(Effect.log)),
)
