import { NodeRuntime } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Conversation from 'effect-harness/Conversation'
import * as Harness from 'effect-harness/Harness'
import * as Storage from 'effect-harness/Storage'
import * as Submission from 'effect-harness/Submission'
import * as Application from '../Application.js'

const job = {
  type: 'input',
  content: 'Fix the flaky login test',
  requestId: 'job-42',
} as const

const HarnessLive = Application.layer.pipe(
  Layer.provide(
    Storage.layerSql.pipe(Layer.provide(SqliteClient.layer({ filename: './agent.sqlite' }))),
  ),
)

// Run this same file again after a crash. The request ID retrieves the saved submission.
export const program = Effect.gen(function* () {
  const harness = yield* Harness.Harness
  const root = yield* harness.root
  const submission = yield* root.pipe(Conversation.submit(job))
  return yield* Submission.wait(submission)
})

NodeRuntime.runMain(program.pipe(Effect.provide(HarnessLive), Effect.flatMap(Effect.log)))
