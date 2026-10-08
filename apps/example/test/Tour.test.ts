import { BunServices } from '@effect/platform-bun'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Quickstart from '../dist/tour/Quickstart.js'
import * as Recovery from '../dist/tour/Recovery.js'
import * as Forks from '../dist/tour/Forks.js'
import * as Multiplayer from '../dist/tour/Multiplayer.js'

describe('offline conversation tour', () => {
  it.live('reads a real workspace through coding tools and deduplicates a SQLite submission', () =>
    Quickstart.run.pipe(
      Effect.tap((result) =>
        Effect.sync(() => {
          assert.strictEqual(result.answer, 'The deployment needs a smoke test.')
          assert.isTrue(result.sameSubmission)
          assert.strictEqual(result.toolEntries, 1)
        }),
      ),
      Effect.provide(BunServices.layer),
    ),
  )
  it.live('recovers after an actual process kill without duplicating committed results', () =>
    Recovery.run.pipe(
      Effect.tap((result) =>
        Effect.sync(() => {
          assert.deepEqual(result.toolInvocations, ['start', 'resume'])
          assert.strictEqual(result.submissions, 1)
          assert.strictEqual(result.completedReplay, 'cached')
        }),
      ),
      Effect.provide(BunServices.layer),
    ),
  )
  it.live('runs fork and parent model requests concurrently with inherited history', () =>
    Forks.run.pipe(
      Effect.tap((result) =>
        Effect.sync(() => {
          assert.strictEqual(result.concurrentRequests, 2)
          assert.notStrictEqual(result.channel, result.thread)
          assert.deepEqual(result.answers, [
            'Reply: Can we roll it back?',
            'Reply: Who is on call?',
          ])
        }),
      ),
      Effect.provide(BunServices.layer),
    ),
  )
  it.live('lets a late observer see and steer active work with serializable changes', () =>
    Multiplayer.run.pipe(
      Effect.tap((result) =>
        Effect.sync(() => {
          assert.isTrue(result.lateClientSawActiveWork)
          assert.strictEqual(result.settledInputs, 2)
          assert.isAbove(result.firstChanges, 1)
          assert.isAbove(result.secondChanges, 1)
        }),
      ),
      Effect.provide(BunServices.layer),
    ),
  )
})
