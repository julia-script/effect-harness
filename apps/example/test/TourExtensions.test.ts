import { BunServices } from '@effect/platform-bun'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Extensions from '../dist/tour/Extensions.js'
import * as Subagent from '../dist/tour/Subagent.js'

describe('runnable extension and owned-conversation tour', () => {
  it.live('uses selected tools, times hot replacements and retains approval across reopening', () =>
    Extensions.run.pipe(
      Effect.map((result) => {
        assert.deepEqual(result.selectedTools, ['search_issues'])
        assert.deepEqual(result.replay, { search_issues: 'safe', deploy: 'unsafe' })
        assert.deepEqual(result.calls, ['search:v1', 'search:v2', 'deploy:after-restart'])
        assert.strictEqual(result.timings.length, 2)
        assert.isTrue(
          result.timings.every(
            (timing) => timing.tool === 'search_issues' && timing.milliseconds >= 0,
          ),
        )
        assert.isTrue(result.committedSections.some((section) => section.includes('v1')))
        assert.isTrue(result.committedSections.some((section) => section.includes('v2')))
        assert.strictEqual(result.approvalRequests, 1)
        assert.strictEqual(result.firstWriterWins, true)
      }),
      Effect.provide(BunServices.layer),
    ),
  )

  it.live('replays the triage tool while reusing its owned child and settled submission', () =>
    Subagent.run.pipe(
      Effect.map((result) => {
        assert.strictEqual(result.toolExecutions, 2)
        assert.strictEqual(result.childModelRequests, 1)
        assert.strictEqual(result.childAnswer, 'bug')
        assert.strictEqual(result.parentAnswer, 'Issue classified as bug')
        assert.strictEqual(result.reusedChild, true)
        assert.strictEqual(result.reusedSubmission, true)
        assert.isAbove(result.childConversationId, 1)
        assert.isAbove(result.childSubmissionId, 0)
        assert.isAbove(result.ownerTaskId, 0)
      }),
      Effect.provide(BunServices.layer),
    ),
  )
})
