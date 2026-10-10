import { assert, describe, it } from '@effect/vitest'
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Conversation from 'effect-harness/Conversation'
import { ExecutionError } from 'effect-harness/ExecutionError'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Submission from 'effect-harness/Submission'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'

const finish = (reason: 'stop' | 'tool-calls') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
})
const model = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: ({ prompt }) =>
      Effect.succeed(
        prompt.content.some((message) => message.role === 'tool')
          ? [{ type: 'text' as const, text: 'done' }, finish('stop')]
          : [
              {
                type: 'tool-call' as const,
                id: 'work-call',
                name: 'work',
                params: {},
                providerExecuted: false,
              },
              finish('tool-calls'),
            ],
      ),
    streamText: () => Stream.empty,
  }),
)
const toolkit = Toolkit.make(Tool.make('work', { success: Schema.String, failure: Schema.String }))
const domain = 'declared domain failure'
const defect = 'resource cleanup defect'
const cleanup = Effect.acquireRelease(Effect.void, () => Effect.die(defect))
const infrastructure = new ExecutionError({
  reason: 'closed',
  operation: 'test.resource',
  message: 'resource is closed',
})

describe('Toolkit scoped failure classification', () => {
  for (const test of [
    { name: 'declared domain failure', handler: Effect.fail(domain), failures: [] },
    {
      name: 'domain failure plus finalizer defect',
      handler: cleanup.pipe(Effect.andThen(Effect.fail(domain))),
      failures: [domain, defect],
    },
    {
      name: 'success plus finalizer defect',
      handler: cleanup.pipe(Effect.as('success')),
      failures: [defect],
    },
    {
      name: 'domain failure plus infrastructure failure',
      handler: Effect.failCause(Cause.combine(Cause.fail(domain), Cause.fail(infrastructure))),
      failures: [domain, infrastructure.message],
    },
  ]) {
    it.effect(test.name, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* HarnessRuntime.make({ tools: toolkit })
          const harness = yield* Harness.make.pipe(
            Effect.provideService(HarnessBackend, runtime.backend),
          )
          const root = yield* harness.root
          const result = yield* Submission.wait(
            yield* Conversation.submit(root, { type: 'input', content: 'work' }),
          )
          const entries = yield* Session.scanEntries(runtime.session, {
            conversationId: root.id,
          }).pipe(Stream.runCollect)
          if (test.failures.length === 0) {
            assert.strictEqual(result.status, 'done')
            const toolResult = entries.find((entry) => entry.kind === 'tool.result')
            assert.isDefined(toolResult)
            assert.include(JSON.stringify(toolResult?.data), domain)
          } else {
            assert.strictEqual(result.status, 'unanswered')
            if (result.status === 'unanswered') {
              for (const message of test.failures) {
                assert.include(JSON.stringify(result.detail), message)
              }
            }
            assert.isFalse(entries.some((entry) => entry.kind === 'tool.result'))
          }
        }),
      ).pipe(
        Effect.provide([Storage.layerMemory, model, toolkit.toLayer({ work: () => test.handler })]),
      ),
    )
  }
})
