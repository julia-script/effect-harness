import * as TestSchema from 'effect/testing/TestSchema'

import * as Duration from 'effect/Duration'

import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'

import * as Agent from 'effect-harness/Agent'

describe('AgentSettingsSchema', () => {
  it.effect(
    'authoritative policy types preserve defaults, explicit undefined and opaque stream options',
    () =>
      Effect.gen(function* () {
        const opaque = { callback: () => 'SDK owned', bytes: new Uint8Array([1, 2]) }
        const settings = yield* Agent.settings({
          stream: opaque,
          retry: { enabled: false },
          compaction: { backgroundTokens: 0 },
          progress: { partialInterval: undefined },
        })
        const decoded = settings
        yield* new TestSchema.Asserts(Schema.toType(Agent.Settings))
          .decoding()
          .succeedEffect(settings, {
            stream: opaque,
            retry: {
              enabled: false,
              maxRetries: 3,
              baseDelay: Duration.seconds(2),
              maxAgentDelay: Duration.minutes(1),
            },
            compaction: {
              enabled: true,
              reserveTokens: 16384,
              keepRecentTokens: 20000,
              backgroundTokens: 0,
            },
            progress: {
              partialInterval: Duration.millis(100),
              outputInterval: Duration.millis(100),
            },
            toolExecution: 'parallel',
            steeringMode: 'one-at-a-time',
            followUpMode: 'one-at-a-time',
          })
        assert.strictEqual(decoded.stream['callback'], opaque.callback)
        assert.strictEqual(decoded.stream['bytes'], opaque.bytes)
        assert.strictEqual(decoded.retry.enabled, false)
        assert.strictEqual(Duration.toMillis(decoded.retry.baseDelay), 2000)
        assert.strictEqual(decoded.compaction.backgroundTokens, 0)
        assert.strictEqual(Duration.toMillis(decoded.progress.partialInterval), 100)
        const state = {
          model: undefined,
          thinking: undefined,
          extensions: undefined,
          tools: undefined,
          instructions: undefined,
          cwd: undefined,
        }
        yield* new TestSchema.Asserts(Agent.State).decoding().succeedEffect(state, {
          model: undefined,
          thinking: undefined,
          extensions: undefined,
          tools: undefined,
          instructions: undefined,
          cwd: undefined,
        })
      }),
  )
})
