import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as AiError from 'effect/ai/AiError'
import * as Record from '../src/Record.ts'
import * as Transcript from '../src/Transcript.ts'
import * as Usage from '../src/Usage.ts'
import * as State from '../src/internal/ConversationState.ts'

const entry = (data: Schema.Json) =>
  Schema.decodeEffect(Record.Entry)({
    id: 1,
    conversationId: 1,
    kind: 'test',
    data,
  })

describe('committed conversation metadata', () => {
  it.effect(
    'round-trips native errors, usage and managed sections into transcript projection',
    () =>
      Effect.gen(function* () {
        const data = {
          harness: {
            status: 'error' as const,
            usage: Usage.make(),
            error: new AiError.AiError({
              module: 'LanguageModel',
              method: 'streamText',
              reason: new AiError.InvalidOutputError({ description: 'Rejected output' }),
            }),
            system: { sections: { instructions: 'Retained instructions' } },
          },
          message: 'Saved request failure',
        }
        const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(State.Data))(data)
        const projected = yield* State.projectEntry(yield* entry(encoded))
        assert.strictEqual(projected.status, 'error')
        assert.deepEqual(projected.usage, data.harness.usage)
        assert.strictEqual(projected.error?.reason._tag, 'InvalidOutputError')
        assert.deepEqual(projected.system, data.harness.system)
        const view = Transcript.derive([projected])
        assert.isTrue(view.messages.some((message) => message.role === 'system'))
        assert.isTrue(view.messages.some((message) => message.role === 'user'))
      }),
  )

  it.effect(
    'rejects malformed reserved metadata instead of silently losing failure information',
    () =>
      Effect.gen(function* () {
        const invalid = yield* entry({ harness: { status: 'not-a-status' } })
        assert.isTrue(yield* Effect.isFailure(State.projectEntry(invalid)))
      }),
  )

  it.effect('preserves application entry data without reserved metadata', () =>
    Effect.gen(function* () {
      const custom = yield* entry({ application: { status: 'custom' } })
      const projected = yield* State.projectEntry(custom)
      assert.strictEqual(projected.id, custom.id)
      assert.isUndefined(projected.status)
      assert.deepEqual(projected.messages, [])
    }),
  )
})
