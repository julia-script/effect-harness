import { assertFailure } from '@effect/vitest/utils'
import * as Result from 'effect/Result'
import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Conversation from 'effect-harness/durable/Conversation'

describe('ConversationConfiguration', () => {
  it.effect('defaults independent tool dispatch to sixteen and rejects invalid limits', () =>
    Effect.gen(function* () {
      const configuration = yield* Conversation.Configuration.pipe(
        Effect.provide(Conversation.layerConfiguration()),
      )
      assert.strictEqual(configuration.toolConcurrency, 16)
      for (const [toolConcurrency, issue] of [
        [0, 'Expected a value greater than 0'],
        [-1, 'Expected a value greater than 0'],
        [0.5, 'Expected an integer'],
        [NaN, 'Expected an integer'],
        [Infinity, 'Expected an integer'],
        [Number.MAX_SAFE_INTEGER + 1, 'Expected an integer'],
      ] as const) {
        const result = yield* Conversation.Configuration.pipe(
          Effect.provide(Conversation.layerConfiguration({ toolConcurrency })),
          Effect.result,
        )
        assertFailure(
          Result.mapError(result, (error) => error.message),
          issue,
        )
      }
    }),
  )
})
