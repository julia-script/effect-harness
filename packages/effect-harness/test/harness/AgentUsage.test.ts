import * as Duration from 'effect/Duration'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as TestSchema from 'effect/testing/TestSchema'
import * as Response from 'effect/ai/Response'
import * as Agent from 'effect-harness/Agent'
import * as Model from 'effect-harness/Model'
import { ModelError, ModelUnsupported } from 'effect-harness/ModelError'
import * as AiError from 'effect/ai/AiError'
import * as Usage from 'effect-harness/Usage'

describe('AgentUsage', () => {
  describe('agent stored configuration and accounting', () => {
    it('replaces whole fields, clears null and preserves undefined', () => {
      const state: Agent.State = {
        extensions: ['a'],
        model: { provider: 'x', modelId: 'y' },
        instructions: 'old',
      }
      assert.deepStrictEqual(
        Agent.configure(state, { extensions: ['b'], model: null, instructions: undefined }),
        { extensions: ['b'], instructions: 'old' },
      )
      assert.deepStrictEqual(state.extensions, ['a'])
    })
    it('edits host defaults, removes win and missing selection order is retained', () => {
      assert.deepStrictEqual(Agent.select({ add: ['c', 'a'], remove: ['a'] }, ['a', 'b', 'b']), [
        'b',
        'c',
      ])
      assert.deepStrictEqual(Agent.select(['z', 'a', 'z'], ['b']), ['z', 'a'])
    })
    it('preserves explicit default progress and all policy fields', () => {
      const settings = Effect.runSync(
        Agent.settings({
          progress: { partialIntervalMs: undefined, outputIntervalMs: 25 },
          retry: { maxRetries: 0 },
        }),
      )
      assert.strictEqual(Duration.toMillis(settings.progress.partialIntervalMs), 100)
      assert.strictEqual(Duration.toMillis(settings.progress.outputIntervalMs), 25)
      assert.strictEqual(Duration.toMillis(settings.retry.baseDelayMs), 2000)
      assert.strictEqual(settings.compaction.reserveTokens, 16384)
    })
    it('three retries means three additional attempts, exponential capped backoff', () => {
      assert.deepStrictEqual(
        [1, 2, 3, 4].map((attempt) => Agent.isRetryAllowed(Agent.defaultRetry, attempt, true)),
        [true, true, true, false],
      )
      assert.deepStrictEqual(
        [1, 2, 3, 20].map((attempt) =>
          Duration.toMillis(Agent.retryDelay(Agent.defaultRetry, attempt)),
        ),
        [2000, 4000, 8000, 60000],
      )
      assert.strictEqual(Agent.isRetryAllowed(Agent.defaultRetry, 1, false), false)
    })
    it('retry decisions honor typed provider and harness reasons despite misleading messages', () => {
      const permanent = new AiError.AiError({
        module: 'Provider',
        method: 'request',
        reason: new AiError.AuthenticationError({
          kind: 'InvalidKey',
          description: '503 overloaded; please retry your request',
        }),
      })
      const transient = new AiError.AiError({
        module: 'Provider',
        method: 'request',
        reason: new AiError.InternalProviderError({ description: 'billing diagnostic' }),
      })
      const local = new ModelError({
        reason: new ModelUnsupported({ message: 'Network error retry delay is unsupported' }),
      })
      assert.strictEqual(
        Agent.isRetryAllowed(Agent.defaultRetry, 1, Model.classify(permanent).retryable),
        false,
      )
      assert.strictEqual(
        Agent.isRetryAllowed(Agent.defaultRetry, 1, Model.classify(transient).retryable),
        true,
      )
      assert.strictEqual(
        Agent.isRetryAllowed(Agent.defaultRetry, 1, Model.classify(local).retryable),
        false,
      )
    })
    it('own-key ledgers preserve proto names, optional counters and immutable revisions', () => {
      const value = {
        ...Usage.zero(),
        input: 2,
        cacheWrite1h: 3,
        reasoning: 4,
        cost: { ...Usage.zero().cost, total: 0.1 },
      }
      const initial = Usage.empty()
      const first = Usage.record(initial, 'tools', '__proto__', value)
      const sum = Usage.sum([first, first])
      assert.strictEqual(Object.getPrototypeOf(sum.tools), Object.prototype)
      assert.strictEqual(Object.hasOwn(initial.tools, '__proto__'), false)
      assert.strictEqual(sum.tools['__proto__']?.input, 4)
      assert.strictEqual(sum.tools['__proto__']?.cacheWrite1h, 6)
      assert.strictEqual(sum.tools['__proto__']?.reasoning, 8)
      assert.strictEqual(sum.tools['__proto__']?.cost.total, 0.2)
    })
    it.effect('round trips schemas and converts native usage without invented prices', () =>
      Effect.gen(function* () {
        const value = Usage.fromResponse(
          new Response.Usage({
            inputTokens: { total: 20, cacheRead: 5, cacheWrite: 3, uncached: undefined },
            outputTokens: { total: 4, text: 2, reasoning: 2 },
          }),
        )
        assert.strictEqual(value.input, 12)
        assert.strictEqual(value.totalTokens, 24)
        assert.strictEqual(value.cost.total, 0)
        const asserts = new TestSchema.Asserts(Usage.Usage)
        yield* asserts.decoding().succeedEffect(value, value)
        yield* asserts.encoding().succeedEffect(value, value)
      }),
    )
  })
})
