import * as TestSchema from 'effect/testing/TestSchema'

import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'

import * as Invocation from 'effect-harness/Invocation'

import * as Model from 'effect-harness/Model'

import * as ToolRegistration from 'effect-harness/ToolRegistration'

import * as Usage from 'effect-harness/Usage'

import * as ModelError from 'effect-harness/ModelError'

import * as Bash from 'effect-harness/tools/Bash'

import * as Read from 'effect-harness/tools/Read'

// This original single regression pins explicit undefined/JSON omission across the cooperating invocation, usage, model, intent and tool-input boundaries; its complete registration is preserved.

describe('InvocationOptionalFields', () => {
  const emptyUsage = {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  }
  it.effect(
    'owned optional result, usage, error, intent and tool input fields admit undefined while JSON omits it',
    () =>
      Effect.gen(function* () {
        yield* new TestSchema.Asserts(Invocation.Result).decoding().succeedEffect(
          {
            content: undefined,
            details: undefined,
            diagnostics: undefined,
            control: { terminate: undefined, reset: { note: undefined }, addTools: undefined },
            usage: undefined,
            isError: undefined,
          },
          {
            content: undefined,
            details: undefined,
            diagnostics: undefined,
            control: { terminate: undefined, reset: { note: undefined }, addTools: undefined },
            usage: undefined,
            isError: undefined,
          },
        )
        const result = yield* Schema.decodeEffect(Invocation.Result)({
          content: undefined,
          details: undefined,
          diagnostics: undefined,
          control: { terminate: undefined, reset: { note: undefined }, addTools: undefined },
          usage: undefined,
          isError: undefined,
        })
        // effect-nit-allow P8-testschema-asserts: this assertion pins exact JSON byte ordering and undefined omission, rather than schema value equivalence.
        assert.strictEqual(
          JSON.stringify(yield* Schema.encodeEffect(Schema.toCodecJson(Invocation.Result))(result)),
          '{"control":{"reset":{}}}',
        )
        yield* new TestSchema.Asserts(Usage.Usage).decoding().succeedEffect(
          {
            ...emptyUsage,
            reasoning: undefined,
            cacheWrite1h: undefined,
            cost: { ...emptyUsage.cost, known: undefined, totalKnown: undefined },
          },
          {
            ...emptyUsage,
            reasoning: undefined,
            cacheWrite1h: undefined,
            cost: { ...emptyUsage.cost, known: undefined, totalKnown: undefined },
          },
        )
        const usage = yield* Schema.decodeEffect(Usage.Usage)({
          ...emptyUsage,
          reasoning: undefined,
          cacheWrite1h: undefined,
          cost: { ...emptyUsage.cost, known: undefined, totalKnown: undefined },
        })
        assert.strictEqual(usage.reasoning, undefined)
        yield* new TestSchema.Asserts(ModelError.ModelNoModelError).decoding().succeedEffect(
          { _tag: 'ModelNoModelError', message: 'missing', cause: undefined, usage: undefined },
          new ModelError.ModelNoModelError({
            message: 'missing',
            cause: undefined,
            usage: undefined,
          }),
        )
        const error = yield* Schema.decodeEffect(ModelError.ModelNoModelError)({
          _tag: 'ModelNoModelError',
          message: 'missing',
          cause: undefined,
          usage: undefined,
        })
        assert.strictEqual(error.usage, undefined)
        yield* new TestSchema.Asserts(Model.RequestOptions).decoding().succeedEffect(
          {
            thinking: 'off',
            options: {},
            sessionId: undefined,
            maxTokens: undefined,
            cache: undefined,
          },
          {
            thinking: 'off',
            options: {},
            sessionId: undefined,
            maxTokens: undefined,
            cache: undefined,
          },
        )
        yield* new TestSchema.Asserts(Model.DeferredDecision)
          .decoding()
          .succeedEffect(
            { handle: {}, pollAfterMs: undefined },
            { handle: {}, pollAfterMs: undefined },
          )
        yield* new TestSchema.Asserts(ToolRegistration.Intent)
          .decoding()
          .succeedEffect(
            { id: 'c', name: 't', args: {}, encodedArgs: undefined, replay: 'safe' },
            { id: 'c', name: 't', args: {}, encodedArgs: undefined, replay: 'safe' },
          )
        yield* new TestSchema.Asserts(Bash.Parameters)
          .decoding()
          .succeedEffect(
            { command: 'true', timeout: undefined },
            { command: 'true', timeout: undefined },
          )
        yield* new TestSchema.Asserts(Read.Parameters)
          .decoding()
          .succeedEffect(
            { path: 'file', offset: undefined, limit: undefined },
            { path: 'file', offset: undefined, limit: undefined },
          )
      }),
  )
})
