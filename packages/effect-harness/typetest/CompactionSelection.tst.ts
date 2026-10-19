import { expect, test } from 'tstyche'
import type * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as AiError from 'effect/ai/AiError'
import type * as Prompt from 'effect/ai/Prompt'
import * as Compaction from 'effect-harness/Compaction'
import type { CompactionError } from 'effect-harness/CompactionError'
import type * as Projection from 'effect-harness/ContextProjection'
import type * as Model from 'effect-harness/Model'
import type * as Settings from 'effect-harness/Settings'
import type * as Identity from 'effect-harness/Identity'
import type * as Usage from 'effect-harness/Usage'

declare const view: Projection.ContextView
declare const policy: Settings.Compaction
declare const selection: Compaction.Selected
declare const config: Compaction.SummaryConfig
declare const response: Model.DeferredResponse
declare const nativeError: AiError.AiError
declare const request: Compaction.SummaryRequest

test('public selection consumes existing policy and canonical native context with no required service', () => {
  expect(Compaction.select(view, policy, { reason: 'manual' })).type.toBe<
    Effect.Effect<Compaction.Result, Compaction.Failure>
  >()
  expect<Compaction.Policy>().type.toBe<Settings.Compaction>()
  expect<Compaction.Selected['source']>().type.toBe<Prompt.Prompt>()
  expect<Compaction.Selected['legacyInterpretation']>().type.toBe<Projection.LegacyInterpretation>()
  expect<{ reason: 'automatic' }>().type.not.toBeAssignableTo<Compaction.Input>()
  expect<Compaction.Result>().type.not.toBeAssignableTo<Compaction.Selected>()
})

test('summary request captures serializable config and distinct prepared/attempt identities', () => {
  expect(Compaction.pinRequest(selection, config)).type.toBe<
    Effect.Effect<Compaction.SummaryRequest, Compaction.Failure>
  >()
  expect<Compaction.SummaryRequest['config']['requestId']>().type.toBe<Identity.RequestId>()
  expect<Compaction.SummaryRequest['config']['attempt']>().type.toBe<Usage.AttemptId>()
  expect<Compaction.SummaryRequest['config']['model']>().type.toBe<Model.Selection>()
  expect<Compaction.SummaryRequest['config']['stream']>().type.toBe<Settings.Stream>()
  expect(
    Schema.encodeEffect(Schema.fromJsonString(Compaction.SummaryRequestSchema))(request),
  ).type.toBe<Effect.Effect<string, Schema.SchemaError>>()
  expect<{
    model: Model.Selection
    stream: Settings.Stream
    maxOutputTokens: number
  }>().type.not.toBeAssignableTo<Compaction.SummaryConfig>()
})

test('native completed model responses validate without provider/runtime services', () => {
  expect(Compaction.validateSummary(response)).type.toBe<Effect.Effect<string, CompactionError>>()
  expect(Compaction.modelFailure(nativeError, true)).type.toBe<CompactionError>()
})
