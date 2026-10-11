import { expect, test } from 'tstyche'
import type * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Prompt from 'effect/ai/Prompt'
import type * as Compaction from '../src/Compaction.js'
import type { CompactionError } from '../src/CompactionError.js'
import * as Plan from '../src/internal/CompactionPlan.js'

declare const selection: Compaction.Selected
declare const input: Plan.CanonicalInput
declare const fingerprint: Plan.Fingerprint
declare const prompt: Prompt.Prompt

test('private planning requires no services and exposes honest failure channels', () => {
  expect(Plan.capture(selection, input)).type.toBe<Effect.Effect<Plan.Fingerprint, Plan.Failure>>()
  expect(Plan.validate(fingerprint, input)).type.toBe<
    Effect.Effect<Plan.Fingerprint, Plan.Failure>
  >()
  expect(Plan.serialize(prompt)).type.toBe<
    Effect.Effect<Plan.Transcript, CompactionError | Schema.SchemaError>
  >()
  expect(Plan.preparePrompt(prompt, 'focus')).type.toBe<
    Effect.Effect<Plan.PreparedTranscript, CompactionError | Schema.SchemaError>
  >()
  expect(Schema.encodeEffect(Schema.fromJsonString(Plan.FingerprintSchema))(fingerprint)).type.toBe<
    Effect.Effect<string, Schema.SchemaError>
  >()
  expect<Plan.PreparedTranscript['prompt']>().type.toBe<Prompt.Prompt>()
  expect<Plan.Fingerprint['version']>().type.toBe<1>()
  expect<{ view: Plan.CanonicalInput['view'] }>().type.not.toBeAssignableTo<Plan.CanonicalInput>()
  expect<{ version: 2 }>().type.not.toBeAssignableTo<Plan.Fingerprint>()
  expect<Plan.Transcript>().type.not.toBeAssignableTo<Prompt.Prompt>()
})
