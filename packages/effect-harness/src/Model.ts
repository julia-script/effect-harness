import type { ProviderAffinity } from './ProviderAffinity.js'
import * as Usage from './Usage.js'
/** Schema-backed model metadata and live Effect LanguageModel handles. */
import type * as Context from 'effect/Context'
import * as Capability from './internal/Capability.js'
import type * as Effect from 'effect/Effect'
import type * as Pipeable from 'effect/Pipeable'
import * as Schema from 'effect/Schema'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import type { ModelError } from './ModelError.js'

export const TypeId = '~effect-harness/Model'

export const ReferenceSchema = Schema.Struct({
  provider: Schema.NonEmptyString,
  modelId: Schema.NonEmptyString,
})
export type Reference = typeof ReferenceSchema.Type

export const CapabilitiesSchema = Schema.Struct({
  tools: Schema.Boolean,
  reasoning: Schema.Boolean,
  images: Schema.Boolean,
  structuredOutput: Schema.Boolean,
})
export type Capabilities = typeof CapabilitiesSchema.Type

/** Limits are explicit caller declarations, not a claim of current remote availability. */
export const DefinitionSchema = Schema.Struct({
  ref: ReferenceSchema,
  pricing: Schema.optionalKey(Usage.PricingSchema),
  capabilities: CapabilitiesSchema,
  contextWindow: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  maxOutputTokens: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
})
export type Definition = typeof DefinitionSchema.Type

export const SelectionSchema = Schema.Struct({
  ref: ReferenceSchema,
  options: Schema.JsonObject,
})
export type Selection = typeof SelectionSchema.Type

export interface Any extends Pipeable.Pipeable {
  readonly [TypeId]: { readonly _Requirements: (_: never) => unknown }
  readonly definition: Definition
  readonly languageModel: typeof LanguageModel.LanguageModel.Service
  readonly options: Schema.Constraint
  readonly configure: (options: never) => unknown
}

export type Requirements<M extends Any> = M extends {
  readonly [TypeId]: { readonly _Requirements: (_: never) => infer R }
}
  ? Exclude<R, ProviderAffinity>
  : never

export interface Model<Options extends Schema.Constraint, R = never> extends Any {
  readonly [TypeId]: { readonly _Requirements: (_: never) => R }
  readonly options: Options
  /**
   * Maps schema-decoded options to provider request services without changing the pinned model.
   * ProviderAffinity is supplied by the runtime. Native LanguageModel callbacks can read
   * it with Effect.serviceOption because their required services are owned by Effect AI.
   */
  readonly configure: (
    options: Options['Type'],
  ) => Effect.Effect<Context.Context<never>, ModelError, R>
}

/** Receives an already constructed provider; provider Layers retain resource ownership. */
export const make: <Options extends Schema.Constraint, R>(input: {
  readonly definition: Definition
  readonly languageModel: typeof LanguageModel.LanguageModel.Service
  readonly options: Options
  readonly configure: (
    options: Options['Type'],
  ) => Effect.Effect<Context.Context<never>, ModelError, R>
}) => Model<Options, R | Options['DecodingServices'] | Options['EncodingServices']> = (input) => ({
  ...input,
  [TypeId]: { _Requirements: (_: never) => _ },
  pipe: Capability.pipe,
})

export type { ModelError } from './ModelError.js'
