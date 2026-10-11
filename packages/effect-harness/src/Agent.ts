import * as Schema from 'effect/Schema'
import * as Model from './Model.js'

/** Stable registry IDs select bound toolkit services; no execution environment is serialized. */
export const ToolkitSelectionSchema = Schema.Struct({
  /** Inherit the supplied default registrations, then append registrations in this order. */
  inherit: Schema.Boolean,
  registrations: Schema.Array(Schema.NonEmptyString),
  disabled: Schema.optionalKey(Schema.Array(Schema.NonEmptyString)),
  /** Explicit bindings never fall back to a different owner. */
  bindings: Schema.optionalKey(
    Schema.Record(
      Schema.NonEmptyString,
      Schema.Struct({
        binding: Schema.NonEmptyString,
        version: Schema.optionalKey(
          Schema.Int.check(
            Schema.isGreaterThan(0),
            Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
          ),
        ),
      }),
    ),
  ),
})
export type ToolkitSelection = typeof ToolkitSelectionSchema.Type

/** Persisted conversation configuration contains names and options, not live capabilities. */
export const StateSchema = Schema.Struct({
  model: Schema.optionalKey(Model.SelectionSchema),
  extensions: Schema.optionalKey(Schema.Array(Schema.NonEmptyString)),
  tools: Schema.optionalKey(Schema.Array(Schema.NonEmptyString)),
  modelTools: Schema.optionalKey(Schema.Array(Schema.NonEmptyString)),
  toolkits: Schema.optionalKey(ToolkitSelectionSchema),
  instructions: Schema.optionalKey(Schema.String),
})
export type State = typeof StateSchema.Type

/** Omission preserves a setting; null clears its conversation override. */
export const ChangeSchema = Schema.Struct({
  model: Schema.optionalKey(Schema.NullOr(Model.SelectionSchema)),
  extensions: Schema.optionalKey(Schema.NullOr(Schema.Array(Schema.NonEmptyString))),
  tools: Schema.optionalKey(Schema.NullOr(Schema.Array(Schema.NonEmptyString))),
  modelTools: Schema.optionalKey(Schema.NullOr(Schema.Array(Schema.NonEmptyString))),
  toolkits: Schema.optionalKey(Schema.NullOr(ToolkitSelectionSchema)),
  instructions: Schema.optionalKey(Schema.NullOr(Schema.String)),
})
export type Change = typeof ChangeSchema.Type
