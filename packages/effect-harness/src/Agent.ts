import * as Schema from 'effect/Schema'
import * as Model from './Model.js'

/** Persisted conversation configuration contains names and options, not live capabilities. */
export const StateSchema = Schema.Struct({
  model: Schema.optionalKey(Model.SelectionSchema),
  extensions: Schema.optionalKey(Schema.Array(Schema.NonEmptyString)),
  tools: Schema.optionalKey(Schema.Array(Schema.NonEmptyString)),
  instructions: Schema.optionalKey(Schema.String),
})
export type State = typeof StateSchema.Type

/** Omission preserves a setting; null clears its conversation override. */
export const ChangeSchema = Schema.Struct({
  model: Schema.optionalKey(Schema.NullOr(Model.SelectionSchema)),
  extensions: Schema.optionalKey(Schema.NullOr(Schema.Array(Schema.NonEmptyString))),
  tools: Schema.optionalKey(Schema.NullOr(Schema.Array(Schema.NonEmptyString))),
  instructions: Schema.optionalKey(Schema.NullOr(Schema.String)),
})
export type Change = typeof ChangeSchema.Type
