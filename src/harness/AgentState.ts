/**
 * What a conversation's agent is made of, as Schemas both the client and the loop use: the stored
 * `$agent` state, a change to it, and the resolved agent by name. `Agent` holds the document
 * and the resolution, on the loop side.
 */
import { Schema } from 'effect'
import { ModelRef, ModelThinkingLevel } from '../model/Message.ts'

/** Which extensions a conversation selects: exactly these, or the default selection edited. */
export const Selection = Schema.Union([
  Schema.Array(Schema.String),
  Schema.Struct({
    add: Schema.optionalKey(Schema.Array(Schema.String)),
    remove: Schema.optionalKey(Schema.Array(Schema.String)),
  }),
])
export type Selection = typeof Selection.Type

/** Which of the selected tools are enabled: exactly these, in order, or all but these. */
export const ToolFilter = Schema.Union([
  Schema.Array(Schema.String),
  Schema.Struct({ remove: Schema.Array(Schema.String) }),
])
export type ToolFilter = typeof ToolFilter.Type

/**
 * Which of the enabled tools the model is offered. Unset: every one except deferred tools. An
 * array: exactly these, in order, deferred ones too. `{ remove, add }`: every one except deferred
 * tools and those in `remove`, then the deferred tools in `add`, in `add` order.
 */
export const ModelTools = Schema.Union([
  Schema.Array(Schema.String),
  Schema.Struct({
    remove: Schema.optionalKey(Schema.Array(Schema.String)),
    add: Schema.optionalKey(Schema.Array(Schema.String)),
  }),
])
export type ModelTools = typeof ModelTools.Type

export const State = Schema.Struct({
  model: Schema.optionalKey(ModelRef),
  reasoning: Schema.optionalKey(ModelThinkingLevel),
  extensions: Schema.optionalKey(Selection),
  tools: Schema.optionalKey(ToolFilter),
  /** Tools a call loads, such as `tool_search`'s, are added to its `add`. */
  modelTools: Schema.optionalKey(ModelTools),
  /** Appended to the system prompt as the last section, `instructions`. */
  instructions: Schema.optionalKey(Schema.String),
  /** Encoded tool config by tool name, over the config its extension installs it with. */
  config: Schema.optionalKey(Schema.Record(Schema.String, Schema.Json)),
})
export type State = typeof State.Type

/** A change to `$agent`: a given field replaces the stored one, and `null` clears it. */
export const Change = Schema.Struct({
  model: Schema.optionalKey(Schema.NullOr(ModelRef)),
  reasoning: Schema.optionalKey(Schema.NullOr(ModelThinkingLevel)),
  extensions: Schema.optionalKey(Schema.NullOr(Selection)),
  tools: Schema.optionalKey(Schema.NullOr(ToolFilter)),
  modelTools: Schema.optionalKey(Schema.NullOr(ModelTools)),
  instructions: Schema.optionalKey(Schema.NullOr(Schema.String)),
  config: Schema.optionalKey(Schema.NullOr(Schema.Record(Schema.String, Schema.Json))),
})
export type Change = typeof Change.Type

/** A resolved agent by name, as a client sees it. */
export const View = Schema.Struct({
  model: Schema.optionalKey(ModelRef),
  reasoning: ModelThinkingLevel,
  extensions: Schema.Array(Schema.String),
  /** The tools the model is offered, in order. */
  tools: Schema.Array(Schema.String),
  /** Enabled deferred tools it is not offered yet, when there are any. */
  deferred: Schema.optionalKey(Schema.Array(Schema.String)),
  sections: Schema.Array(Schema.String),
  instructions: Schema.optionalKey(Schema.String),
})
export type View = typeof View.Type
