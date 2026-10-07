import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'

/** Managed tool declaration; native provider arguments remain opaque serialized JSON. */
export const ToolDeclaration = Schema.Struct({
  name: Schema.String,
  description: SchemaField.optional(Schema.String),
  parameters: Schema.JsonObject,
  provider: SchemaField.optional(
    Schema.Struct({
      id: Schema.String.check(Schema.isPattern(/^[^.]+\..+$/)),
      name: Schema.String,
      args: Schema.Json,
    }),
  ),
})
export type ToolDeclaration = typeof ToolDeclaration.Type
/** Serializable managed section/tool deltas shared by prompt planning and committed transcript metadata. */
export const SystemPatch = Schema.Struct({
  sections: SchemaField.optional(Schema.Record(Schema.String, Schema.NullOr(Schema.String))),
  toolsRemoved: SchemaField.optional(Schema.Array(Schema.String)),
  toolsAdded: SchemaField.optional(Schema.Array(ToolDeclaration)),
})
export type SystemPatch = typeof SystemPatch.Type
