/**
 * Canonical managed-section and tool-declaration codecs.
 *
 * @since 0.0.0
 */
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'

/**
 * Managed tool declaration; native provider arguments remain opaque serialized JSON.
 *
 * @category schemas
 * @since 0.0.0
 */
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
/**
 * SystemPatch tool declaration contract.
 *
 * @category models
 * @since 0.0.0
 */
export type ToolDeclaration = typeof ToolDeclaration.Type
/**
 * Serializable managed section/tool deltas shared by prompt planning and committed transcript metadata.
 *
 * @category schemas
 * @since 0.0.0
 */
export const SystemPatch = Schema.Struct({
  sections: SchemaField.optional(Schema.Record(Schema.String, Schema.NullOr(Schema.String))),
  toolsRemoved: SchemaField.optional(Schema.Array(Schema.String)),
  toolsAdded: SchemaField.optional(Schema.Array(ToolDeclaration)),
})
/**
 * SystemPatch system patch contract.
 *
 * @category models
 * @since 0.0.0
 */
export type SystemPatch = typeof SystemPatch.Type
