import * as Schema from 'effect/Schema'
import * as SchemaTransformation from 'effect/SchemaTransformation'
import * as Prompt from 'effect/ai/Prompt'

const Bytes = Schema.Uint8Array.annotate({
  toCodecJson: () =>
    Schema.link<Uint8Array>()(
      Schema.TaggedStruct('@effect-harness/Bytes', { data: Schema.Uint8ArrayFromBase64 }),
      SchemaTransformation.transform({
        decode: (encoded) => encoded.data,
        encode: (data) => ({ _tag: '@effect-harness/Bytes' as const, data }),
      }),
    ),
})
const FilePart = Prompt.FilePart.mapFields((fields) => ({
  ...fields,
  data: Schema.Union([Schema.String, Bytes, Schema.URL]),
}))

/**
 * Model-visible content uses native Effect text and media parts.
 * JSON codecs tag byte arrays to distinguish them from literal string data.
 */
export const ContentSchema = Schema.Array(Schema.Union([Prompt.TextPart, FilePart]))
export type Content = typeof ContentSchema.Type

export const DiagnosticSchema = Schema.Struct({
  severity: Schema.Literals(['info', 'warning', 'error']),
  message: Schema.String,
  code: Schema.optionalKey(Schema.String),
})
export type Diagnostic = typeof DiagnosticSchema.Type

/** Reporting is separate from the handler's schema-typed final value. */
export const ProgressSchema = Schema.Struct({
  output: Schema.optionalKey(Schema.String),
  details: Schema.optionalKey(Schema.Json),
  diagnostics: Schema.optionalKey(Schema.Array(DiagnosticSchema)),
})
export type Progress = typeof ProgressSchema.Type

export const ResultSchema = Schema.Struct({
  content: ContentSchema,
  isError: Schema.Boolean,
  details: Schema.optionalKey(Schema.Json),
  diagnostics: Schema.Array(DiagnosticSchema),
})
export type Result = typeof ResultSchema.Type

/** Tool media transported inside native provider function results. */
export const Envelope = Schema.TaggedStruct('@effect-harness/ToolContent', {
  content: ContentSchema,
})
export type Envelope = typeof Envelope.Type
