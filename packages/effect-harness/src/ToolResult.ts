import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'

/** Model-visible content reuses Effect's text and media codecs. */
export const ContentSchema = Schema.Array(Prompt.UserMessagePart)
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
