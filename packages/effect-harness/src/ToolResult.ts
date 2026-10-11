import * as Usage from './Usage.js'
import * as Schema from 'effect/Schema'
import * as SchemaTransformation from 'effect/SchemaTransformation'
import * as Prompt from 'effect/ai/Prompt'
import * as ToolControl from './ToolControl.js'

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

/**
 * Handler-owned channels for Tool.makeResult. Only content is sent to the model.
 * Omitted structured output stays absent; details are optional JSON for UI consumers.
 * The same shape is used for successful values and declared failures.
 * spend is opt-in reported cost with caller currency/source; unknown spend is omitted.
 * Hooks may replace it with the rest of the final result; only the committed final
 * declaration is accounted. Interrupted/uncommitted external charges remain unknown.
 */
export interface Output<A = Schema.Json> {
  readonly content: Content
  /** Explicit reported spend, persisted with the final result and never sent to the model. */
  readonly spend?: Usage.Cost
  readonly structuredOutput?: A
  readonly details?: Schema.Json
  readonly diagnostics?: ReadonlyArray<Diagnostic>
  /** Only final model-call receipts may apply controls. Nested projection strips them. */
  readonly control?: ToolControl.Control
}

export interface StrictOutput<A = Schema.Json> extends Output<A> {
  readonly structuredOutput: A
}

/** Wrap a structured-output schema in the independent handler-result channels. */
export const outputSchema = <S extends Schema.Constraint>(structuredOutput: S) =>
  Schema.Struct({
    content: ContentSchema,
    spend: Schema.optionalKey(Usage.CostSchema),
    structuredOutput: Schema.optionalKey(structuredOutput),
    details: Schema.optionalKey(Schema.Json),
    diagnostics: Schema.optionalKey(Schema.Array(DiagnosticSchema)),
    control: Schema.optionalKey(ToolControl.ControlSchema),
  })
export type OutputSchema<S extends Schema.Constraint> = ReturnType<typeof outputSchema<S>>

/** Strict success requires programmatic data. Error envelopes still permit omission. */
export const strictOutputSchema = <S extends Schema.Constraint>(structuredOutput: S) =>
  outputSchema(structuredOutput).mapFields((fields) => ({ ...fields, structuredOutput }))
export type StrictOutputSchema<S extends Schema.Constraint> = ReturnType<
  typeof strictOutputSchema<S>
>

export const ResultSchema = Schema.Struct({
  content: ContentSchema,
  isError: Schema.Boolean,
  spend: Schema.optionalKey(Usage.CostSchema),
  structuredOutput: Schema.optionalKey(Schema.Json),
  details: Schema.optionalKey(Schema.Json),
  diagnostics: Schema.Array(DiagnosticSchema),
  control: Schema.optionalKey(ToolControl.ControlSchema),
})
export type Result = typeof ResultSchema.Type

const NestedFields = {
  isError: Schema.Boolean,
  spend: Schema.optionalKey(Usage.CostSchema),
  details: Schema.optionalKey(Schema.Json),
  diagnostics: Schema.Array(DiagnosticSchema),
}
/** The discriminator keeps programmatic file-shaped JSON distinct from native fallback media. */
export const NestedSchema = Schema.Union([
  Schema.Struct({
    ...NestedFields,
    valueKind: Schema.Literal('structured'),
    structuredOutput: Schema.optionalKey(Schema.Json),
  }),
  Schema.Struct({
    ...NestedFields,
    valueKind: Schema.Literal('content'),
    structuredOutput: Schema.Union([Schema.String, Prompt.TextPart, FilePart, ContentSchema]),
  }),
])
export type Nested = typeof NestedSchema.Type

/** No content becomes empty text, one text becomes its value, one media part stays native. */
export const contentValue = (content: Content): string | Content[number] | Content => {
  if (content.length === 0) return ''
  if (content.length !== 1) return content
  const part = content[0]!
  return part.type === 'text' ? part.text : part
}

/** Keep UI/accounting/diagnostics; never forward controls or the model content channel. */
export const nested = (result: Result, declaredStructuredOutput: boolean): Nested => {
  const common = {
    isError: result.isError,
    diagnostics: result.diagnostics,
    ...(result.spend === undefined ? {} : { spend: result.spend }),
    ...(result.details === undefined ? {} : { details: result.details }),
  }
  if (result.structuredOutput === undefined && !declaredStructuredOutput)
    return { ...common, valueKind: 'content', structuredOutput: contentValue(result.content) }
  return {
    ...common,
    valueKind: 'structured',
    ...(result.structuredOutput === undefined ? {} : { structuredOutput: result.structuredOutput }),
  }
}

/** Tool media transported inside native provider function results. */
export const Envelope = Schema.TaggedStruct('@effect-harness/ToolContent', {
  content: ContentSchema,
})
export type Envelope = typeof Envelope.Type
