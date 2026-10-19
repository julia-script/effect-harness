import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import type * as ToolResult from '../ToolResult.js'

/** A coding operation failed; native causes stay available to direct Effect consumers. */
export class CodingFileError extends Schema.TaggedError<CodingFileError>()('CodingFileError', {
  reason: Schema.Literals([
    'invalidPath',
    'invalidLimits',
    'encoding',
    'beyondEOF',
    'changed',
    'closed',
    'filesystem',
  ]),
  path: Schema.String,
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

/** Serializable failure output, independent of the native cause. */
export const FailureSchema = Schema.Struct({
  reason: CodingFileError.fields.reason,
  path: Schema.String,
  message: Schema.String,
})
export type Failure = typeof FailureSchema.Type

/** Diagnostics must be model-visible too; structured output alone is not sent to the model. */
export const failureOutput = (error: CodingFileError): ToolResult.Output<Failure> => ({
  content: [Prompt.textPart({ text: error.message })],
  structuredOutput: { reason: error.reason, path: error.path, message: error.message },
  diagnostics: [{ severity: 'error', code: error.reason, message: error.message }],
})

export const positiveInteger = (
  path: string,
  field: string,
  value: number,
  maximum = Number.MAX_SAFE_INTEGER,
) =>
  Effect.suspend(() =>
    Number.isSafeInteger(value) && value > 0 && value <= maximum
      ? Effect.void
      : Effect.fail(
          new CodingFileError({
            reason: 'invalidLimits',
            path,
            message: `${field} must be an integer between 1 and ${maximum}`,
          }),
        ),
  )
