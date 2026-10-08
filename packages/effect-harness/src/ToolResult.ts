/**
 * Model-visible tool envelopes and rendered execution diagnostics.
 */
import * as Effect from 'effect/Effect'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import type { Diagnostic, Result } from './Invocation.ts'
import * as Serialization from './Serialization.ts'

/**
 * Versioned model-visible tool-content envelope stored in native Prompt JSON.
 *
 * **Details**
 *
 * Contains ordered native user-message parts and rendered diagnostics. Provider adapters
 * expand supported media inside the original tool-result item; private details, controls and
 * usage stay separate.
 *
 * @category schemas
 */
export const Envelope = Schema.TaggedStruct('@effect-harness/ToolContent', {
  content: Schema.Array(Prompt.UserMessagePart),
})
/**
 * Decoded value validated by the `Envelope` schema.
 *
 * @category models
 */
export type Envelope = typeof Envelope.Type

/**
 * Renders execution diagnostics after the tool's original content, without exposing private details or controls.
 *
 * @category combinators
 */
export function renderDiagnostics(self: ReadonlyArray<Diagnostic>): string {
  return `<harness>\n${self
    .map((diagnostic) => {
      const severity =
        diagnostic.severity ?? (diagnostic.kind === 'tool_error' ? 'error' : 'warning')
      const message =
        diagnostic.message ??
        `${diagnostic.kind}${diagnostic.detail === undefined ? '' : `: ${Serialization.display(diagnostic.detail)}`}`
      return `[${severity}] ${message}`
    })
    .join('\n')}\n</harness>`
}

/**
 * Retains text/file blocks and their native options; bookkeeping stays in the committed entry's data.
 *
 * @category combinators
 */
export const encode = (self: Result): Effect.Effect<Schema.Json, Schema.SchemaError> =>
  Schema.encodeEffect(Schema.toCodecJson(Envelope))({
    _tag: '@effect-harness/ToolContent',
    content: [
      ...(self.content ?? []),
      ...(self.diagnostics === undefined || self.diagnostics.length === 0
        ? []
        : [Prompt.textPart({ text: renderDiagnostics(self.diagnostics) })]),
    ],
  }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)))

/**
 * Decodes the explicit harness tool-content envelope.
 *
 * **Details**
 *
 * Unrelated native tool results are handled by the native provider.
 *
 * @category combinators
 */
export const decode: (
  input: unknown,
  options?: import('effect/SchemaAST').ParseOptions,
) => Effect.Effect<Envelope, Schema.SchemaError> = Schema.decodeUnknownEffect(
  Schema.toCodecJson(Envelope),
)

/**
 * Checks whether a value satisfies the decoded `Envelope` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isEnvelope: (u: unknown) => u is Envelope = Schema.is(Envelope)
