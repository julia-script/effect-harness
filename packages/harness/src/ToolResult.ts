/**
 * Model-visible tool envelopes and rendered execution diagnostics.
 *
 * @since 0.0.0
 */
import * as Effect from 'effect/Effect'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import type { Diagnostic, ToolResult } from './Invocation.ts'
import * as Serialization from './Serialization.ts'

/**
 * Canonical model-visible tool content.
 *
 * **Details**
 *
 * Providers translate this value at their native client boundary.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Envelope = Schema.TaggedStruct('@effect-harness/ToolContent', {
  content: Schema.Array(Prompt.UserMessagePart),
})
/**
 * ToolResult envelope contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Envelope = typeof Envelope.Type

/**
 * Renders execution diagnostics after the tool's original content, without exposing private details or controls.
 *
 * @category combinators
 * @since 0.0.0
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
 * @since 0.0.0
 */
export const encode = (self: ToolResult): Effect.Effect<Schema.Json, Schema.SchemaError> =>
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
 * Decode only the explicit harness envelope; unrelated native tool results are handled by the native provider.
 *
 * @category combinators
 * @since 0.0.0
 */
export const decode: (
  input: unknown,
  options?: import('effect/SchemaAST').ParseOptions,
) => Effect.Effect<Envelope, Schema.SchemaError> = Schema.decodeUnknownEffect(
  Schema.toCodecJson(Envelope),
)

/**
 * Checks whether an unknown value satisfies the Envelope contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isEnvelope: (u: unknown) => u is Envelope = Schema.is(Envelope)
