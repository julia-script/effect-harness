import * as Effect from 'effect/Effect'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import type { Diagnostic, ToolResult } from './Invocation.ts'
import * as Serialization from './Serialization.ts'

/** Canonical model-visible tool content. Providers translate this value at their native client boundary. */
export const Envelope = Schema.TaggedStruct('@effect-harness/ToolContent', {
  content: Schema.Array(Prompt.UserMessagePart),
})
export type Envelope = typeof Envelope.Type

/** Render execution diagnostics after the tool's original content, without exposing private details or controls. */
export function renderDiagnostics(diagnostics: ReadonlyArray<Diagnostic>): string {
  return `<harness>\n${diagnostics
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

/** Retains text/file blocks and their native options; bookkeeping stays in the committed entry's data. */
export const encode = (result: ToolResult): Effect.Effect<Schema.Json, Schema.SchemaError> =>
  Schema.encodeEffect(Schema.toCodecJson(Envelope))({
    _tag: '@effect-harness/ToolContent',
    content: [
      ...(result.content ?? []),
      ...(result.diagnostics === undefined || result.diagnostics.length === 0
        ? []
        : [Prompt.textPart({ text: renderDiagnostics(result.diagnostics) })]),
    ],
  }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)))

/** Decode only the explicit harness envelope; unrelated native tool results are handled by the native provider. */
export const decode: (
  input: unknown,
  options?: import('effect/SchemaAST').ParseOptions,
) => Effect.Effect<Envelope, Schema.SchemaError> = Schema.decodeUnknownEffect(
  Schema.toCodecJson(Envelope),
)

export const isEnvelope: (input: unknown) => input is Envelope = Schema.is(Envelope)
