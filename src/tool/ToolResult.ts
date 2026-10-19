/**
 * Tool results, ported from pi's `ToolExecutionResult` and `appendToolResult`: what a call
 * returns to the model, and the `pi.tool-result` entry that records it.
 *
 * Diagnostics are remarks about a call for the model and the UI, such as truncation or why a call
 * was refused; they are never part of the tool's own result. The entry's message ends with them
 * rendered, so the stored message is exactly what the model sees, and `data` keeps them as a list.
 */
import { Schema } from 'effect'
import type { Effect } from 'effect'
import { ImageContent, TextContent } from '../model/Message.ts'
import type { ToolCall, ToolResultMessage } from '../model/Message.ts'
import type { Tx } from '../session/Session.ts'
import type { TxError } from '../session/SessionError.ts'
import type { EntryRecord } from '../storage/schema/Entry.ts'
import type { ConversationId } from '../storage/schema/Ids.ts'
import type { Content } from './Tool.ts'

/** The entry kind of a tool result. */
export const kind = 'pi.tool-result'

export const Diagnostic = Schema.Struct({
  severity: Schema.Literals(['info', 'warn', 'error']),
  message: Schema.String,
  code: Schema.optionalKey(Schema.String),
})
export type Diagnostic = typeof Diagnostic.Type

/** A result entry's `data`. */
export const Data = Schema.Struct({ diagnostics: Schema.Array(Diagnostic) })
export type Data = typeof Data.Type

/**
 * What a call asks of its generation once its round ends: `addTools` loads tools by name into the
 * conversation's offered tools, so the next request offers them.
 */
export const Control = Schema.Struct({
  addTools: Schema.optionalKey(Schema.Array(Schema.String)),
})
export type Control = typeof Control.Type

/** What a call returns to the model. */
export interface Result {
  readonly output: Content
  readonly isError: boolean
  readonly diagnostics: ReadonlyArray<Diagnostic>
  /** Detail for programs and the UI, such as a diff; the model does not see it. */
  readonly details?: Schema.Json
  readonly control?: Control
}

/**
 * A handler's result in full, for a tool whose success or failure Schema is `Output`: the content
 * the model sees, such as an image, remarks about the call, and details the model does not see.
 * Any other value is shown as text, or as JSON.
 */
export const Output = Schema.Struct({
  type: Schema.Literal('pi.tool-output'),
  content: Schema.Array(Schema.Union([TextContent, ImageContent])),
  diagnostics: Schema.optionalKey(Schema.Array(Diagnostic)),
  details: Schema.optionalKey(Schema.Json),
  control: Schema.optionalKey(Control),
})
export type Output = typeof Output.Type

/** What an `Output` holds besides its content. */
export interface Options {
  readonly diagnostics?: ReadonlyArray<Diagnostic>
  readonly details?: Schema.Json
  readonly control?: Control
}

/** An `Output` with `content`. */
export const output = (content: Content, options: Options = {}): Output => ({
  type: 'pi.tool-output',
  content,
  ...options,
})

/** An `Output` of one text block. */
export const text = (value: string, options: Options = {}): Output =>
  output(value === '' ? [] : [{ type: 'text', text: value }], options)

/** An error diagnostic. */
export const error = (code: string, message: string): Diagnostic => ({
  severity: 'error',
  code,
  message,
})

/** An error result the harness writes itself: no output and one error diagnostic. */
export const failure = (code: string, message: string): Result => ({
  output: [],
  isError: true,
  diagnostics: [error(code, message)],
})

const render = (diagnostics: ReadonlyArray<Diagnostic>) =>
  `<harness>\n${diagnostics.map(({ severity, message }) => `[${severity}] ${message}`).join('\n')}\n</harness>`

/** The message the model sees for `result` of `call`. */
export const message = (
  call: ToolCall,
  result: Result,
  timing: { readonly timestamp: number; readonly durationMs?: number },
): ToolResultMessage => ({
  role: 'toolResult',
  toolCallId: call.id,
  toolName: call.name,
  content:
    result.diagnostics.length === 0
      ? result.output
      : [...result.output, { type: 'text', text: render(result.diagnostics) }],
  ...(result.details === undefined ? {} : { details: result.details }),
  isError: result.isError,
  timestamp: timing.timestamp,
  ...(timing.durationMs === undefined ? {} : { durationMs: timing.durationMs }),
})

/** Append the `pi.tool-result` entry of `result`. */
export const append = (
  tx: Tx,
  conversationId: ConversationId,
  call: ToolCall,
  result: Result,
  timing: { readonly timestamp: number; readonly durationMs?: number },
): Effect.Effect<EntryRecord, TxError> =>
  tx.appendEntry(conversationId, {
    kind,
    model: [{ ...message(call, result, timing) }],
    data: { diagnostics: result.diagnostics.map((diagnostic) => ({ ...diagnostic })) },
  })
