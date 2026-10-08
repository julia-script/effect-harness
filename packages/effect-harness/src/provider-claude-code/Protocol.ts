/**
 * Validated Claude Code protocol frames and accounting fields.
 */
import type * as AiError from 'effect/ai/AiError'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Tool from 'effect/ai/Tool'
import { protocol } from './ClaudeCodeError.ts'

const Count = Schema.Natural.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER))
/**
 * Optional token counts reported by the CLI.
 *
 * @category models
 */
export const Usage = Schema.Struct({
  input_tokens: Schema.optionalKey(Count),
  output_tokens: Schema.optionalKey(Count),
  cache_read_input_tokens: Schema.optionalKey(Count),
  cache_creation_input_tokens: Schema.optionalKey(Count),
})
/**
 * Optional token counts reported by the CLI.
 *
 * @category models
 */
export type Usage = typeof Usage.Type
/**
 * Checks whether a value satisfies the decoded `Usage` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isUsage: (u: unknown) => u is Usage = Schema.is(Usage)
/**
 * CLI model-specific usage, pricing and context limits.
 *
 * @category models
 */
export const ModelUsage = Schema.Struct({
  inputTokens: Count,
  outputTokens: Count,
  thinkingTokens: Schema.optionalKey(Count),
  cacheReadInputTokens: Count,
  cacheCreationInputTokens: Count,
  webSearchRequests: Count,
  costUSD: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  contextWindow: Count,
  maxOutputTokens: Count,
})
/**
 * CLI model-specific usage, pricing and context limits.
 *
 * @category models
 */
export type ModelUsage = typeof ModelUsage.Type
/**
 * Text, thinking or tool-intent block emitted by the CLI.
 *
 * @category models
 */
export const Block = Schema.Union([
  Schema.Struct({ type: Schema.Literal('text'), text: Schema.String }),
  Schema.Struct({
    type: Schema.Literal('thinking'),
    thinking: Schema.String,
    signature: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({ type: Schema.Literal('redacted_thinking'), data: Schema.String }),
  Schema.Struct({
    type: Schema.Literal('tool_use'),
    id: Schema.NonEmptyString,
    name: Schema.NonEmptyString,
    input: Schema.JsonObject,
  }),
])
/**
 * Text, thinking or tool-intent block emitted by the CLI.
 *
 * @category models
 */
export type Block = typeof Block.Type
/**
 * Checks whether a value satisfies the decoded `Block` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isBlock: (u: unknown) => u is Block = Schema.is(Block)
const Message = Schema.Struct({
  id: Schema.NonEmptyString,
  model: Schema.String,
  content: Schema.Array(Block),
  usage: Usage,
  stop_reason: Schema.optionalKey(Schema.NullOr(Schema.String)),
})
const Delta = Schema.Union([
  Schema.Struct({ type: Schema.Literal('text_delta'), text: Schema.String }),
  Schema.Struct({ type: Schema.Literal('thinking_delta'), thinking: Schema.String }),
  Schema.Struct({ type: Schema.Literal('signature_delta'), signature: Schema.String }),
  Schema.Struct({ type: Schema.Literal('input_json_delta'), partial_json: Schema.String }),
])
const Partial = Schema.Union([
  Schema.Struct({ type: Schema.Literal('message_start'), message: Message }),
  Schema.Struct({
    type: Schema.Literal('content_block_start'),
    index: Count,
    content_block: Block,
  }),
  Schema.Struct({ type: Schema.Literal('content_block_delta'), index: Count, delta: Delta }),
  Schema.Struct({ type: Schema.Literal('content_block_stop'), index: Count }),
  Schema.Struct({
    type: Schema.Literal('message_delta'),
    delta: Schema.Struct({ stop_reason: Schema.optionalKey(Schema.NullOr(Schema.String)) }),
    usage: Usage,
  }),
  Schema.Struct({ type: Schema.Literal('message_stop') }),
  Schema.Struct({ type: Schema.Literal('ping') }),
  Schema.Struct({ type: Schema.Literal('error'), error: Schema.Struct({ type: Schema.String }) }),
])
/**
 * Validated stream event emitted by the installed CLI.
 *
 * @category models
 */
export const Event = Schema.Union([
  Schema.Struct({
    type: Schema.Literal('system'),
    subtype: Schema.String,
    tools: Schema.optionalKey(Schema.Array(Schema.String)),
    model: Schema.optionalKey(Schema.String),
    session_id: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal('assistant'),
    message: Message,
    parent_tool_use_id: Schema.optionalKey(Schema.NullOr(Schema.String)),
    uuid: Schema.optionalKey(Schema.String),
    error: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal('stream_event'),
    event: Partial,
    parent_tool_use_id: Schema.optionalKey(Schema.NullOr(Schema.String)),
  }),
  Schema.Struct({
    type: Schema.Literal('result'),
    subtype: Schema.String,
    is_error: Schema.Boolean,
    result: Schema.optionalKey(Schema.String),
    structured_output: Schema.optionalKey(Schema.Json),
    stop_reason: Schema.optionalKey(Schema.NullOr(Schema.String)),
    usage: Usage,
    total_cost_usd: Schema.optionalKey(Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))),
    // effect-nit-allow B-no-lazy-unknown: native CLI modelUsage extension objects
    // remain unchanged in provider metadata; Turn decodes accounting fields once
    // with Schema.Record(Schema.String, Protocol.ModelUsage) before accounting
    // and never uses unchecked provider extension fields for totals.
    modelUsage: Schema.optionalKey(Schema.Record(Schema.String, Schema.JsonObject)),
    session_id: Schema.optionalKey(Schema.String),
    num_turns: Schema.optionalKey(Count),
    permission_denials: Schema.optionalKey(Schema.Array(Schema.Unknown)),
  }),
  Schema.Struct({ type: Schema.Literal('rate_limit_event') }),
  Schema.Struct({
    type: Schema.Literal('auth_status'),
    isAuthenticating: Schema.Boolean,
    error: Schema.optionalKey(Schema.String),
  }),
])
/**
 * Validated stream event emitted by the installed CLI.
 *
 * @category models
 */
export type Event = typeof Event.Type
/**
 * Checks whether a value satisfies the decoded `Event` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isEvent: (u: unknown) => u is Event = Schema.is(Event)

/**
 * Decodes a native CLI event into its validated protocol representation.
 *
 * **Details**
 *
 * The protocol boundary never includes raw stdout/stderr or prompts in failures.
 *
 * @category decoding
 */
export const decode = Effect.fnUntraced(function* (
  line: string,
): Effect.fn.Return<Event, AiError.AiError> {
  const value = yield* Effect.try({
    try: () => Tool.unsafeSecureJsonParse(line),
    catch: () => protocol('Invalid Claude Code JSON frame'),
  })
  return yield* Schema.decodeUnknownEffect(Event)(value).pipe(
    Effect.mapError(() => protocol('Unsupported or malformed Claude Code frame')),
  )
})

/** Checks the decoded ModelUsage contract without decoding or coercing input.
 * @category guards
 */
export const isModelUsage: (u: unknown) => u is ModelUsage = Schema.is(Schema.toType(ModelUsage))
