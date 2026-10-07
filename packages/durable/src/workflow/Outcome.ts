/**
 * Schema-derived task outcomes and classification.
 *
 * @since 0.0.0
 */
import * as Tool from '@effect-harness/harness/Tool'
import { Result as GenerationResult } from './Generation.ts'
import { Result as ToolResult } from './ToolCall.ts'
import { Result as CompactionResult } from './Compaction.ts'
import type * as Record from '../Record.ts'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'

/**
 * Completed schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Completed = Schema.Struct({ status: Schema.Literal('completed'), result: Schema.Json })
/**
 * Failed schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Failed = Schema.Struct({
  status: Schema.Literals(['failed', 'faulted', 'aborted']),
  error: Schema.Struct({ message: Schema.String }),
})
/**
 * Orphaned schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Orphaned = Schema.Struct({ status: Schema.Literal('orphaned'), reason: Schema.String })
/**
 * StructuredOutcome schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const StructuredOutcome = Schema.Union([Completed, Failed, Orphaned])
/**
 * StructuredOutcome contract.
 *
 * @category models
 * @since 0.0.0
 */
export type StructuredOutcome = typeof StructuredOutcome.Type
/**
 * Extension classification recognizes only status/diagnostic conventions.
 *
 * **Details**
 *
 * Custom results remain opaque Json.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ExtensionEnvelope = Schema.Struct({
  status: Schema.optionalKey(Schema.Json),
  receipt: Schema.optionalKey(Schema.Json),
  detail: Schema.optionalKey(Schema.Json),
  error: Schema.optionalKey(Schema.Json),
  reason: Schema.optionalKey(Schema.Json),
})
const envelope = Schema.decodeUnknownOption(ExtensionEnvelope)
const receipt = Schema.decodeUnknownOption(
  Schema.Struct({ status: Schema.optionalKey(Schema.Json) }),
)
const error = Schema.decodeUnknownOption(
  Schema.Struct({ message: Schema.optionalKey(Schema.Json) }),
)
/**
 * Classification contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface Classification {
  readonly rawDirectStatus: Schema.Json | undefined
  readonly directStatus: string | undefined
  readonly status: string | undefined
  readonly message: string | undefined
}
/**
 * Recognizes a structured task outcome without decoding arbitrary extensions.
 *
 * @category combinators
 * @since 0.0.0
 */
export const classify = (input: unknown): Classification | undefined => {
  const decoded = envelope(input)
  if (Option.isNone(decoded)) return undefined
  const value = decoded.value
  const nested = receipt(value.receipt)
  const status = value.status ?? (Option.isSome(nested) ? nested.value.status : undefined)
  const diagnostic = error(value.error)
  const detail =
    value.detail ??
    (Option.isSome(diagnostic) ? diagnostic.value.message : undefined) ??
    value.reason
  return {
    rawDirectStatus: value.status,
    directStatus: typeof value.status === 'string' ? value.status : undefined,
    status: typeof status === 'string' ? status : undefined,
    message: typeof detail === 'string' ? detail : undefined,
  }
}
/**
 * Returns whether a task outcome represents failure.
 *
 * @category combinators
 * @since 0.0.0
 */
export const failed = (input: unknown): boolean => {
  const status = classify(input)?.status
  return (
    status === 'failed' || status === 'faulted' || status === 'orphaned' || status === 'aborted'
  )
}
/**
 * ToolCheckpoint schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ToolCheckpoint = Schema.Struct({ arguments: Schema.Json })

/**
 * ToolOutcome schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ToolOutcome = Schema.Struct({
  execution: Schema.toCodecJson(Tool.Execution),
  receipt: ToolResult,
})
const structured = Schema.decodeUnknownOption(StructuredOutcome)
const generation = Schema.decodeUnknownOption(GenerationResult)
const tool = Schema.decodeUnknownOption(ToolOutcome)
const compaction = Schema.decodeUnknownOption(CompactionResult)
/**
 * Fully decode built-in outcomes first.
 *
 * **Details**
 *
 * Legacy/custom metadata uses the explicit extension convention; no result payload is claimed by that fallback.
 *
 * @category combinators
 * @since 0.0.0
 */
export const classifyTask = (
  task: Pick<Record.Task, 'kind' | 'state'>,
): Classification | undefined => {
  const input = task.state.outcome
  if (Option.isSome(structured(input))) return classify(input)
  const kind = task.kind
  let known: Option.Option<unknown> = Option.none()
  if (
    kind === 'harness.generation' ||
    kind === 'pi.generation' ||
    kind === '@effect-harness/durable/Generation/v1'
  )
    known = generation(input)
  else if (kind === 'harness.tool' || kind === '@effect-harness/durable/ToolCall/v1')
    known = tool(input)
  else if (kind === 'harness.compaction' || kind === '@effect-harness/durable/Compaction/v1')
    known = compaction(input)
  // Invalid/unknown built-in values contribute only safely decoded status metadata.
  return Option.isSome(known) ? classify(known.value) : classify(input)
}
