/**
 * Schema-derived task outcomes and classification.
 */
import * as ToolRegistration from 'effect-harness/ToolRegistration'
import { Result as GenerationResult } from './Generation.ts'
import { Result as ToolResult } from './ToolCall.ts'
import { Result as CompactionResult } from './Compaction.ts'
import type * as Record from '../Record.ts'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'

/**
 * Schema for a completed owned task with its JSON result.
 *
 * @category schemas
 */
export const Completed = Schema.TaggedStruct('Completed', {
  status: Schema.tag('completed'),
  result: Schema.Json,
})
/**
 * Schema for failed, faulted or aborted owned work with a diagnostic message.
 *
 * @category schemas
 */
export const Failed = Schema.TaggedStruct('Failed', {
  status: Schema.Literals(['failed', 'faulted', 'aborted']),
  error: Schema.Struct({ message: Schema.String }),
})
/**
 * Schema for owned work whose required execution code is unavailable.
 *
 * @category schemas
 */
export const Orphaned = Schema.TaggedStruct('Orphaned', {
  status: Schema.tag('orphaned'),
  reason: Schema.String,
})
/**
 * Schema for completed, failed or orphaned owned-task outcomes.
 *
 * @category schemas
 */
export const StructuredOutcome = Schema.Union([Completed, Failed, Orphaned])
/**
 * Decoded value validated by the `StructuredOutcome` schema.
 *
 * @category models
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
 * Direct and interpreted outcome status with an optional diagnostic message.
 *
 * @category models
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
 */
export const classifyOrUndefined = (input: unknown): Classification | undefined => {
  const decoded = envelope(input)
  if (Option.isNone(decoded)) return undefined
  const value = decoded.value
  const nested = receipt(value.receipt)
  const status = value.status ?? Option.getOrUndefined(Option.map(nested, (value) => value.status))
  const diagnostic = error(value.error)
  const detail =
    value.detail ??
    Option.getOrUndefined(Option.map(diagnostic, (value) => value.message)) ??
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
 */
export const isFailed: (input: unknown) => boolean = (input) => {
  const status = classifyOrUndefined(input)?.status
  return (
    status === 'failed' || status === 'faulted' || status === 'orphaned' || status === 'aborted'
  )
}
/**
 * Schema for pinned decoded arguments saved before tool execution.
 *
 * @category schemas
 */
export const ToolCheckpoint = Schema.Struct({ arguments: Schema.Json })

/**
 * Schema for an encoded tool execution and its native terminal receipt.
 *
 * @category schemas
 */
export const ToolOutcome = Schema.Struct({
  execution: Schema.toCodecJson(ToolRegistration.Execution),
  receipt: ToolResult,
})
const structured = Schema.decodeUnknownOption(StructuredOutcome)
const generation = Schema.decodeUnknownOption(GenerationResult)
const tool = Schema.decodeUnknownOption(ToolOutcome)
const compaction = Schema.decodeUnknownOption(CompactionResult)
/**
 * Classifies a built-in task outcome or the explicit extension convention.
 *
 * **Details**
 *
 * Custom metadata uses the explicit extension convention; no result payload is claimed by that fallback.
 *
 * @category combinators
 */
export const classifyTaskOrUndefined = (
  task: Pick<Record.Task, 'kind' | 'state'>,
): Classification | undefined => {
  const input = task.state.outcome
  if (Option.isSome(structured(input))) return classifyOrUndefined(input)
  const kind = task.kind
  let known: Option.Option<unknown> = Option.none()
  if (kind === '@effect-harness/durable/Generation/v1') known = generation(input)
  else if (kind === '@effect-harness/durable/ToolCall/v1') known = tool(input)
  else if (kind === '@effect-harness/durable/Compaction/v1') known = compaction(input)
  // Invalid/unknown built-in values contribute only safely decoded status metadata.
  return Option.match(known, {
    onSome: classifyOrUndefined,
    onNone: () => classifyOrUndefined(input),
  })
}

/** Decoded value of the ToolCheckpoint schema.
 * @category models
 */
export type ToolCheckpoint = typeof ToolCheckpoint.Type

/** Decoded value of the ToolOutcome schema.
 * @category models
 */
export type ToolOutcome = typeof ToolOutcome.Type

/** Decoded value of the Completed schema.
 * @category models
 */
export type Completed = typeof Completed.Type

/** Decoded value of the ExtensionEnvelope schema.
 * @category models
 */
export type ExtensionEnvelope = typeof ExtensionEnvelope.Type

/** Decoded value of the Failed schema.
 * @category models
 */
export type Failed = typeof Failed.Type

/** Decoded value of the Orphaned schema.
 * @category models
 */
export type Orphaned = typeof Orphaned.Type

/** Checks the decoded StructuredOutcome contract without decoding or coercing input.
 * @category guards
 */
export const isStructuredOutcome: (u: unknown) => u is StructuredOutcome = Schema.is(
  Schema.toType(StructuredOutcome),
)

/** Checks the decoded ToolCheckpoint contract without decoding or coercing input.
 * @category guards
 */
export const isToolCheckpoint: (u: unknown) => u is ToolCheckpoint = Schema.is(
  Schema.toType(ToolCheckpoint),
)

/** Checks the decoded ToolOutcome contract without decoding or coercing input.
 * @category guards
 */
export const isToolOutcome: (u: unknown) => u is ToolOutcome = Schema.is(Schema.toType(ToolOutcome))

/** Checks the decoded Completed contract without decoding or coercing input.
 * @category guards
 */
export const isCompleted: (u: unknown) => u is Completed = Schema.is(Schema.toType(Completed))

/** Checks the decoded ExtensionEnvelope contract without decoding or coercing input.
 * @category guards
 */
export const isExtensionEnvelope: (u: unknown) => u is ExtensionEnvelope = Schema.is(
  Schema.toType(ExtensionEnvelope),
)

/** Checks the decoded Orphaned contract without decoding or coercing input.
 * @category guards
 */
export const isOrphaned: (u: unknown) => u is Orphaned = Schema.is(Schema.toType(Orphaned))

/** Checks a decoded owned failure without classifying arbitrary extension outcomes.
 * @category guards
 */
export const isFailedOutcome: (u: unknown) => u is Failed = Schema.is(Schema.toType(Failed))
