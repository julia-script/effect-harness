import * as Tool from '@effect-harness/harness/Tool'
import { Result as GenerationResult } from './Generation.ts'
import { Result as ToolResult } from './ToolCall.ts'
import { Result as CompactionResult } from './Compaction.ts'
import * as Record from '../Record.ts'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'

export const Completed = Schema.Struct({ status: Schema.Literal('completed'), result: Schema.Json })
export const Failed = Schema.Struct({
  status: Schema.Literals(['failed', 'faulted', 'aborted']),
  error: Schema.Struct({ message: Schema.String }),
})
export const Orphaned = Schema.Struct({ status: Schema.Literal('orphaned'), reason: Schema.String })
export const StructuredOutcome = Schema.Union([Completed, Failed, Orphaned])
export type StructuredOutcome = typeof StructuredOutcome.Type
/** Extension classification recognizes only status/diagnostic conventions. Custom results remain opaque Json. */
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
export const classify = (input: unknown) => {
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
export const failed = (input: unknown) => {
  const status = classify(input)?.status
  return (
    status === 'failed' || status === 'faulted' || status === 'orphaned' || status === 'aborted'
  )
}
export const ToolCheckpoint = Schema.Struct({ arguments: Schema.Json })

export const ToolOutcome = Schema.Struct({
  execution: Schema.toCodecJson(Tool.Execution),
  receipt: ToolResult,
})
const structured = Schema.decodeUnknownOption(StructuredOutcome)
const generation = Schema.decodeUnknownOption(GenerationResult)
const tool = Schema.decodeUnknownOption(ToolOutcome)
const compaction = Schema.decodeUnknownOption(CompactionResult)
/** Fully decode built-in outcomes first. Legacy/custom metadata uses the explicit extension convention; no result payload is claimed by that fallback. */
export const classifyTask = (task: Pick<Record.Task, 'kind' | 'state'>) => {
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
