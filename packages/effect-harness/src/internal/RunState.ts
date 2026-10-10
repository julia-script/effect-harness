/** Schema-backed durable checkpoints. A started tool intent is persisted before its handler runs. */
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Record from '../Record.js'
import * as Agent from '../Agent.js'
import * as Submission from '../Submission.js'
import * as Tool from '../Tool.js'
import * as ToolResult from '../ToolResult.js'
import * as PreparedRequest from '../PreparedRequest.js'
import * as Usage from '../Usage.js'
import * as LiveState from '../LiveState.js'

const shared = { submissionId: Record.SubmissionId }
const running = {
  ...shared,
  prompt: Prompt.Prompt,
  agent: Agent.StateSchema,
  ordinal: Schema.optionalKey(Schema.Natural),
  preparation: Schema.optionalKey(PreparedRequest.SnapshotSchema),
  attempt: Schema.optionalKey(Usage.AttemptId),
  retries: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
  ),
}
const prepared = {
  ...running,
  preparation: PreparedRequest.SnapshotSchema,
  attempt: Usage.AttemptId,
  retries: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
}
export const RunStateSchema = Schema.Union([
  Schema.TaggedStruct('queued', {
    phase: Schema.tag('queued'),
    ...shared,
    draft: Submission.InputDraftSchema,
  }),
  Schema.TaggedStruct('request', { phase: Schema.tag('request'), ...running }),
  Schema.TaggedStruct('streaming', {
    phase: Schema.tag('streaming'),
    ...prepared,
    revision: Schema.Natural,
    partial: Schema.optionalKey(LiveState.PartialSchema),
  }),
  Schema.TaggedStruct('retry', {
    phase: Schema.tag('retry'),
    ...prepared,
    until: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
  }),
  Schema.TaggedStruct('tools', {
    phase: Schema.tag('tools'),
    ...running,
    calls: Schema.Array(
      Schema.Struct({ call: Tool.CallSchema, replay: Tool.ReplaySchema, started: Schema.Boolean }),
    ),
    index: Schema.Natural,
    results: Schema.Array(ToolResult.ResultSchema),
  }),
])
export type RunState = typeof RunStateSchema.Type
export const encode = Schema.encodeEffect(Schema.toCodecJson(RunStateSchema))
export const decode = Schema.decodeUnknownEffect(Schema.toCodecJson(RunStateSchema))
export const Messages = Schema.Array(Schema.toCodecJson(Prompt.Message))

/** Validate committed prefixes without repairing, discarding or replaying stored work. */
export const validPrefix = (state: RunState): boolean => {
  if (state._tag !== state.phase) return false
  if (
    state._tag !== 'queued' &&
    state.preparation !== undefined &&
    state.preparation.ordinal !== state.ordinal
  )
    return false
  if (state._tag !== 'tools') return true
  return (
    state.index <= state.calls.length &&
    state.results.length === state.index &&
    new Set(state.calls.map((item) => item.call.id)).size === state.calls.length &&
    state.calls.slice(0, state.index).every((item) => item.started)
  )
}
