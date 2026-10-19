/** Serializable built-in compaction handles. A queued summary has a submission receipt to await separately. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Record from './Record.js'
import * as Compaction from './Compaction.js'
import * as Task from './Task.js'
import * as Prepared from './PreparedRequest.js'
import * as Request from './CompactionRequest.js'
import { HarnessBackend } from './HarnessBackend.js'
import { HarnessError } from './HarnessError.js'
import * as Plan from './internal/CompactionPlan.js'
import { compactionBackend } from './internal/ConversationInvocation.js'

export const kind = 'harness.compaction'
export const HandleSchema = Schema.Struct({
  id: Record.TaskId,
  conversationId: Record.ConversationId,
})
export type Handle = typeof HandleSchema.Type
export const OptionsSchema = Schema.Struct({ instructions: Schema.optionalKey(Schema.String) })
export type Options = typeof OptionsSchema.Type
export const InputSchema = Schema.Struct({
  reason: Compaction.ReasonSchema,
  instructions: Schema.optionalKey(Schema.String),
  blocking: Schema.Boolean,
})
export const ResultSchema = Schema.Union([
  Schema.TaggedStruct('Noop', { reason: Schema.String }),
  Schema.TaggedStruct('Placed', { entryId: Record.EntryId }),
  Schema.TaggedStruct('Queued', { submissionId: Record.SubmissionId }),
])
export type Result = typeof ResultSchema.Type
export const FailureSchema = Schema.Struct({ reason: Schema.String, message: Schema.String })
export const OutcomeSchema = Task.outcomeSchema({ success: ResultSchema, failure: FailureSchema })
export type Outcome = typeof OutcomeSchema.Type
export const EnvelopeSchema = Schema.Struct({
  preparation: Prepared.SnapshotSchema,
  capability: Prepared.BindingSchema,
  policy: Request.PolicySchema,
  fingerprint: Plan.FingerprintSchema,
  omittedMedia: Plan.TranscriptSchema.fields.omittedMedia,
})
export type Envelope = typeof EnvelopeSchema.Type
const saved = {
  envelope: EnvelopeSchema,
  attempt: Request.PolicySchema.fields.attempt,
  retries: Schema.Natural,
}
export const CheckpointSchema = Schema.Union([
  Schema.Struct({ phase: Schema.Literal('select') }),
  Schema.Struct({
    phase: Schema.Literal('supplied'),
    fingerprint: Plan.FingerprintSchema,
    summary: Schema.NonEmptyString,
  }),
  Schema.Struct({ phase: Schema.Literal('summarize'), ...saved, started: Schema.Boolean }),
  Schema.Struct({
    phase: Schema.Literal('retry'),
    ...saved,
    until: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
  }),
])
export type Checkpoint = typeof CheckpointSchema.Type
export const contract = {
  kind,
  version: 1,
  input: InputSchema,
  checkpoint: CheckpointSchema,
  success: ResultSchema,
  failure: FailureSchema,
} as const
const unsupported = () =>
  new HarnessError({
    reason: 'invalid',
    operation: 'compaction.task',
    message: 'Backend does not support compaction task handles',
  })
/** Read the actual durable task. Native returned identities retain caller authority; copied DTOs use the ambient backend. */
export const read = (handle: Handle): Effect.Effect<Record.Task, HarnessError, HarnessBackend> =>
  Effect.gen(function* () {
    const backend = compactionBackend(handle) ?? (yield* HarnessBackend)
    if (backend.compactionTask === undefined) return yield* unsupported()
    return yield* backend.compactionTask(handle)
  })
/** Observer interruption never cancels durable compaction. Queued means admission, not successful placement. */
export const wait = (handle: Handle): Effect.Effect<Outcome, HarnessError, HarnessBackend> =>
  Effect.gen(function* () {
    const backend = compactionBackend(handle) ?? (yield* HarnessBackend)
    if (backend.waitCompaction === undefined) return yield* unsupported()
    return yield* backend.waitCompaction(handle)
  })
/** Request native supervised cancellation and wait for cleanup. */
export const abort = (handle: Handle): Effect.Effect<void, HarnessError, HarnessBackend> =>
  Effect.gen(function* () {
    const backend = compactionBackend(handle) ?? (yield* HarnessBackend)
    if (backend.abortCompaction === undefined) return yield* unsupported()
    yield* backend.abortCompaction(handle)
  })
