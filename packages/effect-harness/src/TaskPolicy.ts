/** Persisted scheduler policies. Helpers prepare records; they never schedule or dispatch work. */
import * as Schema from 'effect/Schema'
import type * as Record from './Record.js'

export const RestartPolicySchema = Schema.Literals(['resume', 'abandon'])
export type RestartPolicy = typeof RestartPolicySchema.Type
export const AbortIntentSchema = Schema.Union([
  Schema.TaggedStruct('restart', {}),
  Schema.TaggedStruct('request', { reason: Schema.optionalKey(Schema.String) }),
])
export type AbortIntent = typeof AbortIntentSchema.Type

/**
 * Version of these scheduler semantics, independent of the task definition version.
 * This reader rejects unsupported markers. Older readers discard unknown task fields,
 * so this marker alone cannot prevent downgrade. Production activation requires an
 * enforceable storage-container minimum-reader guard; it is not enabled by this module.
 */
export const SCHEDULER_FORMAT_VERSION = 1
export const MINIMUM_READER_VERSION = 1
export const SchedulerFormatSchema = Schema.Struct({
  version: Schema.Literal(SCHEDULER_FORMAT_VERSION),
  minimumReader: Schema.Literal(MINIMUM_READER_VERSION),
})
export type SchedulerFormat = typeof SchedulerFormatSchema.Type

/** Missing policy in a legacy record means resume. Does not rewrite legacy data. */
export const restartPolicy = (self: Record.Task): RestartPolicy => self.restartPolicy ?? 'resume'

/** Missing intent on a legacy abort mark means an ordinary request, never restart abandonment. */
export const abortIntent = (self: Record.Task): AbortIntent | undefined =>
  self.abortRequested ? (self.abortIntent ?? { _tag: 'request' }) : undefined

/** First ordinary request wins; an ordinary request replaces a prior restart mark. */
export const requestAbort = (self: Record.Task, reason?: string): Record.Task => {
  if (self.state.status === 'terminal' || abortIntent(self)?._tag === 'request') return self
  return {
    ...self,
    abortRequested: true,
    abortIntent: { _tag: 'request', ...(reason === undefined ? {} : { reason }) },
  }
}

/**
 * Prepare the durable restart mark, retaining any earlier user request or held outcome.
 * The scheduler must cascade ordinary descendants and wait for them before running
 * cleanup in a fresh invocation Scope. Missing definitions remain blocked for restart
 * cleanup; this helper does not orphan, resume, or retire any task documents.
 */
export const recover = (self: Record.Task): Record.Task => {
  if (self.state.status === 'terminal') return self
  const marked =
    restartPolicy(self) === 'abandon' && !self.abortRequested
      ? { ...self, abortRequested: true, abortIntent: { _tag: 'restart' as const } }
      : self
  return marked.state.status === 'running'
    ? { ...marked, state: { ...marked.state, status: 'pending' } }
    : marked
}

/** Outcome transitions discard invocation memos. Settlement/retirement remains transactional. */
export const withState = (self: Record.Task, state: Record.TaskState): Record.Task => {
  if (state.status !== 'terminal' && state.status !== 'completing') return { ...self, state }
  const { memos: _memos, ...record } = self
  return { ...record, state }
}
