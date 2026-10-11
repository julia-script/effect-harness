import type * as Usage from './Usage.js'
import type * as Observation from './Observation.js'
/** Data-only boundary implemented by a local runtime or a transport adapter. */
import * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Option from 'effect/Option'
import type * as Stream from 'effect/Stream'
import * as Record from './Record.js'
import * as Agent from './Agent.js'
import * as Submission from './Submission.js'
import type * as StorageRecord from './Record.js'
import type { HarnessError } from './HarnessError.js'

export const CreateOptionsSchema = Schema.Struct({ agent: Schema.optionalKey(Agent.StateSchema) })
export type CreateOptions = typeof CreateOptionsSchema.Type
export const ForkOptionsSchema = Schema.Struct({ at: Schema.optionalKey(Record.EntryId) })
export type ForkOptions = typeof ForkOptionsSchema.Type
export const SubmissionRefSchema = Schema.Struct({
  id: Record.SubmissionId,
  conversationId: Record.ConversationId,
})
export type SubmissionRef = typeof SubmissionRefSchema.Type
export const SubmitSchema = Schema.Struct({
  conversationId: Record.ConversationId,
  draft: Submission.DraftSchema,
})
export const ConfigureSchema = Schema.Struct({
  conversationId: Record.ConversationId,
  change: Agent.ChangeSchema,
})
export const ForkSchema = Schema.Struct({
  conversationId: Record.ConversationId,
  options: Schema.optionalKey(ForkOptionsSchema),
})
export const AbortOptionsSchema = Schema.Struct({
  includeBackground: Schema.optionalKey(Schema.Boolean),
})
export type AbortOptions = typeof AbortOptionsSchema.Type
export const ResetOptionsSchema = Schema.Struct({
  handoff: Schema.optionalKey(Schema.String),
  requestId: Schema.optionalKey(Schema.NonEmptyString),
})
export type ResetOptions = typeof ResetOptionsSchema.Type
export const WatchSchema = Schema.Struct({
  conversationId: Record.ConversationId,
  after: Schema.optionalKey(Record.EntryId),
})
export type Watch = typeof WatchSchema.Type

export interface HarnessBackendService {
  readonly observations?: Observation.Capabilities
  /** Native adapters validate caller authority before preparing a reset payload. */
  readonly reset?: (
    conversationId: Record.ConversationId,
    options?: ResetOptions,
  ) => Effect.Effect<SubmissionRef, HarnessError>
  readonly compact?: (
    conversationId: Record.ConversationId,
    options?: import('./CompactionTask.js').Options,
  ) => Effect.Effect<import('./CompactionTask.js').Handle, HarnessError>
  readonly compactionTask?: (
    handle: import('./CompactionTask.js').Handle,
  ) => Effect.Effect<Record.Task, HarnessError>
  readonly waitCompaction?: (
    handle: import('./CompactionTask.js').Handle,
  ) => Effect.Effect<import('./CompactionTask.js').Outcome, HarnessError>
  readonly abortCompaction?: (
    handle: import('./CompactionTask.js').Handle,
  ) => Effect.Effect<void, HarnessError>
  /** Optional for older transports. Undefined is unsupported, never a zero usage report. */
  readonly usage?: (
    conversationId?: Record.ConversationId,
  ) => Effect.Effect<Usage.Summary, HarnessError>
  readonly root: Effect.Effect<Record.ConversationId, HarnessError>
  readonly create: (options?: CreateOptions) => Effect.Effect<Record.ConversationId, HarnessError>
  readonly conversation: (
    id: Record.ConversationId,
  ) => Effect.Effect<Option.Option<Record.ConversationId>, HarnessError>
  readonly submit: (request: typeof SubmitSchema.Type) => Effect.Effect<SubmissionRef, HarnessError>
  readonly configure: (request: typeof ConfigureSchema.Type) => Effect.Effect<void, HarnessError>
  readonly agent: (id: Record.ConversationId) => Effect.Effect<Agent.State, HarnessError>
  readonly fork: (
    request: typeof ForkSchema.Type,
  ) => Effect.Effect<Record.ConversationId, HarnessError>
  readonly abort: (
    id: Record.ConversationId,
    options?: AbortOptions,
  ) => Effect.Effect<void, HarnessError>
  readonly read: (id: Record.SubmissionId) => Effect.Effect<Submission.Record, HarnessError>
  readonly wait: (id: Record.SubmissionId) => Effect.Effect<Submission.Settled, HarnessError>
  readonly withdraw: (id: Record.SubmissionId) => Effect.Effect<Submission.Withdrawal, HarnessError>
  readonly entries: (request: Watch) => Stream.Stream<Record.Entry, HarnessError>
  readonly snapshot: (
    address: StorageRecord.DocumentAddress,
  ) => Effect.Effect<Option.Option<StorageRecord.StoredDocument>, HarnessError>
  readonly watch: (
    address: StorageRecord.DocumentAddress,
  ) => Stream.Stream<StorageRecord.StoredDocument, HarnessError>
  readonly waitForIdle: Effect.Effect<void, HarnessError>
}
export class HarnessBackend extends Context.Service<HarnessBackend, HarnessBackendService>()(
  'effect-harness/HarnessBackend',
) {}
