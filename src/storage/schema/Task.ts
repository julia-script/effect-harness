import { Schema } from 'effect'
import { ConversationId, TaskId, Version } from './Ids.ts'
import { JsonObject } from './Json.ts'

/** JSON-safe error snapshot persisted instead of a runtime error value. */
export const TaskOutcomeError = Schema.Struct({
  message: Schema.String,
  /** Optional structured diagnostic data for inspection or recovery. */
  detail: Schema.optionalKey(Schema.Json),
})
export type TaskOutcomeError = typeof TaskOutcomeError.Type

/** Durable reason and optional result recorded when a task becomes terminal. */
export const TaskOutcome = Schema.Union([
  Schema.Struct({ status: Schema.Literal('completed'), result: Schema.Json }),
  /** Expected task or domain failure explicitly committed by its implementation. */
  Schema.Struct({
    status: Schema.Literal('failed'),
    error: TaskOutcomeError,
    result: Schema.optionalKey(Schema.Json),
  }),
  /** Explicit cancellation handled by the task's abort protocol. */
  Schema.Struct({
    status: Schema.Literal('aborted'),
    reason: Schema.optionalKey(Schema.String),
    result: Schema.optionalKey(Schema.Json),
  }),
  /** Task that cannot resume because its definition or migration is unavailable. */
  Schema.Struct({ status: Schema.Literal('orphaned'), reason: Schema.String }),
  /** Runtime-detected contract failure, such as an uncaught throw or no durable progress. */
  Schema.Struct({ status: Schema.Literal('faulted'), error: TaskOutcomeError }),
])
export type TaskOutcome = typeof TaskOutcome.Type

/** How a waiting task treats the tasks it waits on. */
export const JoinPolicy = Schema.Literals(['failFast', 'allSettled'])
export type JoinPolicy = typeof JoinPolicy.Type

/** States in which the task can still run code; each carries the checkpoint it resumes from. */
export const LiveTaskState = Schema.Union([
  /** Eligible for scheduling. */
  Schema.Struct({ status: Schema.Literal('pending'), checkpoint: Schema.Json }),
  /** Reserved by one in-memory task invocation. */
  Schema.Struct({ status: Schema.Literal('running'), checkpoint: Schema.Json }),
  /** Parked until every task in `on` is terminal; then resumes at `checkpoint`. */
  Schema.Struct({
    status: Schema.Literal('waiting'),
    checkpoint: Schema.Json,
    on: Schema.Array(TaskId),
    policy: JoinPolicy,
  }),
])
export type LiveTaskState = typeof LiveTaskState.Type

/** States whose outcome is decided; no more task code runs. */
export const SettledTaskState = Schema.Union([
  /** Becomes terminal once no ordinary owned work below is live. */
  Schema.Struct({ status: Schema.Literal('completing'), outcome: TaskOutcome }),
  /** Permanently settled durable result receipt. */
  Schema.Struct({ status: Schema.Literal('terminal'), outcome: TaskOutcome }),
])
export type SettledTaskState = typeof SettledTaskState.Type

/** Complete durable execution state of a task. */
export const TaskState = Schema.Union([LiveTaskState, SettledTaskState])
export type TaskState = typeof TaskState.Type

export const TaskStatus = Schema.Literals([
  'pending',
  'running',
  'waiting',
  'completing',
  'terminal',
])
export type TaskStatus = typeof TaskStatus.Type

const TaskRecordFields = {
  id: TaskId,
  conversationId: ConversationId,
  /** Registered task definition name. */
  kind: Schema.String,
  /** Definition version used to migrate live input and checkpoints. */
  version: Version,
  /** Original task input retained while the task is live or terminal. */
  input: Schema.Json,
  /** Owning task of a child task; absent for a task its conversation owns. Immutable. */
  owner: Schema.optionalKey(TaskId),
  /** Whether this conversation-owned task is excluded from idle waits, aborts, and cascades. */
  background: Schema.Boolean,
  /** Durable abort mark checked before run-mode progress is committed. */
  abortRequested: Schema.Boolean,
  /** Why the mark was set when not by an abort request. */
  abortReason: Schema.optionalKey(Schema.Literal('restart')),
  /** Set from the task's `abandonOnRestart` option. Immutable. */
  abandonOnRestart: Schema.optionalKey(Schema.Literal(true)),
  /** Wall-clock milliseconds of the first change to `running`. */
  startedAt: Schema.optionalKey(Schema.Finite),
  /** Wall-clock milliseconds of the change to `terminal`; absent while live. */
  endedAt: Schema.optionalKey(Schema.Finite),
}

/**
 * Complete replacement record for one durable task state machine. Memos (small first-writer-wins
 * values) are only retained while the task can run.
 */
export const TaskRecord = Schema.Union([
  Schema.Struct({
    ...TaskRecordFields,
    state: LiveTaskState,
    memos: Schema.optionalKey(JsonObject),
  }),
  Schema.Struct({ ...TaskRecordFields, state: SettledTaskState }),
])
export type TaskRecord = typeof TaskRecord.Type
