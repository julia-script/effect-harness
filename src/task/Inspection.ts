/**
 * Scheduler inspection: derived scheduling state of live tasks, never persisted. Kept apart from
 * `Scheduler` so clients can decode it without the scheduler runtime.
 */
import { Schema } from 'effect'
import { TaskId } from '../storage/schema/Ids.ts'
import { TaskRecord } from '../storage/schema/Task.ts'

/** A live task's scheduling state, derived and never persisted. */
export const TaskInspectionState = Schema.Union([
  /** An invocation is active. */
  Schema.Struct({ kind: Schema.Literal('running') }),
  /** The next pass reserves it; `migrates` when its definition is newer and has `migrate`. */
  Schema.Struct({ kind: Schema.Literal('ready'), migrates: Schema.Boolean }),
  /**
   * Waits for these live tasks: the live part of its `on`, or, when abort-marked, its live
   * ordinary owned work, which must end before its abort handler starts.
   */
  Schema.Struct({ kind: Schema.Literal('waiting'), on: Schema.Array(TaskId) }),
  /** Outcome held until its ordinary owned work drains. */
  Schema.Struct({ kind: Schema.Literal('completing') }),
  /** No installed definition can take it; aborting it settles it as `orphaned`. */
  Schema.Struct({
    kind: Schema.Literal('blocked'),
    reason: Schema.Literals(['missing_task', 'task_too_old', 'migration_failed']),
    message: Schema.optionalKey(Schema.String),
  }),
])
export type TaskInspectionState = typeof TaskInspectionState.Type

export const TaskInspection = Schema.Struct({ record: TaskRecord, state: TaskInspectionState })
export type TaskInspection = typeof TaskInspection.Type

/** Scheduling state and every live task, read on the Session line. */
export const Inspection = Schema.Struct({
  scheduling: Schema.Literals(['running', 'closing']),
  tasks: Schema.Array(TaskInspection),
})
export type Inspection = typeof Inspection.Type
