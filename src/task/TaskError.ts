/**
 * Errors the task runtime and the `Scheduler` report.
 */
import { Schema } from 'effect'
import { TaskId } from '../storage/schema/Ids.ts'

/** No task has this ID. */
export class TaskNotFound extends Schema.TaggedError<
  TaskNotFound,
  { readonly brand: unique symbol }
>('effect-harness/task/TaskError/TaskNotFound')('TaskNotFound', {
  message: Schema.String,
  taskId: TaskId,
}) {}

/**
 * A runtime operation ran after its invocation ended. Work a handler leaves running past its
 * phase, such as a detached fiber, can no longer read or write through the runtime.
 */
export class InvocationEnded extends Schema.TaggedError<
  InvocationEnded,
  { readonly brand: unique symbol }
>('effect-harness/task/TaskError/InvocationEnded')('InvocationEnded', {
  message: Schema.String,
  taskId: TaskId,
}) {}

/**
 * A runtime commit was refused: the task is no longer running, it carries an abort mark and the
 * invocation is a run, or the state it returned is invalid, such as a wait on a missing task or
 * one that closes a wait cycle. Nothing of the commit is written.
 */
export class TaskRejected extends Schema.TaggedError<
  TaskRejected,
  { readonly brand: unique symbol }
>('effect-harness/task/TaskError/TaskRejected')('TaskRejected', {
  message: Schema.String,
  taskId: TaskId,
}) {}

/** The Scheduler's scope closed: it starts nothing and refuses waits and runtime commits. */
export class SchedulerClosed extends Schema.TaggedError<
  SchedulerClosed,
  { readonly brand: unique symbol }
>('effect-harness/task/TaskError/SchedulerClosed')('SchedulerClosed', {
  message: Schema.String,
}) {}
