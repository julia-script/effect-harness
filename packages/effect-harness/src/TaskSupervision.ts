/** Native durable ownership over a registered scheduler and its Session. */
import * as Context from 'effect/Context'
import * as Layer from 'effect/Layer'
import type * as Session from './Session.js'
import * as TaskScheduler from './TaskScheduler.js'

export type Service = TaskScheduler.Service
export type Failure = TaskScheduler.Failure
export type Host<R = never> = TaskScheduler.Host<R>
export type Graph = TaskScheduler.Graph
export type GraphNode = TaskScheduler.GraphNode
export type GraphFrame = TaskScheduler.GraphFrame
export type Inspection = TaskScheduler.Inspection

/**
 * Open one native supervisor per Session. Create children and the waiting checkpoint in
 * the same TaskExecution.commit. Ordered receipts survive reopen; allSettled drains every
 * child, while failFast aborts unfinished direct siblings and resumes after their cleanup.
 * Completion holds until ordinary owned work drains. Background supervisors are real
 * conversation-owned tasks created with createBackgroundSupervisor; ordinary cascades stop
 * at them. Explicit abort(includeBackground) captures and awaits the reached task set.
 *
 * On reopen, abandon tasks receive durable restart marks before dispatch. Missing definitions
 * remain blocked for cleanup; explicit abort converts restart intent and permits orphaning.
 * Abort cleanup runs in a fresh Scope and must be idempotent across uncommitted external work.
 * Native agent/settings/context reads are provided; external conversation requests require
 * a Host adapter. This supervisor does not yet adopt the legacy harness.run executor.
 */
export const make = TaskScheduler.make

export class TaskSupervision extends Context.Service<TaskSupervision, Service>()(
  'effect-harness/TaskSupervision',
) {}

export const layer = <R = never>(
  session: Session.Session,
  options: { readonly host?: Host<R> } = {},
) => Layer.effect(TaskSupervision, make(session, options))
