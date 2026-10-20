/* oxlint-disable effecttsgo/any-unknown-in-error-context -- task code may fail with anything, and a failure of any type faults its task */
import { Effect, Exit } from 'effect'
import type { TaskRecord } from '../../storage/schema/Task.ts'
import type { Snapshot } from '../Registry.ts'
import { causeMessage, lookup, report, runTaskCode, withState } from './state.ts'
import type { Erased, State } from './state.ts'

/** Why a task cannot be reserved under a registry snapshot. Derived, never persisted. */
export type BlockedReason = 'missing_task' | 'task_too_old' | 'migration_failed'

export type Resolution =
  | { readonly kind: 'ready'; readonly task: Erased; readonly record: TaskRecord }
  | { readonly kind: 'blocked'; readonly reason: BlockedReason }

/** A definition that can take a record, or why none can; deciding it runs no task code. */
export type Fit =
  | { readonly kind: 'fit'; readonly task: Erased; readonly migrates: boolean }
  | { readonly kind: 'blocked'; readonly reason: BlockedReason; readonly message?: string }

export const fit = (state: State, record: TaskRecord, task: Erased | undefined): Fit => {
  if (task === undefined) return { kind: 'blocked', reason: 'missing_task' }
  if (task.version === record.version) return { kind: 'fit', task, migrates: false }
  if (task.version < record.version) return { kind: 'blocked', reason: 'task_too_old' }
  const failed = state.failedMigrations.get(record.id)
  if (failed?.task === task) {
    return { kind: 'blocked', reason: 'migration_failed', message: failed.message }
  }
  return { kind: 'fit', task, migrates: true }
}

/** Why a newer definition without `migrate` cannot take an older record. */
export const missingMigration = (task: Erased, record: TaskRecord) =>
  `Task ${task.name} version ${task.version} has no migration from ${record.version}`

/** Whether a definition can take a running task: same version, or newer with a migration. */
export const canReserve = (task: Erased, record: TaskRecord): boolean =>
  task.version === record.version || (task.version > record.version && task.migrate !== undefined)

/**
 * Resolve a live record's definition by kind, migrating an older stored version. A failed
 * migration is reported once and retried only when the registry resolves another definition.
 */
export const resolve = Effect.fnUntraced(function* (
  state: State,
  record: TaskRecord,
  snapshot: Snapshot,
) {
  const found = fit(state, record, lookup(snapshot, record.kind))
  if (found.kind === 'blocked') return found
  const { task } = found
  const live = record.state
  if (!found.migrates || !('checkpoint' in live)) {
    const ready: Resolution = { kind: 'ready', task, record }
    return ready
  }
  const { migrate } = task
  const exit =
    migrate === undefined
      ? Exit.die(new Error(missingMigration(task, record)))
      : yield* runTaskCode(() => migrate(record.input, live.checkpoint, record.version))
  if (Exit.isFailure(exit)) {
    state.failedMigrations.set(record.id, { task, message: causeMessage(exit.cause) })
    yield* report(`Task ${record.id} failed to migrate from version ${record.version}`, exit.cause)
    const blocked: Resolution = { kind: 'blocked', reason: 'migration_failed' }
    return blocked
  }
  const migrated = withState(
    { ...record, version: task.version, input: exit.value.input },
    { ...live, checkpoint: exit.value.checkpoint },
  )
  const ready: Resolution = { kind: 'ready', task, record: migrated }
  return ready
})
