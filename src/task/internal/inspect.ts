import { Effect } from 'effect'
import type { TaskRecord } from '../../storage/schema/Task.ts'
import type { Snapshot } from '../Registry.ts'
import type { Inspection, TaskInspectionState } from '../Scheduler.ts'
import { loadScopes } from './chains.ts'
import { waitingOn } from './reserve.ts'
import { fit, missingMigration } from './resolve.ts'
import { lookup } from './state.ts'
import type { State } from './state.ts'

const inspectTask = (state: State, record: TaskRecord, snapshot: Snapshot): TaskInspectionState => {
  if (state.invocations.has(record.id)) return { kind: 'running' }
  if (record.state.status === 'completing') return { kind: 'completing' }
  const on = waitingOn(state, record)
  if (on.length > 0) return { kind: 'waiting', on }
  const found = fit(state, record, lookup(snapshot, record.kind))
  if (found.kind === 'blocked') {
    const { reason, message } = found
    return { kind: 'blocked', reason, ...(message === undefined ? {} : { message }) }
  }
  if (found.migrates && found.task.migrate === undefined) {
    const message = missingMigration(found.task, record)
    return { kind: 'blocked', reason: 'migration_failed', message }
  }
  return { kind: 'ready', migrates: found.migrates }
}

/**
 * Scheduling state and every live task with its derived state, read on the Session line. Runs no
 * task code: a pending migration shows as `ready` with `migrates`, and only a migration already
 * tried, or one that cannot exist, shows as failed.
 */
export const inspect = (state: State) =>
  state.session.commit(() =>
    Effect.gen(function* () {
      yield* loadScopes(state)
      const snapshot = yield* state.registry.snapshot
      const tasks = [...state.live.values()].map((record) => ({
        record,
        state: inspectTask(state, record, snapshot),
      }))
      const inspection: Inspection = {
        scheduling: state.closing ? 'closing' : 'running',
        tasks,
      }
      return inspection
    }),
  )
