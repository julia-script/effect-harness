import { Effect } from 'effect'
import type { Seq, TaskId } from '../../../storage/schema/Ids.ts'
import type { TaskRecord } from '../../../storage/schema/Task.ts'
import type { StorageWrite } from '../../../storage/schema/Write.ts'
import type { CommitChange, CommitPublication, DocumentCommitChange } from '../../Session.ts'
import {
  checkpoint,
  planDocument,
  rejectForkSourceWrites,
  resolvePublicationOwners,
  retireTerminalTasks,
} from './plans.ts'
import type { Plan } from './plans.ts'
import { committedTask, currentTask, rejected } from './state.ts'
import type { TxState } from './state.ts'
import { resolveSubmissions } from './submissions.ts'

/** The batch of one transaction, and the document plans adoption applies after it commits. */
export interface Assembly {
  readonly writes: ReadonlyArray<StorageWrite>
  readonly plans: ReadonlyArray<Plan>
}

/**
 * New owned work needs a live owner, judged on the owner's final candidate: not `completing`,
 * terminal, or abort-marked. A task cannot create owned work in the commit that finishes it.
 */
const validateOwners = (state: TxState) => {
  const owners: Array<{ readonly what: string; readonly taskId: TaskId }> = []
  for (const write of state.writes) {
    if (write.type !== 'conversation' || write.value.owner === undefined) continue
    owners.push({ what: 'Conversation owner task', taskId: write.value.owner.taskId })
  }
  for (const task of state.tasks.values()) {
    const record = task.write?.kind === 'create' ? task.write.record : undefined
    if (record?.owner !== undefined) owners.push({ what: 'Task owner', taskId: record.owner })
  }
  return Effect.forEach(
    owners,
    ({ what, taskId }) =>
      Effect.flatMap(currentTask(state, taskId), (task) => {
        const problem = ownerProblem(task)
        return problem === undefined ? Effect.void : rejected(`${what} ${taskId} ${problem}`)
      }),
    { discard: true },
  )
}

const ownerProblem = (task: TaskRecord | undefined): string | undefined => {
  if (task === undefined) return 'does not exist'
  const status = task.state.status
  if (status === 'terminal' || status === 'completing') return `is ${status}`
  return task.abortRequested ? 'is abort-marked' : undefined
}

const replacementProblem = (committed: TaskRecord | undefined, record: TaskRecord) => {
  if (committed === undefined) return 'does not exist'
  if (committed.state.status === 'terminal') return 'is already terminal'
  return committed.conversationId === record.conversationId
    ? undefined
    : 'cannot change conversations'
}

/** A replaced task must exist, be live, and stay in its conversation. */
const validateReplacements = (state: TxState) =>
  Effect.forEach(
    state.tasks,
    ([id, task]) => {
      const write = task.write
      if (write?.kind !== 'replace') return Effect.void
      return Effect.flatMap(committedTask(state, id), (committed) => {
        const problem = replacementProblem(committed, write.record)
        return problem === undefined ? Effect.void : rejected(`Task ${id} ${problem}`)
      })
    },
    { discard: true },
  )

/** Validate everything staged and assemble the atomic batch. */
export const assemble = Effect.fnUntraced(function* (state: TxState) {
  const plans: Array<Plan> = []
  for (const entry of state.documents) {
    const plan = planDocument(entry)
    if (plan !== undefined) plans.push(plan)
  }
  yield* rejectForkSourceWrites(state, plans)
  yield* validateOwners(state)
  yield* validateReplacements(state)
  yield* retireTerminalTasks(state, plans)
  yield* resolvePublicationOwners(state, plans)
  yield* resolveSubmissions(state)
  const writes: Array<StorageWrite> = [...state.writes]
  for (const value of state.submissions.values()) writes.push({ type: 'submission', value })
  for (const task of state.tasks.values()) {
    if (task.write !== undefined) writes.push({ type: 'task', value: task.write.record })
  }
  for (const plan of plans) {
    yield* checkpoint(plan)
    if (plan.content !== undefined) writes.push(plan.content)
    if (plan.retire) writes.push({ type: 'document.retire', id: plan.record.id })
  }
  const assembly: Assembly = { writes, plans }
  return assembly
})

/** The publication of an assembled batch that committed as `seq`. */
export const publication = (
  assembly: Assembly,
  documents: ReadonlyArray<DocumentCommitChange>,
  seq: Seq,
): CommitPublication => {
  const changes: Array<CommitChange> = []
  for (const write of assembly.writes) {
    if (
      write.type === 'conversation' ||
      write.type === 'entry' ||
      write.type === 'task' ||
      write.type === 'submission'
    ) {
      changes.push(write)
    }
  }
  changes.push(...documents)
  return { seq, changes }
}
