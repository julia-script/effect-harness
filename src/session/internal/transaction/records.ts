import { Clock, Effect } from 'effect'
import type { Schema } from 'effect'
import type { EntryRecord } from '../../../storage/schema/Entry.ts'
import { EntryId, SubmissionId, TaskId } from '../../../storage/schema/Ids.ts'
import type { ConversationId } from '../../../storage/schema/Ids.ts'
import type { SubmissionCreate, SubmissionRecord } from '../../../storage/schema/Submission.ts'
import type { ConversationRecord } from '../../../storage/schema/Conversation.ts'
import type { TaskRecord } from '../../../storage/schema/Task.ts'
import type { EntryDraft, SubmissionSettlement, TaskKind, TaskOptions } from '../../Session.ts'
import { assertOpen, currentTask, rejected, requireConversation, taskEntry } from './state.ts'
import type { TxState } from './state.ts'

/** Append an entry with a fresh ID; values are detached from the caller's draft. */
export const appendEntry = Effect.fnUntraced(function* (
  state: TxState,
  conversationId: ConversationId,
  draft: EntryDraft,
) {
  yield* requireConversation(state, conversationId)
  yield* assertOpen(state)
  const id = yield* state.host.guard(state.host.storage.mintId(EntryId))
  yield* assertOpen(state)
  const { head, ...rest } = structuredClone(draft)
  const byTaskId = state.scope.taskId
  const record: EntryRecord = {
    ...rest,
    id,
    conversationId,
    ...(head === undefined ? {} : { head: head === 'self' ? id : head }),
    ...(byTaskId === undefined ? {} : { byTaskId }),
  }
  state.writes.push({ type: 'entry', value: record })
  return record
})

/** The live owner of a child task, which also fixes the child's conversation. */
const childOwner = Effect.fnUntraced(function* (
  state: TxState,
  taskId: TaskId,
  options: TaskOptions,
) {
  // Validated again against the owner's final candidate during assembly.
  const owner = yield* currentTask(state, taskId)
  yield* assertOpen(state)
  if (owner === undefined) return yield* rejected(`Task owner ${taskId} does not exist`)
  if (options.background === true) return yield* rejected('A child task cannot be background')
  if (options.conversationId !== undefined && options.conversationId !== owner.conversationId) {
    return yield* rejected(`A child task lives in its owner's conversation ${owner.conversationId}`)
  }
  return owner
})

/** Create a pending task at the checkpoint its kind starts from. */
export const createTask = Effect.fnUntraced(function* <I extends Schema.Json>(
  state: TxState,
  kind: TaskKind<I>,
  input: I,
  options: TaskOptions,
) {
  const ownership = options.ownership
  const owner =
    ownership.kind === 'task' ? yield* childOwner(state, ownership.taskId, options) : undefined
  const conversationId =
    owner?.conversationId ?? options.conversationId ?? state.scope.conversationId
  if (conversationId === undefined) {
    return yield* rejected('Tx.createTask() requires options.conversationId')
  }
  yield* requireConversation(state, conversationId)
  yield* assertOpen(state)
  const checkpoint = structuredClone(kind.initial(input))
  const id = yield* state.host.guard(state.host.storage.mintId(TaskId))
  yield* assertOpen(state)
  const record: TaskRecord = {
    id,
    conversationId,
    kind: kind.name,
    version: kind.version,
    input: structuredClone(input),
    ...(owner === undefined ? {} : { owner: owner.id }),
    background: options.background ?? false,
    abortRequested: false,
    ...(options.abandonOnRestart === true ? { abandonOnRestart: true as const } : {}),
    state: { status: 'pending', checkpoint },
  }
  state.tasks.set(id, { write: { kind: 'create', record } })
  return id
})

/** Create a raw submission record with a fresh ID. */
export const createSubmission = Effect.fnUntraced(function* (
  state: TxState,
  create: SubmissionCreate,
) {
  yield* requireConversation(state, create.conversationId)
  yield* assertOpen(state)
  const id = yield* state.host.guard(state.host.storage.mintId(SubmissionId))
  yield* assertOpen(state)
  const record: SubmissionRecord = { ...structuredClone(create), id }
  state.submissions.set(id, record)
  return record
})

/** A change resolved during assembly; it counts as a table write. */
const stageChange = (state: TxState, stage: () => void) =>
  Effect.flatMap(assertOpen(state), () =>
    Effect.sync(() => {
      state.hasTableWrite = true
      stage()
    }),
  )

export const settleSubmission = (
  state: TxState,
  id: SubmissionId,
  settlement: SubmissionSettlement,
) =>
  stageChange(state, () => {
    state.submissionChanges.push({ id, change: structuredClone(settlement) })
  })

export const placeSubmission = (state: TxState, id: SubmissionId, entry: EntryId) =>
  stageChange(state, () => {
    state.submissionChanges.push({ id, change: { status: 'placed', entry } })
  })

/**
 * Lifecycle times: `startedAt` on the first change to `running`, `endedAt` on the change to
 * `terminal`. Once set, they carry over from the replaced record.
 */
const stampTimes = (value: TaskRecord, candidate: TaskRecord | undefined, now: number) => {
  const status = value.state.status
  const startedAt =
    candidate?.startedAt ?? value.startedAt ?? (status === 'running' ? now : undefined)
  const endedAt = candidate?.endedAt ?? value.endedAt ?? (status === 'terminal' ? now : undefined)
  return {
    ...value,
    ...(startedAt === undefined ? {} : { startedAt }),
    ...(endedAt === undefined ? {} : { endedAt }),
  }
}

const setTaskProblem = (value: TaskRecord, candidate: TaskRecord | undefined) => {
  if (candidate?.state.status === 'terminal') return 'already has a terminal candidate'
  return candidate !== undefined && candidate.conversationId !== value.conversationId
    ? 'cannot change conversations'
    : undefined
}

/** Replace one task record completely; validated against committed state during assembly. */
export const setTask = Effect.fnUntraced(function* (state: TxState, value: TaskRecord) {
  yield* assertOpen(state)
  state.hasTableWrite = true
  const task = taskEntry(state, value.id)
  const candidate = task.write?.record
  const problem = setTaskProblem(value, candidate)
  if (problem !== undefined) return yield* rejected(`Task ${value.id} ${problem}`)
  const now = yield* Clock.currentTimeMillis
  const record = stampTimes(structuredClone(value), candidate, now)
  task.write = { kind: task.write?.kind === 'create' ? 'create' : 'replace', record }
  return record
})

/** Every task candidate the transaction staged, in staging order. */
export const stagedConversations = (state: TxState): ReadonlyArray<ConversationRecord> =>
  state.writes.flatMap((write) => (write.type === 'conversation' ? [write.value] : []))

export const stagedTasks = (state: TxState): ReadonlyArray<TaskRecord> => {
  const records: Array<TaskRecord> = []
  for (const task of state.tasks.values()) {
    if (task.write !== undefined) records.push(task.write.record)
  }
  return records
}
