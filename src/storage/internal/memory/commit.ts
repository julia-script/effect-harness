import { Effect } from 'effect'
import { Seq } from '../../schema/Ids.ts'
import type { ConversationRecord } from '../../schema/Conversation.ts'
import type { DocumentPoint } from '../../schema/Document.ts'
import type { EntryRecord } from '../../schema/Entry.ts'
import type { SubmissionRecord } from '../../schema/Submission.ts'
import type { TaskRecord } from '../../schema/Task.ts'
import type { StorageWrite } from '../../schema/Write.ts'
import {
  applyDocumentActions,
  checkDocumentActions,
  materializeDocument,
  prepareDocumentActions,
  resolveDocumentCopies,
} from './documents.ts'
import type { DocumentAction, ResolvedWrite } from './documents.ts'
import {
  ensureOpen,
  freeze,
  getOrCreate,
  insertMapId,
  insertSorted,
  removeSorted,
} from './state.ts'
import type { State, TableName } from './state.ts'
import { StorageError } from '../../StorageError.ts'

/** The table and ID a write creates or replaces; document changes and retirements claim none. */
export const claimOf = (
  write: StorageWrite,
): { readonly table: TableName; readonly id: number } | undefined => {
  if (write.type === 'document.create' || write.type === 'document.copy') {
    return { table: 'document', id: write.record.id }
  }
  if (write.type === 'document.change' || write.type === 'document.retire') return undefined
  return { table: write.type, id: write.value.id }
}

/** Why a write cannot claim its ID, given the IDs claimed earlier in the same batch. */
const claimProblem = (
  recordTypes: ReadonlyMap<number, TableName>,
  claimed: ReadonlyMap<number, TableName>,
  table: TableName,
  id: number,
): string | undefined => {
  const existing = recordTypes.get(id)
  const earlier = claimed.get(id)
  if (table === 'conversation' || table === 'entry' || table === 'document') {
    // Immutable records are created exactly once.
    if (existing !== undefined) return `ID ${id} already belongs to ${existing}`
    if (earlier !== undefined) return `ID ${id} is written more than once`
    return undefined
  }
  // Tasks and submissions are replaced as complete records.
  if (existing !== undefined && existing !== table) return `ID ${id} already belongs to ${existing}`
  if (earlier !== undefined && earlier !== table) return `ID ${id} is written as two record types`
  return undefined
}

/**
 * Enforce global ID ownership: one table per ID, and immutable records are created once.
 * `recordTypes` holds the table of every stored ID the batch claims.
 */
export const checkGlobalIds = (
  recordTypes: ReadonlyMap<number, TableName>,
  writes: ReadonlyArray<StorageWrite>,
): Effect.Effect<void, StorageError> => {
  const claimed = new Map<number, TableName>()
  for (const write of writes) {
    const claim = claimOf(write)
    if (claim === undefined) continue
    const problem = claimProblem(recordTypes, claimed, claim.table, claim.id)
    if (problem !== undefined) return Effect.fail(new StorageError({ message: problem }))
    claimed.set(claim.id, claim.table)
  }
  return Effect.void
}

const claim = (state: State, id: number, table: TableName): void => {
  state.recordTypes.set(id, table)
  state.nextId = Math.max(state.nextId, id + 1)
}

export const applyConversation = (state: State, value: ConversationRecord): void => {
  claim(state, value.id, 'conversation')
  state.conversations.set(value.id, value)
  insertSorted(state.conversationIds, value.id)
  if (value.owner !== undefined) {
    insertMapId(state.conversationIdsByOwnerConversation, value.owner.conversationId, value.id)
    insertMapId(state.conversationIdsByOwnerTask, value.owner.taskId, value.id)
  }
}

export const applyEntry = (state: State, value: EntryRecord, seq: Seq): void => {
  claim(state, value.id, 'entry')
  state.entries.set(value.id, value)
  state.entryCommitSeqs.set(value.id, seq)
  insertMapId(state.entryIds, value.conversationId, value.id)
  if (value.head !== undefined) insertMapId(state.headEntryIds, value.conversationId, value.id)
}

export const applyTask = (state: State, value: TaskRecord): void => {
  claim(state, value.id, 'task')
  const previous = state.tasks.get(value.id)
  if (previous === undefined) {
    insertSorted(state.taskIds, value.id)
    insertSorted(state.taskIdsByStatus[value.state.status], value.id)
  } else if (previous.state.status !== value.state.status) {
    removeSorted(state.taskIdsByStatus[previous.state.status], value.id)
    insertSorted(state.taskIdsByStatus[value.state.status], value.id)
  }
  state.tasks.set(value.id, value)
}

const unindexRequest = (state: State, previous: SubmissionRecord): void => {
  if (previous.requestId === undefined) return
  const requests = state.submissionIdsByRequest.get(previous.conversationId)
  if (requests?.get(previous.requestId) !== previous.id) return
  requests.delete(previous.requestId)
  if (requests.size === 0) state.submissionIdsByRequest.delete(previous.conversationId)
}

export const applySubmission = (state: State, value: SubmissionRecord): void => {
  claim(state, value.id, 'submission')
  const previous = state.submissions.get(value.id)
  if (previous === undefined) {
    insertSorted(state.submissionIds, value.id)
    insertSorted(state.submissionIdsByStatus[value.status], value.id)
  } else {
    if (previous.status !== value.status) {
      removeSorted(state.submissionIdsByStatus[previous.status], value.id)
      insertSorted(state.submissionIdsByStatus[value.status], value.id)
    }
    unindexRequest(state, previous)
  }
  state.submissions.set(value.id, value)
  if (value.requestId !== undefined) {
    getOrCreate(state.submissionIdsByRequest, value.conversationId, () => new Map()).set(
      value.requestId,
      value.id,
    )
  }
}

const applyRecord = (state: State, write: ResolvedWrite, seq: Seq): void => {
  switch (write.type) {
    case 'conversation':
      applyConversation(state, write.value)
      break
    case 'entry':
      applyEntry(state, write.value, seq)
      break
    case 'task':
      applyTask(state, write.value)
      break
    case 'submission':
      applySubmission(state, write.value)
      break
    case 'document.create':
    case 'document.change':
    case 'document.retire':
      break
  }
}

/** A validated, detached batch whose application can no longer fail. */
export interface PreparedCommit {
  readonly seq: Seq
  /** Deeply frozen detached writes with copies resolved, for persistence. */
  readonly writes: ReadonlyArray<ResolvedWrite>
  /** The batch's document commands grouped by document, as `apply` will apply them. */
  readonly documentActions: ReadonlyMap<number, DocumentAction>
  /** Apply the batch once; later calls return the same sequence and change nothing. */
  readonly apply: () => Seq
}

/**
 * Validate and detach one batch without changing observable state. Everything that can reject
 * the batch runs here, so a rejected batch has no effect. Replays pass the persisted `seq`.
 */
export const prepareCommit = Effect.fnUntraced(function* (
  state: State,
  writes: ReadonlyArray<StorageWrite>,
  seq: Seq = Seq.make(state.nextSeq),
) {
  yield* ensureOpen(state)
  if (seq < state.nextSeq) {
    return yield* new StorageError({ message: `Commit sequence ${seq} does not strictly increase` })
  }
  const materialize = (id: number, at: DocumentPoint) => materializeDocument(state, id, at)
  const detached = freeze(yield* resolveDocumentCopies(materialize, structuredClone(writes)))
  yield* checkGlobalIds(state.recordTypes, detached)
  const documentActions = yield* prepareDocumentActions(detached)
  yield* checkDocumentActions(state, documentActions)
  let applied = false
  const prepared: PreparedCommit = {
    seq,
    writes: detached,
    documentActions,
    apply: () => {
      if (applied) return seq
      applied = true
      for (const write of detached) applyRecord(state, write, seq)
      applyDocumentActions(state, documentActions, seq)
      state.nextSeq = seq + 1
      return seq
    },
  }
  return prepared
})

/** Atomically persist one batch: a rejected batch has no effect. */
export const commit = (
  state: State,
  writes: ReadonlyArray<StorageWrite>,
): Effect.Effect<Seq, StorageError> =>
  Effect.map(prepareCommit(state, writes), (prepared) => prepared.apply())
