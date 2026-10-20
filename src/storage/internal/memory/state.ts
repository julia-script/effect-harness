import { Effect } from 'effect'
import type { ConversationRecord } from '../../schema/Conversation.ts'
import type {
  DocumentAddress,
  DocumentContent,
  DocumentCreate,
  DocumentPoint,
  DocumentRecord,
  DocumentScope,
} from '../../schema/Document.ts'
import type { EntryRecord } from '../../schema/Entry.ts'
import type { DocumentId, Seq, SubmissionId } from '../../schema/Ids.ts'
import type { SubmissionRecord, SubmissionStatus } from '../../schema/Submission.ts'
import type { TaskRecord, TaskStatus } from '../../schema/Task.ts'
import { StorageError } from '../../StorageError.ts'

/** The table that owns an ID; every record kind shares one numeric namespace. */
export type TableName = 'conversation' | 'entry' | 'task' | 'submission' | 'document'

/** One stored document revision stamped with the commit that wrote it. */
export type DocumentRevision = DocumentContent & { readonly seq: Seq }

export interface StoredDocumentState {
  record: DocumentRecord
  revisions: DocumentRevision[]
}

export interface DocumentAddressIndex {
  /** Every incarnation ever created at the address, ascending. */
  readonly ids: number[]
  currentId?: DocumentId
}

/**
 * Tables and secondary indexes of one in-memory store. Index arrays hold IDs sorted ascending.
 * Stored records are deeply frozen detached copies.
 */
export interface State {
  readonly recordTypes: Map<number, TableName>
  readonly conversations: Map<number, ConversationRecord>
  readonly conversationIds: number[]
  readonly conversationIdsByOwnerConversation: Map<number, number[]>
  readonly conversationIdsByOwnerTask: Map<number, number[]>
  readonly entries: Map<number, EntryRecord>
  readonly entryIds: Map<number, number[]>
  readonly headEntryIds: Map<number, number[]>
  readonly entryCommitSeqs: Map<number, Seq>
  readonly tasks: Map<number, TaskRecord>
  readonly taskIds: number[]
  readonly taskIdsByStatus: Record<TaskStatus, number[]>
  readonly submissions: Map<number, SubmissionRecord>
  readonly submissionIds: number[]
  readonly submissionIdsByStatus: Record<SubmissionStatus, number[]>
  readonly submissionIdsByRequest: Map<number, Map<string, SubmissionId>>
  readonly documents: Map<number, StoredDocumentState>
  readonly documentAddresses: Map<string, DocumentAddressIndex>
  readonly documentIdsByScope: Map<string, number[]>
  nextId: number
  nextSeq: number
  closed: boolean
}

export const makeState = (): State => ({
  recordTypes: new Map(),
  conversations: new Map(),
  conversationIds: [],
  conversationIdsByOwnerConversation: new Map(),
  conversationIdsByOwnerTask: new Map(),
  entries: new Map(),
  entryIds: new Map(),
  headEntryIds: new Map(),
  entryCommitSeqs: new Map(),
  tasks: new Map(),
  taskIds: [],
  taskIdsByStatus: { pending: [], running: [], waiting: [], completing: [], terminal: [] },
  submissions: new Map(),
  submissionIds: [],
  submissionIdsByStatus: { queued: [], placed: [], done: [], unanswered: [] },
  submissionIdsByRequest: new Map(),
  documents: new Map(),
  documentAddresses: new Map(),
  documentIdsByScope: new Map(),
  // ID 1 is reserved for the root conversation.
  nextId: 2,
  nextSeq: 1,
  closed: false,
})

/** The state while the store is open; fails once its scope has closed. */
export const ensureOpen = (state: State): Effect.Effect<State, StorageError> =>
  Effect.suspend(() =>
    state.closed
      ? Effect.fail(new StorageError({ message: 'MemoryStorage is closed' }))
      : Effect.succeed(state),
  )

export const freeze = <T>(value: T): T => {
  if (value === null || typeof value !== 'object' || Object.isFrozen(value)) return value
  for (const child of Object.values(value)) freeze(child)
  return Object.freeze(value)
}

export const getOrCreate = <K, V>(map: Map<K, V>, key: K, create: () => V): V => {
  const existing = map.get(key)
  if (existing !== undefined) return existing
  const value = create()
  map.set(key, value)
  return value
}

/** The first index whose ID is at least `target`. */
export const lowerBound = (ids: ReadonlyArray<number>, target: number): number => {
  let low = 0
  let high = ids.length
  while (low < high) {
    const middle = (low + high) >>> 1
    if ((ids[middle] ?? Number.POSITIVE_INFINITY) < target) low = middle + 1
    else high = middle
  }
  return low
}

/** The first index whose ID is greater than `target`. */
export const upperBound = (ids: ReadonlyArray<number>, target: number): number => {
  let low = 0
  let high = ids.length
  while (low < high) {
    const middle = (low + high) >>> 1
    if ((ids[middle] ?? Number.POSITIVE_INFINITY) <= target) low = middle + 1
    else high = middle
  }
  return low
}

export const insertSorted = (ids: number[], id: number): void => {
  if ((ids.at(-1) ?? Number.NEGATIVE_INFINITY) < id) ids.push(id)
  else ids.splice(lowerBound(ids, id), 0, id)
}

export const removeSorted = (ids: number[], id: number): void => {
  const index = lowerBound(ids, id)
  if (ids[index] === id) ids.splice(index, 1)
}

export const insertMapId = <K>(index: Map<K, number[]>, key: K, id: number): void => {
  insertSorted(
    getOrCreate(index, key, (): number[] => []),
    id,
  )
}

const scopeParts = (scope: DocumentScope): ReadonlyArray<string | number> => {
  if (scope.kind === 'conversation') return ['conversation', scope.conversationId]
  if (scope.kind === 'task') return ['task', scope.taskId]
  return ['session']
}

export const scopeKey = (scope: DocumentScope): string => JSON.stringify(scopeParts(scope))

export const addressKey = (address: DocumentAddress): string =>
  JSON.stringify([
    address.kind,
    scopeKey(address.scope),
    address.key === undefined ? ['singleton'] : ['family', address.key],
  ])

export const recordAddressKey = (record: DocumentRecord | DocumentCreate): string =>
  addressKey(
    record.key === undefined
      ? { kind: record.kind, scope: record.scope }
      : { kind: record.kind, scope: record.scope, key: record.key },
  )

/** Incarnations are alive on the half-open commit range from creation to retirement. */
export const isAliveAt = (record: DocumentRecord, at: DocumentPoint): boolean => {
  if (at === 'current') return record.retiredAt === undefined
  return record.createdAt <= at && (record.retiredAt === undefined || at < record.retiredAt)
}

/** History and fork behavior; only conversation documents declare them. */
export const semantics = (record: DocumentRecord | DocumentCreate): string | undefined =>
  'history' in record ? `${record.history}/${record.fork}` : undefined

/** Session, task, and latest-history conversation documents keep only current content. */
export const isCurrentOnly = (record: DocumentRecord | DocumentCreate): boolean =>
  !('history' in record) || record.history === 'latest'
