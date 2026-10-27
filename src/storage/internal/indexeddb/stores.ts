import { Effect, Schema } from 'effect'
import { ConversationRecord } from '../../schema/Conversation.ts'
import { DocumentContent, DocumentRecord } from '../../schema/Document.ts'
import { EntryRecord } from '../../schema/Entry.ts'
import { DocumentId, Seq } from '../../schema/Ids.ts'
import { SubmissionRecord } from '../../schema/Submission.ts'
import { TaskRecord } from '../../schema/Task.ts'
import { StorageError } from '../../StorageError.ts'
import {
  applyConversation,
  applyEntry,
  applySubmission,
  applyTask,
  claimOf,
} from '../memory/commit.ts'
import type { PreparedCommit } from '../memory/commit.ts'
import { restoreDocument } from '../memory/documents.ts'
import type { DocumentRevision, State, TableName } from '../memory/state.ts'
import { freeze, isCurrentOnly, makeState } from '../memory/state.ts'
import type { Key } from './idb.ts'

/**
 * Object stores of the IndexedDB backend. Each table keeps its latest records whole, keyed by ID,
 * so the database holds current state rather than a log: replaced tasks and submissions, and the
 * history a current-only document supersedes, are overwritten or deleted in the commit that
 * obsoletes them. Records are structured-cloned in and decoded with their schemas on the way out,
 * so data a newer or foreign writer left behind fails as a `StorageError`.
 */

export const VERSION = 1
export const FORMAT = 1

export const META = 'meta'
export const CONVERSATIONS = 'conversations'
export const ENTRIES = 'entries'
export const TASKS = 'tasks'
export const SUBMISSIONS = 'submissions'
export const DOCUMENTS = 'documents'
/** Keyed by `[documentId, seq]`, so a scan returns each document's revisions in commit order. */
export const REVISIONS = 'revisions'

export const STORES = [
  META,
  CONVERSATIONS,
  ENTRIES,
  TASKS,
  SUBMISSIONS,
  DOCUMENTS,
  REVISIONS,
] as const

/** The key of the single `meta` row. */
const META_KEY = 'state'

/** Once the ID space is exhausted, the next ID is one past the largest safe integer. */
const MetaRow = Schema.Struct({
  format: Schema.Literal(FORMAT),
  nextId: Schema.Finite,
  nextSeq: Seq,
})
type MetaRow = typeof MetaRow.Type

const EntryRow = Schema.Struct({ record: EntryRecord, seq: Seq })
const RevisionRow = Schema.Struct({ id: DocumentId, seq: Seq, content: DocumentContent })
type RevisionRow = typeof RevisionRow.Type

/** Everything one load reads, in the order of `STORES`. */
const Snapshot = Schema.Tuple([
  Schema.Array(MetaRow),
  Schema.Array(ConversationRecord),
  Schema.Array(EntryRow),
  Schema.Array(TaskRecord),
  Schema.Array(SubmissionRecord),
  Schema.Array(DocumentRecord),
  Schema.Array(RevisionRow),
])

const corrupt = (message: string, cause?: unknown) =>
  new StorageError(
    cause === undefined
      ? { message: `Corrupt IndexedDB storage: ${message}` }
      : { message: `Corrupt IndexedDB storage: ${message}`, cause },
  )

/** Create every object store a database of this version is missing. */
export const upgrade = (db: {
  readonly objectStoreNames: { readonly contains: (name: string) => boolean }
  readonly createObjectStore: (name: string) => unknown
}): void => {
  for (const store of STORES) {
    if (!db.objectStoreNames.contains(store)) db.createObjectStore(store)
  }
}

const claimOnce = (state: State, table: TableName, id: number) =>
  state.recordTypes.has(id)
    ? Effect.fail(corrupt(`ID ${id} is stored as ${state.recordTypes.get(id)} and ${table}`))
    : Effect.void

/** The revisions of each document, in the commit order the store keeps them in. */
const revisionsById = (rows: ReadonlyArray<RevisionRow>) => {
  const revisions = new Map<number, Array<DocumentRevision>>()
  for (const { id, seq, content } of rows) {
    const list = revisions.get(id) ?? []
    list.push({ ...content, seq })
    revisions.set(id, list)
  }
  return revisions
}

/** Rebuild the in-memory state from the rows of every store, one result per store. */
export const restore = Effect.fnUntraced(function* (rows: ReadonlyArray<unknown>) {
  const [metas, conversations, entries, tasks, submissions, documents, revisionRows] = freeze(
    yield* Schema.decodeUnknownEffect(Snapshot)(rows).pipe(
      Effect.mapError((cause) => corrupt('a row does not match its schema', cause)),
    ),
  )
  const state = makeState()
  const meta = metas[0]
  if (meta !== undefined) {
    state.nextId = Math.max(state.nextId, meta.nextId)
    state.nextSeq = meta.nextSeq
  }
  for (const value of conversations) {
    yield* claimOnce(state, 'conversation', value.id)
    applyConversation(state, value)
  }
  for (const { record, seq } of entries) {
    yield* claimOnce(state, 'entry', record.id)
    if (seq >= state.nextSeq) return yield* corrupt(`entry ${record.id} is from a later commit`)
    applyEntry(state, record, seq)
  }
  for (const value of tasks) {
    yield* claimOnce(state, 'task', value.id)
    applyTask(state, value)
  }
  for (const value of submissions) {
    yield* claimOnce(state, 'submission', value.id)
    applySubmission(state, value)
  }
  const revisions = revisionsById(revisionRows)
  for (const record of documents) {
    yield* claimOnce(state, 'document', record.id)
    if (record.createdAt >= state.nextSeq) {
      return yield* corrupt(`document ${record.id} is from a later commit`)
    }
    restoreDocument(state, { record, revisions: revisions.get(record.id) ?? [] })
    revisions.delete(record.id)
  }
  const orphan = revisions.keys().next()
  if (orphan.done !== true) return yield* corrupt(`revisions of unknown document ${orphan.value}`)
  return state
})

/** One request of a commit's transaction. */
export type Operation =
  | { readonly type: 'put'; readonly store: string; readonly key: Key; readonly value: unknown }
  | { readonly type: 'delete'; readonly store: string; readonly key: Key }

const put = (store: string, key: Key, value: unknown): Operation => ({
  type: 'put',
  store,
  key,
  value,
})

const remove = (store: string, key: Key): Operation => ({ type: 'delete', store, key })

/** The requests that store one batch's record writes. */
const recordOperations = (prepared: PreparedCommit): ReadonlyArray<Operation> =>
  prepared.writes.flatMap((write): ReadonlyArray<Operation> => {
    switch (write.type) {
      case 'conversation':
        return [put(CONVERSATIONS, write.value.id, write.value)]
      case 'entry':
        return [put(ENTRIES, write.value.id, { record: write.value, seq: prepared.seq })]
      case 'task':
        return [put(TASKS, write.value.id, write.value)]
      case 'submission':
        return [put(SUBMISSIONS, write.value.id, write.value)]
      case 'document.create':
      case 'document.change':
      case 'document.retire':
        return []
    }
  })

/**
 * The requests that leave each document as `applyDocumentActions` will: the stamped record, the
 * new revision, and deletion of the revisions a current-only document no longer keeps.
 */
const documentOperations = (state: State, prepared: PreparedCommit): ReadonlyArray<Operation> => {
  const { seq } = prepared
  const operations: Array<Operation> = []
  for (const [id, action] of prepared.documentActions) {
    const existing = state.documents.get(id)
    const created =
      action.create === undefined
        ? undefined
        : { ...action.create, createdAt: seq, ...(action.retire ? { retiredAt: seq } : {}) }
    const before = existing?.record ?? created
    if (before === undefined) continue
    const record = action.retire && created === undefined ? { ...before, retiredAt: seq } : before
    if (record !== existing?.record) operations.push(put(DOCUMENTS, id, record))
    const currentOnly = isCurrentOnly(record)
    const dropsHistory = (action.retire || action.content?.kind === 'base') && currentOnly
    if (dropsHistory) {
      for (const revision of existing?.revisions ?? []) {
        operations.push(remove(REVISIONS, [id, revision.seq]))
      }
    }
    if (action.content !== undefined && !(action.retire && currentOnly)) {
      operations.push(put(REVISIONS, [id, seq], { id, seq, content: action.content }))
    }
  }
  return operations
}

/** The ID every later mint must exceed once the batch is stored. */
const nextIdAfter = (state: State, prepared: PreparedCommit): number =>
  prepared.writes.reduce(
    (next, write) => Math.max(next, (claimOf(write)?.id ?? 0) + 1),
    state.nextId,
  )

/** Every request that stores one prepared batch, read from the state before it applies. */
export const planCommit = (state: State, prepared: PreparedCommit): ReadonlyArray<Operation> => {
  const meta: MetaRow = {
    format: FORMAT,
    nextId: nextIdAfter(state, prepared),
    nextSeq: Seq.make(prepared.seq + 1),
  }
  return [
    ...recordOperations(prepared),
    ...documentOperations(state, prepared),
    put(META, META_KEY, meta),
  ]
}
