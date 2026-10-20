import { Effect, Predicate } from 'effect'
import type { SqlError } from 'effect/sql'
import { ConversationRecord } from '../../schema/Conversation.ts'
import type { DocumentPoint } from '../../schema/Document.ts'
import { EntryRecord } from '../../schema/Entry.ts'
import type { Seq } from '../../schema/Ids.ts'
import { SubmissionRecord } from '../../schema/Submission.ts'
import { TaskRecord } from '../../schema/Task.ts'
import type { StorageWrite } from '../../schema/Write.ts'
import { StorageError } from '../../StorageError.ts'
import { checkGlobalIds, claimOf } from '../memory/commit.ts'
import { prepareDocumentActions, resolveDocumentCopies } from '../memory/documents.ts'
import type { ResolvedWrite } from '../memory/documents.ts'
import type { TableName } from '../memory/state.ts'
import { applyDocumentActions, checkDocumentActions } from './documentWrites.ts'
import { materializeDocument } from './documents.ts'
import {
  MetadataRow,
  RecordTypeRow,
  decodeFirst,
  decodeRows,
  encodeJson,
  flag,
  indexedString,
} from './rows.ts'
import type { Db } from './rows.ts'

/**
 * One commit as one SQL transaction, after pi's `SqliteStorage.commit`. The batch is validated
 * with the memory backend's rules against rows read in the transaction, so a rejected batch rolls
 * back with nothing applied.
 */

type Failure = StorageError | SqlError.SqlError

/** The singleton row holding the next free ID and sequence. */
export const metadata = (db: Db) => {
  const { sql, tables } = db
  return sql`SELECT next_id, next_seq FROM ${sql(tables.metadata)} WHERE singleton = 1`.pipe(
    Effect.flatMap(decodeFirst(MetadataRow)),
    Effect.filterOrFail(
      Predicate.isNotUndefined,
      () => new StorageError({ message: 'SQL storage metadata is missing' }),
    ),
  )
}

/** The stored table of every ID the batch claims. */
const recordTypes = Effect.fnUntraced(function* (db: Db, writes: ReadonlyArray<StorageWrite>) {
  const { sql, tables } = db
  const claimed = [...new Set(writes.flatMap((write) => claimOf(write)?.id ?? []))]
  if (claimed.length === 0) return new Map<number, TableName>()
  const rows = yield* sql`SELECT id, record_type FROM ${sql(tables.recordIds)}
    WHERE ${sql.in('id', claimed)}`.pipe(Effect.flatMap(decodeRows(RecordTypeRow)))
  return new Map<number, TableName>(rows.map((row) => [row.id, row.record_type]))
})

const encodeConversation = encodeJson(ConversationRecord)
const encodeEntry = encodeJson(EntryRecord)
const encodeTask = encodeJson(TaskRecord)
const encodeSubmission = encodeJson(SubmissionRecord)

/** The table and columns of a conversation, entry, task, or submission row. */
interface RecordRow {
  readonly table: 'conversations' | 'entries' | 'tasks' | 'submissions'
  readonly columns: Readonly<Record<string, unknown>>
}

const conversationRow = (value: ConversationRecord): Effect.Effect<RecordRow, StorageError> =>
  Effect.map(encodeConversation(value), (record) => ({
    table: 'conversations',
    columns: {
      id: value.id,
      owner_conversation_id: value.owner?.conversationId ?? null,
      owner_task_id: value.owner?.taskId ?? null,
      record,
    },
  }))

const entryRow = (value: EntryRecord, seq: Seq): Effect.Effect<RecordRow, StorageError> =>
  Effect.map(encodeEntry(value), (record) => ({
    table: 'entries',
    columns: {
      id: value.id,
      conversation_id: value.conversationId,
      head: value.head ?? null,
      commit_seq: seq,
      record,
    },
  }))

const taskRow = (value: TaskRecord): Effect.Effect<RecordRow, StorageError> =>
  Effect.map(encodeTask(value), (record) => ({
    table: 'tasks',
    columns: {
      id: value.id,
      conversation_id: value.conversationId,
      kind: indexedString(value.kind),
      status: value.state.status,
      abort_requested: flag(value.abortRequested),
      background: flag(value.background),
      record,
    },
  }))

const submissionRow = (value: SubmissionRecord): Effect.Effect<RecordRow, StorageError> =>
  Effect.map(encodeSubmission(value), (record) => ({
    table: 'submissions',
    columns: {
      id: value.id,
      conversation_id: value.conversationId,
      request_id: value.requestId === undefined ? null : indexedString(value.requestId),
      status: value.status,
      record,
    },
  }))

/** The row a write stores; document writes have none here. */
const recordRow = (
  write: ResolvedWrite,
  seq: Seq,
): Effect.Effect<RecordRow | undefined, StorageError> => {
  if (write.type === 'conversation') return conversationRow(write.value)
  if (write.type === 'entry') return entryRow(write.value, seq)
  if (write.type === 'task') return taskRow(write.value)
  if (write.type === 'submission') return submissionRow(write.value)
  return Effect.undefined
}

/**
 * Write one conversation, entry, task, or submission. `stored` holds the IDs whose rows exist:
 * a task or submission already stored, or written earlier in the batch, is replaced in place.
 */
const applyRecord = Effect.fnUntraced(function* (
  db: Db,
  write: ResolvedWrite,
  seq: Seq,
  stored: Set<number>,
): Effect.fn.Return<void, Failure> {
  const { sql, tables } = db
  const claim = claimOf(write)
  const row = yield* recordRow(write, seq)
  if (claim === undefined || row === undefined) return
  const table = sql(tables[row.table])
  if (stored.has(claim.id)) {
    yield* sql`UPDATE ${table} SET ${sql.update(row.columns, ['id'])} WHERE id = ${claim.id}`
    return
  }
  stored.add(claim.id)
  yield* sql`INSERT INTO ${sql(tables.recordIds)} (id, record_type)
    VALUES (${claim.id}, ${claim.table})`
  yield* sql`INSERT INTO ${table} ${sql.insert(row.columns)}`
})

/** The ID every later mint must exceed once the batch is stored. */
const nextIdAfter = (nextId: number, writes: ReadonlyArray<StorageWrite>): number =>
  writes.reduce((next, write) => Math.max(next, (claimOf(write)?.id ?? 0) + 1), nextId)

/**
 * Persist one batch in one transaction and return its sequence with the next free ID. Everything
 * runs in the transaction, so any failure rolls the whole batch back.
 */
export const commit = (
  db: Db,
  writes: ReadonlyArray<StorageWrite>,
  nextId: number,
): Effect.Effect<{ readonly seq: Seq; readonly nextId: number }, Failure> =>
  db.sql.withTransaction(
    Effect.gen(function* () {
      const { sql, tables } = db
      const { next_id: storedNextId, next_seq: seq } = yield* metadata(db)
      const materialize = (id: number, at: DocumentPoint) => materializeDocument(db, id, at)
      const resolved = yield* resolveDocumentCopies(materialize, writes)
      const types = yield* recordTypes(db, resolved)
      yield* checkGlobalIds(types, resolved)
      const actions = yield* prepareDocumentActions(resolved)
      yield* checkDocumentActions(db, actions)
      const stored = new Set(types.keys())
      for (const write of resolved) yield* applyRecord(db, write, seq, stored)
      yield* applyDocumentActions(db, actions, seq)
      const next = nextIdAfter(Math.max(nextId, storedNextId), resolved)
      yield* sql`UPDATE ${sql(tables.metadata)} SET next_id = ${next}, next_seq = ${seq + 1}
        WHERE singleton = 1`
      return { seq, nextId: next }
    }),
  )
