import { Effect } from 'effect'
import type { Schema } from 'effect'
import type { SqlError, Statement } from 'effect/sql'
import type { ConversationRecord } from '../../schema/Conversation.ts'
import type {
  ConversationQuery,
  Cursor,
  Page,
  SubmissionQuery,
  TaskQuery,
} from '../../schema/Scan.ts'
import type { SubmissionRecord } from '../../schema/Submission.ts'
import type { TaskRecord } from '../../schema/Task.ts'
import type { StorageError, StorageRequestError } from '../../StorageError.ts'
import { scanStart } from '../scan.ts'
import type { ScanStart } from '../scan.ts'
import {
  ConversationRow,
  SubmissionRow,
  TaskRow,
  decodeFirst,
  decodeRows,
  flag,
  indexedString,
  page,
} from './rows.ts'
import type { Db } from './rows.ts'
import type { Tables } from './tables.ts'

/** Reads of conversations, tasks, and submissions: lookups by ID and filtered ID-ordered scans. */

type Read<A> = Effect.Effect<A, StorageError | SqlError.SqlError>
type Scan<A> = Effect.Effect<Page<A>, StorageRequestError | StorageError | SqlError.SqlError>
type RecordRow<A> = Schema.Decoder<{ readonly record: A }>

const recordById = <A>(
  db: Db,
  table: keyof Tables,
  row: RecordRow<A>,
  id: number,
): Read<A | undefined> => {
  const { sql, tables } = db
  return sql`SELECT record FROM ${sql(tables[table])} WHERE id = ${id}`.pipe(
    Effect.flatMap(decodeFirst(row)),
    Effect.map((found) => found?.record),
  )
}

export const conversation = (db: Db, id: number) =>
  recordById(db, 'conversations', ConversationRow, id)

export const task = (db: Db, id: number) => recordById(db, 'tasks', TaskRow, id)

export const submission = (db: Db, id: number) => recordById(db, 'submissions', SubmissionRow, id)

/** One page of a table in ID order, after the cursor's ID, reading one past `limit`. */
const scanTable = <A extends { readonly id: number }>(
  db: Db,
  table: keyof Tables,
  row: RecordRow<A>,
  filters: ReadonlyArray<Statement.Fragment>,
  start: ScanStart,
  limit: number,
): Effect.Effect<Page<A>, StorageError | SqlError.SqlError> => {
  const { sql, tables } = db
  const ascending = start.order === 'ascending'
  const after =
    start.after === undefined
      ? []
      : [ascending ? sql`id > ${start.after}` : sql`id < ${start.after}`]
  const direction = sql.literal(ascending ? 'ASC' : 'DESC')
  return sql`SELECT record FROM ${sql(tables[table])} WHERE ${sql.and([...filters, ...after])}
    ORDER BY id ${direction} LIMIT ${limit + 1}`.pipe(
    Effect.flatMap(decodeRows(row)),
    Effect.map((rows) =>
      page(
        rows.map(({ record }) => record),
        limit,
        start.order,
      ),
    ),
  )
}

export const scanConversations = (
  db: Db,
  query: ConversationQuery,
  limit: number,
  cursor?: Cursor,
): Scan<ConversationRecord> => {
  const { sql } = db
  const filters = [
    ...(query.ownerConversationId === undefined
      ? []
      : [sql`owner_conversation_id = ${query.ownerConversationId}`]),
    ...(query.ownerTaskId === undefined ? [] : [sql`owner_task_id = ${query.ownerTaskId}`]),
  ]
  return Effect.flatMap(scanStart(query.order, cursor, 'ascending'), (start) =>
    scanTable(db, 'conversations', ConversationRow, filters, start, limit),
  )
}

export const scanTasks = (
  db: Db,
  query: TaskQuery,
  limit: number,
  cursor?: Cursor,
): Scan<TaskRecord> => {
  const { sql } = db
  const filters = [
    ...(query.conversationId === undefined ? [] : [sql`conversation_id = ${query.conversationId}`]),
    ...(query.kind === undefined ? [] : [sql`kind = ${indexedString(query.kind)}`]),
    ...(query.status === undefined ? [] : [sql`status = ${query.status}`]),
    ...(query.abortRequested === undefined
      ? []
      : [sql`abort_requested = ${flag(query.abortRequested)}`]),
    ...(query.background === undefined ? [] : [sql`background = ${flag(query.background)}`]),
  ]
  return Effect.flatMap(scanStart(query.order, cursor, 'ascending'), (start) =>
    scanTable(db, 'tasks', TaskRow, filters, start, limit),
  )
}

export const scanSubmissions = (
  db: Db,
  query: SubmissionQuery,
  limit: number,
  cursor?: Cursor,
): Scan<SubmissionRecord> => {
  const { sql } = db
  const filters = [
    ...(query.conversationId === undefined ? [] : [sql`conversation_id = ${query.conversationId}`]),
    ...(query.status === undefined ? [] : [sql`status = ${query.status}`]),
  ]
  return Effect.flatMap(scanStart(query.order, cursor, 'ascending'), (start) =>
    scanTable(db, 'submissions', SubmissionRow, filters, start, limit),
  )
}

export const submissionByRequest = (
  db: Db,
  conversationId: number,
  requestId: string,
): Read<SubmissionRecord | undefined> => {
  const { sql, tables } = db
  return sql`SELECT record FROM ${sql(tables.submissions)}
    WHERE conversation_id = ${conversationId} AND request_id = ${indexedString(requestId)}`.pipe(
    Effect.flatMap(decodeFirst(SubmissionRow)),
    Effect.map((found) => found?.record),
  )
}
