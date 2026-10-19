import { Effect } from 'effect'
import type { SqlError } from 'effect/sql'
import type {
  DocumentAddress,
  DocumentCreate,
  DocumentPoint,
  DocumentRecord,
  DocumentScope,
  StoredDocument,
} from '../../schema/Document.ts'
import type { Cursor, DocumentQuery, Page } from '../../schema/Scan.ts'
import { StorageError, StorageRequestError } from '../../StorageError.ts'
import { applyBatches, isJsonObject } from '../delta.ts'
import { isAliveAt, isCurrentOnly } from '../memory/state.ts'
import { scanStart } from '../scan.ts'
import {
  DocumentRow,
  MaterializeRow,
  decodeFirst,
  decodeRows,
  indexedString,
  page,
} from './rows.ts'
import type { Db } from './rows.ts'

/** Document reads: address lookups, scope scans, and materialization from stored revisions. */

type Failure = StorageRequestError | StorageError | SqlError.SqlError

/** The scope columns: the session scope has no owner, stored as 0. */
export const scopeColumns = (scope: DocumentScope) => {
  if (scope.kind === 'conversation') return { scopeKind: scope.kind, ownerId: scope.conversationId }
  if (scope.kind === 'task') return { scopeKind: scope.kind, ownerId: scope.taskId }
  return { scopeKind: scope.kind, ownerId: 0 }
}

/** The indexed columns of a logical address; `family` tells a singleton from a keyed member. */
export const addressColumns = (address: DocumentAddress | DocumentCreate | DocumentRecord) => ({
  kind: indexedString(address.kind),
  ...scopeColumns(address.scope),
  family: address.key === undefined ? 0 : 1,
  keyValue: indexedString(address.key ?? ''),
})

/** The `WHERE` condition selecting every incarnation at one address. */
export const addressCondition = (
  { sql }: Db,
  address: DocumentAddress | DocumentCreate | DocumentRecord,
) => {
  const parts = addressColumns(address)
  return sql.and([
    sql`kind = ${parts.kind}`,
    sql`scope_kind = ${parts.scopeKind}`,
    sql`owner_id = ${parts.ownerId}`,
    sql`family = ${parts.family}`,
    sql`key_value = ${parts.keyValue}`,
  ])
}

/** The condition selecting incarnations alive at a point. */
const aliveCondition = ({ sql }: Db, at: DocumentPoint) =>
  at === 'current'
    ? sql`retired_at IS NULL`
    : sql`created_at <= ${at} AND (retired_at IS NULL OR retired_at > ${at})`

export const documentRecord = (db: Db, id: number) => {
  const { sql, tables } = db
  return sql`SELECT record FROM ${sql(tables.documents)} WHERE id = ${id}`.pipe(
    Effect.flatMap(decodeFirst(DocumentRow)),
    Effect.map((row) => row?.record),
  )
}

export const findDocument = (
  db: Db,
  address: DocumentAddress,
  at: DocumentPoint,
): Effect.Effect<DocumentRecord | undefined, StorageError | SqlError.SqlError> => {
  const { sql, tables } = db
  return sql`SELECT record FROM ${sql(tables.documents)}
    WHERE ${addressCondition(db, address)} AND ${aliveCondition(db, at)}
    ORDER BY created_at DESC LIMIT 1`.pipe(
    Effect.flatMap(decodeFirst(DocumentRow)),
    Effect.map((row) => row?.record),
  )
}

/** Document scans run ascending only, so their cursor needs no order. */
export const scanDocuments = (
  db: Db,
  query: DocumentQuery,
  limit: number,
  cursor?: Cursor,
): Effect.Effect<Page<DocumentRecord>, Failure> => {
  const { sql, tables } = db
  const scope = scopeColumns(query.scope)
  return Effect.flatMap(scanStart('ascending', cursor, 'ascending'), ({ after }) => {
    const conditions = [
      sql`scope_kind = ${scope.scopeKind}`,
      sql`owner_id = ${scope.ownerId}`,
      ...(after === undefined ? [] : [sql`id > ${after}`]),
      ...(query.kind === undefined ? [] : [sql`kind = ${indexedString(query.kind)}`]),
      aliveCondition(db, query.at),
    ]
    return sql`SELECT record FROM ${sql(tables.documents)} WHERE ${sql.and(conditions)}
      ORDER BY id LIMIT ${limit + 1}`.pipe(
      Effect.flatMap(decodeRows(DocumentRow)),
      Effect.map((rows) =>
        page(
          rows.map(({ record }) => record),
          limit,
          'ascending',
        ),
      ),
    )
  })
}

const corrupt = (message: string) => Effect.fail(new StorageError({ message }))

/**
 * The record joined to every revision from the latest base at or before the point, in one
 * statement, so a commit between reading the record and its revisions cannot be observed.
 */
const documentWithRevisions = (db: Db, id: number, at: DocumentPoint) => {
  const { sql, tables } = db
  const upper = at === 'current' ? Number.MAX_SAFE_INTEGER : at
  return sql`SELECT d.record AS record, r.content AS content
    FROM ${sql(tables.documents)} d
    LEFT JOIN ${sql(tables.revisions)} r ON r.document_id = d.id AND r.seq <= ${upper}
      AND r.seq >= (
        SELECT MAX(b.seq) FROM ${sql(tables.revisions)} b
        WHERE b.document_id = d.id AND b.kind = 'base' AND b.seq <= ${upper}
      )
    WHERE d.id = ${id}
    ORDER BY r.seq`.pipe(Effect.flatMap(decodeRows(MaterializeRow)))
}

/** Replay one incarnation at a point, after the memory backend's `materializeDocument`. */
export const materializeDocument = Effect.fnUntraced(function* (
  db: Db,
  id: number,
  at: DocumentPoint,
): Effect.fn.Return<StoredDocument | undefined, Failure> {
  const rows = yield* documentWithRevisions(db, id, at)
  const record = rows[0]?.record
  if (record === undefined) return undefined
  if (at !== 'current' && isCurrentOnly(record)) {
    const message = `Document ${id} does not retain historical content`
    return yield* new StorageRequestError({ message })
  }
  if (!isAliveAt(record, at)) return undefined
  const [base, ...tail] = rows.flatMap(({ content }) => (content === null ? [] : [content]))
  if (base?.kind !== 'base') return yield* corrupt(`Document ${id} is missing a required base`)
  // Every delta after a base must share its version; a version change needs a new base.
  const deltas = tail.flatMap((revision) =>
    revision.kind === 'delta' && revision.version === base.version ? [revision.ops] : [],
  )
  if (deltas.length !== tail.length) {
    return yield* corrupt(`Document ${id} crosses a stored version boundary without a base`)
  }
  const value = yield* applyBatches(base.value, deltas)
  if (!isJsonObject(value))
    return yield* corrupt(`Document ${id} does not materialize to an object`)
  const stored: StoredDocument = {
    record,
    version: base.version,
    value,
    deltasSinceBase: deltas.length,
  }
  return stored
})
