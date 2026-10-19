import { Effect } from 'effect'
import type { SqlError } from 'effect/sql'
import { DocumentContent, DocumentRecord } from '../../schema/Document.ts'
import type { DocumentCreate } from '../../schema/Document.ts'
import type { Seq } from '../../schema/Ids.ts'
import { StorageError } from '../../StorageError.ts'
import type { DocumentAction } from '../memory/documents.ts'
import { isCurrentOnly, recordAddressKey } from '../memory/state.ts'
import { addressColumns, addressCondition, documentRecord } from './documents.ts'
import { IdRow, VersionRow, decodeFirst, encodeJson } from './rows.ts'
import type { Db } from './rows.ts'

/** Checking and applying one commit's document actions inside its transaction. */

type Failure = StorageError | SqlError.SqlError

const reject = (message: string) => Effect.fail(new StorageError({ message }))

const latestVersion = (db: Db, id: number) => {
  const { sql, tables } = db
  return sql`SELECT version FROM ${sql(tables.revisions)} WHERE document_id = ${id}
    ORDER BY seq DESC LIMIT 1`.pipe(
    Effect.flatMap(decodeFirst(VersionRow)),
    Effect.map((row) => row?.version),
  )
}

const currentId = (db: Db, record: DocumentRecord | DocumentCreate) => {
  const { sql, tables } = db
  return sql`SELECT id FROM ${sql(tables.documents)}
    WHERE ${addressCondition(db, record)} AND retired_at IS NULL`.pipe(
    Effect.flatMap(decodeFirst(IdRow)),
    Effect.map((row) => row?.id),
  )
}

/** Why one document's actions cannot apply, or its record when they can. */
const checkAction = Effect.fnUntraced(function* (
  db: Db,
  id: number,
  action: DocumentAction,
): Effect.fn.Return<DocumentRecord | DocumentCreate, Failure> {
  const existing = yield* documentRecord(db, id)
  const record = action.create ?? existing
  if (record === undefined) return yield* reject(`Unknown document: ${id}`)
  if (action.create !== undefined && existing !== undefined) {
    return yield* reject(`Document ${id} already exists`)
  }
  if (existing?.retiredAt !== undefined) return yield* reject(`Document ${id} is retired`)
  if (action.content?.kind === 'delta') {
    const previous = yield* latestVersion(db, id)
    if (previous === undefined) return yield* reject(`Document ${id} delta has no base`)
    if (previous !== action.content.version) {
      return yield* reject(`Document ${id} version transition requires a base`)
    }
  }
  return record
})

/** Reject actions on unknown or retired documents, and addresses left with two current incarnations. */
export const checkDocumentActions = Effect.fnUntraced(function* (
  db: Db,
  actions: ReadonlyMap<number, DocumentAction>,
): Effect.fn.Return<void, Failure> {
  const current = new Map<string, number | undefined>()
  const liveCounts = new Map<string, number>()
  for (const [id, action] of actions) {
    const record = yield* checkAction(db, id, action)
    const key = recordAddressKey(record)
    if (!current.has(key)) current.set(key, yield* currentId(db, record))
    const currentAtAddress = current.get(key)
    let live = liveCounts.get(key) ?? (currentAtAddress === undefined ? 0 : 1)
    if (action.retire && currentAtAddress === id) live--
    if (action.create !== undefined && !action.retire) live++
    liveCounts.set(key, live)
  }
  const conflict = [...liveCounts.values()].some((live) => live > 1)
  return yield* conflict
    ? reject('Document address already has a current incarnation')
    : Effect.void
})

const encodeRecord = encodeJson(DocumentRecord)
const encodeContent = encodeJson(DocumentContent)

const insertDocument = Effect.fnUntraced(function* (db: Db, record: DocumentRecord) {
  const { sql, tables } = db
  const parts = addressColumns(record)
  const encoded = yield* encodeRecord(record)
  yield* sql`INSERT INTO ${sql(tables.recordIds)} (id, record_type) VALUES (${record.id}, 'document')`
  yield* sql`INSERT INTO ${sql(tables.documents)}
    (id, kind, family, key_value, scope_kind, owner_id, created_at, retired_at, record)
    VALUES (${record.id}, ${parts.kind}, ${parts.family}, ${parts.keyValue}, ${parts.scopeKind},
      ${parts.ownerId}, ${record.createdAt}, ${record.retiredAt ?? null}, ${encoded})`
})

const insertRevision = Effect.fnUntraced(function* (
  db: Db,
  id: number,
  content: DocumentContent,
  seq: Seq,
) {
  const { sql, tables } = db
  const encoded = yield* encodeContent(content)
  yield* sql`INSERT INTO ${sql(tables.revisions)} (document_id, seq, kind, version, content)
    VALUES (${id}, ${seq}, ${content.kind}, ${content.version}, ${encoded})`
})

const dropRevisions = (db: Db, id: number) => {
  const { sql, tables } = db
  return sql`DELETE FROM ${sql(tables.revisions)} WHERE document_id = ${id}`
}

const retire = Effect.fnUntraced(function* (db: Db, record: DocumentRecord, seq: Seq) {
  const { sql, tables } = db
  const encoded = yield* encodeRecord({ ...record, retiredAt: seq })
  yield* sql`UPDATE ${sql(tables.documents)} SET retired_at = ${seq}, record = ${encoded}
    WHERE id = ${record.id}`
})

/** Apply one checked document's actions; every write is a statement of the commit's transaction. */
const applyAction = Effect.fnUntraced(function* (
  db: Db,
  id: number,
  action: DocumentAction,
  seq: Seq,
): Effect.fn.Return<void, Failure> {
  const { create, content } = action
  // Created and retired in one commit gives an empty lifetime.
  const created: DocumentRecord | undefined =
    create === undefined
      ? undefined
      : { ...create, createdAt: seq, ...(action.retire ? { retiredAt: seq } : {}) }
  const record = created ?? (yield* documentRecord(db, id))
  if (record === undefined) return yield* reject(`Unknown document: ${id}`)
  if (created !== undefined) yield* insertDocument(db, created)
  if (content !== undefined) {
    // Current-only documents drop the history a new base supersedes.
    if (created === undefined && content.kind === 'base' && isCurrentOnly(record)) {
      yield* dropRevisions(db, id)
    }
    yield* insertRevision(db, id, content, seq)
  }
  if (action.retire && created === undefined) yield* retire(db, record, seq)
  return yield* action.retire && isCurrentOnly(record)
    ? Effect.asVoid(dropRevisions(db, id))
    : Effect.void
})

export const applyDocumentActions = (
  db: Db,
  actions: ReadonlyMap<number, DocumentAction>,
  seq: Seq,
): Effect.Effect<void, Failure> =>
  Effect.forEach(actions, ([id, action]) => applyAction(db, id, action, seq), { discard: true })
