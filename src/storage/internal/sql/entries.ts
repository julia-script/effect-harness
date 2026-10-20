import { Effect } from 'effect'
import type { SqlError } from 'effect/sql'
import type { EntryRecord, StoredEntry } from '../../schema/Entry.ts'
import type { ConversationId, EntryId } from '../../schema/Ids.ts'
import type { Cursor, EntryQuery, Page } from '../../schema/Scan.ts'
import type { HeadMarker } from '../../Storage.ts'
import { StorageRequestError } from '../../StorageError.ts'
import type { StorageError } from '../../StorageError.ts'
import { scanStart } from '../scan.ts'
import { conversation } from './reads.ts'
import { EntryRow, decodeFirst, decodeRows, page } from './rows.ts'
import type { Db } from './rows.ts'

/**
 * Entry reads through fork ancestry, after pi's `SqliteStorage`: a conversation sees its own
 * entries and each ancestor's up to the fork point, one query per conversation on the chain.
 * Conversations and entries are immutable, so the queries need no shared snapshot.
 */

type Failure = StorageRequestError | StorageError | SqlError.SqlError

interface Segment {
  readonly conversationId: number
  /** Newest entry ID of this conversation that is visible from the scanned one. */
  readonly upper: number
}

/** The fork chain from a conversation toward its root, each capped at its child's fork point. */
const forkSegments = Effect.fnUntraced(function* (
  db: Db,
  conversationId: ConversationId,
  minEntryId: number,
  maxEntryId: number,
) {
  let current = yield* conversation(db, conversationId)
  if (current === undefined) {
    return yield* new StorageRequestError({ message: `Unknown conversation: ${conversationId}` })
  }
  const segments: Segment[] = []
  let upper = maxEntryId
  while (current !== undefined) {
    segments.push({ conversationId: current.id, upper })
    if (current.parent === undefined) break
    upper = Math.min(upper, current.parent.at)
    if (upper < minEntryId) break
    current = yield* conversation(db, current.parent.conversationId)
  }
  const chain: ReadonlyArray<Segment> = segments
  return chain
})

/** Entries of one segment within the bounds, in one direction, at most `limit` of them. */
const segmentEntries = (
  db: Db,
  segment: Segment,
  minEntryId: number,
  descending: boolean,
  limit: number,
): Effect.Effect<ReadonlyArray<EntryRecord>, StorageError | SqlError.SqlError> => {
  const { sql, tables } = db
  const bounds = [
    sql`conversation_id = ${segment.conversationId}`,
    ...(Number.isFinite(minEntryId) ? [sql`id >= ${minEntryId}`] : []),
    ...(Number.isFinite(segment.upper) ? [sql`id <= ${segment.upper}`] : []),
  ]
  const direction = sql.literal(descending ? 'DESC' : 'ASC')
  return sql`SELECT record, commit_seq FROM ${sql(tables.entries)} WHERE ${sql.and(bounds)}
    ORDER BY id ${direction} LIMIT ${limit}`.pipe(
    Effect.flatMap(decodeRows(EntryRow)),
    Effect.map((rows) => rows.map(({ record }) => record)),
  )
}

export const storedEntry = (
  db: Db,
  id: number,
): Effect.Effect<StoredEntry | undefined, StorageError | SqlError.SqlError> => {
  const { sql, tables } = db
  return sql`SELECT record, commit_seq FROM ${sql(tables.entries)} WHERE id = ${id}`.pipe(
    Effect.flatMap(decodeFirst(EntryRow)),
    Effect.map((row) =>
      row === undefined ? undefined : { entry: row.record, commitSeq: row.commit_seq },
    ),
  )
}

export const visibleEntry = Effect.fnUntraced(function* (
  db: Db,
  conversationId: ConversationId,
  id: EntryId,
): Effect.fn.Return<StoredEntry | undefined, Failure> {
  const segments = yield* forkSegments(db, conversationId, id, id)
  const stored = yield* storedEntry(db, id)
  const visible = segments.some(
    (segment) => segment.conversationId === stored?.entry.conversationId && id <= segment.upper,
  )
  return visible ? stored : undefined
})

export const findLatestHeadMarker = Effect.fnUntraced(function* (
  db: Db,
  conversationId: ConversationId,
  atOrBeforeEntryId?: EntryId,
): Effect.fn.Return<HeadMarker | undefined, Failure> {
  const { sql, tables } = db
  const maxEntryId = atOrBeforeEntryId ?? Number.POSITIVE_INFINITY
  const segments = yield* forkSegments(db, conversationId, Number.NEGATIVE_INFINITY, maxEntryId)
  for (const segment of segments) {
    const bounds = [
      sql`conversation_id = ${segment.conversationId}`,
      sql`head IS NOT NULL`,
      ...(Number.isFinite(segment.upper) ? [sql`id <= ${segment.upper}`] : []),
    ]
    const row = yield* sql`SELECT record, commit_seq FROM ${sql(tables.entries)}
      WHERE ${sql.and(bounds)} ORDER BY id DESC LIMIT 1`.pipe(Effect.flatMap(decodeFirst(EntryRow)))
    const head = row?.record.head
    if (row !== undefined && head !== undefined) return { ...row.record, head }
  }
  return undefined
})

export const scanEntries = Effect.fnUntraced(function* (
  db: Db,
  query: EntryQuery,
  limit: number,
  cursor?: Cursor,
): Effect.fn.Return<Page<EntryRecord>, Failure> {
  const { order, after } = yield* scanStart(query.order, cursor, 'descending')
  // The cursor narrows the bound on the side the scan moves away from.
  let minEntryId = query.minEntryId ?? Number.NEGATIVE_INFINITY
  let maxEntryId = query.maxEntryId ?? Number.POSITIVE_INFINITY
  if (after !== undefined && order === 'descending') maxEntryId = Math.min(maxEntryId, after - 1)
  if (after !== undefined && order === 'ascending') minEntryId = Math.max(minEntryId, after + 1)
  const segments = yield* forkSegments(db, query.conversationId, minEntryId, maxEntryId)
  const descending = order === 'descending'
  // Newest first walks the chain from the scanned conversation; oldest first from the root.
  const ordered = descending ? segments : segments.toReversed()
  const visible: EntryRecord[] = []
  for (const segment of ordered) {
    if (visible.length > limit) break
    const remaining = limit + 1 - visible.length
    visible.push(...(yield* segmentEntries(db, segment, minEntryId, descending, remaining)))
  }
  return page(visible, limit, order)
})
