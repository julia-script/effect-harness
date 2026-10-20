import { Effect } from 'effect'
import { nextCursor, scanStart } from '../scan.ts'
import type { ScanStart } from '../scan.ts'
import { StorageRequestError } from '../../StorageError.ts'
import type { ConversationRecord } from '../../schema/Conversation.ts'
import type { DocumentRecord } from '../../schema/Document.ts'
import type { EntryRecord, StoredEntry } from '../../schema/Entry.ts'
import type { ConversationId, EntryId } from '../../schema/Ids.ts'
import type {
  ConversationQuery,
  Cursor,
  DocumentQuery,
  EntryQuery,
  Page,
  ScanOrder,
  SubmissionQuery,
  TaskQuery,
} from '../../schema/Scan.ts'
import type { SubmissionRecord } from '../../schema/Submission.ts'
import type { TaskRecord } from '../../schema/Task.ts'
import type { HeadMarker } from '../../Storage.ts'
import { isAliveAt, lowerBound, scopeKey, upperBound } from './state.ts'
import type { State } from './state.ts'

interface Segment {
  readonly conversationId: number
  /** Newest entry ID of this conversation that is visible from the scanned one. */
  readonly upper: number
}

const page = <T extends { readonly id: number }>(
  values: ReadonlyArray<T>,
  limit: number,
  order: ScanOrder,
): Page<T> => {
  const items = structuredClone(values.slice(0, limit))
  const last = items.at(-1)
  return values.length <= limit || last === undefined
    ? { items }
    : { items, next: nextCursor(last.id, order) }
}

/** Indexes of sorted `ids` in scan order, after the cursor's ID when there is one. */
function* scanIndexes(ids: ReadonlyArray<number>, start: ScanStart): Generator<number> {
  if (start.order === 'ascending') {
    const first = start.after === undefined ? 0 : upperBound(ids, start.after)
    for (let index = first; index < ids.length; index++) yield index
  } else {
    const end = start.after === undefined ? ids.length : lowerBound(ids, start.after)
    for (let index = end - 1; index >= 0; index--) yield index
  }
}

/** Page through matching records of a sorted index, reading one past `limit` to detect more. */
const scanIndex = <T extends { readonly id: number }>(
  ids: ReadonlyArray<number>,
  start: ScanStart,
  limit: number,
  lookup: (id: number) => T | undefined,
  matches: (value: T) => boolean,
): Page<T> => {
  const values: T[] = []
  for (const index of scanIndexes(ids, start)) {
    if (values.length > limit) break
    const value = lookup(ids[index] ?? 0)
    if (value !== undefined && matches(value)) values.push(value)
  }
  return page(values, limit, start.order)
}

/** The fork chain from a conversation toward its root, each capped at its child's fork point. */
const forkSegments = (
  state: State,
  conversationId: ConversationId,
  minEntryId: number,
  maxEntryId: number,
): Effect.Effect<ReadonlyArray<Segment>, StorageRequestError> => {
  let current = state.conversations.get(conversationId)
  if (current === undefined) {
    return Effect.fail(
      new StorageRequestError({ message: `Unknown conversation: ${conversationId}` }),
    )
  }
  const segments: Segment[] = []
  let upper = maxEntryId
  while (current !== undefined) {
    segments.push({ conversationId: current.id, upper })
    if (current.parent === undefined) break
    upper = Math.min(upper, current.parent.at)
    if (upper < minEntryId) break
    current = state.conversations.get(current.parent.conversationId)
  }
  return Effect.succeed(segments)
}

/** Visible entries newest first, following the fork chain toward the root. */
function* visibleDescending(
  state: State,
  segments: ReadonlyArray<Segment>,
  minEntryId: number,
): Generator<EntryRecord> {
  for (const segment of segments) {
    const ids = state.entryIds.get(segment.conversationId) ?? []
    for (let index = upperBound(ids, segment.upper) - 1; index >= 0; index--) {
      const id = ids[index] ?? 0
      if (id < minEntryId) break
      const entry = state.entries.get(id)
      if (entry !== undefined) yield entry
    }
  }
}

/** Visible entries oldest first: the fork chain's segments from the root conversation forward. */
function* visibleAscending(
  state: State,
  segments: ReadonlyArray<Segment>,
  minEntryId: number,
): Generator<EntryRecord> {
  for (const segment of segments.toReversed()) {
    const ids = state.entryIds.get(segment.conversationId) ?? []
    for (let index = lowerBound(ids, minEntryId); index < ids.length; index++) {
      const id = ids[index] ?? 0
      if (id > segment.upper) break
      const entry = state.entries.get(id)
      if (entry !== undefined) yield entry
    }
  }
}

export const storedEntry = (state: State, id: number): StoredEntry | undefined => {
  const entry = state.entries.get(id)
  const commitSeq = state.entryCommitSeqs.get(id)
  return entry === undefined || commitSeq === undefined
    ? undefined
    : { entry: structuredClone(entry), commitSeq }
}

export const visibleEntry = (
  state: State,
  conversationId: ConversationId,
  id: EntryId,
): Effect.Effect<StoredEntry | undefined, StorageRequestError> =>
  Effect.map(forkSegments(state, conversationId, id, id), (segments) => {
    const visible = visibleDescending(state, segments, id).next()
    return visible.done === true ? undefined : storedEntry(state, visible.value.id)
  })

export const findLatestHeadMarker = (
  state: State,
  conversationId: ConversationId,
  atOrBeforeEntryId?: EntryId,
): Effect.Effect<HeadMarker | undefined, StorageRequestError> => {
  const maxEntryId = atOrBeforeEntryId ?? Number.POSITIVE_INFINITY
  return Effect.map(
    forkSegments(state, conversationId, Number.NEGATIVE_INFINITY, maxEntryId),
    (segments) => {
      const markers = segments.flatMap((segment) => {
        const ids = state.headEntryIds.get(segment.conversationId) ?? []
        const id = ids[upperBound(ids, segment.upper) - 1]
        const entry = id === undefined ? undefined : state.entries.get(id)
        return entry?.head === undefined ? [] : [{ ...entry, head: entry.head }]
      })
      return structuredClone(markers[0])
    },
  )
}

export const scanEntries = Effect.fnUntraced(function* (
  state: State,
  query: EntryQuery,
  limit: number,
  cursor?: Cursor,
) {
  const { order, after } = yield* scanStart(query.order, cursor, 'descending')
  // The cursor narrows the bound on the side the scan moves away from.
  let minEntryId = query.minEntryId ?? Number.NEGATIVE_INFINITY
  let maxEntryId = query.maxEntryId ?? Number.POSITIVE_INFINITY
  if (after !== undefined && order === 'descending') maxEntryId = Math.min(maxEntryId, after - 1)
  if (after !== undefined && order === 'ascending') minEntryId = Math.max(minEntryId, after + 1)
  const segments = yield* forkSegments(state, query.conversationId, minEntryId, maxEntryId)
  const entries =
    order === 'descending'
      ? visibleDescending(state, segments, minEntryId)
      : visibleAscending(state, segments, minEntryId)
  const visible: EntryRecord[] = []
  for (const entry of entries) {
    visible.push(entry)
    if (visible.length > limit) break
  }
  return page(visible, limit, order)
})

export const scanConversations = (
  state: State,
  query: ConversationQuery,
  limit: number,
  cursor?: Cursor,
): Effect.Effect<Page<ConversationRecord>, StorageRequestError> => {
  let ids = state.conversationIds
  if (query.ownerTaskId !== undefined) {
    ids = state.conversationIdsByOwnerTask.get(query.ownerTaskId) ?? []
  } else if (query.ownerConversationId !== undefined) {
    ids = state.conversationIdsByOwnerConversation.get(query.ownerConversationId) ?? []
  }
  return Effect.map(scanStart(query.order, cursor, 'ascending'), (start) =>
    scanIndex(
      ids,
      start,
      limit,
      (id) => state.conversations.get(id),
      (value) =>
        query.ownerConversationId === undefined ||
        value.owner?.conversationId === query.ownerConversationId,
    ),
  )
}

export const scanTasks = (
  state: State,
  query: TaskQuery,
  limit: number,
  cursor?: Cursor,
): Effect.Effect<Page<TaskRecord>, StorageRequestError> =>
  Effect.map(scanStart(query.order, cursor, 'ascending'), (start) =>
    scanIndex(
      query.status === undefined ? state.taskIds : state.taskIdsByStatus[query.status],
      start,
      limit,
      (id) => state.tasks.get(id),
      (value) =>
        (query.conversationId === undefined || value.conversationId === query.conversationId) &&
        (query.kind === undefined || value.kind === query.kind) &&
        (query.abortRequested === undefined || value.abortRequested === query.abortRequested) &&
        (query.background === undefined || value.background === query.background),
    ),
  )

export const scanSubmissions = (
  state: State,
  query: SubmissionQuery,
  limit: number,
  cursor?: Cursor,
): Effect.Effect<Page<SubmissionRecord>, StorageRequestError> =>
  Effect.map(scanStart(query.order, cursor, 'ascending'), (start) =>
    scanIndex(
      query.status === undefined ? state.submissionIds : state.submissionIdsByStatus[query.status],
      start,
      limit,
      (id) => state.submissions.get(id),
      (value) =>
        query.conversationId === undefined || value.conversationId === query.conversationId,
    ),
  )

/** Document scans run ascending only, so their cursor needs no order. */
export const scanDocuments = (
  state: State,
  query: DocumentQuery,
  limit: number,
  cursor?: Cursor,
): Effect.Effect<Page<DocumentRecord>, StorageRequestError> =>
  Effect.map(scanStart('ascending', cursor, 'ascending'), (start) =>
    scanIndex(
      state.documentIdsByScope.get(scopeKey(query.scope)) ?? [],
      start,
      limit,
      (id) => state.documents.get(id)?.record,
      (record) =>
        (query.kind === undefined || record.kind === query.kind) && isAliveAt(record, query.at),
    ),
  )
