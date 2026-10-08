/** Adapter-neutral indexed rows and atomic batch validation. */
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Semaphore from 'effect/Semaphore'
import * as Stream from 'effect/Stream'
import type * as Scope from 'effect/Scope'
import * as Record from '../../Record.ts'
import type * as Persistence from '../../Persistence.ts'
import {
  ClosedError,
  ConflictError,
  CorruptError,
  PoisonedError,
  rejected,
  type StorageError,
} from '../../StorageError.ts'
import {
  applyWrites,
  emptyState,
  type State,
  detachedEffect,
  materialize,
  validate,
} from '../../internal/records.ts'

export const Row = Schema.Union([
  Schema.TaggedStruct('conversation', { value: Record.Conversation }),
  Schema.TaggedStruct('entry', {
    value: Schema.Struct({ entry: Record.Entry, commitSeq: Record.Seq }),
  }),
  Schema.TaggedStruct('task', { value: Record.Task }),
  Schema.TaggedStruct('submission', { value: Record.Submission }),
  Schema.TaggedStruct('document', { value: Record.StoredDocument }),
])
export type Row = typeof Row.Type
export type Kind = Row['_tag']
export type Filter = Readonly<{
  conversationId?: number | undefined
  kind?: string | undefined
  ownerTaskId?: number | undefined
  ownerConversationId?: number | undefined
  status?: string | undefined
  background?: boolean | undefined
  abortRequested?: boolean | undefined
  requestId?: string | undefined
  address?: string | undefined
  scope?: string | undefined
  minId?: number | undefined
  maxId?: number | undefined
  at?: Record.Point | undefined
  type?: string
}>
export interface Driver {
  readonly metadata: Effect.Effect<Persistence.Metadata, StorageError>
  readonly get: (id: number) => Effect.Effect<Option.Option<Row>, StorageError>
  readonly page: (
    kind: Kind,
    after: number,
    limit: number,
    filter: Filter,
  ) => Effect.Effect<ReadonlyArray<Row>, StorageError>
  readonly save: (
    rows: ReadonlyArray<Row>,
    metadata: Persistence.Metadata,
  ) => Effect.Effect<void, StorageError>
}
export const idOf = (row: Row): number => {
  if (row._tag === 'entry') return row.value.entry.id
  if (row._tag === 'document') return row.value.record.id
  return row.value.id
}
export const indexOf = (row: Row) => {
  if (row._tag === 'document')
    return {
      scope: Record.scopeKey(row.value.record.scope),
      address: Record.addressKey(row.value.record),
      kind: row.value.record.kind,
      createdAt: row.value.record.createdAt,
      retiredAt: row.value.record.retiredAt,
    }
  if (row._tag === 'conversation')
    return {
      ownerTaskId: row.value.owner?.taskId,
      ownerConversationId: row.value.owner?.conversationId,
    }
  if (row._tag === 'task')
    return {
      conversationId: row.value.conversationId,
      kind: row.value.kind,
      status: row.value.state.status,
      ownerTaskId: row.value.owner,
      background: row.value.background,
      abortRequested: row.value.abortRequested,
    }
  if (row._tag === 'submission')
    return {
      conversationId: row.value.conversationId,
      requestId: row.value.requestId,
      status: row.value.status,
      type: row.value.type,
    }
  return { conversationId: row.value.entry.conversationId, kind: row.value.entry.kind }
}
export const matches = (row: Row, filter: Filter): boolean => {
  const index = indexOf(row)
  const id = idOf(row)
  if (filter.minId !== undefined && id < filter.minId) return false
  if (filter.maxId !== undefined && id > filter.maxId) return false
  if (
    filter.at !== undefined &&
    row._tag === 'document' &&
    !Record.isAlive(row.value.record, filter.at)
  )
    return false
  for (const key of [
    'conversationId',
    'kind',
    'ownerTaskId',
    'ownerConversationId',
    'status',
    'background',
    'abortRequested',
    'requestId',
    'address',
    'scope',
    'type',
  ] as const) {
    if (filter[key] !== undefined && Reflect.get(index, key) !== filter[key]) return false
  }
  return true
}
const put = (state: State, row: Row): State => {
  switch (row._tag) {
    case 'conversation':
      return { ...state, conversations: [...state.conversations, row.value] }
    case 'entry':
      return { ...state, entries: [...state.entries, row.value] }
    case 'task':
      return { ...state, tasks: [...state.tasks, row.value] }
    case 'submission':
      return { ...state, submissions: [...state.submissions, row.value] }
    case 'document':
      return { ...state, documents: [...state.documents, row.value] }
  }
}
const rowsOf = (state: State): Array<Row> => [
  ...state.conversations.map((value): Row => ({ _tag: 'conversation', value })),
  ...state.entries.map((value): Row => ({ _tag: 'entry', value })),
  ...state.tasks.map((value): Row => ({ _tag: 'task', value })),
  ...state.submissions.map((value): Row => ({ _tag: 'submission', value })),
  ...state.documents.map((value): Row => ({ _tag: 'document', value })),
]

/** Only rows touched by this batch and its document copy/address dependencies are loaded. */
export const make = Effect.fn('Persistence.make')(function* (
  driver: Driver,
): Effect.fn.Return<Persistence.Service, never, Scope.Scope> {
  const lock = yield* Semaphore.make(1)
  let closed = false
  let poisoned = false
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      closed = true
    }),
  )
  const open = Effect.suspend(() => {
    if (closed) return Effect.fail(rejected('Persistence is closed', ClosedError))
    if (poisoned)
      return Effect.fail(rejected('Persistence has an uncertain commit; reopen it', PoisonedError))
    return Effect.void
  })
  const scanRows = (kind: Kind, filter: Filter = {}) =>
    Stream.paginate(0, (after) =>
      open.pipe(
        Effect.andThen(driver.page(kind, after, 64, filter)),
        Effect.map(
          (rows) =>
            [rows, rows.length === 64 ? Option.some(idOf(rows[63]!)) : Option.none()] as const,
        ),
      ),
    )
  const get = Effect.fn('Persistence.get')(function* (id: number) {
    yield* open
    return yield* driver.get(id)
  })
  const conversation = (id: Record.ConversationId) =>
    get(id).pipe(
      Effect.map(
        Option.flatMap((row) =>
          row._tag === 'conversation' ? Option.some(row.value) : Option.none(),
        ),
      ),
    )
  const entryRecord = (id: Record.EntryId) =>
    get(id).pipe(
      Effect.map(
        Option.flatMap((row) => (row._tag === 'entry' ? Option.some(row.value) : Option.none())),
      ),
    )
  const task = (id: Record.TaskId) =>
    get(id).pipe(
      Effect.map(
        Option.flatMap((row) => (row._tag === 'task' ? Option.some(row.value) : Option.none())),
      ),
    )
  const submission = (id: Record.SubmissionId) =>
    get(id).pipe(
      Effect.map(
        Option.flatMap((row) =>
          row._tag === 'submission' ? Option.some(row.value) : Option.none(),
        ),
      ),
    )
  const document = Effect.fn('Persistence.document')(function* (
    id: Record.DocumentId,
    at: Record.Point = 'current',
  ) {
    const row = yield* get(id)
    return Option.isSome(row) && row.value._tag === 'document'
      ? yield* materialize(row.value.value, at)
      : Option.none()
  })
  const findDocument = Effect.fn('Persistence.findDocument')(function* (
    address: Record.Address,
    at: Record.Point = 'current',
  ) {
    const rows = yield* open.pipe(
      Effect.andThen(driver.page('document', 0, 2, { address: Record.addressKey(address), at })),
    )
    if (rows.length > 1)
      return yield* rejected('Document address has overlapping incarnations', CorruptError)
    const row = rows[0]
    return row?._tag === 'document' ? yield* materialize(row.value, at) : Option.none()
  })
  const commit = Effect.fn('Persistence.commit')(function* (
    input: ReadonlyArray<Record.Write>,
    allocatedNextId?: number,
  ) {
    return yield* lock.withPermit(
      Effect.uninterruptible(
        Effect.gen(function* () {
          yield* open
          const writes = yield* validate(Schema.Array(Record.Write), input)
          const meta = yield* driver.metadata
          if (
            allocatedNextId !== undefined &&
            (!Number.isInteger(allocatedNextId) ||
              allocatedNextId > Number.MAX_SAFE_INTEGER + 1 ||
              allocatedNextId < meta.nextId)
          )
            return yield* rejected('Invalid allocation state')
          const ids = new Set<number>()
          const affected = new Set<number>()
          const addresses = new Set<string>()
          for (const write of writes) {
            let id: number
            if ('value' in write) id = write.value.id
            else if ('record' in write) id = write.record.id
            else id = write.id
            ids.add(id)
            affected.add(id)
            if (write._tag === 'document.copy') ids.add(write.source.id)
            if (write._tag === 'document.create' || write._tag === 'document.copy')
              addresses.add(Record.addressKey(write.record))
          }
          let state: State = {
            ...emptyState(),
            nextId: meta.nextId,
            nextSeq: meta.revision + 1,
          }
          const loaded = new Set<number>()
          for (const id of ids) {
            const row = yield* driver.get(id)
            if (Option.isSome(row)) {
              state = put(state, row.value)
              loaded.add(id)
            }
          }
          for (const write of writes) {
            if (write._tag === 'document.change' || write._tag === 'document.retire') continue
            const id = 'value' in write ? write.value.id : write.record.id
            if (!loaded.has(id) && id < meta.nextId && id !== Record.ROOT_CONVERSATION_ID)
              return yield* rejected('A committed allocation cannot be reused', ConflictError)
          }
          for (const address of addresses) {
            for (const row of yield* driver.page('document', 0, 2, { address, at: 'current' })) {
              if (!loaded.has(idOf(row))) {
                state = put(state, row)
                loaded.add(idOf(row))
              }
            }
          }
          const next = yield* applyWrites(state, writes)
          for (const write of writes) {
            if (write._tag !== 'submission' || write.value.requestId === undefined) continue
            for (const row of yield* driver.page('submission', 0, 2, {
              conversationId: write.value.conversationId,
              requestId: write.value.requestId,
            })) {
              if (idOf(row) !== write.value.id)
                return yield* rejected('Request identity is already admitted', ConflictError)
            }
            if (
              writes.some(
                (other) =>
                  other._tag === 'submission' &&
                  other.value.id !== write.value.id &&
                  other.value.conversationId === write.value.conversationId &&
                  other.value.requestId === write.value.requestId,
              )
            )
              return yield* rejected('Request identity is duplicated in this batch', ConflictError)
          }
          const seq = yield* validate(Record.Seq, meta.revision + 1)
          const documents: Array<Record.Publication> = []
          for (const stored of next.documents) {
            if (!affected.has(stored.record.id)) continue
            const snapshot = yield* materialize(stored, 'current')
            const command = writes.find(
              (write) =>
                (write._tag === 'document.change' || write._tag === 'document.retire') &&
                write.id === stored.record.id,
            )
            const ops =
              command?._tag === 'document.change'
                ? (command.publicationOps ??
                  (command.content._tag === 'delta'
                    ? command.content.ops
                    : [['replace', command.content.value] as const]))
                : []
            documents.push({
              record: stored.record,
              ...(Option.isSome(snapshot)
                ? { version: snapshot.value.version, value: snapshot.value.value }
                : { value: null }),
              ops,
            })
          }
          const metadata = {
            revision: seq,
            nextId: Math.max(next.nextId, allocatedNextId ?? meta.nextId),
          }
          yield* driver
            .save(
              rowsOf(next).filter((row) => affected.has(idOf(row))),
              metadata,
            )
            .pipe(
              Effect.tapError((error) =>
                Effect.sync(() => {
                  if (error.certainty === 'uncertain') poisoned = true
                }),
              ),
            )
          return yield* detachedEffect({ seq, writes, documents })
        }),
      ),
    )
  })
  const scanEntries = (query: Persistence.EntryQuery): Stream.Stream<Record.Entry, StorageError> =>
    Stream.unwrap(
      Effect.gen(function* () {
        yield* open
        const metadata = yield* driver.metadata
        const max = Math.min(query.maxEntryId ?? Number.MAX_SAFE_INTEGER, metadata.nextId - 1)
        const ancestors: Array<{ id: Record.ConversationId; max: number }> = []
        const seen = new Set<number>()
        let current = query.conversationId
        let cap = max
        while (true) {
          if (seen.has(current))
            return yield* rejected('Conversation ancestry is cyclic', CorruptError)
          seen.add(current)
          const record = yield* conversation(current)
          if (Option.isNone(record)) return yield* rejected('Conversation is absent')
          ancestors.push({ id: current, max: cap })
          if (record.value.parent === undefined) break
          current = record.value.parent.conversationId
          cap = Math.min(cap, record.value.parent.at)
        }
        return Stream.fromIterable(ancestors.reverse()).pipe(
          Stream.flatMap((ancestor) =>
            scanRows('entry', {
              conversationId: ancestor.id,
              minId: query.minEntryId,
              maxId: ancestor.max,
            }),
          ),
          Stream.filter((row): row is Extract<Row, { _tag: 'entry' }> => row._tag === 'entry'),
          Stream.map((row) => row.value.entry),
        )
      }),
    )
  return {
    metadata: open.pipe(Effect.andThen(driver.metadata)),
    conversation,
    entryRecord,
    entry: (id) => entryRecord(id).pipe(Effect.map(Option.map((row) => row.entry))),
    task,
    submission,
    document,
    findDocument,
    commit,
    scanEntries,
    submissionByRequest: (conversationId, requestId) =>
      scanRows('submission', { conversationId, requestId }).pipe(
        Stream.filter(
          (row): row is Extract<Row, { _tag: 'submission' }> => row._tag === 'submission',
        ),
        Stream.map((row) => row.value),
        Stream.runHead,
      ),
    scanConversations: (query = {}) =>
      scanRows('conversation', query).pipe(
        Stream.filter(
          (row): row is Extract<Row, { _tag: 'conversation' }> => row._tag === 'conversation',
        ),
        Stream.map((row) => row.value),
      ),
    scanTasks: (query = {}) =>
      scanRows('task', { ...query, ownerTaskId: query.owner }).pipe(
        Stream.filter((row): row is Extract<Row, { _tag: 'task' }> => row._tag === 'task'),
        Stream.map((row) => row.value),
      ),
    scanSubmissions: (query = {}) =>
      scanRows('submission', query).pipe(
        Stream.filter(
          (row): row is Extract<Row, { _tag: 'submission' }> => row._tag === 'submission',
        ),
        Stream.map((row) => row.value),
      ),
    scanDocuments: (query) =>
      scanRows('document', {
        scope: Record.scopeKey(query.scope),
        kind: query.kind,
        at: query.at ?? 'current',
      }).pipe(
        Stream.filter((row): row is Extract<Row, { _tag: 'document' }> => row._tag === 'document'),
        Stream.map((row) => row.value.record),
      ),
    seal: Effect.sync(() => {
      closed = true
    }),
    isClosed: Effect.sync(() => closed || poisoned),
  }
})
