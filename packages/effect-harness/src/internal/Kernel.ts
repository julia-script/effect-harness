/** Shared storage operations over private row access; each backend owns its atomic boundary. */
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Domain from '../Record.js'
import * as Records from './records.js'
import type { Storage } from '../Storage.js'
import * as Errors from '../StorageError.js'
import * as Data from '../Record.js'
import * as Sequence from '../Sequence.js'
import * as Metadata from './Metadata.js'
import * as Row from './Row.js'
import * as Commit from './Commit.js'

const WriteBatchJson = Schema.fromJsonString(Schema.Array(Data.StorageWriteSchema))

export interface Access {
  readonly metadata: Effect.Effect<Metadata.Metadata, Errors.StorageError>
  readonly get: (id: number) => Effect.Effect<Option.Option<Row.Row>, Errors.StorageError>
  readonly page: (
    kind: Row.Kind,
    after: number | undefined,
    limit: number,
    filter: Row.Filter,
    order: Data.ScanOrder,
  ) => Effect.Effect<ReadonlyArray<Row.Row>, Errors.StorageError>
  readonly save: (
    rows: ReadonlyArray<Row.Row>,
    metadata: Metadata.Metadata,
  ) => Effect.Effect<void, Errors.StorageError>
  readonly exclusive: <A>(
    effect: Effect.Effect<A, Errors.StorageError>,
  ) => Effect.Effect<A, Errors.StorageError>
}

export const make = Effect.fnUntraced(function* (access: Access) {
  let closed = false
  let uncertain = false
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      closed = true
    }),
  )
  const open = Effect.suspend(() => {
    if (closed) return Effect.fail(Errors.make('closed', 'access', 'Storage scope is closed'))
    if (uncertain)
      return Effect.fail(
        Errors.make('uncertain', 'access', 'Reopen storage to reconcile an uncertain write'),
      )
    return Effect.void
  })
  const mutate = <A>(effect: Effect.Effect<A, Errors.StorageError>) =>
    open.pipe(
      Effect.andThen(access.exclusive(Effect.uninterruptible(open.pipe(Effect.andThen(effect))))),
      Effect.tapError((error) =>
        Effect.sync(() => {
          if (error.reason === 'uncertain') uncertain = true
        }),
      ),
    )
  const get = Effect.fnUntraced(function* (id: number) {
    yield* open
    yield* Schema.decodeEffect(Data.StorageIdSchema)(id).pipe(
      Effect.mapError(Errors.invalid('lookup')),
    )
    return yield* access.get(id)
  })
  const scan = (kind: Row.Kind, filter: Row.Filter, order: Data.ScanOrder = 'ascending') =>
    Stream.paginate(undefined, (after: number | undefined) =>
      open.pipe(
        Effect.andThen(access.page(kind, after, 64, filter, order)),
        Effect.map((rows) => {
          const last = rows.at(-1)
          return [
            rows,
            rows.length === 64 && last !== undefined ? Option.some(Row.idOf(last)) : Option.none(),
          ] as const
        }),
      ),
    )
  const conversation = Effect.fnUntraced(function* (id: Domain.ConversationId) {
    const row = yield* get(id)
    return Option.isSome(row) && row.value._tag === 'conversation'
      ? Option.some(structuredClone(row.value.value))
      : Option.none()
  })
  const ancestry = Effect.fnUntraced(function* (
    conversationId: Domain.ConversationId,
    max: number,
  ) {
    const ancestors: Array<{ id: Domain.ConversationId; max: number }> = []
    const seen = new Set<number>()
    let current = conversationId
    let cap = max
    while (true) {
      if (seen.has(current))
        return yield* Errors.make('corrupt', 'ancestry', 'Conversation ancestry is cyclic')
      seen.add(current)
      const record = yield* conversation(current)
      if (Option.isNone(record))
        return yield* Errors.make('notFound', 'ancestry', 'Conversation is absent')
      ancestors.push({ id: current, max: cap })
      if (record.value.parent === undefined) return ancestors
      current = record.value.parent.conversationId
      cap = Math.min(cap, record.value.parent.at)
    }
  })
  const scanEntries = (query: Data.EntryQuery): Stream.Stream<Domain.Entry, Errors.StorageError> =>
    Stream.unwrap(
      Effect.gen(function* () {
        yield* open
        const valid = yield* Schema.decodeEffect(Data.EntryQuerySchema)(query).pipe(
          Effect.mapError(Errors.invalid('scanEntries')),
        )
        const metadata = yield* access.metadata
        const ancestors = yield* ancestry(
          valid.conversationId,
          Math.min(valid.maxEntryId ?? Number.MAX_SAFE_INTEGER, metadata.nextId - 1),
        )
        const order = valid.order ?? 'descending'
        if (order === 'ascending') ancestors.reverse()
        return Stream.fromIterable(ancestors).pipe(
          Stream.flatMap((ancestor) =>
            scan(
              'entry',
              {
                conversationId: ancestor.id,
                minId: valid.minEntryId,
                maxId: ancestor.max,
              },
              order,
            ),
          ),
          Stream.filter((row): row is Extract<Row.Row, { _tag: 'entry' }> => row._tag === 'entry'),
          Stream.map((row) => structuredClone(row.value.entry)),
        )
      }),
    )
  const document = Effect.fnUntraced(function* (
    id: Domain.DocumentId,
    at: Data.DocumentPoint = 'current',
  ) {
    const point = yield* Schema.decodeEffect(Data.DocumentPointSchema)(at).pipe(
      Effect.mapError(Errors.invalid('document')),
    )
    const row = yield* get(id)
    if (Option.isNone(row) || row.value._tag !== 'document') return Option.none()
    if (!Row.matches(row.value, { at: point })) return Option.none()
    const domainPoint =
      point === 'current'
        ? point
        : yield* Schema.decodeEffect(Domain.Seq)(point).pipe(
            Effect.mapError(Errors.invalid('document')),
          )
    const value = yield* Records.materialize(row.value.value, domainPoint).pipe(
      Effect.mapError(Commit.mapError('document')),
    )
    if (Option.isNone(value)) return Option.none()
    return Option.some(
      structuredClone({
        record: yield* Schema.decodeEffect(Data.DocumentRecordSchema)(value.value.record).pipe(
          Effect.mapError(Errors.invalid('document')),
        ),
        version: value.value.version,
        value: value.value.value,
        deltasSinceBase: value.value.deltasSinceBase,
      }),
    )
  })
  const commit = Effect.fnUntraced(function* (input: Iterable<Data.StorageWrite>) {
    yield* open
    const materialized = yield* Effect.try({
      try: () => Array.from(input),
      catch: (cause) => Errors.make('invalid', 'commit', 'Cannot read write iterable', cause),
    })
    // The JSON codec validates the complete batch and gives storage its own copy.
    const writes = yield* Schema.encodeEffect(WriteBatchJson)(materialized).pipe(
      Effect.flatMap(Schema.decodeEffect(WriteBatchJson)),
      Effect.mapError(Errors.invalid('commit')),
    )
    return yield* mutate(
      Effect.gen(function* () {
        const metadata = yield* access.metadata
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
            addresses.add(Domain.addressKey(write.record))
        }
        const loaded = new Map<number, Row.Row>()
        for (const id of ids) {
          const row = yield* access.get(id)
          if (Option.isSome(row)) loaded.set(id, row.value)
        }
        for (const address of addresses) {
          for (const row of yield* access.page(
            'document',
            undefined,
            2,
            { address, at: 'current' },
            'ascending',
          ))
            loaded.set(Row.idOf(row), row)
        }
        const requests = new Map<string, number>()
        for (const write of writes) {
          if (write._tag !== 'submission' || write.value.requestId === undefined) continue
          const key = JSON.stringify([write.value.conversationId, write.value.requestId])
          const previous = requests.get(key)
          if (previous !== undefined && previous !== write.value.id)
            return yield* Errors.make(
              'conflict',
              'commit',
              'Request identity is duplicated in this batch',
            )
          requests.set(key, write.value.id)
          const rows = yield* access.page(
            'submission',
            undefined,
            2,
            {
              conversationId: write.value.conversationId,
              requestId: write.value.requestId,
            },
            'ascending',
          )
          if (rows.some((row) => Row.idOf(row) !== write.value.id))
            return yield* Errors.make('conflict', 'commit', 'Request identity is already admitted')
        }
        // These shared pure validators own record/document consistency, not backend lifecycles.
        const domainWrites = yield* Schema.decodeEffect(Schema.Array(Domain.Write))(writes).pipe(
          Effect.mapError(Errors.invalid('commit')),
        )
        const next = yield* Records.applyWrites(
          Commit.stateOf(metadata, loaded.values()),
          domainWrites,
        ).pipe(Effect.mapError(Commit.mapError('commit')))
        const sequence = yield* Schema.decodeEffect(Sequence.SequenceSchema)(metadata.nextSeq).pipe(
          Effect.mapError(Errors.invalid('commit')),
        )
        const nextMetadata = yield* Schema.decodeEffect(Metadata.Metadata)({
          ...metadata,
          nextId: next.nextId,
          nextSeq: next.nextSeq,
        }).pipe(Effect.mapError(Errors.invalid('commit')))
        yield* access.save(
          Commit.rowsOf(next).filter((row) => affected.has(Row.idOf(row))),
          nextMetadata,
        )
        return sequence
      }),
    )
  })
  const mintId = <I extends Data.StorageId>(): Effect.Effect<I, Errors.StorageError> =>
    mutate(
      Effect.gen(function* () {
        const metadata = yield* access.metadata
        const id = yield* Schema.decodeEffect(Data.StorageIdSchema)(metadata.nextId).pipe(
          Effect.mapError(Errors.invalid('mintId')),
        )
        yield* access.save([], { ...metadata, nextId: metadata.nextId + 1 })
        // The caller chooses the erased record-kind brand of a globally unique numeric ID.
        return id as I
      }),
    )
  return {
    commit,
    mintId,
    conversation,
    document,
    scanEntries,
    entry: Effect.fnUntraced(function* (id, options) {
      const row = yield* get(id)
      if (Option.isNone(row) || row.value._tag !== 'entry') return Option.none()
      const stored = row.value.value
      if (options !== undefined) {
        const ancestors = yield* ancestry(options.conversationId, id)
        if (
          !ancestors.some(
            (ancestor) => ancestor.id === stored.entry.conversationId && id <= ancestor.max,
          )
        )
          return Option.none()
      }
      return Option.some(
        structuredClone(
          yield* Schema.decodeEffect(Data.StoredEntrySchema)(row.value.value).pipe(
            Effect.mapError(Errors.invalid('entry')),
          ),
        ),
      )
    }),
    findLatestHeadMarker: (conversationId, atOrBeforeEntryId) =>
      scanEntries({
        conversationId,
        ...(atOrBeforeEntryId === undefined ? {} : { maxEntryId: atOrBeforeEntryId }),
      }).pipe(
        Stream.filter((entry): entry is Data.HeadMarker => entry.head !== undefined),
        Stream.runHead,
      ),
    task: Effect.fnUntraced(function* (id) {
      const row = yield* get(id)
      return Option.isSome(row) && row.value._tag === 'task'
        ? Option.some(structuredClone(row.value.value))
        : Option.none()
    }),
    submission: Effect.fnUntraced(function* (id) {
      const row = yield* get(id)
      return Option.isSome(row) && row.value._tag === 'submission'
        ? Option.some(structuredClone(row.value.value))
        : Option.none()
    }),
    submissionByRequest: Effect.fnUntraced(function* (conversationId, requestId) {
      yield* Schema.decodeEffect(Domain.ConversationId)(conversationId).pipe(
        Effect.mapError(Errors.invalid('submissionByRequest')),
      )
      yield* Schema.decodeEffect(Schema.String)(requestId).pipe(
        Effect.mapError(Errors.invalid('submissionByRequest')),
      )
      return yield* scan('submission', { conversationId, requestId }).pipe(
        Stream.filter(
          (row): row is Extract<Row.Row, { _tag: 'submission' }> => row._tag === 'submission',
        ),
        Stream.map((row) => structuredClone(row.value)),
        Stream.runHead,
      )
    }),
    findDocument: Effect.fnUntraced(function* (address, at = 'current') {
      yield* open
      const valid = yield* Schema.decodeEffect(Data.DocumentAddressSchema)(address).pipe(
        Effect.mapError(Errors.invalid('findDocument')),
      )
      const point = yield* Schema.decodeEffect(Data.DocumentPointSchema)(at).pipe(
        Effect.mapError(Errors.invalid('findDocument')),
      )
      const rows = yield* access.page(
        'document',
        undefined,
        2,
        { address: Domain.addressKey(valid), at: point },
        'ascending',
      )
      if (rows.length > 1)
        return yield* Errors.make(
          'corrupt',
          'findDocument',
          'Document address has overlapping incarnations',
        )
      const row = rows[0]
      return row?._tag === 'document'
        ? Option.some(
            structuredClone(
              yield* Schema.decodeEffect(Data.DocumentRecordSchema)(row.value.record).pipe(
                Effect.mapError(Errors.invalid('findDocument')),
              ),
            ),
          )
        : Option.none()
    }),
    scanConversations: (query = {}) =>
      Stream.unwrap(
        Schema.decodeEffect(Data.ConversationQuerySchema)(query).pipe(
          Effect.mapError(Errors.invalid('scanConversations')),
          Effect.map((valid) =>
            scan('conversation', valid, valid.order).pipe(
              Stream.filter(
                (row): row is Extract<Row.Row, { _tag: 'conversation' }> =>
                  row._tag === 'conversation',
              ),
              Stream.map((row) => structuredClone(row.value)),
            ),
          ),
        ),
      ),
    scanTasks: (query = {}) =>
      Stream.unwrap(
        Schema.decodeEffect(Data.TaskQuerySchema)(query).pipe(
          Effect.mapError(Errors.invalid('scanTasks')),
          Effect.map((valid) =>
            scan('task', valid, valid.order).pipe(
              Stream.filter(
                (row): row is Extract<Row.Row, { _tag: 'task' }> => row._tag === 'task',
              ),
              Stream.map((row) => structuredClone(row.value)),
            ),
          ),
        ),
      ),
    scanSubmissions: (query = {}) =>
      Stream.unwrap(
        Schema.decodeEffect(Data.SubmissionQuerySchema)(query).pipe(
          Effect.mapError(Errors.invalid('scanSubmissions')),
          Effect.map((valid) =>
            scan('submission', valid, valid.order).pipe(
              Stream.filter(
                (row): row is Extract<Row.Row, { _tag: 'submission' }> => row._tag === 'submission',
              ),
              Stream.map((row) => structuredClone(row.value)),
            ),
          ),
        ),
      ),
    scanDocuments: (query) =>
      Stream.unwrap(
        Schema.decodeEffect(Data.DocumentQuerySchema)(query).pipe(
          Effect.mapError(Errors.invalid('scanDocuments')),
          Effect.map((valid) =>
            scan(
              'document',
              {
                scope: Domain.scopeKey(valid.scope),
                kind: valid.kind,
                at: valid.at ?? 'current',
              },
              valid.order,
            ).pipe(
              Stream.filter(
                (row): row is Extract<Row.Row, { _tag: 'document' }> => row._tag === 'document',
              ),
              Stream.mapEffect((row) =>
                Schema.decodeEffect(Data.DocumentRecordSchema)(row.value.record).pipe(
                  Effect.mapError(Errors.invalid('scanDocuments')),
                  Effect.map((record) => structuredClone(record)),
                ),
              ),
            ),
          ),
        ),
      ),
  } satisfies Storage['Service']
})
