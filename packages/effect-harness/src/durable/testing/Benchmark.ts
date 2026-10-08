/**
 * Deterministic storage workloads and benchmark reports.
 */
import * as MutableHashMap from 'effect/MutableHashMap'
import { constant } from 'effect/Function'
import * as Option from 'effect/Option'
import { validate } from '../storage/internal/state.ts'
import * as Schema from 'effect/Schema'
import * as Effect from 'effect/Effect'
import * as Record from '../Record.ts'
import { Session } from '../Session.ts'
import type { Transaction } from '../Session.ts'
import { Store } from '../Store.ts'
import { rejected, type StorageError } from '../StorageError.ts'

/**
 * Named counts of entries, tasks and documents in a deterministic workload.
 *
 * @category models
 */
export interface Scale {
  readonly name: string
  readonly entryCount: number
  readonly taskCount: number
  readonly documentCount: number
}
/**
 * In-memory benchmark workload sizes.
 *
 * @category models
 */
export const STORAGE_MEMORY_SCALES: ReadonlyArray<Scale> = [
  { name: '1k', entryCount: 1000, taskCount: 200, documentCount: 200 },
  { name: '10k', entryCount: 10000, taskCount: 2000, documentCount: 2000 },
]
/**
 * Timing benchmark workload size.
 *
 * @category models
 */
export const TIMING_SCALE: Scale = {
  name: 'timing',
  entryCount: 1000,
  taskCount: 300,
  documentCount: 300,
}
const Counter = Schema.Struct({ count: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)) })
const counterValue = (record: Option.Option<{ readonly value: Schema.JsonObject }>) =>
  Option.match(record, {
    onNone: () => Effect.succeed(Number.NaN),
    onSome: (document) =>
      Schema.decodeUnknownEffect(Counter)(document.value).pipe(
        Effect.map((value) => value.count),
        Effect.mapError((cause) =>
          rejected('Invalid benchmark counter document', undefined, cause),
        ),
      ),
  })
const TAILS = [0, 16, 128, 1024] as const
/**
 * Stable record identities and historical cutoffs used by benchmark cases.
 *
 * @category models
 */
export interface Dataset {
  readonly firstEntryId: Record.EntryId
  readonly filteredTaskCount: number
  readonly exactDocumentId: Record.DocumentId
  readonly exactDocumentKey: string
  readonly replayDocumentIds: ReadonlyMap<number, Record.DocumentId>
  readonly historicalDocumentId: Record.DocumentId
  readonly ancientAt: Record.Seq
  readonly recentAt: Record.Seq
  readonly deepestConversationId: Record.ConversationId
  readonly ancestorHeadEntryId: Record.EntryId
}
/**
 * Returns the number of primary records in a benchmark scale.
 *
 * @category combinators
 */
export const storageBenchmarkPrimaryRecordCount = (scale: Scale): number =>
  1 + scale.entryCount + scale.taskCount + scale.documentCount + TAILS.length + 1 + 8 * 33
const task = (id: Record.TaskId, index: number): Record.Task => {
  const status = (['pending', 'running', 'terminal'] as const)[index % 3] ?? 'pending'
  return {
    id,
    conversationId: Record.ROOT_CONVERSATION_ID,
    kind: index % 4 === 0 ? 'benchmark.filtered' : 'benchmark.other',
    version: 1,
    input: { index },
    background: index % 5 === 0,
    abortRequested: index % 7 === 0,
    state: { status },
  }
}
const entries = Effect.fnUntraced(function* (
  tx: Transaction,
  count: number,
  kind: string,
  conversationId = Record.ROOT_CONVERSATION_ID,
) {
  const ids: Array<Record.EntryId> = []
  for (let index = 0; index < count; index++) {
    const value = yield* tx.appendEntry(conversationId, {
      kind,
      data: { index, text: 'x'.repeat(96) },
      ...(index === 0 && kind === 'benchmark.entry' ? { head: 'self' as const } : {}),
    })
    ids.push(value.id)
  }
  return ids
})
const creation = Effect.fnUntraced(function* (
  tx: Transaction,
  kind: string,
  scope: Record.Scope,
  extra: Partial<Record.DocumentCreate> = {},
) {
  const id = yield* tx.mint(Record.DocumentId)
  yield* tx.write({
    _tag: 'document.create',
    record: { id, kind, scope, ...extra },
    content: { _tag: 'base', version: 1, value: Counter.make({ count: 0 }) },
  })
  return id
})
/**
 * Seeds exact lookups, filtered scans, long replay tails, old bases and deep fork ancestry.
 *
 * @category combinators
 */
export const seedStorageBenchmark = Effect.fnUntraced(function* (
  scale: Scale = TIMING_SCALE,
): Effect.fn.Return<Dataset, StorageError, Session | Store> {
  if (
    ![scale.entryCount, scale.taskCount, scale.documentCount].every(
      (n) => Number.isSafeInteger(n) && n > 0,
    )
  )
    return yield* rejected('Benchmark counts must be positive safe integers')
  const session = yield* Session
  const delta = (id: Record.DocumentId, count: number) =>
    session.transaction((tx) =>
      tx
        .write({
          _tag: 'document.change',
          id,
          content: { _tag: 'delta', version: 1, ops: [['set', ['count'], count]] },
        })
        .pipe(Effect.as(null)),
    )
  const store = yield* Store
  yield* session.root()
  let firstEntryId: Record.EntryId | undefined
  for (let start = 0; start < scale.entryCount; start += 100) {
    const ids = yield* session.transaction((tx) =>
      entries(tx, Math.min(100, scale.entryCount - start), 'benchmark.entry'),
    )
    if (firstEntryId === undefined) firstEntryId = ids[0]
  }
  for (let start = 0; start < scale.taskCount; start += 100)
    yield* session.transaction(
      Effect.fnUntraced(function* (tx) {
        for (let index = start; index < Math.min(start + 100, scale.taskCount); index++) {
          const id = yield* tx.mint(Record.TaskId)
          yield* tx.write({ _tag: 'task', value: task(id, index) })
        }
        return null
      }),
    )
  let exactDocumentId: Record.DocumentId | undefined
  for (let start = 0; start < scale.documentCount; start += 100)
    exactDocumentId = yield* session
      .transaction(
        Effect.fnUntraced(function* (tx) {
          let last: Record.DocumentId | undefined
          for (let index = start; index < Math.min(start + 100, scale.documentCount); index++)
            last = yield* creation(
              tx,
              'benchmark.family',
              { _tag: 'session' },
              { key: `key-${index}` },
            )
          return last ?? 0
        }),
      )
      .pipe(
        Effect.filterOrElse(
          (id): id is Record.DocumentId => id !== 0,
          () => rejected('Missing benchmark document'),
        ),
      )
  const replayDocumentIds = MutableHashMap.empty<number, Record.DocumentId>()
  for (const tail of TAILS) {
    const id = yield* session.transaction((tx) =>
      creation(tx, `benchmark.replay.${tail}`, { _tag: 'session' }),
    )
    MutableHashMap.set(replayDocumentIds, tail, id)
    for (let count = 1; count <= tail; count++) yield* delta(id, count)
  }
  const historicalDocumentId = yield* session.transaction((tx) =>
    creation(
      tx,
      'benchmark.history',
      { _tag: 'conversation', conversationId: Record.ROOT_CONVERSATION_ID },
      { history: 'rewindable', fork: 'asOf' },
    ),
  )
  for (let count = 1; count <= 128; count++) yield* delta(historicalDocumentId, count)
  const ancientAt = yield* validate(Record.Seq, (yield* store.read).nextSeq - 1)
  yield* session.transaction((tx) =>
    tx
      .write({
        _tag: 'document.change',
        id: historicalDocumentId,
        content: { _tag: 'base', version: 1, value: Counter.make({ count: 128 }) },
      })
      .pipe(Effect.as(null)),
  )
  for (let count = 129; count <= 256; count++) yield* delta(historicalDocumentId, count)
  const recentAt = yield* validate(Record.Seq, (yield* store.read).nextSeq - 1)
  if (firstEntryId === undefined || exactDocumentId === undefined)
    return yield* rejected('Incomplete benchmark scale')
  let parent = Record.ROOT_CONVERSATION_ID
  let cutoff = firstEntryId
  for (let depth = 0; depth < 8; depth++) {
    const next = yield* session.transaction(
      Effect.fnUntraced(function* (tx) {
        const id = yield* tx.mint(Record.ConversationId)
        yield* tx.write({
          _tag: 'conversation',
          value: { id, parent: { conversationId: parent, at: cutoff } },
        })
        const ids = yield* entries(tx, 32, 'benchmark.fork', id)
        return { id, cutoff: ids.at(-1) ?? cutoff }
      }),
    )
    parent = next.id
    cutoff = next.cutoff
  }
  return {
    firstEntryId,
    filteredTaskCount: Math.min(50, Math.ceil(scale.taskCount / 60)),
    exactDocumentId,
    exactDocumentKey: `key-${scale.documentCount - 1}`,
    // effect-nit-allow P1-stdlib-collection-replacements: Dataset publishes a native ReadonlyMap; snapshot the private primitive index at that interoperability boundary.
    replayDocumentIds: new Map(replayDocumentIds),
    historicalDocumentId,
    ancientAt,
    recentAt,
    deepestConversationId: parent,
    ancestorHeadEntryId: firstEntryId,
  }
})
/**
 * Named read workload with an observable numeric result and its expected value.
 *
 * @category models
 */
export interface ReadBenchmark {
  readonly name: string
  readonly run: (dataset: Dataset) => Effect.Effect<number, StorageError, Session>
  readonly expected: (dataset: Dataset) => number
}
/**
 * Committed storage read benchmark definitions.
 *
 * Counter replay/historical reads include schema validation in their measured run;
 * missing documents retain the benchmark's NaN sentinel.
 *
 * @category models
 */
export const STORAGE_READ_BENCHMARKS: ReadonlyArray<ReadBenchmark> = [
  {
    name: 'exact entry lookup',
    run: (d) =>
      Session.use((s) =>
        s.entry(d.firstEntryId).pipe(
          Effect.map((entry) =>
            entry.pipe(
              Option.map((e) => e.entry.id),
              Option.getOrElse(() => -1),
            ),
          ),
        ),
      ),
    expected: (d) => d.firstEntryId,
  },
  {
    name: 'entry page scan (100)',
    run: () =>
      Session.use((s) =>
        s
          .scanEntries({ conversationId: Record.ROOT_CONVERSATION_ID }, 100)
          .pipe(Effect.map((p) => p.items.length)),
      ),
    expected: constant(100),
  },
  {
    name: 'filtered task scan (50)',
    run: () =>
      Session.use((s) =>
        s
          .scanTasks({ kind: 'benchmark.filtered', status: 'pending', background: true }, 50)
          .pipe(Effect.map((p) => p.items.length)),
      ),
    expected: (d) => d.filteredTaskCount,
  },
  {
    name: 'exact document address among many',
    run: (d) =>
      Session.use((s) =>
        s
          .findDocument({
            kind: 'benchmark.family',
            key: d.exactDocumentKey,
            scope: { _tag: 'session' },
          })
          .pipe(
            Effect.map((record) =>
              record.pipe(
                Option.map((r) => r.id),
                Option.getOrElse(() => -1),
              ),
            ),
          ),
      ),
    expected: (d) => d.exactDocumentId,
  },
  ...TAILS.map((tail): ReadBenchmark => ({
    name: `document replay tail (${tail})`,
    run: (d) =>
      Session.use((s) => {
        const id = Option.fromUndefinedOr(d.replayDocumentIds.get(tail))
        return Option.match(id, {
          onNone: () => rejected('Missing replay benchmark'),
          onSome: (id) => s.document(id).pipe(Effect.flatMap(counterValue)),
        })
      }),
    expected: constant(tail),
  })),
  {
    name: 'ancient historical read before newer base',
    run: (d) =>
      Session.use((s) =>
        s.document(d.historicalDocumentId, d.ancientAt).pipe(Effect.flatMap(counterValue)),
      ),
    expected: constant(128),
  },
  {
    name: 'recent historical read after newer base',
    run: (d) =>
      Session.use((s) =>
        s.document(d.historicalDocumentId, d.recentAt).pipe(Effect.flatMap(counterValue)),
      ),
    expected: constant(256),
  },
  {
    name: 'fork-depth history scan (100)',
    run: (d) =>
      Session.use((s) =>
        s
          .scanEntries({ conversationId: d.deepestConversationId }, 100)
          .pipe(Effect.map((p) => p.items.length)),
      ),
    expected: constant(100),
  },
  {
    name: 'fork-depth head lookup',
    run: (d) =>
      Session.use((s) =>
        s.transaction((tx) =>
          tx.latestHeadMarker(d.deepestConversationId).pipe(
            Effect.map((record) =>
              record.pipe(
                Option.map((r) => r.id),
                Option.getOrElse(() => -1),
              ),
            ),
          ),
        ),
      ),
    expected: (d) => d.ancestorHeadEntryId,
  },
]
/**
 * Named mutation workload with an observable numeric result and expected value.
 *
 * @category models
 */
export interface WriteBenchmark {
  readonly name: string
  readonly expected: number
  readonly run: Effect.Effect<number, StorageError, Session>
}
/**
 * Seeds the controlled records required by a write benchmark.
 *
 * @category combinators
 */
export const seedStorageWriteBenchmark = Session.use((session) =>
  session
    .root()
    .pipe(
      Effect.andThen(session.transaction((tx) => entries(tx, 100, 'benchmark.baseline'))),
      Effect.asVoid,
    ),
)
/**
 * Transactional storage write benchmark definitions.
 *
 * @category models
 */
export const STORAGE_WRITE_BENCHMARKS: ReadonlyArray<WriteBenchmark> = [
  ...[1, 100].map((count) => ({
    name: `commit ${count} ${count === 1 ? 'entry' : 'entries'}`,
    expected: count,
    run: Session.use((session) =>
      session.transaction((tx) =>
        entries(tx, count, 'benchmark.write').pipe(Effect.map((records) => records.length)),
      ),
    ),
  })),
  {
    name: 'commit mixed entry/task/submission/document',
    expected: 4,
    run: Session.use((session) =>
      session.transaction(
        Effect.fnUntraced(function* (tx) {
          const entry = yield* tx.appendEntry(Record.ROOT_CONVERSATION_ID, {
            kind: 'benchmark.mixed',
          })
          const id = yield* tx.mint(Record.TaskId)
          yield* tx.write({ _tag: 'task', value: task(id, id) })
          yield* tx.createSubmission({
            _tag: 'WriteDone' as const,
            conversationId: Record.ROOT_CONVERSATION_ID,
            type: 'write',
            status: 'done',
            entry: entry.id,
          })
          yield* creation(tx, 'benchmark.mixed', { _tag: 'session' }, { key: String(id) })
          return 4
        }),
      ),
    ),
  },
]
