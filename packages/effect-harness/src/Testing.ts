/** Runner-neutral, scoped storage contract checks for adapter authors. */
import type * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Equal from 'effect/Equal'
import * as Option from 'effect/Option'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Identity from './Identity.js'
import * as Record from './Record.js'
import * as Sequence from './Sequence.js'
import type * as Storage from './Storage.js'
import { StorageError } from './StorageError.js'

/** A failed contract assertion; adapter and factory failures retain their original error types. */
export class StorageConformanceError extends Schema.TaggedError<StorageConformanceError>()(
  'StorageConformanceError',
  {
    caseName: Schema.String,
    assertion: Schema.String,
    message: Schema.String,
    actual: Schema.Defect(),
    expected: Schema.Defect(),
  },
) {}

/** The captured public storage service, with no runner or platform requirements. */
export type StorageService = Context.Service.Shape<typeof Storage.Storage>

/** Optional checks are omitted unless explicitly enabled. Entry ancestry is always checked. */
export interface StorageConformanceCapabilities {
  /** Retained rewindable document revisions, historical address scans and copies. */
  readonly history?: boolean
  /** Durable state, sequence and reserved IDs survive closing and reopening. */
  readonly reopen?: boolean
  /** Initialization, writes and ID allocation reject ambient client transactions. */
  readonly sqlAmbient?: boolean
  /** Failed transaction control seals all access until reconciliation and reopening. */
  readonly sqlUncertain?: boolean
}

/**
 * One empty, isolated backing store per factory evaluation.
 *
 * `open` must build a fresh service in the supplied Scope on every evaluation.
 * Register backing-store cleanup in the factory's Scope; register connection/service
 * cleanup in `open`'s Scope. Reopen cases close the old service before evaluating
 * `open` again over the same backing store. Memory adapters can omit reopen.
 */
export interface StorageConformanceFixture<E = never, R = never> {
  readonly open: Effect.Effect<StorageService, E | StorageError, R | Scope.Scope>
  /** Run on the same SQL client captured by open; needed only for sqlAmbient. */
  readonly withAmbientTransaction?: <A, E2, R2>(
    effect: Effect.Effect<A, E2, R2>,
  ) => Effect.Effect<A, E | E2, R | R2>
  /**
   * Arrange a real COMMIT/ROLLBACK control failure and invoke storage.commit(writes).
   * Return its original outcome. Do not simulate StorageError in a healthy adapter.
   * Needed only for sqlUncertain; the fixture chooses the failure mode.
   */
  readonly uncertainCommit?: (
    storage: StorageService,
    writes: ReadonlyArray<Record.StorageWrite>,
  ) => Effect.Effect<Sequence.Sequence, E | StorageError, R>
  /** Reconcile the connection and remove failure injection before reopening. */
  readonly recoverTransaction?: Effect.Effect<void, E, R>
}

/** Factory errors and service requirements are preserved in every returned case. */
export interface StorageConformanceOptions<E = never, R = never, FactoryError = E> {
  readonly make: Effect.Effect<StorageConformanceFixture<E, R>, FactoryError, R | Scope.Scope>
  readonly capabilities?: StorageConformanceCapabilities
}

/** Pass run directly to an Effect test runner, or run it at your application's runtime boundary. */
export interface StorageConformanceCase<E = never, R = never> {
  readonly name: string
  readonly run: Effect.Effect<void, E | StorageError | StorageConformanceError, R>
}

const root = Record.ROOT_CONVERSATION_ID
const entryId = (id: number) => Record.EntryId.make(id)
const taskId = (id: number) => Record.TaskId.make(id)
const documentId = (id: number) => Record.DocumentId.make(id)
const entry = (id: number, conversationId = root): Record.Entry => ({
  id: entryId(id),
  conversationId,
  kind: 'testing.note',
  data: { text: `entry ${id}` },
})
const task = (id: number, conversationId = root): Record.Task => ({
  id: taskId(id),
  conversationId,
  kind: 'testing.task',
  version: 1,
  input: null,
  background: false,
  abortRequested: false,
  state: { status: 'pending', checkpoint: { phase: 'initial' } },
})
const submission = (id: number, conversationId = root): Record.InputQueued => ({
  _tag: 'InputQueued',
  id: Record.SubmissionId.make(id),
  conversationId,
  requestId: Identity.RequestId.make('testing.request'),
  type: 'input',
  status: 'queued',
})
const document = (
  id: number,
  history: 'latest' | 'rewindable' = 'latest',
  conversationId = root,
): Record.DocumentCreate => ({
  id: documentId(id),
  kind: 'testing.counter',
  scope: { _tag: 'conversation', conversationId },
  history,
  fork: history === 'rewindable' ? 'asOf' : 'current',
})
const base = (count: number, version = 1): Record.Content => ({
  _tag: 'base',
  version,
  value: { count },
})

const assertions = (caseName: string) => {
  const same = (assertion: string, actual: unknown, expected: unknown) =>
    Effect.suspend(() =>
      Equal.equals(actual, expected)
        ? Effect.void
        : Effect.fail(
            new StorageConformanceError({
              caseName,
              assertion,
              message: `${caseName}: ${assertion}`,
              actual,
              expected,
            }),
          ),
    )
  const some = <A>(
    assertion: string,
    actual: Option.Option<A>,
  ): Effect.Effect<A, StorageConformanceError> =>
    Effect.fromOption(actual).pipe(
      Effect.mapError(
        () =>
          new StorageConformanceError({
            caseName,
            assertion,
            message: `${caseName}: ${assertion}`,
            actual,
            expected: 'Some',
          }),
      ),
    )
  const failure = <A, E, R>(
    assertion: string,
    effect: Effect.Effect<A, E, R>,
    reason: StorageError['reason'],
  ) =>
    Effect.gen(function* () {
      const result = yield* Effect.result(effect)
      if (Result.isSuccess(result))
        return yield* same(assertion, { success: result.success }, { reason })
      const error = result.failure
      yield* same(assertion, error instanceof StorageError ? error.reason : error, reason)
    })
  return { same, some, failure }
}

/**
 * Creates reusable Effect cases without importing a test runner, Node or a platform layer.
 *
 * Each run evaluates make exactly once in its own scope, even on failure/interruption.
 * Core cases require current documents, forks, ownership, deduplication and atomic batches.
 * Capabilities opt into stronger guarantees; they never suppress a failure in an enabled
 * case. Enabled SQL capabilities without the required fixture hooks fail explicitly.
 * SQL uncertainty probes must use a failed write known to roll back after reconciliation.
 * This checks adapter contracts, not physical crash/power-loss durability or Session behavior.
 *
 * @example
 * ```ts
 * import { Context, Effect, Layer } from 'effect'
 * import * as Storage from 'effect-harness/Storage'
 * import * as Testing from 'effect-harness/Testing'
 * const cases = Testing.storageConformance({
 *   make: Effect.succeed({
 *     open: Layer.build(Storage.layerMemory).pipe(
 *       Effect.map(context => Context.get(context, Storage.Storage)),
 *     ),
 *   }),
 *   capabilities: { history: true },
 * })
 * // In @effect/vitest: for (const c of cases) it.effect(c.name, () => c.run)
 * ```
 */
export const storageConformance = <E = never, R = never, FactoryError = E>(
  options: StorageConformanceOptions<E, R, FactoryError>,
): ReadonlyArray<StorageConformanceCase<E | FactoryError, Exclude<R, Scope.Scope>>> => {
  const cases: Array<StorageConformanceCase<E | FactoryError, Exclude<R, Scope.Scope>>> = []
  const add = (
    name: string,
    run: (
      fixture: StorageConformanceFixture<E, R>,
      assert: ReturnType<typeof assertions>,
    ) => Effect.Effect<void, E | StorageError | StorageConformanceError, R>,
  ) => {
    cases.push({
      name,
      run: Effect.scoped(Effect.flatMap(options.make, (fixture) => run(fixture, assertions(name)))),
    })
  }
  const core = (
    name: string,
    run: (
      storage: StorageService,
      assert: ReturnType<typeof assertions>,
    ) => Effect.Effect<void, StorageError | StorageConformanceError>,
  ) =>
    add(name, (fixture, assert) =>
      Effect.scoped(Effect.flatMap(fixture.open, (storage) => run(storage, assert))),
    )

  core('storage: identity allocation and iterable writes', (storage, { same }) =>
    Effect.gen(function* () {
      const ids = yield* Effect.all(
        Array.from({ length: 20 }, () => storage.mintId<Record.EntryId>()),
        { concurrency: 'unbounded' },
      )
      yield* same(
        'fresh IDs reserve root and are unique',
        [...ids].sort((a, b) => a - b),
        Array.from({ length: 20 }, (_, i) => i + 2),
      )
      const seq = yield* storage.commit(
        new Set<Record.StorageWrite>([
          { _tag: 'conversation', value: { id: root } },
          ...ids.map((id): Record.StorageWrite => ({ _tag: 'entry', value: entry(id) })),
        ]),
      )
      yield* same('first committed sequence', seq, 1)
      yield* same(
        'iterable writes persist',
        (yield* Stream.runCollect(storage.scanEntries({ conversationId: root }))).length,
        20,
      )
      yield* same('successful empty batches advance the sequence', yield* storage.commit([]), 2)
    }),
  )

  core('storage: mixed batch rollback', (storage, { same, some, failure }) =>
    Effect.gen(function* () {
      const initialTask = task(3)
      const initialSubmission = submission(4)
      yield* storage.commit([
        { _tag: 'conversation', value: { id: root } },
        { _tag: 'entry', value: entry(2) },
        { _tag: 'task', value: initialTask },
        { _tag: 'submission', value: initialSubmission },
        { _tag: 'document.create', record: document(5), content: base(0) },
      ])
      yield* failure(
        'duplicate identity rejects the entire mixed batch',
        storage.commit([
          {
            _tag: 'task',
            value: {
              ...initialTask,
              state: { status: 'running', checkpoint: { phase: 'running' } },
            },
          },
          {
            _tag: 'submission',
            value: {
              ...initialSubmission,
              _tag: 'InputUnanswered',
              status: 'unanswered',
              reason: 'failed',
            },
          },
          { _tag: 'document.change', id: documentId(5), content: base(9) },
          { _tag: 'entry', value: entry(6) },
          { _tag: 'conversation', value: { id: root } },
        ]),
        'conflict',
      )
      yield* same(
        'rollback preserves task',
        yield* storage.task(taskId(3)),
        Option.some(initialTask),
      )
      yield* same(
        'rollback preserves submission',
        yield* storage.submission(initialSubmission.id),
        Option.some(initialSubmission),
      )
      yield* same(
        'rollback preserves document',
        (yield* some('existing document', yield* storage.document(documentId(5)))).value,
        { count: 0 },
      )
      yield* same(
        'rollback hides earlier entry writes',
        yield* storage.entry(entryId(6)),
        Option.none(),
      )
      yield* same('rollback does not reserve staged IDs', yield* storage.mintId(), 6)
      yield* same('rollback does not advance sequence', yield* storage.commit([]), 2)
    }),
  )

  core('storage: identity and ownership records', (storage, { same, failure }) =>
    Effect.gen(function* () {
      const owner = task(2)
      const owned = { ...task(3), owner: owner.id }
      const child: Record.Conversation = {
        id: Record.ConversationId.make(4),
        owner: { conversationId: root, taskId: owner.id },
      }
      yield* storage.commit([
        { _tag: 'conversation', value: { id: root } },
        { _tag: 'task', value: owner },
        { _tag: 'task', value: owned },
        { _tag: 'conversation', value: child },
      ])
      yield* failure(
        'IDs are globally unique across record kinds',
        storage.commit([{ _tag: 'entry', value: entry(2) }]),
        'conflict',
      )
      yield* failure(
        'conversation ownership cannot change',
        storage.commit([
          {
            _tag: 'conversation',
            value: { ...child, owner: { conversationId: root, taskId: owned.id } },
          },
        ]),
        'conflict',
      )
      yield* same(
        'rejected batch preserves task ownership',
        yield* storage.task(owned.id),
        Option.some(owned),
      )
      yield* same(
        'owner edge filters',
        (yield* Stream.runCollect(
          storage.scanConversations({ ownerConversationId: root, ownerTaskId: owner.id }),
        )).map((c) => c.id),
        [child.id],
      )
      yield* same('ownership rejection does not consume sequence', yield* storage.commit([]), 2)
      const running: Record.Task = {
        ...owned,
        state: { status: 'running', checkpoint: { phase: 'running' } },
      }
      yield* storage.commit([{ _tag: 'task', value: running }])
      yield* same(
        'task state update retains owner metadata',
        yield* storage.task(owned.id),
        Option.some(running),
      )
      yield* same(
        'task scan retains owned record',
        yield* Stream.runCollect(storage.scanTasks({ conversationId: root, status: 'running' })),
        [running],
      )
    }),
  )

  core('storage: conversation-scoped request identity', (storage, { same, failure }) =>
    Effect.gen(function* () {
      const other = Record.ConversationId.make(2)
      yield* storage.commit([
        { _tag: 'conversation', value: { id: root } },
        { _tag: 'conversation', value: { id: other } },
      ])
      yield* failure(
        'duplicate requests in one batch reject all writes',
        storage.commit([
          { _tag: 'submission', value: submission(3) },
          { _tag: 'submission', value: submission(4) },
        ]),
        'conflict',
      )
      yield* same(
        'failed request is not indexed',
        yield* storage.submissionByRequest(root, Identity.RequestId.make('testing.request')),
        Option.none(),
      )
      yield* storage.commit([
        { _tag: 'submission', value: submission(3) },
        { _tag: 'submission', value: submission(4, other) },
      ])
      yield* failure(
        'persisted duplicate request rejects',
        storage.commit([{ _tag: 'submission', value: submission(5) }]),
        'conflict',
      )
      yield* same(
        'request lookup respects conversation',
        yield* storage.submissionByRequest(other, Identity.RequestId.make('testing.request')),
        Option.some(submission(4, other)),
      )
      yield* same(
        'submission scan filters status and conversation',
        (yield* Stream.runCollect(
          storage.scanSubmissions({ conversationId: root, status: 'queued' }),
        )).map((s) => s.id),
        [3],
      )
    }),
  )

  core('storage: detached input and returned values', (storage, { same, some }) =>
    Effect.gen(function* () {
      const value = { count: 0 }
      const data = { text: 'original' }
      yield* storage.commit([
        { _tag: 'conversation', value: { id: root } },
        { _tag: 'entry', value: { ...entry(2), data } },
        {
          _tag: 'document.create',
          record: document(3),
          content: { _tag: 'base', version: 1, value },
        },
      ])
      value.count = 99
      data.text = 'mutated input'
      const stored = yield* some('document exists', yield* storage.document(documentId(3)))
      yield* same('input values are detached', stored.value, { count: 0 })
      Reflect.set(stored.value, 'count', 100)
      const read = yield* some('entry exists', yield* storage.entry(entryId(2)))
      yield* same('entry input is detached', read.entry.data, { text: 'original' })
      if (typeof read.entry.data === 'object' && read.entry.data !== null)
        Reflect.set(read.entry.data, 'text', 'mutated read')
      yield* same(
        'returned documents are detached',
        (yield* some('document still exists', yield* storage.document(documentId(3)))).value,
        { count: 0 },
      )
      yield* same(
        'returned entries are detached',
        (yield* some('entry still exists', yield* storage.entry(entryId(2)))).entry.data,
        { text: 'original' },
      )
    }),
  )

  core('storage: fork ancestry and head markers', (storage, { same, some }) =>
    Effect.gen(function* () {
      const child = Record.ConversationId.make(5)
      yield* storage.commit([
        { _tag: 'conversation', value: { id: root } },
        { _tag: 'entry', value: entry(2) },
        { _tag: 'entry', value: { ...entry(3), head: entryId(2) } },
        { _tag: 'entry', value: entry(4) },
      ])
      yield* storage.commit([
        {
          _tag: 'conversation',
          value: { id: child, parent: { conversationId: root, at: entryId(3) } },
        },
        { _tag: 'entry', value: { ...entry(6, child), head: entryId(6) } },
      ])
      yield* storage.commit([{ _tag: 'entry', value: entry(7) }])
      yield* same(
        'fork excludes parent writes after cutoff',
        (yield* Stream.runCollect(storage.scanEntries({ conversationId: child }))).map((e) => e.id),
        [6, 3, 2],
      )
      yield* same(
        'ascending visible history',
        (yield* Stream.runCollect(
          storage.scanEntries({ conversationId: child, order: 'ascending' }),
        )).map((e) => e.id),
        [2, 3, 6],
      )
      yield* same(
        'entry lookup applies fork visibility',
        yield* storage.entry(entryId(4), { conversationId: child }),
        Option.none(),
      )
      yield* same(
        'inherited entry keeps its commit sequence',
        (yield* some(
          'inherited entry exists',
          yield* storage.entry(entryId(3), { conversationId: child }),
        )).commitSeq,
        1,
      )
      yield* same(
        'latest head belongs to child',
        (yield* some('child head exists', yield* storage.findLatestHeadMarker(child))).head,
        6,
      )
      yield* same(
        'head cutoff finds inherited marker',
        (yield* some(
          'inherited head exists',
          yield* storage.findLatestHeadMarker(child, entryId(3)),
        )).head,
        2,
      )
    }),
  )

  core('storage: current document changes and incarnations', (storage, { same, some, failure }) =>
    Effect.gen(function* () {
      const address: Record.DocumentAddress = {
        kind: 'testing.counter',
        scope: { _tag: 'conversation', conversationId: root },
      }
      yield* storage.commit([
        { _tag: 'conversation', value: { id: root } },
        { _tag: 'document.create', record: document(2), content: base(0) },
      ])
      yield* failure(
        'invalid delta rolls back earlier batch writes',
        storage.commit([
          { _tag: 'entry', value: entry(3) },
          {
            _tag: 'document.change',
            id: documentId(2),
            content: { _tag: 'delta', version: 1, ops: [['set', ['absent', 'count'], 1]] },
          },
        ]),
        'corrupt',
      )
      yield* same(
        'invalid delta hides transient entry',
        yield* storage.entry(entryId(3)),
        Option.none(),
      )
      yield* same(
        'invalid delta preserves content',
        (yield* some('document exists', yield* storage.document(documentId(2)))).value,
        { count: 0 },
      )
      yield* storage.commit([
        {
          _tag: 'document.change',
          id: documentId(2),
          content: { _tag: 'delta', version: 1, ops: [['set', ['count'], 3]] },
        },
      ])
      yield* same(
        'delta materializes current value',
        (yield* some('changed document exists', yield* storage.document(documentId(2)))).value,
        { count: 3 },
      )
      yield* failure(
        'live address cannot have two incarnations',
        storage.commit([{ _tag: 'document.create', record: document(4), content: base(9) }]),
        'conflict',
      )
      yield* storage.commit([
        { _tag: 'document.retire', id: documentId(2) },
        { _tag: 'document.create', record: document(4), content: base(10, 2) },
      ])
      yield* same(
        'retired incarnation is hidden at current',
        yield* storage.document(documentId(2)),
        Option.none(),
      )
      yield* same(
        'address resolves to replacement',
        (yield* some('replacement exists', yield* storage.findDocument(address))).id,
        4,
      )
      yield* same(
        'replacement schema version',
        (yield* some('replacement content exists', yield* storage.document(documentId(4)))).version,
        2,
      )
      yield* same(
        'current scan omits retired incarnation',
        (yield* Stream.runCollect(storage.scanDocuments({ scope: address.scope }))).map(
          (d) => d.id,
        ),
        [4],
      )
    }),
  )

  add('storage: closed scope rejects escaped service access', (fixture, { failure }) =>
    Effect.gen(function* () {
      const storage = yield* Effect.scoped(fixture.open)
      yield* failure('closed commit', storage.commit([]), 'closed')
      yield* failure('closed allocation', storage.mintId(), 'closed')
      yield* failure('closed read', storage.conversation(root), 'closed')
      yield* failure('closed scan', Stream.runCollect(storage.scanTasks()), 'closed')
    }),
  )

  if (options.capabilities?.history === true)
    core('storage: historical document revisions and copies', (storage, { same, some }) =>
      Effect.gen(function* () {
        const address: Record.DocumentAddress = {
          kind: 'testing.counter',
          scope: { _tag: 'conversation', conversationId: root },
        }
        const first = yield* storage.commit([
          { _tag: 'conversation', value: { id: root } },
          { _tag: 'document.create', record: document(2, 'rewindable'), content: base(0) },
        ])
        const changed = yield* storage.commit([
          {
            _tag: 'document.change',
            id: documentId(2),
            content: { _tag: 'delta', version: 1, ops: [['set', ['count'], 3]] },
          },
        ])
        yield* storage.commit([{ _tag: 'document.change', id: documentId(2), content: base(9, 2) }])
        yield* same(
          'history keeps exact old schema and content',
          yield* storage.document(documentId(2), first),
          Option.some({
            record: { ...document(2, 'rewindable'), createdAt: first },
            version: 1,
            value: { count: 0 },
            deltasSinceBase: 0,
          }),
        )
        yield* same(
          'history materializes old deltas',
          (yield* some('historical delta exists', yield* storage.document(documentId(2), changed)))
            .value,
          { count: 3 },
        )
        const child = Record.ConversationId.make(3)
        yield* storage.commit([
          { _tag: 'conversation', value: { id: child } },
          {
            _tag: 'document.copy',
            record: document(4, 'rewindable', child),
            source: { id: documentId(2), at: changed },
          },
        ])
        yield* same(
          'copy selects historical content and version',
          (yield* some('copied document exists', yield* storage.document(documentId(4)))).value,
          { count: 3 },
        )
        yield* same(
          'copy keeps historical version',
          (yield* some('copied version exists', yield* storage.document(documentId(4)))).version,
          1,
        )
        const retired = yield* storage.commit([
          { _tag: 'document.retire', id: documentId(2) },
          { _tag: 'document.create', record: document(5, 'rewindable'), content: base(10, 2) },
        ])
        yield* same(
          'historical address keeps old incarnation',
          (yield* some('old address exists', yield* storage.findDocument(address, first))).id,
          2,
        )
        yield* same(
          'historical scan keeps old incarnation',
          (yield* Stream.runCollect(
            storage.scanDocuments({ scope: address.scope, at: first }),
          )).map((d) => d.id),
          [2],
        )
        yield* same(
          'old content remains until retirement boundary',
          (yield* some(
            'pre-retirement content exists',
            yield* storage.document(documentId(2), Sequence.make(retired - 1)),
          )).value,
          { count: 9 },
        )
        yield* same(
          'retirement sequence hides old content',
          yield* storage.document(documentId(2), retired),
          Option.none(),
        )
      }),
    )

  if (options.capabilities?.reopen === true)
    add('storage: reopened state and reserved identities', (fixture, { same, some, failure }) =>
      Effect.gen(function* () {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const storage = yield* fixture.open
            yield* storage.commit([
              { _tag: 'conversation', value: { id: root } },
              { _tag: 'entry', value: entry(2) },
              { _tag: 'task', value: task(3) },
              { _tag: 'submission', value: submission(4) },
              {
                _tag: 'document.create',
                record: document(
                  5,
                  options.capabilities?.history === true ? 'rewindable' : 'latest',
                ),
                content: base(7),
              },
            ])
            yield* storage.commit([
              { _tag: 'document.change', id: documentId(5), content: base(8, 2) },
            ])
            yield* same('uncommitted reserved identity', yield* storage.mintId(), 6)
            yield* failure(
              'failed batch before close',
              storage.commit([
                { _tag: 'entry', value: entry(7) },
                { _tag: 'conversation', value: { id: root } },
              ]),
              'conflict',
            )
          }),
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            const storage = yield* fixture.open
            yield* same(
              'reopen restores conversation',
              yield* storage.conversation(root),
              Option.some({ id: root }),
            )
            yield* same(
              'reopen restores entry sequence',
              (yield* some('reopened entry exists', yield* storage.entry(entryId(2)))).commitSeq,
              1,
            )
            yield* same(
              'reopen restores task',
              yield* storage.task(taskId(3)),
              Option.some(task(3)),
            )
            yield* same(
              'reopen restores request index',
              yield* storage.submissionByRequest(root, Identity.RequestId.make('testing.request')),
              Option.some(submission(4)),
            )
            yield* same(
              'reopen restores document',
              (yield* some('reopened document exists', yield* storage.document(documentId(5))))
                .value,
              { count: 8 },
            )
            yield* same(
              'reopen restores document version',
              (yield* some(
                'reopened document version exists',
                yield* storage.document(documentId(5)),
              )).version,
              2,
            )
            if (options.capabilities?.history === true) {
              const old = yield* some(
                'reopened historical revision exists',
                yield* storage.document(documentId(5), Sequence.make(1)),
              )
              yield* same('reopen retains exact historical content', old.value, { count: 7 })
              yield* same('reopen retains historical schema version', old.version, 1)
            }
            yield* same(
              'failed write stays absent after reopen',
              yield* storage.entry(entryId(7)),
              Option.none(),
            )
            yield* same('reserved IDs survive reopen', yield* storage.mintId(), 7)
            yield* same('sequence survives reopen and rollback', yield* storage.commit([]), 3)
          }),
        )
      }),
    )

  if (options.capabilities?.sqlAmbient === true)
    add('storage: SQL ambient transaction rejection', (fixture, { same, failure }) =>
      Effect.gen(function* () {
        const ambient = fixture.withAmbientTransaction
        if (ambient === undefined)
          return yield* same(
            'sqlAmbient requires withAmbientTransaction',
            undefined,
            'fixture hook',
          )
        yield* failure(
          'ambient initialization rejects',
          ambient(Effect.scoped(fixture.open)),
          'invalid',
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            const storage = yield* fixture.open
            yield* failure(
              'ambient commit rejects',
              ambient(storage.commit([{ _tag: 'conversation', value: { id: root } }])),
              'invalid',
            )
            yield* failure('ambient allocation rejects', ambient(storage.mintId()), 'invalid')
            yield* same(
              'ambient rejection persists no state',
              yield* storage.conversation(root),
              Option.none(),
            )
            yield* same('ambient rejection reserves no IDs', yield* storage.mintId(), 2)
            yield* same(
              'ambient rejection consumes no sequence',
              yield* storage.commit([{ _tag: 'conversation', value: { id: root } }]),
              1,
            )
          }),
        )
      }),
    )

  if (options.capabilities?.sqlUncertain === true)
    add(
      'storage: SQL uncertain transaction sealing and recovery',
      (fixture, { same, some, failure }) =>
        Effect.gen(function* () {
          const uncertain = fixture.uncertainCommit
          const recover = fixture.recoverTransaction
          if (uncertain === undefined || recover === undefined)
            return yield* same(
              'sqlUncertain requires uncertainCommit and recoverTransaction',
              undefined,
              'fixture hooks',
            )
          const writes: ReadonlyArray<Record.StorageWrite> = [{ _tag: 'entry', value: entry(2) }]
          yield* Effect.scoped(
            Effect.gen(function* () {
              const storage = yield* fixture.open
              yield* storage.commit([{ _tag: 'conversation', value: { id: root } }])
              yield* failure(
                'transaction control error is typed uncertain',
                uncertain(storage, writes),
                'uncertain',
              )
              yield* failure('uncertain service rejects commit', storage.commit([]), 'uncertain')
              yield* failure('uncertain service rejects allocation', storage.mintId(), 'uncertain')
              yield* failure(
                'uncertain service rejects read',
                storage.entry(entryId(2)),
                'uncertain',
              )
              yield* failure(
                'uncertain service rejects scan',
                Stream.runCollect(storage.scanTasks()),
                'uncertain',
              )
              yield* recover
              yield* failure(
                'physical recovery does not unseal old service',
                storage.conversation(root),
                'uncertain',
              )
            }),
          )
          yield* Effect.scoped(
            Effect.gen(function* () {
              const storage = yield* fixture.open
              yield* same(
                'reconciled failed write is absent',
                yield* storage.entry(entryId(2)),
                Option.none(),
              )
              yield* same('reopened sequence reconciles rollback', yield* storage.commit(writes), 2)
              yield* same(
                'reopened service is usable',
                (yield* some('recovered entry exists', yield* storage.entry(entryId(2)))).entry,
                entry(2),
              )
            }),
          )
        }),
    )
  return cases
}
