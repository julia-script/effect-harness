/** Runner-neutral, scoped storage and bound service contract checks for adapter authors. */
import type * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Equal from 'effect/Equal'
import * as Exit from 'effect/Exit'
import * as Option from 'effect/Option'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
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
 * Core cases include lossless string indexes (including lone UTF-16 surrogates), own
 * prototype-sensitive JSON keys, multi-page ID ordering, four-generation ancestor cutoffs,
 * waiting/completing task records, passive-write replacements and safe-integer allocator exhaustion. Fork probes tighten
 * every ancestor cutoff; they do not impose a maximum supported ancestry depth.
 * Core cases require current documents, forks, ownership, deduplication and atomic batches.
 * Capabilities opt into stronger guarantees; they never suppress a failure in an enabled
 * case. Reopen closes each structural service before checking its actual records and indexes
 * through a fresh service; it also retains allocator exhaustion. History also checks long delta tails across a root replacement. Enabled SQL capabilities without the required fixture hooks fail explicitly.
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

  const structural = (
    name: string,
    run: (
      storage: StorageService,
      assert: ReturnType<typeof assertions>,
      reopen: Effect.Effect<StorageService, E | StorageError, R>,
    ) => Effect.Effect<void, E | StorageError | StorageConformanceError, R>,
  ) =>
    add(name, (fixture, assert) =>
      Effect.scoped(
        Effect.gen(function* () {
          const parent = yield* Effect.scope
          let scope = yield* Scope.fork(parent)
          const open = () => Effect.provideService(fixture.open, Scope.Scope, scope)
          const storage = yield* open()
          const reopen = Effect.gen(function* () {
            yield* Scope.close(scope, Exit.void)
            scope = yield* Scope.fork(parent)
            return yield* open()
          })
          yield* run(storage, assert, reopen)
        }),
      ),
    )

  structural('storage: safe-integer allocator exhaustion', (initial, { same, failure }, reopen) =>
    Effect.gen(function* () {
      let storage = initial
      const maximum = Number.MAX_SAFE_INTEGER
      const seq = yield* storage.commit([
        { _tag: 'conversation', value: { id: root } },
        { _tag: 'entry', value: entry(maximum) },
      ])
      const verify = Effect.gen(function* () {
        for (let attempt = 0; attempt < 2; attempt++)
          yield* failure(
            'exhausted allocator rejects repeated mint attempts',
            storage.mintId(),
            'invalid',
          )
        yield* same(
          'exhaustion preserves max-ID entry and sequence',
          yield* storage.entry(entryId(maximum)),
          Option.some({ entry: entry(maximum), commitSeq: seq }),
        )
        yield* same(
          'exhaustion does not append or wrap identities',
          (yield* Stream.runCollect(storage.scanEntries({ conversationId: root }))).map(
            (e) => e.id,
          ),
          [maximum],
        )
        yield* same(
          'exhaustion does not advance commit sequence',
          yield* storage.commit([]),
          seq + 1,
        )
      })
      yield* verify
      if (options.capabilities?.reopen === true) {
        storage = yield* reopen
        for (let attempt = 0; attempt < 2; attempt++)
          yield* failure('reopened allocator remains exhausted', storage.mintId(), 'invalid')
        yield* same(
          'reopened exhaustion preserves original entry',
          yield* storage.entry(entryId(maximum)),
          Option.some({ entry: entry(maximum), commitSeq: seq }),
        )
        yield* same(
          'reopened exhaustion preserves commit sequence',
          yield* storage.commit([]),
          seq + 2,
        )
      }
    }),
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

  // These new cases compare mutable adapter values only through fresh structural copies.
  // Expected snapshots are detached and recursively frozen before adapter exposure.
  const immutableExpected = <A>(value: A): A => {
    const copy = structuredClone(value)
    const freeze = (nested: unknown): void => {
      if (typeof nested !== 'object' || nested === null) return
      for (const child of Object.values(nested)) freeze(child)
      Object.freeze(nested)
    }
    freeze(copy)
    return copy
  }
  const freshSame =
    (same: ReturnType<typeof assertions>['same']) =>
    (assertion: string, actual: unknown, expected: unknown) =>
      same(assertion, structuredClone(actual), structuredClone(expected))

  structural('storage: prototype-sensitive JSON keys', (initial, { same: compare, some }, reopen) =>
    Effect.gen(function* () {
      let storage = initial
      const same = freshSame(compare)
      const data = {
        ['__proto__']: { polluted: false },
        constructor: { label: 'stored' },
        toString: 'value',
      }
      const expectedData = immutableExpected(data)
      yield* storage.commit([
        { _tag: 'conversation', value: { id: root } },
        { _tag: 'entry', value: { ...entry(2), data } },
      ])
      data.__proto__.polluted = true
      data.constructor.label = 'mutated'
      const first = (yield* some('prototype entry exists', yield* storage.entry(entryId(2)))).entry
        .data
      yield* compare(
        'JSON keys keep the ordinary object prototype',
        Object.getPrototypeOf(first),
        Object.prototype,
      )
      yield* same(
        'prototype key remains an own property',
        typeof first === 'object' && first !== null && Object.hasOwn(first, '__proto__'),
        true,
      )
      yield* same('prototype-sensitive input is detached', first, expectedData)
      if (typeof first === 'object' && first !== null) {
        Reflect.set(first, '__proto__', { polluted: true })
        Reflect.set(first, 'constructor', { label: 'read mutation' })
      }
      yield* same(
        'prototype-sensitive reads are detached',
        (yield* some('prototype entry still exists', yield* storage.entry(entryId(2)))).entry.data,
        expectedData,
      )
      yield* same(
        'JSON data does not pollute Object.prototype',
        Reflect.get(Object.prototype, 'polluted'),
        undefined,
      )
      if (options.capabilities?.reopen === true) {
        storage = yield* reopen
        yield* same(
          'reopen retains prototype-sensitive own keys',
          (yield* some('reopened prototype entry exists', yield* storage.entry(entryId(2)))).entry
            .data,
          expectedData,
        )
        const data = (yield* some(
          'reopened prototype entry exists',
          yield* storage.entry(entryId(2)),
        )).entry.data
        yield* compare(
          'reopen keeps ordinary JSON prototype',
          Object.getPrototypeOf(data),
          Object.prototype,
        )
        yield* same(
          'reopen keeps prototype key own',
          typeof data === 'object' && data !== null && Object.hasOwn(data, '__proto__'),
          true,
        )
      }
    }),
  )

  structural(
    'storage: lossless indexed string identities',
    (initial, { same: compare, some }, reopen) =>
      Effect.gen(function* () {
        let storage = initial
        const same = freshSame(compare)
        const strings = immutableExpected(['\ud800', '\ud801'])
        const expectedSubmissions = immutableExpected(
          strings.map((text, index) => ({
            ...submission(3 + index * 4),
            requestId: Identity.RequestId.make(text),
          })),
        )
        yield* storage.commit([{ _tag: 'conversation', value: { id: root } }])
        for (const [index, text] of strings.entries()) {
          const offset = index * 4
          yield* storage.commit([
            { _tag: 'task', value: { ...task(2 + offset), kind: text } },
            {
              _tag: 'submission',
              value: { ...submission(3 + offset), requestId: Identity.RequestId.make(text) },
            },
            {
              _tag: 'document.create',
              record: { ...document(4 + offset), kind: text },
              content: base(index),
            },
            {
              _tag: 'document.create',
              record: { ...document(5 + offset), kind: 'testing.family', key: text },
              content: base(index),
            },
          ])
        }
        for (const [index, text] of strings.entries()) {
          const offset = index * 4
          yield* same(
            'task kind lookup keeps distinct surrogate identities',
            (yield* Stream.runCollect(storage.scanTasks({ kind: text }))).map((t) => t.id),
            [2 + offset],
          )
          yield* same(
            'task payload keeps exact surrogate',
            (yield* some('surrogate task exists', yield* storage.task(taskId(2 + offset)))).kind,
            text,
          )
          yield* same(
            'request lookup keeps distinct surrogate identities',
            yield* storage.submissionByRequest(root, Identity.RequestId.make(text)),
            Option.some(expectedSubmissions[index]),
          )
          yield* same(
            'document kind lookup keeps distinct surrogate identities',
            (yield* some(
              'surrogate kind exists',
              yield* storage.findDocument({ kind: text, scope: document(4).scope }),
            )).id,
            4 + offset,
          )
          yield* same(
            'family key lookup keeps distinct surrogate identities',
            (yield* some(
              'surrogate family exists',
              yield* storage.findDocument({
                kind: 'testing.family',
                scope: document(5).scope,
                key: text,
              }),
            )).id,
            5 + offset,
          )
          yield* same(
            'document kind scan keeps distinct surrogate identities',
            (yield* Stream.runCollect(
              storage.scanDocuments({ scope: document(4).scope, kind: text }),
            )).map((d) => d.id),
            [4 + offset],
          )
        }
        if (options.capabilities?.reopen === true) {
          storage = yield* reopen
          for (const [index, text] of strings.entries()) {
            const offset = index * 4
            yield* same(
              'reopen keeps surrogate task index',
              (yield* Stream.runCollect(storage.scanTasks({ kind: text }))).map((t) => t.id),
              [2 + offset],
            )
            yield* same(
              'reopen keeps exact surrogate task payload',
              (yield* some(
                'reopened surrogate task exists',
                yield* storage.task(taskId(2 + offset)),
              )).kind,
              text,
            )
            yield* same(
              'reopen keeps surrogate request index',
              yield* storage.submissionByRequest(root, Identity.RequestId.make(text)),
              Option.some(expectedSubmissions[index]),
            )
            yield* same(
              'reopen keeps surrogate document index',
              (yield* some(
                'reopened surrogate kind exists',
                yield* storage.findDocument({ kind: text, scope: document(4).scope }),
              )).id,
              4 + offset,
            )
            yield* same(
              'reopen keeps surrogate family index',
              (yield* some(
                'reopened surrogate family exists',
                yield* storage.findDocument({
                  kind: 'testing.family',
                  scope: document(5).scope,
                  key: text,
                }),
              )).id,
              5 + offset,
            )
            yield* same(
              'reopen keeps surrogate document scan',
              (yield* Stream.runCollect(
                storage.scanDocuments({ scope: document(4).scope, kind: text }),
              )).map((d) => d.id),
              [4 + offset],
            )
          }
        }
      }),
  )

  structural('storage: out-of-ID-order entry streams', (initial, { same: compare, some }, reopen) =>
    Effect.gen(function* () {
      let storage = initial
      const same = freshSame(compare)
      // Multiplication by 37 permutes 130 IDs; every scan crosses a 64-row native page.
      const ids = Array.from({ length: 130 }, (_, index) => 2 + ((index * 37) % 130))
      const ascending = immutableExpected(Array.from({ length: 130 }, (_, index) => index + 2))
      yield* storage.commit([
        { _tag: 'conversation', value: { id: root } },
        ...ids.map((id): Record.StorageWrite => ({
          _tag: 'entry',
          value: { ...entry(id), ...(id === 70 ? { head: entryId(10) } : {}) },
        })),
      ])
      yield* same(
        'shuffled entries stream completely in ascending ID order',
        (yield* Stream.runCollect(
          storage.scanEntries({ conversationId: root, order: 'ascending' }),
        )).map((e) => e.id),
        ascending,
      )
      yield* same(
        'shuffled entries stream completely in descending ID order',
        (yield* Stream.runCollect(storage.scanEntries({ conversationId: root }))).map((e) => e.id),
        [...ascending].reverse(),
      )
      yield* same(
        'entry bounds are inclusive across pages',
        (yield* Stream.runCollect(
          storage.scanEntries({
            conversationId: root,
            minEntryId: entryId(20),
            maxEntryId: entryId(100),
            order: 'ascending',
          }),
        )).map((e) => e.id),
        Array.from({ length: 81 }, (_, index) => index + 20),
      )
      yield* same(
        'head lookup uses entry ID rather than commit order',
        (yield* some('shuffled head exists', yield* storage.findLatestHeadMarker(root))).id,
        70,
      )
      let appended = false
      const visible = yield* Stream.runCollect(
        storage.scanEntries({ conversationId: root, order: 'ascending' }).pipe(
          Stream.tap(() => {
            if (appended) return Effect.void
            appended = true
            return storage.commit([{ _tag: 'entry', value: entry(132) }]).pipe(Effect.asVoid)
          }),
        ),
      )
      yield* same(
        'entry stream retains its initial upper cutoff',
        visible.map((e) => e.id),
        ascending,
      )
      yield* same(
        'new scan sees the appended entry',
        (yield* Stream.runCollect(storage.scanEntries({ conversationId: root }))).map((e) => e.id),
        [132, ...ascending.toReversed()],
      )
      if (options.capabilities?.reopen === true) {
        storage = yield* reopen
        yield* same(
          'reopen keeps complete ascending shuffled stream',
          (yield* Stream.runCollect(
            storage.scanEntries({ conversationId: root, order: 'ascending' }),
          )).map((e) => e.id),
          [...ascending, 132],
        )
        yield* same(
          'reopen keeps complete descending shuffled stream',
          (yield* Stream.runCollect(storage.scanEntries({ conversationId: root }))).map(
            (e) => e.id,
          ),
          [132, ...ascending.toReversed()],
        )
        yield* same(
          'reopen keeps inclusive stream bounds',
          (yield* Stream.runCollect(
            storage.scanEntries({
              conversationId: root,
              minEntryId: entryId(20),
              maxEntryId: entryId(100),
              order: 'ascending',
            }),
          )).map((e) => e.id),
          Array.from({ length: 81 }, (_, i) => 20 + i),
        )
        yield* same(
          'reopen keeps shuffled head',
          (yield* some('reopened head exists', yield* storage.findLatestHeadMarker(root))).id,
          70,
        )
      }
    }),
  )

  structural('storage: multigeneration fork cutoffs', (initial, { same: compare, some }, reopen) =>
    Effect.gen(function* () {
      let storage = initial
      const same = freshSame(compare)
      const child = Record.ConversationId.make(5)
      const grandchild = Record.ConversationId.make(9)
      const inheritedFork = Record.ConversationId.make(14)
      const first = yield* storage.commit([
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
        { _tag: 'entry', value: entry(6, child) },
        { _tag: 'entry', value: entry(7, child) },
      ])
      yield* storage.commit([{ _tag: 'entry', value: entry(8) }])
      const leaf = yield* storage.commit([
        {
          _tag: 'conversation',
          value: { id: grandchild, parent: { conversationId: child, at: entryId(6) } },
        },
        { _tag: 'entry', value: { ...entry(10, grandchild), head: entryId(10) } },
        { _tag: 'entry', value: entry(11, grandchild) },
      ])
      yield* storage.commit([
        { _tag: 'entry', value: entry(12, child) },
        { _tag: 'entry', value: entry(13) },
      ])
      yield* same(
        'every ancestor cutoff excludes siblings and later parent writes',
        (yield* Stream.runCollect(storage.scanEntries({ conversationId: grandchild }))).map(
          (e) => e.id,
        ),
        [11, 10, 6, 3, 2],
      )
      yield* same(
        'multigeneration ascending history retains ancestor order',
        (yield* Stream.runCollect(
          storage.scanEntries({ conversationId: grandchild, order: 'ascending' }),
        )).map((e) => e.id),
        [2, 3, 6, 10, 11],
      )
      yield* same(
        'multigeneration range applies at each ancestor',
        (yield* Stream.runCollect(
          storage.scanEntries({
            conversationId: grandchild,
            minEntryId: entryId(3),
            maxEntryId: entryId(6),
            order: 'ascending',
          }),
        )).map((e) => e.id),
        [3, 6],
      )
      for (const id of [4, 7, 8, 12, 13])
        yield* same(
          'scoped lookup excludes capped ancestors',
          yield* storage.entry(entryId(id), { conversationId: grandchild }),
          Option.none(),
        )
      yield* same(
        'root entry preserves original commit sequence',
        (yield* some(
          'visible root entry exists',
          yield* storage.entry(entryId(3), { conversationId: grandchild }),
        )).commitSeq,
        first,
      )
      yield* same(
        'leaf entry preserves original commit sequence',
        (yield* some(
          'visible leaf entry exists',
          yield* storage.entry(entryId(11), { conversationId: grandchild }),
        )).commitSeq,
        leaf,
      )
      yield* same(
        'multigeneration latest head is local',
        (yield* some('leaf head exists', yield* storage.findLatestHeadMarker(grandchild))).head,
        10,
      )
      yield* same(
        'multigeneration historical head is inherited',
        (yield* some(
          'ancestor head exists',
          yield* storage.findLatestHeadMarker(grandchild, entryId(6)),
        )).head,
        2,
      )
      yield* same(
        'cutoff before first head has no marker',
        yield* storage.findLatestHeadMarker(grandchild, entryId(2)),
        Option.none(),
      )
      // A fourth generation forks at an inherited entry, tightening all earlier caps.
      yield* storage.commit([
        {
          _tag: 'conversation',
          value: { id: inheritedFork, parent: { conversationId: grandchild, at: entryId(3) } },
        },
        { _tag: 'entry', value: entry(15, inheritedFork) },
      ])
      yield* same(
        'fork at inherited entry tightens every ancestor cap',
        (yield* Stream.runCollect(storage.scanEntries({ conversationId: inheritedFork }))).map(
          (e) => e.id,
        ),
        [15, 3, 2],
      )
      yield* same(
        'inherited fork hides immediate-parent entries',
        yield* storage.entry(entryId(10), { conversationId: inheritedFork }),
        Option.none(),
      )
      if (options.capabilities?.reopen === true) {
        storage = yield* reopen
        yield* same(
          'reopen keeps all ancestor cutoffs',
          (yield* Stream.runCollect(storage.scanEntries({ conversationId: grandchild }))).map(
            (e) => e.id,
          ),
          [11, 10, 6, 3, 2],
        )
        yield* same(
          'reopen keeps tightened inherited fork',
          (yield* Stream.runCollect(storage.scanEntries({ conversationId: inheritedFork }))).map(
            (e) => e.id,
          ),
          [15, 3, 2],
        )
        for (const id of [4, 7, 8, 12, 13])
          yield* same(
            'reopen excludes capped ancestor lookup',
            yield* storage.entry(entryId(id), { conversationId: grandchild }),
            Option.none(),
          )
        yield* same(
          'reopen keeps root original sequence',
          (yield* some(
            'reopened root entry exists',
            yield* storage.entry(entryId(3), { conversationId: grandchild }),
          )).commitSeq,
          first,
        )
        yield* same(
          'reopen keeps leaf original sequence',
          (yield* some(
            'reopened leaf entry exists',
            yield* storage.entry(entryId(11), { conversationId: grandchild }),
          )).commitSeq,
          leaf,
        )
        yield* same(
          'reopen keeps inherited head',
          (yield* some(
            'reopened inherited head exists',
            yield* storage.findLatestHeadMarker(grandchild, entryId(6)),
          )).head,
          2,
        )
      }
    }),
  )

  structural(
    'storage: waiting and completing task records',
    (initial, { same: compare, some }, reopen) =>
      Effect.gen(function* () {
        let storage = initial
        const same = freshSame(compare)
        const owner = task(2)
        const waiting: Record.Task = {
          ...task(3),
          owner: owner.id,
          state: {
            status: 'waiting',
            checkpoint: { phase: 'next' },
            on: [owner.id],
            policy: 'allSettled',
          },
          memos: { ['__proto__']: { kept: true }, saved: 1 },
        }
        const completing: Record.Task = {
          ...task(4),
          owner: owner.id,
          state: {
            status: 'completing',
            outcome: { status: 'failed', error: { message: 'held' } },
          },
        }
        const expectedOwner = immutableExpected(owner)
        const expectedWaiting = immutableExpected(waiting)
        const expectedCompleting = immutableExpected(completing)
        yield* storage.commit([
          { _tag: 'conversation', value: { id: root } },
          ...[owner, waiting, completing].map((value): Record.StorageWrite => ({
            _tag: 'task',
            value,
          })),
        ])
        yield* same(
          'waiting record retains owner checkpoint wait policy and memos',
          yield* storage.task(expectedWaiting.id),
          Option.some(expectedWaiting),
        )
        yield* same(
          'completing record retains outcome without checkpoint or memos',
          yield* storage.task(expectedCompleting.id),
          Option.some(expectedCompleting),
        )
        yield* same(
          'waiting status scan returns exact record',
          yield* Stream.runCollect(storage.scanTasks({ status: 'waiting' })),
          [expectedWaiting],
        )
        yield* same(
          'completing status scan returns exact record',
          yield* Stream.runCollect(storage.scanTasks({ status: 'completing' })),
          [expectedCompleting],
        )
        yield* same(
          'pending status scan excludes waiting and completing',
          yield* Stream.runCollect(storage.scanTasks({ status: 'pending' })),
          [expectedOwner],
        )
        const read = yield* some('waiting record exists', yield* storage.task(expectedWaiting.id))
        Reflect.set(read.state, 'policy', 'failFast')
        if (read.memos !== undefined) {
          Reflect.set(read.memos, 'saved', 99)
          const nested = read.memos.__proto__
          if (typeof nested === 'object' && nested !== null) Reflect.set(nested, 'kept', false)
        }
        if (read.state.status === 'waiting') {
          const checkpoint = read.state.checkpoint
          if (typeof checkpoint === 'object' && checkpoint !== null)
            Reflect.set(checkpoint, 'phase', 'read mutation')
          if (read.state.on !== undefined) Reflect.set(read.state.on, '0', taskId(4))
        }
        yield* same(
          'waiting reads detach wait state and memo values',
          yield* storage.task(expectedWaiting.id),
          Option.some(expectedWaiting),
        )
        const detachedWaiting = yield* some(
          'waiting record still exists',
          yield* storage.task(expectedWaiting.id),
        )
        yield* same(
          'waiting reads detach wait state and memo values',
          detachedWaiting.state.policy,
          'allSettled',
        )
        yield* same(
          'waiting reads detach wait state and memo values',
          detachedWaiting.memos?.saved,
          1,
        )
        const completingRead = yield* some(
          'completing record exists',
          yield* storage.task(expectedCompleting.id),
        )
        const outcome = completingRead.state.outcome
        if (typeof outcome === 'object' && outcome !== null && 'error' in outcome) {
          const error = outcome.error
          if (typeof error === 'object' && error !== null)
            Reflect.set(error, 'message', 'read mutation')
        }
        yield* same(
          'completing reads detach nested outcome values',
          yield* storage.task(expectedCompleting.id),
          Option.some(expectedCompleting),
        )
        if (options.capabilities?.reopen === true) {
          storage = yield* reopen
          yield* same(
            'reopen keeps waiting checkpoint owner policy and memos',
            yield* storage.task(expectedWaiting.id),
            Option.some(expectedWaiting),
          )
          yield* same(
            'reopen keeps completing outcome',
            yield* storage.task(expectedCompleting.id),
            Option.some(expectedCompleting),
          )
          yield* same(
            'reopen keeps waiting status index',
            yield* Stream.runCollect(storage.scanTasks({ status: 'waiting' })),
            [expectedWaiting],
          )
          yield* same(
            'reopen keeps completing status index',
            yield* Stream.runCollect(storage.scanTasks({ status: 'completing' })),
            [expectedCompleting],
          )
        }
        const resumed: Record.Task = {
          ...task(3),
          owner: expectedOwner.id,
          state: { status: 'running', checkpoint: { phase: 'resumed' } },
        }
        const terminal: Record.Task = {
          ...expectedCompleting,
          state: { status: 'terminal', outcome: { status: 'failed', error: { message: 'held' } } },
        }
        const expectedResumed = immutableExpected(resumed)
        const expectedTerminal = immutableExpected(terminal)
        yield* storage.commit([
          { _tag: 'task', value: resumed },
          { _tag: 'task', value: terminal },
        ])
        yield* same(
          'task replacement removes old wait fields and memos',
          yield* storage.task(expectedResumed.id),
          Option.some(expectedResumed),
        )
        yield* same(
          'task replacement removes stale status index',
          yield* Stream.runCollect(storage.scanTasks({ status: 'completing' })),
          [],
        )
        yield* same(
          'task replacement removes waiting index',
          yield* Stream.runCollect(storage.scanTasks({ status: 'waiting' })),
          [],
        )
        yield* same(
          'terminal status scan returns exact outcome',
          yield* Stream.runCollect(storage.scanTasks({ status: 'terminal' })),
          [expectedTerminal],
        )
        if (options.capabilities?.reopen === true) {
          storage = yield* reopen
          yield* same(
            'reopen keeps resumed task replacement',
            yield* storage.task(expectedResumed.id),
            Option.some(expectedResumed),
          )
          yield* same(
            'reopen removes stale waiting status',
            yield* Stream.runCollect(storage.scanTasks({ status: 'waiting' })),
            [],
          )
          yield* same(
            'reopen removes stale completing status',
            yield* Stream.runCollect(storage.scanTasks({ status: 'completing' })),
            [],
          )
          yield* same(
            'reopen keeps terminal outcome',
            yield* Stream.runCollect(storage.scanTasks({ status: 'terminal' })),
            [expectedTerminal],
          )
        }
      }),
  )

  structural(
    'storage: passive write submission records',
    (initial, { same: compare, some }, reopen) =>
      Effect.gen(function* () {
        let storage = initial
        const same = freshSame(compare)
        const queued: Record.WriteQueued = {
          _tag: 'WriteQueued',
          id: Record.SubmissionId.make(2),
          conversationId: root,
          requestId: Identity.RequestId.make('testing.passive.done'),
          type: 'write',
          status: 'queued',
        }
        const failed: Record.WriteQueued = {
          ...queued,
          id: Record.SubmissionId.make(3),
          requestId: Identity.RequestId.make('testing.passive.failed'),
        }
        const expectedQueued = immutableExpected([queued, failed])
        yield* storage.commit([
          { _tag: 'conversation', value: { id: root } },
          { _tag: 'submission', value: queued },
          { _tag: 'submission', value: failed },
        ])
        yield* same(
          'queued passive writes retain exact records',
          yield* Stream.runCollect(storage.scanSubmissions({ status: 'queued' })),
          expectedQueued,
        )
        if (options.capabilities?.reopen === true) {
          storage = yield* reopen
          yield* same(
            'reopen keeps queued passive writes',
            yield* Stream.runCollect(storage.scanSubmissions({ status: 'queued' })),
            expectedQueued,
          )
        }
        const done: Record.WriteDone = {
          ...expectedQueued[0]!,
          _tag: 'WriteDone',
          status: 'done',
          entry: entryId(4),
        }
        const unanswered: Record.WriteUnanswered = {
          ...expectedQueued[1]!,
          _tag: 'WriteUnanswered',
          status: 'unanswered',
          reason: 'closed',
          detail: { retryable: false },
        }
        const expectedDone = immutableExpected(done)
        const expectedUnanswered = immutableExpected(unanswered)
        yield* storage.commit([
          { _tag: 'entry', value: entry(4) },
          { _tag: 'submission', value: done },
          { _tag: 'submission', value: unanswered },
        ])
        yield* same(
          'passive done has entry without input answer',
          yield* storage.submission(expectedDone.id),
          Option.some(expectedDone),
        )
        yield* same(
          'passive unanswered has detail without placement',
          yield* storage.submission(expectedUnanswered.id),
          Option.some(expectedUnanswered),
        )
        yield* same(
          'passive request index follows done replacement',
          yield* storage.submissionByRequest(root, expectedDone.requestId!),
          Option.some(expectedDone),
        )
        yield* same(
          'passive request index follows unanswered replacement',
          yield* storage.submissionByRequest(root, expectedUnanswered.requestId!),
          Option.some(expectedUnanswered),
        )
        yield* same(
          'passive replacement removes queued index',
          yield* Stream.runCollect(storage.scanSubmissions({ status: 'queued' })),
          [],
        )
        yield* same(
          'passive done status scan returns exact record',
          yield* Stream.runCollect(storage.scanSubmissions({ status: 'done' })),
          [expectedDone],
        )
        yield* same(
          'passive unanswered status scan returns exact record',
          yield* Stream.runCollect(storage.scanSubmissions({ status: 'unanswered' })),
          [expectedUnanswered],
        )
        const read = yield* some(
          'passive unanswered exists',
          yield* storage.submission(expectedUnanswered.id),
        )
        if (read.status === 'unanswered' && typeof read.detail === 'object' && read.detail !== null)
          Reflect.set(read.detail, 'retryable', true)
        yield* same(
          'passive reads detach nested detail values',
          yield* storage.submission(expectedUnanswered.id),
          Option.some(expectedUnanswered),
        )
        if (options.capabilities?.reopen === true) {
          storage = yield* reopen
          yield* same(
            'reopen keeps passive done',
            yield* storage.submission(expectedDone.id),
            Option.some(expectedDone),
          )
          yield* same(
            'reopen keeps passive unanswered detail',
            yield* storage.submission(expectedUnanswered.id),
            Option.some(expectedUnanswered),
          )
          yield* same(
            'reopen keeps done request lookup',
            yield* storage.submissionByRequest(root, expectedDone.requestId!),
            Option.some(expectedDone),
          )
          yield* same(
            'reopen keeps unanswered request lookup',
            yield* storage.submissionByRequest(root, expectedUnanswered.requestId!),
            Option.some(expectedUnanswered),
          )
          yield* same(
            'reopen removes passive queued status',
            yield* Stream.runCollect(storage.scanSubmissions({ status: 'queued' })),
            [],
          )
          yield* same(
            'reopen keeps passive done status',
            yield* Stream.runCollect(storage.scanSubmissions({ status: 'done' })),
            [expectedDone],
          )
          yield* same(
            'reopen keeps passive unanswered status',
            yield* Stream.runCollect(storage.scanSubmissions({ status: 'unanswered' })),
            [expectedUnanswered],
          )
        }
      }),
  )

  if (options.capabilities?.history === true)
    structural(
      'storage: long document replacement tails',
      (firstStorage, { same: compare, some }, reopen) =>
        Effect.gen(function* () {
          let storage = firstStorage
          const same = freshSame(compare)
          const initial = {
            revision: 0,
            rows: Array.from({ length: 512 }, (_, value) => ({ value, stable: `row-${value}` })),
          }
          const expectedInitial = immutableExpected(initial)
          const created = yield* storage.commit([
            { _tag: 'conversation', value: { id: root } },
            {
              _tag: 'document.create',
              record: document(2, 'rewindable'),
              content: { _tag: 'base', version: 1, value: initial },
            },
          ])
          const before = structuredClone(expectedInitial)
          let beforeAt = created
          for (let revision = 1; revision <= 24; revision++) {
            const index = (revision * 17) % before.rows.length
            before.rows[index]!.value = -revision
            before.revision = revision
            beforeAt = yield* storage.commit([
              {
                _tag: 'document.change',
                id: documentId(2),
                content: {
                  _tag: 'delta',
                  version: 1,
                  ops: [
                    ['set', ['rows', index, 'value'], -revision],
                    ['set', ['revision'], revision],
                  ],
                },
              },
            ])
          }
          const replacement = {
            revision: 100,
            rows: Array.from({ length: 512 }, (_, value) => ({
              value: 10000 + value,
              stable: `new-${value}`,
            })),
          }
          const expectedBefore = immutableExpected(before)
          const atReplacement = immutableExpected(replacement)
          const replaced = yield* storage.commit([
            {
              _tag: 'document.change',
              id: documentId(2),
              content: { _tag: 'delta', version: 1, ops: [['replace', replacement]] },
            },
          ])
          replacement.rows[0]!.value = -999
          const current = structuredClone(atReplacement)
          for (let revision = 101; revision <= 124; revision++) {
            const index = (revision * 19) % current.rows.length
            current.rows[index]!.value = -revision
            current.revision = revision
            yield* storage.commit([
              {
                _tag: 'document.change',
                id: documentId(2),
                content: {
                  _tag: 'delta',
                  version: 1,
                  ops: [
                    ['set', ['rows', index, 'value'], -revision],
                    ['set', ['revision'], revision],
                  ],
                },
              },
            ])
          }
          for (const [at, expected] of [
            [created, expectedInitial],
            [beforeAt, expectedBefore],
            [replaced, atReplacement],
          ] as const) {
            const historical = yield* some(
              'long historical document exists',
              yield* storage.document(documentId(2), at),
            )
            yield* same(
              'long tail preserves exact historical cuts around replacement',
              historical.value,
              expected,
            )
            const rows = historical.value.rows
            if (Array.isArray(rows)) {
              const row: unknown = rows[0]
              if (typeof row === 'object' && row !== null)
                Reflect.set(row, 'stable', 'read mutation')
            }
            yield* same(
              'long historical returned values remain detached',
              (yield* some(
                'long historical document still exists',
                yield* storage.document(documentId(2), at),
              )).value,
              expected,
            )
          }
          const expectedCurrent = immutableExpected(current)
          const stored = yield* some(
            'long current document exists',
            yield* storage.document(documentId(2)),
          )
          yield* same(
            'long replacement tail materializes current value',
            stored.value,
            expectedCurrent,
          )
          yield* same('root replacement delta retains schema version', stored.version, 1)
          yield* same(
            'root replacement remains a delta in retained history',
            stored.deltasSinceBase,
            49,
          )
          Reflect.set(stored.value, 'revision', -1)
          const reread = yield* some(
            'long current document still exists',
            yield* storage.document(documentId(2)),
          )
          yield* same(
            'long tail returned values remain detached',
            reread.value.revision,
            expectedCurrent.revision,
          )
          yield* same(
            'long tail returned values remain detached',
            reread.value,
            structuredClone(expectedCurrent),
          )
          if (options.capabilities?.reopen === true) {
            storage = yield* reopen
            for (const [at, expected] of [
              [created, expectedInitial],
              [beforeAt, expectedBefore],
              [replaced, atReplacement],
            ] as const) {
              const saved = yield* some(
                'reopened historical document exists',
                yield* storage.document(documentId(2), at),
              )
              yield* same(
                'reopen keeps exact long historical replacement cuts',
                saved.value,
                expected,
              )
              yield* same('reopen keeps historical schema', saved.version, 1)
            }
            const saved = yield* some(
              'reopened current document exists',
              yield* storage.document(documentId(2)),
            )
            yield* same('reopen keeps exact long current tail', saved.value, expectedCurrent)
            yield* same(
              'reopen keeps long-tail schema and delta count',
              [saved.version, saved.deltasSinceBase],
              [1, 49],
            )
          }
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

export { fileSystemConformance, ServiceConformanceError } from './testing/FileSystem.js'
export type {
  ConformanceCase,
  ScopedFixture,
  FileSystemError,
  FileSystemFixture,
  FileSystemCapabilities,
  FileSystemOptions,
} from './testing/FileSystem.js'
export { processConformance } from './testing/Process.js'
export type { ProcessError, ProcessFixture, ProcessOptions } from './testing/Process.js'
export { imagesConformance } from './testing/Images.js'
export type {
  ImageSample,
  OrientationSample,
  ImagesFixture,
  ImagesOptions,
} from './testing/Images.js'
export { toolkitIsolationConformance } from './testing/ToolkitIsolation.js'
export type {
  ToolkitIsolationFixture,
  ToolkitIsolationOptions,
} from './testing/ToolkitIsolation.js'
