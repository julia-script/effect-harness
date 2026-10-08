/**
 * Scoped transactions, document drafts and committed read services.
 */
import * as MutableHashMap from 'effect/MutableHashMap'
import * as Function from 'effect/Function'
import * as Struct from 'effect/Struct'
import * as Data from 'effect/Data'
import * as Predicate from 'effect/Predicate'
import * as Request from 'effect/Request'
import * as Arr from 'effect/Array'
import * as RequestResolver from 'effect/RequestResolver'
import type * as Identity from './Identity.ts'
import * as Context from 'effect/Context'
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as FiberHandle from 'effect/FiberHandle'
import * as Deferred from 'effect/Deferred'
import * as HashMap from 'effect/HashMap'
import * as HashSet from 'effect/HashSet'
import * as Ref from 'effect/Ref'
import * as SynchronizedRef from 'effect/SynchronizedRef'
import * as Scope from 'effect/Scope'
import * as Exit from 'effect/Exit'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Result from 'effect/Result'
import * as Semaphore from 'effect/Semaphore'
import type * as Stream from 'effect/Stream'
import * as Document from './Document.ts'
import { address, typed } from './Document.ts'
import * as Option from 'effect/Option'
import * as Observation from './Observation.ts'
import * as Record from './Record.ts'
import {
  rejected,
  type StorageError,
  ClosedError,
  RevokedError,
  ReadAfterWriteError,
  NotFoundError,
  InvalidError,
} from './StorageError.ts'
import { makeCandidate, Store } from './Store.ts'
import {
  applyOps,
  detached,
  detachedEffect,
  documentsInScope,
  findDocument,
  materialize,
  page,
  validate,
  visibleEntries,
} from './storage/internal/state.ts'
/**
 * Transaction ownership variants.
 *
 * @category combinators
 */
export const Ownership = Data.taggedEnum<Session.Ownership>()
/**
 * Callback-local record access and mutable drafts for one atomic domain transaction.
 *
 * **Details**
 *
 * All writes publish together when the callback succeeds. Document drafts reflect their
 * staged mutations; table reads refer to the transaction snapshot.
 *
 * **Gotchas**
 *
 * Read all required tables before staging table writes. Later table reads fail with
 * ReadAfterWrite. The transaction and every draft are revoked when the callback ends; copy
 * data while the draft is active.
 *
 * @category models
 */
export interface Transaction {
  /**
   * Returns or creates reserved root conversation 1 within the transaction.
   */
  readonly ensureRoot: Effect.Effect<Record.Conversation, StorageError>
  /**
   * Allocates and validates an identity before it can be committed.
   */
  readonly mint: <S extends Schema.Constraint>(
    schema: S,
  ) => Effect.Effect<S['Type'], StorageError, S['DecodingServices']>
  /**
   * Returns the requested conversation, or None when it is absent.
   */
  readonly conversation: (
    id: Record.ConversationId,
  ) => Effect.Effect<Option.Option<Record.Conversation>, StorageError>
  /**
   * Returns the requested visible entry, or None when it is absent.
   */
  readonly entry: (id: Record.EntryId) => Effect.Effect<Option.Option<Record.Entry>, StorageError>
  /**
   * Returns the requested domain task, or None when it is absent.
   */
  readonly task: (id: Record.TaskId) => Effect.Effect<Option.Option<Record.Task>, StorageError>
  /**
   * Returns the requested submission, or None when it is absent.
   */
  readonly submission: (
    id: Record.SubmissionId,
  ) => Effect.Effect<Option.Option<Record.Submission>, StorageError>
  /**
   * Scans matching conversations using a positive page size and optional continuation cursor.
   */
  readonly scanConversations: (
    query: Session.ConversationQuery,
    limit: number,
    cursor?: Record.Cursor,
  ) => Effect.Effect<Record.Page<Record.Conversation>, StorageError>
  /**
   * Scans visible conversation history in descending entry order within the requested bounds.
   */
  readonly scanEntries: (
    query: Session.EntryQuery,
    limit: number,
    cursor?: Record.Cursor,
  ) => Effect.Effect<Record.Page<Record.Entry>, StorageError>
  /**
   * Scans domain tasks matching the supplied filters and continuation cursor.
   */
  readonly scanTasks: (
    query: Session.TaskQuery,
    limit: number,
    cursor?: Record.Cursor,
  ) => Effect.Effect<Record.Page<Record.Task>, StorageError>
  /**
   * Scans admitted submissions matching the supplied filters and continuation cursor.
   */
  readonly scanSubmissions: (
    query: Session.SubmissionQuery,
    limit: number,
    cursor?: Record.Cursor,
  ) => Effect.Effect<Record.Page<Record.Submission>, StorageError>
  /**
   * Finds the submission admitted under a conversation and stable request identity.
   */
  readonly submissionByRequest: (
    conversationId: Record.ConversationId,
    requestId: Identity.RequestId,
  ) => Effect.Effect<Option.Option<Record.Submission>, StorageError>
  /**
   * Finds the newest reset or compaction marker at or before the optional entry cutoff.
   */
  readonly latestHeadMarker: (
    conversationId: Record.ConversationId,
    atOrBefore?: Record.EntryId,
  ) => Effect.Effect<Option.Option<Record.Entry>, StorageError>
  /**
   * Creates a conversation with explicit ownerless or task ownership and captured
   * initialization hooks.
   */
  readonly createConversation: (options: {
    readonly ownership: Session.Ownership
  }) => Effect.Effect<Record.Conversation, StorageError>
  /**
   * Creates a conversation inheriting visible parent history through a valid parent entry
   * cutoff.
   */
  readonly forkConversation: (
    parent: Record.ConversationId,
    at: Record.EntryId,
    options: { readonly ownership: Session.Ownership },
  ) => Effect.Effect<Record.Conversation, StorageError>
  /**
   * Stages an entry and assigns its conversation-local durable metadata.
   */
  readonly appendEntry: (
    conversationId: Record.ConversationId,
    draft: Record.Entry.Draft,
  ) => Effect.Effect<Record.Entry, StorageError>
  /**
   * Stages a domain task and returns its allocated identity.
   */
  readonly createTask: (
    value: Omit<Record.Task, 'id'>,
  ) => Effect.Effect<Record.TaskId, StorageError>
  /**
   * Stages an admitted input or passive write with an allocated identity.
   */
  readonly createSubmission: (
    value: Record.Submission.Create,
  ) => Effect.Effect<Record.Submission, StorageError>
  /**
   * Associates a queued submission with the entry where it was applied.
   */
  readonly placeSubmission: (
    id: Record.SubmissionId,
    entry: Record.EntryId,
  ) => Effect.Effect<void, StorageError>
  /**
   * Stages a terminal answer or unanswered reason for a submission.
   */
  readonly settleSubmission: (
    id: Record.SubmissionId,
    value:
      | { readonly status: 'done'; readonly answer: Record.EntryId }
      | {
          readonly status: 'unanswered'
          readonly reason: string
          readonly detail?: Schema.Json | undefined
        },
  ) => Effect.Effect<void, StorageError>
  /**
   * Stages a domain mutation in the current atomic commit.
   */
  readonly write: (value: Record.Write) => Effect.Effect<void, StorageError>
  /**
   * Acquires a mutable document draft, creating its initial value when needed. The draft is
   * revoked when the callback ends.
   */
  readonly doc: <T extends object>(
    token: Document.Document<T>,
    target?: Document.Document.Target,
  ) => Effect.Effect<Document.Document.Draft<T>, StorageError>
  /**
   * Retires the addressed document incarnation and ends watches bound to that incarnation.
   */
  readonly retire: <T extends object>(
    token: Document.Document<T>,
    target?: Document.Document.Target,
  ) => Effect.Effect<void, StorageError>
}
/**
 * Service for atomic initialization and recovery of conversation documents.
 *
 * **Details**
 *
 * run participates in each newly created conversation transaction. Optional recover
 * initializes existing conversations without repeating creation callbacks.
 *
 * **Gotchas**
 *
 * The callback uses the caller’s active Transaction; it must obey the same read-before-write
 * and draft-lifetime rules.
 *
 * @category services
 */
export class CreationHook extends Context.Service<
  CreationHook,
  {
    readonly run: (
      tx: Transaction,
      conversation: Record.Conversation,
    ) => Effect.Effect<void, StorageError>
    readonly recover?:
      | ((tx: Transaction, conversation: Record.Conversation) => Effect.Effect<void, StorageError>)
      | undefined
  }
>()('effect-harness/durable/Session/CreationHook') {}
/**
 * Scoped service for atomic domain mutations and committed reads.
 *
 * **When to use**
 *
 * Use when you need conversations, documents and receipts to share a storage lifetime.
 *
 * **Details**
 *
 * Build the service from a Store. Construction captures an optional CreationHook for new
 * conversations and recovery initialization. Scoped release seals admission and
 * observations, joins registered cleanup, then releases storage.
 *
 * **Gotchas**
 *
 * Session closure pauses recoverable execution; it does not persist a task-abort outcome.
 * Keep the native engine alive when reopening a Session.
 *
 * @category services
 */
export class Session extends Context.Service<Session, Session.Service>()(
  'effect-harness/durable/Session',
) {}

const conversationPage = (
  state: Record.State,
  query: Session.ConversationQuery,
  limit: number,
  cursor?: Record.Cursor,
) =>
  page(
    Arr.filter(
      state.conversations,
      (item) =>
        (query.ownerConversationId === undefined ||
          item.owner?.conversationId === query.ownerConversationId) &&
        (query.ownerTaskId === undefined || item.owner?.taskId === query.ownerTaskId),
    ),
    limit,
    cursor,
  )
const taskPage = (
  state: Record.State,
  query: Session.TaskQuery,
  limit: number,
  cursor?: Record.Cursor,
) =>
  page(
    Arr.filter(
      state.tasks,
      (item) =>
        (query.conversationId === undefined || item.conversationId === query.conversationId) &&
        (query.kind === undefined || item.kind === query.kind) &&
        (query.status === undefined || item.state.status === query.status) &&
        (query.background === undefined || item.background === query.background) &&
        (query.abortRequested === undefined || item.abortRequested === query.abortRequested),
    ),
    limit,
    cursor,
  )
const submissionPage = (
  state: Record.State,
  query: Session.SubmissionQuery,
  limit: number,
  cursor?: Record.Cursor,
) =>
  page(
    Arr.filter(
      state.submissions,
      (item) =>
        (query.conversationId === undefined || item.conversationId === query.conversationId) &&
        (query.status === undefined || item.status === query.status),
    ),
    limit,
    cursor,
  )
const entryPage = Effect.fnUntraced(function* (
  state: Record.State,
  query: Session.EntryQuery,
  limit: number,
  cursor?: Record.Cursor,
) {
  if (
    !Number.isSafeInteger(limit) ||
    limit <= 0 ||
    (cursor !== undefined && !Number.isSafeInteger(cursor.after))
  )
    return yield* rejected('Invalid entry scan size or cursor')
  const entries = Arr.filter(
    yield* visibleEntries(state, query.conversationId, query.minEntryId, query.maxEntryId),
    (item) => cursor === undefined || item.id < cursor.after,
  )
  const items = entries.slice(0, limit)
  const last = items.at(-1)
  return Record.makePage({
    items,
    ...(entries.length > limit && last !== undefined ? { next: { after: last.id } } : {}),
  })
})
const detachedOptional = <A>(
  option: Option.Option<A>,
): Effect.Effect<Option.Option<A>, StorageError> =>
  Option.match(option, {
    onNone: () => Effect.succeedNone,
    onSome: (value) => detachedEffect(value).pipe(Effect.asSome),
  })

class DraftMutationError extends Schema.TaggedError<DraftMutationError>(
  '@effect-harness/durable/Session/DraftMutationError',
)('DraftMutationError', { message: Schema.String, cause: Schema.optionalKey(Schema.Defect()) }) {}
// effect-nit-allow P2-no-throw-in-effect-code, P1-throw-only-in-unsafe-orthrow:
// Native synchronous Proxy traps cannot return Effect/Result failures. Only this private
// sentinel is thrown for invalid draft operations and translated by transaction's
// targeted catchDefect. All unrelated defects retain their original Cause.
const draftValueUnsafe = (input: unknown): Schema.Json =>
  Result.match(
    Result.try({
      try: () => Schema.decodeUnknownSync(Schema.Json)(input),
      catch: (cause) => new DraftMutationError({ message: 'Document value must be JSON', cause }),
    }),
    {
      onSuccess: Function.identity,
      onFailure: (error) => {
        throw error
      },
    },
  )
const draftObjectUnsafe = (value: unknown): Schema.JsonObject =>
  Result.match(Schema.decodeUnknownResult(Schema.JsonObject)(value), {
    onSuccess: (value) => cloneDraftUnsafe(value),
    onFailure: (cause) => {
      throw new DraftMutationError({ message: 'Document draft must remain JSON', cause })
    },
  })
const cloneDraftUnsafe = <A>(value: A): A =>
  Result.match(detached(value), {
    onSuccess: Function.identity,
    onFailure: (error) => {
      throw new DraftMutationError({ message: error.message, cause: error.cause })
    },
  })
interface Acquired {
  readonly definition: {
    readonly version: number
    readonly history?: 'latest' | 'rewindable' | undefined
    readonly fork?: 'asOf' | 'current' | 'initial' | undefined
  }
  readonly prepare: Effect.Effect<Schema.JsonObject, StorageError>
  readonly checkpoint: Effect.Effect<boolean, StorageError>
  readonly address: Record.Address
  readonly id: Record.DocumentId
  readonly stored?: Document.Document.Snapshot | undefined
  readonly staged?: Record.DocumentCreate | undefined
  readonly value: object
  readonly ops: Array<Record.Op>
  retire: boolean
}

const draft = <T extends object>(value: T, active: () => boolean, ops: Array<Record.Op>): T => {
  const proxies = new WeakMap<object, object>()
  const wrap = (object: object, path: ReadonlyArray<string | number>): object => {
    const previous = proxies.get(object)
    if (previous !== undefined) return previous
    const checkUnsafe = () => {
      if (!active()) throw new DraftMutationError({ message: 'Document draft is revoked' })
    }
    const proxy = new Proxy(object, {
      get(target, key, receiver) {
        checkUnsafe()
        const item: unknown = Reflect.get(target, key, receiver)
        if (item !== null && typeof item === 'object') {
          if (!Object.hasOwn(target, key)) return undefined
          return wrap(item, [...path, Array.isArray(target) ? Number(key) : String(key)])
        }
        return item
      },
      set(target, key, item: unknown) {
        checkUnsafe()
        if (Array.isArray(target) && key === 'length') {
          if (
            !Predicate.isNumber(item) ||
            !Number.isSafeInteger(item) ||
            item < 0 ||
            item > 4294967295
          )
            throw new DraftMutationError({ message: 'Invalid array length' })
          Reflect.set(target, key, item)
          ops.push(['replace', draftObjectUnsafe(value)])
          return true
        }
        if (typeof key === 'symbol')
          throw new DraftMutationError({ message: 'Symbol document keys are not JSON' })
        const valid = draftValueUnsafe(item)
        const segment = Array.isArray(target) ? Number(key) : String(key)
        Object.defineProperty(target, key, {
          value: cloneDraftUnsafe(valid),
          enumerable: true,
          configurable: true,
          writable: true,
        })
        ops.push(['set', Arr.append(path, segment), cloneDraftUnsafe(valid)])
        return true
      },
      deleteProperty(target, key) {
        checkUnsafe()
        const segment = Array.isArray(target) ? Number(key) : String(key)
        Reflect.deleteProperty(target, key)
        ops.push(['delete', Arr.append(path, segment)])
        return true
      },
      defineProperty(target, key, descriptor) {
        checkUnsafe()
        if (
          typeof key === 'symbol' ||
          descriptor.get !== undefined ||
          descriptor.set !== undefined ||
          descriptor.enumerable === false ||
          descriptor.configurable === false ||
          descriptor.writable === false ||
          !Predicate.hasProperty(descriptor, 'value')
        )
          throw new DraftMutationError({
            message: 'Document descriptors must be writable enumerable JSON data',
          })
        const valid = draftValueUnsafe(descriptor.value)
        if (Array.isArray(target) && key === 'length') {
          if (
            !Predicate.isNumber(valid) ||
            !Number.isSafeInteger(valid) ||
            valid < 0 ||
            valid > 4294967295
          )
            throw new DraftMutationError({ message: 'Invalid array length' })
          Reflect.set(target, key, valid)
          ops.push(['replace', draftObjectUnsafe(value)])
          return true
        }
        Object.defineProperty(target, key, {
          value: cloneDraftUnsafe(valid),
          enumerable: true,
          configurable: true,
          writable: true,
        })
        ops.push([
          'set',
          Arr.append(path, Array.isArray(target) ? Number(key) : String(key)),
          cloneDraftUnsafe(valid),
        ])
        return true
      },
      setPrototypeOf() {
        checkUnsafe()
        throw new DraftMutationError({ message: 'Document prototypes cannot change' })
      },
      preventExtensions() {
        checkUnsafe()
        throw new DraftMutationError({ message: 'Document drafts must remain mutable' })
      },
      ownKeys(target) {
        checkUnsafe()
        return Reflect.ownKeys(target)
      },
      getOwnPropertyDescriptor(target, key) {
        checkUnsafe()
        return Reflect.getOwnPropertyDescriptor(target, key)
      },
    })
    proxies.set(object, proxy)
    return proxy
  }
  // Proxy preserves the target shape. It is intentionally revoked by transaction scope.
  return wrap(value, []) as T
}

/**
 * One projection completes with its own Exit; the resolver never retains results across writes.
 */
class SnapshotRead<A> extends Request.Class<
  {
    readonly readContext: object
    readonly project: (state: Record.State) => Effect.Effect<A, StorageError>
  },
  A,
  StorageError
> {}

/**
 * Acquires a scoped Session over the supplied Store.
 *
 * **Details**
 *
 * Captures the optional CreationHook from the construction context. The owning Scope
 * controls admission, observer lifetime and joined cleanup.
 *
 * **Gotchas**
 *
 * Provide conversation creation hooks before construction when using built-in executors. A
 * hook added only at invocation time does not change the captured initializer.
 *
 * @category constructors
 */
export const make: Effect.Effect<Session.Service, never, Scope.Scope | Store> = Effect.gen(
  function* () {
    const underlying = yield* Store
    const creationHook = yield* Effect.serviceOption(CreationHook)
    const cleanupScope = yield* Effect.acquireRelease(Scope.make(), (scope, exit) =>
      Scope.close(scope, exit),
    )
    const handle = yield* FiberHandle.make<boolean, never>().pipe(Scope.provide(cleanupScope))
    const started = yield* Ref.make(false)
    const terminal = yield* Deferred.make<void, StorageError>()
    const lifecycle = yield* Ref.make<{
      readonly sealed: boolean
      readonly cleanups: ReadonlyArray<Effect.Effect<void>>
    }>({ sealed: false, cleanups: [] })
    const usable = Ref.get(lifecycle).pipe(
      Effect.flatMap(({ sealed }) =>
        sealed ? Effect.fail(rejected('Session is closed', ClosedError)) : Effect.void,
      ),
    )
    const onClose: Session.Service['onClose'] = (cleanup) =>
      Effect.acquireRelease(
        Ref.modify(lifecycle, (state) => {
          if (state.sealed) return [undefined, state] as const
          const registration = Effect.suspend(Function.constant(cleanup))
          return [
            { registration },
            { ...state, cleanups: [...state.cleanups, registration] },
          ] as const
        }).pipe(
          Effect.filterOrFail(Predicate.isNotUndefined, () =>
            rejected('Session is closed', ClosedError),
          ),
        ),
        ({ registration }) =>
          Ref.update(lifecycle, (state) =>
            state.sealed
              ? state
              : {
                  ...state,
                  cleanups: Arr.filter(state.cleanups, (entry) => entry !== registration),
                },
          ),
      ).pipe(Effect.asVoid)
    const shutdown = Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (!(yield* Ref.getAndSet(started, true))) {
          const admitted = yield* Ref.modify(
            lifecycle,
            (state) => [state.cleanups.toReversed(), { sealed: true, cleanups: [] }] as const,
          )
          yield* FiberHandle.run(
            handle,
            Effect.gen(function* () {
              // Seal direct Store admission before suspending inner Activity bodies.
              const sealExit = yield* underlying.seal.pipe(Effect.exit)
              const exits = yield* Effect.forEach(admitted, (cleanup) => Effect.exit(cleanup))
              let failure: Cause.Cause<StorageError> | undefined
              for (const exit of [sealExit, ...exits])
                if (Exit.isFailure(exit))
                  failure = failure === undefined ? exit.cause : Cause.combine(failure, exit.cause)
              if (failure !== undefined) return yield* Effect.failCause(failure)
              // Store release follows this finalizer in Layer/Scope reverse order.
              // Waiting for its terminal receipt here would deadlock that release.
            }).pipe(Effect.uninterruptible, Deferred.into(terminal)),
          )
        }
        yield* restore(Deferred.await(terminal))
      }),
    )
    yield* Effect.addFinalizer(() => shutdown.pipe(Effect.orDie))
    const awaitClosed = Effect.gen(function* () {
      const sessionExit = yield* Deferred.await(terminal).pipe(Effect.exit)
      const storeExit = yield* underlying.awaitClosed.pipe(Effect.exit)
      if (Exit.isFailure(sessionExit)) {
        return yield* Effect.failCause(
          Exit.isFailure(storeExit)
            ? Cause.combine(sessionExit.cause, storeExit.cause)
            : sessionExit.cause,
        )
      }
      return yield* storeExit
    })
    // Seal Session admission before body cleanup; already admitted Store operations
    // retain their normal transaction/read lifetime until underlying resource release.
    // The Store validates keyed JSON results; this private forwarding signature
    // preserves both public overloads without erasing callback environments.
    const underlyingTransact = underlying.transact as <A, E, R>(
      change: (state: Record.State) => Effect.Effect<import('./Store.ts').Store.Candidate<A>, E, R>,
      options?: Store.CommitOptions,
    ) => Effect.Effect<A, StorageError | E, R>
    const store = Store.of({
      ...underlying,
      read: usable.pipe(Effect.andThen(underlying.read)),
      committed: usable.pipe(Effect.andThen(underlying.committed)),
      transact: <A, E, R>(
        change: (
          state: Record.State,
        ) => Effect.Effect<import('./Store.ts').Store.Candidate<A>, E, R>,
        options?: Store.CommitOptions,
      ) => usable.pipe(Effect.andThen(underlyingTransact(change, options))),
      journal: (after) => usable.pipe(Effect.andThen(underlying.journal(after))),
    })
    const migrationCache = yield* Document.makeMigrationCache
    // Public overloads constrain keyed results; runtime validation is authoritative at the Store boundary.
    const transact: <A, E, R>(
      change: (state: Record.State) => Effect.Effect<import('./Store.ts').Store.Candidate<A>, E, R>,
      options?: Store.CommitOptions,
    ) => Effect.Effect<A, StorageError | E, R> = store.transact as <A, E, R>(
      change: (state: Record.State) => Effect.Effect<import('./Store.ts').Store.Candidate<A>, E, R>,
      options?: Store.CommitOptions,
    ) => Effect.Effect<A, StorageError | E, R>
    const transaction = <A, E, R>(
      change: (tx: Transaction) => Effect.Effect<A, E, R>,
      options?: Store.CommitOptions,
    ): Effect.Effect<A, StorageError | E, R> =>
      transact(
        Effect.fnUntraced(function* (original) {
          const active = yield* Ref.make(true)
          const tableWritten = yield* Ref.make(false)
          const nextId = yield* Ref.make(original.nextId)
          const mintPermit = yield* Semaphore.make(1)
          const acquisitionPermit = yield* Semaphore.make(1)
          const acquired = yield* Ref.make(HashMap.empty<string, Acquired>())
          const acquiredOrder = yield* Ref.make<ReadonlyArray<string>>([])
          const forkDocuments = yield* Ref.make(HashSet.empty<Record.DocumentId>())
          const forkParents = yield* Ref.make(HashSet.empty<Record.ConversationId>())
          const pendingOperations = yield* Ref.make(0)
          const track = <A, R>(effect: Effect.Effect<A, StorageError, R>) =>
            Effect.uninterruptibleMask((restore) =>
              Ref.update(pendingOperations, (count) => count + 1).pipe(
                Effect.andThen(restore(effect)),
                Effect.ensuring(Ref.update(pendingOperations, (count) => count - 1)),
              ),
            )
          const localConversations = yield* Ref.make(
            HashMap.fromIterable(original.conversations.map((item) => [item.id, item] as const)),
          )
          const taskOrder = yield* Ref.make<ReadonlyArray<Record.TaskId>>(
            original.tasks.map((task) => task.id),
          )
          const localTasks = yield* Ref.make(
            HashMap.fromIterable(original.tasks.map((item) => [item.id, item] as const)),
          )
          interface Staged {
            readonly writes: ReadonlyArray<Record.Write>
            readonly submissions: HashMap.HashMap<Record.SubmissionId, Record.Submission>
          }
          const transitions = yield* SynchronizedRef.make<Staged>({
            writes: [],
            submissions: HashMap.fromIterable(
              original.submissions.map((item) => [item.id, item] as const),
            ),
          })
          const abortingAncestor = Effect.fnUntraced(function* (
            conversationId: Record.ConversationId,
          ) {
            const seen = new Set<Record.TaskId>()
            const ownerOf = (conversation: Record.Conversation) =>
              Option.fromUndefinedOr(conversation.owner?.taskId)
            let owner = HashMap.get(yield* Ref.get(localConversations), conversationId).pipe(
              Option.flatMap(ownerOf),
            )
            while (Option.isSome(owner) && !seen.has(owner.value)) {
              seen.add(owner.value)
              const taskOption = HashMap.get(yield* Ref.get(localTasks), owner.value)
              if (Option.isNone(taskOption)) return false
              const task = taskOption.value
              if (task.state.status === 'terminal') return false
              if (task.abortRequested) return true
              if (task.background) return false
              if (task.owner !== undefined) owner = Option.some(task.owner)
              else
                owner = HashMap.get(yield* Ref.get(localConversations), task.conversationId).pipe(
                  Option.flatMap(ownerOf),
                )
            }
            return false
          })
          const retiredAddresses = yield* Ref.make(HashSet.empty<string>())
          const acquisitions = yield* Ref.make(
            HashMap.empty<string, Effect.Effect<object, StorageError>>(),
          )
          const open = Ref.get(active).pipe(
            Effect.flatMap((active) =>
              active ? Effect.void : Effect.fail(rejected('Transaction is revoked', RevokedError)),
            ),
          )
          const read = Effect.gen(function* () {
            yield* open
            if (yield* Ref.get(tableWritten))
              return yield* rejected(
                'Table reads after the first table write are forbidden',
                ReadAfterWriteError,
              )
          })
          const mint = Effect.fnUntraced(function* <S extends Schema.Constraint>(schema: S) {
            yield* open
            const id = yield* validate(schema, yield* Ref.get(nextId))
            yield* Ref.update(nextId, (id) => id + 1)
            return id
          }, Semaphore.withPermit(mintPermit))
          const writeWithinTransition = Effect.fnUntraced(
            function* (state: Staged, value: Record.Write) {
              yield* open
              let valid = yield* validate(Record.Write, value)
              if (valid._tag === 'task' && valid.value.state.status === 'terminal') {
                const task = Struct.omit(valid.value, ['memos'])
                valid = { _tag: 'task', value: task }
              }
              if (valid._tag === 'task') {
                const previous = HashMap.get(yield* Ref.get(localTasks), valid.value.id)
                if (
                  Option.isSome(previous) &&
                  (previous.value.state.status === 'terminal' ||
                    previous.value.conversationId !== valid.value.conversationId)
                )
                  return yield* rejected('Task is terminal or cannot change conversations')
              }
              const detachedWrite = yield* detachedEffect(valid)
              if (valid._tag === 'conversation')
                yield* Ref.update(localConversations, HashMap.set(valid.value.id, valid.value))
              if (valid._tag === 'task') {
                const task = valid.value
                if (!HashMap.has(yield* Ref.get(localTasks), task.id))
                  yield* Ref.update(taskOrder, (ids) => [...ids, task.id])
                yield* Ref.update(localTasks, HashMap.set(task.id, task))
              }
              if (
                valid._tag === 'conversation' ||
                valid._tag === 'entry' ||
                valid._tag === 'task' ||
                valid._tag === 'submission'
              )
                yield* Ref.set(tableWritten, true)
              return {
                writes: [...state.writes, detachedWrite],
                submissions:
                  valid._tag === 'submission'
                    ? HashMap.set(state.submissions, valid.value.id, valid.value)
                    : state.submissions,
              }
            },
            // The terminal check and all staged/local table updates form one bounded
            // transition. Waiting for admission remains interruptible; admitted writes
            // settle before another callback can check the same task or revoke the tx.
            Effect.uninterruptible,
          )
          const write = (value: Record.Write) =>
            SynchronizedRef.modifyEffect(transitions, (state) =>
              writeWithinTransition(state, value).pipe(
                Effect.map((next) => [undefined, next] as const),
              ),
            )
          const owner = Effect.fnUntraced(function* (ownership: Session.Ownership) {
            if (ownership._tag === 'ownerless') return {}
            const taskOption = HashMap.get(yield* Ref.get(localTasks), ownership.taskId)

            if (
              Option.isNone(taskOption) ||
              taskOption.value.abortRequested ||
              taskOption.value.state.status === 'terminal' ||
              taskOption.value.state.status === 'completing'
            )
              return yield* rejected('Conversation owner must be a live task')
            const task = taskOption.value
            return { owner: { taskId: task.id, conversationId: task.conversationId } }
          })
          const doc = Effect.fnUntraced(function* <T extends object>(
            token: Document.Document<T>,
            target: Document.Document.Target = {},
          ): Effect.fn.Return<Document.Document.Draft<T>, StorageError> {
            yield* open
            const logical = yield* address(token, target)
            const key = Record.addressKey(logical)
            const { acquisition } = yield* acquisitionPermit.withPermit(
              Effect.gen(function* () {
                const cached = HashMap.get(yield* Ref.get(acquisitions), key)
                if (Option.isSome(cached)) return { acquisition: cached.value }
                const acquisition = yield* Effect.cached(
                  Effect.gen(function* () {
                    yield* open
                    if (
                      logical.scope._tag === 'conversation' &&
                      !HashMap.has(yield* Ref.get(localConversations), logical.scope.conversationId)
                    )
                      return yield* rejected('Document conversation is absent', NotFoundError)
                    if (logical.scope._tag === 'task') {
                      const taskOption = HashMap.get(
                        yield* Ref.get(localTasks),
                        logical.scope.taskId,
                      )

                      if (Option.isNone(taskOption) || taskOption.value.state.status === 'terminal')
                        return yield* rejected('Document task is absent or settled', NotFoundError)
                    }
                    const staged = HashSet.has(yield* Ref.get(retiredAddresses), key)
                      ? Option.none()
                      : Arr.findLast(
                          (yield* SynchronizedRef.get(transitions)).writes,
                          (write) =>
                            (write._tag === 'document.create' || write._tag === 'document.copy') &&
                            Record.addressKey(write.record) === key,
                        )
                    const persisted = HashSet.has(yield* Ref.get(retiredAddresses), key)
                      ? Option.none()
                      : findDocument(original, logical, 'current')
                    let stored = yield* Option.match(persisted, {
                      onNone: () =>
                        Effect.succeed(
                          Option.none<Document.Document.Snapshot<Schema.JsonObject>>(),
                        ),
                      onSome: (document) => materialize(document, 'current'),
                    })
                    let stagedRecord: Record.DocumentCreate | undefined
                    if (
                      Option.isSome(staged) &&
                      (staged.value._tag === 'document.create' ||
                        staged.value._tag === 'document.copy')
                    ) {
                      const stagedWrite = staged.value
                      stagedRecord = stagedWrite.record
                      let content: Record.Content
                      if (stagedWrite._tag === 'document.create') content = stagedWrite.content
                      else {
                        const source = Arr.findFirst(
                          original.documents,
                          (item) => item.record.id === stagedWrite.source.id,
                        )
                        if (Option.isNone(source))
                          return yield* rejected('Staged copy source is absent', NotFoundError)
                        const sourceValueOption = yield* materialize(
                          source.value,
                          stagedWrite.source.at,
                        )
                        if (Option.isNone(sourceValueOption))
                          return yield* rejected('Staged copy source is not alive', NotFoundError)
                        const sourceValue = sourceValueOption.value
                        content = {
                          _tag: 'base',
                          version: sourceValue.version,
                          value: sourceValue.value,
                        }
                      }
                      if (content._tag !== 'base')
                        return yield* rejected('Staged document creation requires a base')
                      stored = Option.some(
                        Document.makeSnapshot({
                          record: {
                            ...stagedWrite.record,
                            createdAt: yield* validate(Record.Seq, original.nextSeq),
                          },
                          version: content.version,
                          value: yield* detachedEffect(content.value),
                          deltasSinceBase: 0,
                        }),
                      )
                    }
                    let value: T
                    let id: Record.DocumentId
                    if (Option.isNone(stored)) {
                      value = yield* Effect.try({
                        try: () => token.definition.initial(target.seed),
                        catch: (cause) =>
                          rejected('Document initializer failed', InvalidError, cause),
                      })
                      value = yield* validate(Schema.toType(token.definition.schema), value)
                      yield* Document.encode(token, value)
                      id = yield* mint(Record.DocumentId)
                    } else {
                      value = (yield* typed(token, stored.value, migrationCache)).value
                      id = stored.value.record.id
                    }
                    yield* open
                    const ops: Array<Record.Op> = []
                    const mutable = yield* detachedEffect(value)
                    const record: Acquired = {
                      definition: token.definition,
                      address: logical,
                      id,
                      value: mutable,
                      ops,
                      retire: false,
                      prepare: validate(Schema.toType(token.definition.schema), mutable).pipe(
                        Effect.flatMap((valid) => Document.encode(token, valid)),
                      ),
                      checkpoint: Effect.try({
                        try: () =>
                          token.definition.checkpointWhen?.(mutable, ops, {
                            deltasSinceBase: stored.pipe(
                              Option.map((snapshot) => snapshot.deltasSinceBase),
                              Option.getOrElse(() => 0),
                            ),
                          }) ?? false,
                        catch: (cause) =>
                          rejected('Checkpoint predicate failed', InvalidError, cause),
                      }),
                      ...Option.match(stored, {
                        onNone: () => ({}),
                        onSome: (stored) => ({ stored }),
                      }),
                      ...(stagedRecord === undefined ? {} : { staged: stagedRecord }),
                    }
                    yield* Ref.update(acquired, HashMap.set(key, record))
                    yield* Ref.update(acquiredOrder, (keys) => [...keys, key])
                    return draft(record.value, () => Ref.getUnsafe(active), ops)
                  }),
                )
                yield* Ref.update(acquisitions, HashMap.set(key, acquisition))
                return { acquisition }
              }),
            )
            return (yield* acquisition) as Document.Document.Draft<T>
          })
          const rawTx: Transaction = {
            mint,
            write,
            doc,
            ensureRoot: Effect.gen(function* () {
              const existing = HashMap.get(
                yield* Ref.get(localConversations),
                Record.ROOT_CONVERSATION_ID,
              )
              if (Option.isSome(existing)) return yield* detachedEffect(existing.value)
              const root = { id: Record.ROOT_CONVERSATION_ID }
              return yield* write({ _tag: 'conversation', value: root }).pipe(
                Effect.andThen(
                  Option.match(creationHook, {
                    onNone: () => Effect.void,
                    onSome: (hook) => hook.run(tx, root),
                  }),
                ),
                Effect.as(root),
              )
            }),
            conversation: Effect.fnUntraced(function* (id) {
              yield* read
              return yield* detachedOptional(
                Arr.findFirst(original.conversations, (item) => item.id === id),
              )
            }),
            entry: Effect.fnUntraced(function* (id) {
              yield* read
              return yield* detachedOptional(
                Arr.findFirst(original.entries, (item) => item.entry.id === id).pipe(
                  Option.map((item) => item.entry),
                ),
              )
            }),
            task: Effect.fnUntraced(function* (id) {
              yield* read
              return yield* detachedOptional(
                Arr.findFirst(original.tasks, (item) => item.id === id),
              )
            }),
            submission: Effect.fnUntraced(function* (id) {
              yield* read
              return yield* detachedOptional(
                Arr.findFirst(original.submissions, (item) => item.id === id),
              )
            }),
            scanConversations: Effect.fnUntraced(function* (query, limit, cursor) {
              yield* read
              return yield* conversationPage(original, query, limit, cursor)
            }),
            scanEntries: Effect.fnUntraced(function* (query, limit, cursor) {
              yield* read
              return yield* entryPage(original, query, limit, cursor)
            }),
            scanTasks: Effect.fnUntraced(function* (query, limit, cursor) {
              yield* read
              return yield* taskPage(original, query, limit, cursor)
            }),
            scanSubmissions: Effect.fnUntraced(function* (query, limit, cursor) {
              yield* read
              return yield* submissionPage(original, query, limit, cursor)
            }),
            latestHeadMarker: Effect.fnUntraced(function* (conversationId, atOrBefore) {
              yield* read
              return Arr.findFirst(
                yield* visibleEntries(original, conversationId, 0, atOrBefore),
                (item) => item.head !== undefined,
              )
            }),
            submissionByRequest: Effect.fnUntraced(function* (conversationId, requestId) {
              yield* read
              return yield* detachedOptional(
                Arr.findFirst(
                  original.submissions,
                  (item) => item.conversationId === conversationId && item.requestId === requestId,
                ),
              )
            }),
            createConversation: Effect.fnUntraced(function* (options) {
              const ownership = yield* owner(options.ownership)
              const id = yield* mint(Record.ConversationId)
              const value = { id, ...ownership }
              yield* write({ _tag: 'conversation', value })
              if (Option.isSome(creationHook)) yield* creationHook.value.run(tx, value)
              return yield* detachedEffect(value)
            }),
            forkConversation: Effect.fnUntraced(function* (parent, at, options) {
              yield* open
              yield* Ref.update(forkParents, HashSet.add(parent))
              const visible = yield* visibleEntries(original, parent)
              const entryOption = Arr.findFirst(visible, (item) => item.id === at)
              const committedOption = Arr.findFirst(
                original.entries,
                (item) => item.entry.id === at,
              )
              if (Option.isNone(entryOption) || Option.isNone(committedOption))
                return yield* rejected('Fork cutoff is not visible', NotFoundError)
              const entry = entryOption.value
              const committed = committedOption.value
              const ownership = yield* owner(options.ownership)
              const id = yield* mint(Record.ConversationId)
              const selected = MutableHashMap.empty<
                string,
                {
                  /**
                   * Reads the selected persisted document incarnation and cutoff as an untyped snapshot.
                   */
                  readonly document: Record.StoredDocument
                  readonly at: Record.Point
                }
              >()
              for (const document of original.documents) {
                const record = document.record
                if (record.scope._tag !== 'conversation') continue
                if (
                  record.fork === 'asOf' &&
                  record.scope.conversationId === entry.conversationId &&
                  Record.isAlive(record, committed.commitSeq)
                )
                  MutableHashMap.set(
                    selected,
                    Record.addressKey({
                      ...record,
                      scope: { _tag: 'conversation', conversationId: id },
                    }),
                    { document, at: committed.commitSeq },
                  )
                if (
                  record.fork === 'current' &&
                  record.scope.conversationId === parent &&
                  Record.isAlive(record, 'current')
                ) {
                  const key = Record.addressKey({
                    ...record,
                    scope: { _tag: 'conversation', conversationId: id },
                  })
                  if (MutableHashMap.has(selected, key))
                    return yield* rejected('Fork selects ambiguous document source')
                  MutableHashMap.set(selected, key, { document, at: 'current' })
                }
              }
              for (const source of MutableHashMap.values(selected)) {
                yield* Ref.update(forkDocuments, HashSet.add(source.document.record.id))
                const documentId = yield* mint(Record.DocumentId)
                const record = Struct.omit(source.document.record, ['createdAt', 'retiredAt'])
                yield* write({
                  _tag: 'document.copy',
                  record: {
                    ...record,
                    id: documentId,
                    scope: { _tag: 'conversation', conversationId: id },
                  },
                  source: { id: source.document.record.id, at: source.at },
                })
              }
              const value = { id, parent: { conversationId: parent, at }, ...ownership }
              yield* write({ _tag: 'conversation', value })
              if (Option.isSome(creationHook)) yield* creationHook.value.run(tx, value)
              return yield* detachedEffect(value)
            }),
            appendEntry: Effect.fnUntraced(function* (conversationId, input) {
              yield* open
              if (!HashMap.has(yield* Ref.get(localConversations), conversationId))
                return yield* rejected('Entry conversation is absent', NotFoundError)
              const id = yield* mint(Record.EntryId)
              const { head, ...draftValueUnsafe } = input
              const value = {
                ...draftValueUnsafe,
                id,
                conversationId,
                ...(head === undefined ? {} : { head: head === 'self' ? id : head }),
              }
              yield* write({ _tag: 'entry', value })
              return yield* detachedEffect(value)
            }),
            createTask: Effect.fnUntraced(function* (input) {
              yield* open
              if (!HashMap.has(yield* Ref.get(localConversations), input.conversationId))
                return yield* rejected('Task conversation is absent', NotFoundError)
              if (yield* abortingAncestor(input.conversationId))
                return yield* rejected('Task conversation has an aborting ancestor')
              if (input.owner !== undefined) {
                const parentOption = HashMap.get(yield* Ref.get(localTasks), input.owner)

                if (
                  Option.isNone(parentOption) ||
                  parentOption.value.abortRequested ||
                  parentOption.value.conversationId !== input.conversationId ||
                  parentOption.value.state.status === 'terminal' ||
                  parentOption.value.state.status === 'completing' ||
                  input.background
                )
                  return yield* rejected('Invalid task owner')
              }
              const id = yield* mint(Record.TaskId)
              yield* write({ _tag: 'task', value: { ...input, id } })
              return id
            }),
            createSubmission: Effect.fnUntraced(function* (input) {
              yield* open
              if (!HashMap.has(yield* Ref.get(localConversations), input.conversationId))
                return yield* rejected('Submission conversation is absent', NotFoundError)
              if (yield* abortingAncestor(input.conversationId))
                return yield* rejected('Submission conversation has an aborting ancestor')
              const id = yield* mint(Record.SubmissionId)
              const value = yield* validate(Record.Submission, { ...input, id })
              yield* write({ _tag: 'submission', value })
              return yield* detachedEffect(value)
            }),
            placeSubmission: (id, entry) =>
              SynchronizedRef.modifyEffect(
                transitions,
                Effect.fnUntraced(function* (state) {
                  yield* open
                  const currentOption = HashMap.get(state.submissions, id)
                  if (Option.isNone(currentOption))
                    return yield* rejected('Submission is absent', NotFoundError)
                  const current = currentOption.value
                  if (current.status === 'done' || current.status === 'unanswered')
                    return [undefined, state] as const
                  if (current.status !== 'queued')
                    return yield* rejected('Only queued submissions may be placed')
                  const value = yield* validate(
                    Record.Submission,
                    current.type === 'input'
                      ? { ...current, _tag: 'InputPlaced', entry, status: 'placed' }
                      : { ...current, _tag: 'WriteDone', entry, status: 'done' },
                  )
                  const next = yield* writeWithinTransition(state, {
                    _tag: 'submission',
                    value,
                  })
                  return [undefined, next] as const
                }, Effect.uninterruptible),
              ),
            settleSubmission: (id, settlement) =>
              SynchronizedRef.modifyEffect(
                transitions,
                Effect.fnUntraced(function* (state) {
                  yield* open
                  const currentOption = HashMap.get(state.submissions, id)
                  if (Option.isNone(currentOption))
                    return yield* rejected('Submission is absent', NotFoundError)
                  const current = currentOption.value
                  if (current.status === 'done' || current.status === 'unanswered')
                    return [undefined, state] as const
                  if (
                    settlement.status === 'done' &&
                    (current.type !== 'input' || current.status !== 'placed')
                  )
                    return yield* rejected('Only placed input may be answered')
                  const unansweredTag =
                    current.type === 'input' ? 'InputUnanswered' : 'WriteUnanswered'
                  const value = yield* validate(Record.Submission, {
                    ...current,
                    ...settlement,
                    _tag: settlement.status === 'done' ? 'InputDone' : unansweredTag,
                  })
                  const next = yield* writeWithinTransition(state, {
                    _tag: 'submission',
                    value,
                  })
                  return [undefined, next] as const
                }, Effect.uninterruptible),
              ),
            retire: Effect.fnUntraced(function* (token, target = {}) {
              yield* open
              const logical = yield* address(token, target)
              const key = Record.addressKey(logical)
              yield* Ref.update(retiredAddresses, HashSet.add(key))
              const itemOption = HashMap.get(yield* Ref.get(acquired), key)
              if (Option.isSome(itemOption)) {
                const item = itemOption.value
                yield* Ref.update(acquisitions, HashMap.remove(key))
                yield* Ref.update(acquired, HashMap.remove(key))
                yield* Ref.update(acquiredOrder, (keys) => [
                  ...Arr.filter(keys, (entry) => entry !== key),
                  `${key}\0retired:${item.id}`,
                ])
                yield* Ref.update(acquired, (entries) =>
                  HashMap.set(entries, `${key}\0retired:${item.id}`, { ...item, retire: true }),
                )
                return
              }
              const stored = findDocument(original, logical, 'current')
              if (
                Option.isSome(stored) &&
                !(yield* SynchronizedRef.get(transitions)).writes.some(
                  (write) =>
                    write._tag === 'document.retire' && write.id === stored.value.record.id,
                )
              )
                yield* write({
                  _tag: 'document.retire',
                  id: stored.value.record.id,
                })
            }),
          }
          const tx: Transaction = {
            ensureRoot: track(rawTx.ensureRoot),
            mint: (schema) => track(rawTx.mint(schema)),
            conversation: (id) => track(rawTx.conversation(id)),
            entry: (id) => track(rawTx.entry(id)),
            task: (id) => track(rawTx.task(id)),
            submission: (id) => track(rawTx.submission(id)),
            scanConversations: (query, limit, cursor) =>
              track(rawTx.scanConversations(query, limit, cursor)),
            scanEntries: (query, limit, cursor) => track(rawTx.scanEntries(query, limit, cursor)),
            scanTasks: (query, limit, cursor) => track(rawTx.scanTasks(query, limit, cursor)),
            scanSubmissions: (query, limit, cursor) =>
              track(rawTx.scanSubmissions(query, limit, cursor)),
            submissionByRequest: (id, request) => track(rawTx.submissionByRequest(id, request)),
            latestHeadMarker: (id, atOrBefore) => track(rawTx.latestHeadMarker(id, atOrBefore)),
            createConversation: (options) => track(rawTx.createConversation(options)),
            forkConversation: (parent, at, options) =>
              track(rawTx.forkConversation(parent, at, options)),
            appendEntry: (id, entry) => track(rawTx.appendEntry(id, entry)),
            createTask: (value) => track(rawTx.createTask(value)),
            createSubmission: (value) => track(rawTx.createSubmission(value)),
            placeSubmission: (id, entry) => track(rawTx.placeSubmission(id, entry)),
            settleSubmission: (id, value) => track(rawTx.settleSubmission(id, value)),
            write: (value) => track(rawTx.write(value)),
            doc: (token, target) => track(rawTx.doc(token, target)),
            retire: (token, target) => track(rawTx.retire(token, target)),
          }
          const result = yield* change(tx).pipe(
            Effect.catchDefect((defect) =>
              defect instanceof DraftMutationError
                ? Effect.fail(rejected(defect.message, InvalidError, defect.cause))
                : Effect.die(defect),
            ),
            Effect.ensuring(Ref.set(active, false)),
          )
          if ((yield* Ref.get(pendingOperations)) !== 0)
            return yield* rejected('Transaction callback settled before its pending operations')
          const writes = [...(yield* SynchronizedRef.get(transitions)).writes]
          for (const key of yield* Ref.get(acquiredOrder)) {
            const itemOption = HashMap.get(yield* Ref.get(acquired), key)
            if (Option.isNone(itemOption)) continue
            const item = itemOption.value
            const definition = item.definition
            const value = yield* item.prepare
            if (item.staged !== undefined) {
              const previousIndex = Arr.findFirstIndex(
                writes,
                (write) =>
                  (write._tag === 'document.create' || write._tag === 'document.copy') &&
                  write.record.id === item.id,
              ).pipe(Option.getOrElse(() => -1))
              if (previousIndex >= 0) writes.splice(previousIndex, 1)
              writes.push({
                _tag: 'document.create',
                record: item.staged,
                content: { _tag: 'base', version: definition.version, value },
              })
            } else if (item.stored === undefined) {
              yield* validate(Record.DocumentId, item.id)
              writes.push({
                _tag: 'document.create',
                record: {
                  ...item.address,
                  id: item.id,
                  ...(definition.history === undefined ? {} : { history: definition.history }),
                  ...(definition.fork === undefined ? {} : { fork: definition.fork }),
                },
                content: { _tag: 'base', version: definition.version, value },
              })
            } else if (item.stored.version !== definition.version)
              writes.push({
                _tag: 'document.change',
                id: item.id,
                content: { _tag: 'base', version: definition.version, value },
              })
            else if (Arr.isReadonlyArrayNonEmpty(item.ops)) {
              const checkpoint = yield* item.checkpoint
              // Preserve the exact draft operation batch unless array methods changed index structure.
              const hasArrayMutation = item.ops.some(
                (op) =>
                  op[0] === 'replace' || (op[0] === 'delete' && typeof op[1].at(-1) === 'number'),
              )
              // Domain codecs may omit decoded tags or transform native values. Replay
              // remains granular only when it produces the exact encoded final value.
              const replayed = hasArrayMutation
                ? undefined
                : yield* applyOps(item.stored.value, item.ops)
              const preservesEncoding =
                replayed !== undefined && Schema.toEquivalence(Schema.JsonObject)(replayed, value)
              const ops: ReadonlyArray<Record.Op> = preservesEncoding
                ? item.ops
                : [['replace', value]]
              writes.push({
                _tag: 'document.change',
                id: item.id,
                content: checkpoint
                  ? { _tag: 'base', version: definition.version, value }
                  : { _tag: 'delta', version: definition.version, ops },
                ...(checkpoint ? { publicationOps: ops } : {}),
              })
            }
            if (item.retire) writes.push({ _tag: 'document.retire', id: item.id })
          }
          for (const write of writes) {
            if (
              ((write._tag === 'task' &&
                !original.tasks.some((task) => task.id === write.value.id)) ||
                (write._tag === 'submission' &&
                  !original.submissions.some((item) => item.id === write.value.id))) &&
              (yield* abortingAncestor(write.value.conversationId))
            )
              return yield* rejected('New work cannot enter an aborting owned conversation')
            let affected: Option.Option<Record.DocumentCreate> = Option.none()
            if (write._tag === 'document.create') affected = Option.some(write.record)
            else if (write._tag === 'document.change' || write._tag === 'document.retire')
              affected = Arr.findFirst(
                original.documents,
                (item) => item.record.id === write.id,
              ).pipe(Option.map((item) => item.record))
            if (
              (write._tag === 'document.change' || write._tag === 'document.retire') &&
              HashSet.has(yield* Ref.get(forkDocuments), write.id)
            )
              return yield* rejected(
                'Cannot change a copied fork source document in its fork transaction',
              )
            if (
              Option.isSome(affected) &&
              affected.value.scope._tag === 'conversation' &&
              affected.value.fork === 'current' &&
              HashSet.has(yield* Ref.get(forkParents), affected.value.scope.conversationId)
            )
              return yield* rejected(
                'Cannot change current-policy documents in their fork transaction',
              )
            let ownerId: Record.TaskId | undefined
            if (write._tag === 'conversation') ownerId = write.value.owner?.taskId
            else if (
              write._tag === 'task' &&
              !original.tasks.some((task) => task.id === write.value.id)
            )
              ownerId = write.value.owner
            if (ownerId !== undefined) {
              const taskOption = HashMap.get(yield* Ref.get(localTasks), ownerId)
              if (
                Option.isNone(taskOption) ||
                taskOption.value.abortRequested ||
                taskOption.value.state.status === 'terminal' ||
                taskOption.value.state.status === 'completing'
              )
                return yield* rejected('New owned work requires a live non-aborting owner')
            }
          }
          for (const id of yield* Ref.get(taskOrder)) {
            const taskOption = HashMap.get(yield* Ref.get(localTasks), id)
            if (Option.isSome(taskOption) && taskOption.value.state.status === 'terminal') {
              const task = taskOption.value
              for (const document of original.documents)
                if (
                  document.record.scope._tag === 'task' &&
                  document.record.scope.taskId === task.id &&
                  document.record.retiredAt === undefined &&
                  !writes.some(
                    (write) => write._tag === 'document.retire' && write.id === document.record.id,
                  )
                )
                  writes.push({
                    _tag: 'document.retire',
                    id: document.record.id,
                  })
              for (const write of writes)
                if (
                  write._tag === 'document.create' &&
                  write.record.scope._tag === 'task' &&
                  write.record.scope.taskId === task.id &&
                  !writes.some(
                    (item) => item._tag === 'document.retire' && item.id === write.record.id,
                  )
                )
                  writes.push({
                    _tag: 'document.retire',
                    id: write.record.id,
                  })
            }
          }
          return makeCandidate({
            state: { ...original, nextId: yield* Ref.get(nextId) },
            writes,
            result,
          })
        }),
        options,
      )
    const reads = RequestResolver.makeGrouped<SnapshotRead<unknown>, object>({
      key: (entry) => entry.request.readContext,
      resolver: Effect.fnUntraced(function* (entries) {
        const state = yield* store.read.pipe(Effect.provideContext(entries[0].context), Effect.exit)
        yield* Effect.forEach(
          entries,
          Effect.fnUntraced(function* (entry) {
            const exit = Exit.isFailure(state)
              ? state
              : yield* entry.request
                  .project(state.value)
                  .pipe(Effect.provideContext(entry.context), Effect.exit)
            // RequestResolver's completion protocol requires its synchronous entry callback.
            entry.completeUnsafe(exit)
          }),
          { discard: true },
        )
      }),
    })
    const project = Effect.fnUntraced(function* <A>(
      projection: (state: Record.State) => Effect.Effect<A, StorageError>,
    ): Effect.fn.Return<A, StorageError> {
      // A Store may provide a shared view key; otherwise caller contexts remain separate.
      const readContext = yield* store.readContext ?? Effect.context<never>()
      return yield* Effect.request(new SnapshotRead({ readContext, project: projection }), reads)
    })
    const snapshot: Session.Service['snapshot'] = Effect.fnUntraced(function* (token, target = {}) {
      const logical = yield* address(token, target)
      return yield* project(
        Effect.fnUntraced(function* (state) {
          const document = findDocument(state, logical, 'current')
          if (Option.isNone(document)) return Option.none()
          const snapshotValue = yield* materialize(document.value, 'current')
          if (Option.isNone(snapshotValue)) return Option.none()
          return Option.some(yield* typed(token, snapshotValue.value, migrationCache))
        }),
      )
    })
    const service = Session.of({
      committed: store.committed,
      initialize: (conversationId) =>
        transaction(
          Effect.fnUntraced(function* (tx) {
            const conversationOption = yield* tx.conversation(conversationId)
            if (Option.isNone(conversationOption))
              return yield* rejected('Conversation is absent', NotFoundError)
            const conversation = conversationOption.value
            if (Option.isSome(creationHook) && creationHook.value.recover !== undefined)
              yield* creationHook.value.recover(tx, conversation)
          }),
        ),
      transaction,
      snapshot,
      root: (initialize) =>
        transaction(
          Effect.fnUntraced(function* (tx) {
            const existing = yield* tx.conversation(Record.ROOT_CONVERSATION_ID)
            if (Option.isSome(existing)) return existing.value
            const root = yield* tx.ensureRoot
            if (initialize !== undefined) yield* initialize(tx)
            return root
          }),
        ),
      snapshotAsOf: Effect.fnUntraced(function* (token, conversationId, at, target = {}) {
        if (token.definition.scope !== 'conversation' || token.definition.history !== 'rewindable')
          return yield* rejected('Historical snapshot requires rewindable conversation document')
        return yield* project(
          Effect.fnUntraced(function* (state) {
            const visible = yield* visibleEntries(state, conversationId)
            const entryOption = Arr.findFirst(visible, (item) => item.id === at)
            const persistedOption = Arr.findFirst(state.entries, (item) => item.entry.id === at)
            if (Option.isNone(entryOption) || Option.isNone(persistedOption))
              return yield* rejected('Historical entry is not visible', NotFoundError)
            const entry = entryOption.value
            const persisted = persistedOption.value
            const logical = yield* address(token, { ...target, owner: entry.conversationId })
            const document = findDocument(state, logical, persisted.commitSeq)
            if (Option.isNone(document)) return Option.none()
            const snapshotValue = yield* materialize(document.value, persisted.commitSeq)
            if (Option.isNone(snapshotValue)) return Option.none()
            return Option.some(yield* typed(token, snapshotValue.value, migrationCache))
          }),
        )
      }),
      state: (token, target) => Observation.state(store, token, target, migrationCache),
      watchDoc: (token, target) => Observation.watch(store, token, target, migrationCache),
      commits: Observation.commits(store),
      conversation: (id) =>
        project((state) =>
          detachedOptional(Arr.findFirst(state.conversations, (item) => item.id === id)),
        ),
      entry: (id, conversationId) =>
        project(
          Effect.fnUntraced(function* (state) {
            if (
              conversationId !== undefined &&
              !(yield* visibleEntries(state, conversationId)).some((item) => item.id === id)
            )
              return Option.none()
            return yield* detachedOptional(
              Arr.findFirst(state.entries, (item) => item.entry.id === id),
            )
          }),
        ),
      task: (id) =>
        project((state) => detachedOptional(Arr.findFirst(state.tasks, (item) => item.id === id))),
      submission: (id) =>
        project((state) =>
          detachedOptional(Arr.findFirst(state.submissions, (item) => item.id === id)),
        ),
      submissionByRequest: (conversationId, requestId) =>
        project((state) =>
          detachedOptional(
            Arr.findFirst(
              state.submissions,
              (item) => item.conversationId === conversationId && item.requestId === requestId,
            ),
          ),
        ),
      latestHeadMarker: (conversationId, atOrBefore) =>
        project(
          Effect.fnUntraced(function* (state) {
            return Arr.findFirst(
              yield* visibleEntries(state, conversationId, 0, atOrBefore),
              (item) => item.head !== undefined,
            )
          }),
        ),
      scanConversations: (query, limit, cursor) =>
        project((state) => conversationPage(state, query, limit, cursor)),
      scanEntries: (query, limit, cursor) =>
        project((state) => entryPage(state, query, limit, cursor)),
      scanTasks: (query, limit, cursor) =>
        project((state) => taskPage(state, query, limit, cursor)),
      scanSubmissions: (query, limit, cursor) =>
        project((state) => submissionPage(state, query, limit, cursor)),
      scanDocuments: (query, limit, cursor) =>
        project((state) =>
          page(
            Arr.filter(
              documentsInScope(state, query.scope, query.at).map((item) => item.record),
              (item) => query.kind === undefined || item.kind === query.kind,
            ),
            limit,
            cursor,
          ),
        ),
      findDocument: (logical, at = 'current') =>
        project((state) =>
          detachedOptional(
            findDocument(state, logical, at).pipe(Option.map((document) => document.record)),
          ),
        ),
      document: (id, at = 'current') =>
        project(
          Effect.fnUntraced(function* (state) {
            const document = Arr.findFirst(state.documents, (item) => item.record.id === id)
            return yield* Option.match(document, {
              onNone: () => Effect.succeedNone,
              onSome: (document) => materialize(document, at),
            })
          }),
        ),
      isClosed: Ref.get(lifecycle).pipe(Effect.map((state) => state.sealed)),
      onClose,
      awaitClosed,
    })
    return service
  },
)
/**
 * Provides a scoped Session from a Store.
 *
 * **Details**
 *
 * Share the Layer value to share one Session lifetime. Provide CreationHook while building
 * this Layer when new conversations need domain initialization.
 *
 * @category layers
 */
export const layer: Layer.Layer<Session, never, Store> = Layer.effect(Session, make)

/**
 * Type-level contracts for `Session`.
 *
 */
export declare namespace Session {
  /**
   * Optional owner filters for a conversation scan.
   *
   * @category models
   */
  export interface ConversationQuery {
    readonly ownerConversationId?: Record.ConversationId | undefined
    readonly ownerTaskId?: Record.TaskId | undefined
  }
  /**
   * Conversation and optional entry bounds for a visible-history scan.
   *
   * @category models
   */
  export interface EntryQuery {
    readonly conversationId: Record.ConversationId
    readonly minEntryId?: Record.EntryId | undefined
    readonly maxEntryId?: Record.EntryId | undefined
  }
  /**
   * Optional conversation, kind, status and ownership-state filters for a task scan.
   *
   * @category models
   */
  export type TaskQuery = Partial<
    Pick<Record.Task, 'conversationId' | 'kind' | 'abortRequested' | 'background'>
  > & { readonly status?: Record.Task['state']['status'] | undefined }
  /**
   * Optional conversation and status filters for a submission scan.
   *
   * @category models
   */
  export type SubmissionQuery = Partial<Pick<Record.Submission, 'conversationId' | 'status'>>
  /**
   * Document scope, read cutoff and optional kind filter.
   *
   * @category models
   */
  export interface DocumentQuery {
    readonly scope: Record.Scope
    readonly at: Record.Point
    readonly kind?: string | undefined
  }
  /**
   * Transaction ownership variants.
   *
   * @category models
   */
  export type Ownership = Data.TaggedEnum<{
    ownerless: {}
    /**
     * Returns the requested domain task, or None when it is absent.
     */
    task: { readonly taskId: Record.TaskId }
  }>
  /**
   * Domain operations exposed by a scoped Session.
   *
   * @category models
   */
  export interface Service {
    /**
     * Saved domain facts, excluding uncommitted transaction candidates.
     */
    readonly committed: Effect.Effect<Record.State, StorageError>
    /**
     * Returns or creates reserved root conversation 1, atomically running initialization on
     * creation.
     */
    readonly root: (
      initialize?: (tx: Transaction) => Effect.Effect<void, StorageError>,
    ) => Effect.Effect<Record.Conversation, StorageError>
    /**
     * Applies the host's recovery initializer to an existing conversation atomically.
     */
    readonly initialize: (
      conversationId: Record.ConversationId,
    ) => Effect.Effect<void, StorageError>
    /**
     * Runs an atomic callback; keyed transactions replay the saved JSON-safe or void result
     * without rerunning it.
     */
    readonly transaction: Transaction.Function
    /**
     * Reads and decodes the latest committed document value; None means no live incarnation
     * exists.
     */
    readonly snapshot: <T extends object>(
      token: Document.Document<T>,
      target?: Document.Document.Target,
    ) => Effect.Effect<Option.Option<Document.Document.Snapshot<T>>, StorageError>
    /**
     * Reads a conversation document at an entry cutoff using its declared history policy.
     */
    readonly snapshotAsOf: <T extends object>(
      token: Document.Document<T>,
      conversationId: Record.ConversationId,
      at: Record.EntryId,
      target?: Omit<Document.Document.Target, 'owner'>,
    ) => Effect.Effect<Option.Option<Document.Document.Snapshot<T>>, StorageError>
    /**
     * Maintains a scoped live value for the current document incarnation; None means acquisition
     * found no document.
     */
    readonly state: <T extends object>(
      token: Document.Document<T>,
      target?: Document.Document.Target,
    ) => Effect.Effect<
      Option.Option<Observation.State<T>>,
      StorageError,
      import('effect/Scope').Scope
    >
    /**
     * Acquires an initial document snapshot and a scoped stream of committed changes for that
     * incarnation.
     */
    readonly watchDoc: <T extends object>(
      token: Document.Document<T>,
      target?: Document.Document.Target,
    ) => Effect.Effect<
      Option.Option<Observation.Watch<T>>,
      StorageError,
      import('effect/Scope').Scope
    >
    /**
     * Streams retained frames saved after subscription starts. Retention is bounded; use
     * View/Event when consumers need reset-based resynchronization.
     */
    readonly commits: Stream.Stream<Record.Frame, StorageError>
    /**
     * Returns the requested conversation, or None when it is absent.
     */
    readonly conversation: (
      id: Record.ConversationId,
    ) => Effect.Effect<Option.Option<Record.Conversation>, StorageError>
    /**
     * Returns the requested visible entry, or None when it is absent.
     */
    readonly entry: (
      id: Record.EntryId,
      conversationId?: Record.ConversationId,
    ) => Effect.Effect<
      Option.Option<{
        /**
         * Returns the requested visible entry, or None when it is absent.
         */
        readonly entry: Record.Entry
        readonly commitSeq: Record.Seq
      }>,
      StorageError
    >
    /**
     * Returns the requested domain task, or None when it is absent.
     */
    readonly task: (id: Record.TaskId) => Effect.Effect<Option.Option<Record.Task>, StorageError>
    /**
     * Returns the requested submission, or None when it is absent.
     */
    readonly submission: (
      id: Record.SubmissionId,
    ) => Effect.Effect<Option.Option<Record.Submission>, StorageError>
    /**
     * Finds the submission admitted under a conversation and stable request identity.
     */
    readonly submissionByRequest: Transaction['submissionByRequest']
    /**
     * Finds the newest reset or compaction marker at or before the optional entry cutoff.
     */
    readonly latestHeadMarker: Transaction['latestHeadMarker']
    /**
     * Scans matching conversations using a positive page size and optional continuation cursor.
     */
    readonly scanConversations: Transaction['scanConversations']
    /**
     * Scans visible conversation history in descending entry order within the requested bounds.
     */
    readonly scanEntries: Transaction['scanEntries']
    /**
     * Scans domain tasks matching the supplied filters and continuation cursor.
     */
    readonly scanTasks: Transaction['scanTasks']
    /**
     * Scans admitted submissions matching the supplied filters and continuation cursor.
     */
    readonly scanSubmissions: Transaction['scanSubmissions']
    /**
     * Scans document records matching a scope, cutoff and optional kind.
     */
    readonly scanDocuments: (
      query: DocumentQuery,
      limit: number,
      cursor?: Record.Cursor,
    ) => Effect.Effect<Record.Page<Record.Document>, StorageError>
    /**
     * Finds the live or historically selected document record at a logical address.
     */
    readonly findDocument: (
      address: Record.Address,
      at?: Record.Point,
    ) => Effect.Effect<Option.Option<Record.Document>, StorageError>
    /**
     * Reads the selected persisted document incarnation and cutoff as an untyped snapshot.
     */
    readonly document: (
      id: Record.DocumentId,
      at?: Record.Point,
    ) => Effect.Effect<Option.Option<Document.Document.Snapshot>, StorageError>
    /**
     * Admission-closure flag sealed at the start of owning Scope release, before handler or storage cleanup finishes.
     */
    readonly isClosed: Effect.Effect<boolean>
    /**
     * Registers an owner-local body cleanup in the invocation caller Scope.
     */
    readonly onClose: (
      cleanup: Effect.Effect<void>,
    ) => Effect.Effect<void, StorageError, Scope.Scope>
    /**
     * Observation of both persistent cleanup receipts after Scope release; cancelling only abandons this wait.
     */
    readonly awaitClosed: Effect.Effect<void, StorageError>
  }
}

/**
 * Type-level contracts for `Transaction`.
 *
 */
export declare namespace Transaction {
  /**
   * Commits a transaction callback and optionally persists its replay result.
   *
   * **Details**
   *
   * Unkeyed callbacks may return arbitrary values. A key stores the JSON-safe or void result
   * atomically with all writes; replay returns that result without evaluating the callback. A
   * fingerprint rejects reuse for different content.
   *
   * **Gotchas**
   *
   * Receipt replay protects domain facts. It cannot make external network, file or process
   * actions exactly once. Keep those actions in native Activities with an explicit replay
   * policy.
   *
   * **Example** (Replaying a committed update)
   *
   * ```ts
   * import assert from 'node:assert/strict'
   * import * as Document from 'effect-harness/durable/Document'
   * import * as Session from 'effect-harness/durable/Session'
   * import * as Store from 'effect-harness/durable/Store'
   * import * as Effect from 'effect/Effect'
   * import * as Layer from 'effect/Layer'
   * import * as Option from 'effect/Option'
   * import * as Schema from 'effect/Schema'
   *
   * const Counter = Document.defineUnsafe({
   *   kind: 'counter', version: 1, scope: 'session',
   *   schema: Schema.Struct({ count: Schema.Number }),
   *   initial: () => ({ count: 0 }),
   * })
   * const sessions = Session.layer.pipe(Layer.provide(Store.layerMemory))
   * const program = Effect.gen(function* () {
   *   const session = yield* Session.Session
   *   const increment = session.transaction(Effect.fnUntraced(function* (tx) {
   *     const counter = yield* tx.doc(Counter)
   *     counter.count += 1
   *     return counter.count
   *   }), { key: 'increment-once' })
   *
   *   assert.equal(yield* increment, 1)
   *   assert.equal(yield* increment, 1)
   *   const saved = yield* session.snapshot(Counter)
   *   assert.equal(Option.getOrThrow(saved).value.count, 1)
   * }).pipe(Effect.provide(sessions))
   *
   * await Effect.runPromise(program)
   * ```
   *
   * @category models
   */
  export interface Function {
    <A, E, R>(
      change: (tx: Transaction) => Effect.Effect<A, E, R>,
      options?: Store.UnkeyedOptions,
    ): Effect.Effect<A, StorageError | E, R>
    <A extends Schema.Json | void, E, R>(
      change: (tx: Transaction) => Effect.Effect<A, E, R>,
      options: Store.CommitOptions,
    ): Effect.Effect<A, StorageError | E, R>
  }
}
