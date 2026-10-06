import * as Context from 'effect/Context'
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Scope from 'effect/Scope'
import * as Exit from 'effect/Exit'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Result from 'effect/Result'
import * as Semaphore from 'effect/Semaphore'
import * as Stream from 'effect/Stream'
import * as Document from './Document.ts'
import { address, typed } from './Document.ts'
import * as Option from 'effect/Option'
import * as Observation from './Observation.ts'
import * as Record from './Record.ts'
import {
  rejected,
  StorageError,
  Closed,
  Revoked,
  ReadAfterWrite,
  NotFound,
  Invalid,
} from './StorageError.ts'
import { Store, type CommitOptions, type UnkeyedOptions } from './Store.ts'
import {
  detached,
  detachedEffect,
  documentsInScope,
  findDocument,
  materialize,
  page,
  validate,
  visibleEntries,
} from './storage/State.ts'

export interface ConversationQuery {
  readonly ownerConversationId?: Record.ConversationId
  readonly ownerTaskId?: Record.TaskId
}
export interface EntryQuery {
  readonly conversationId: Record.ConversationId
  readonly minEntryId?: Record.EntryId
  readonly maxEntryId?: Record.EntryId
}
export type TaskQuery = Partial<
  Pick<Record.Task, 'conversationId' | 'kind' | 'abortRequested' | 'background'>
> & { readonly status?: Record.Task['state']['status'] }
export type SubmissionQuery = Partial<Pick<Record.Submission, 'conversationId' | 'status'>>
export interface DocumentQuery {
  readonly scope: Record.Scope
  readonly at: Record.Point
  readonly kind?: string
}
export type Ownership =
  | { readonly kind: 'ownerless' }
  | { readonly kind: 'task'; readonly taskId: Record.TaskId }
export interface Transaction {
  readonly ensureRoot: Effect.Effect<Record.Conversation, StorageError>
  readonly mint: <S extends Schema.Constraint>(
    schema: S,
  ) => Effect.Effect<S['Type'], StorageError, S['DecodingServices']>
  readonly conversation: (
    id: Record.ConversationId,
  ) => Effect.Effect<Record.Conversation | undefined, StorageError>
  readonly entry: (id: Record.EntryId) => Effect.Effect<Record.Entry | undefined, StorageError>
  readonly task: (id: Record.TaskId) => Effect.Effect<Record.Task | undefined, StorageError>
  readonly submission: (
    id: Record.SubmissionId,
  ) => Effect.Effect<Record.Submission | undefined, StorageError>
  readonly scanConversations: (
    query: ConversationQuery,
    limit: number,
    cursor?: Record.Cursor,
  ) => Effect.Effect<Record.Page<Record.Conversation>, StorageError>
  readonly scanEntries: (
    query: EntryQuery,
    limit: number,
    cursor?: Record.Cursor,
  ) => Effect.Effect<Record.Page<Record.Entry>, StorageError>
  readonly scanTasks: (
    query: TaskQuery,
    limit: number,
    cursor?: Record.Cursor,
  ) => Effect.Effect<Record.Page<Record.Task>, StorageError>
  readonly scanSubmissions: (
    query: SubmissionQuery,
    limit: number,
    cursor?: Record.Cursor,
  ) => Effect.Effect<Record.Page<Record.Submission>, StorageError>
  readonly submissionByRequest: (
    conversationId: Record.ConversationId,
    requestId: string,
  ) => Effect.Effect<Record.Submission | undefined, StorageError>
  readonly latestHeadMarker: (
    conversationId: Record.ConversationId,
    atOrBefore?: Record.EntryId,
  ) => Effect.Effect<Record.Entry | undefined, StorageError>
  readonly createConversation: (options: {
    readonly ownership: Ownership
  }) => Effect.Effect<Record.Conversation, StorageError>
  readonly forkConversation: (
    parent: Record.ConversationId,
    at: Record.EntryId,
    options: { readonly ownership: Ownership },
  ) => Effect.Effect<Record.Conversation, StorageError>
  readonly appendEntry: (
    conversationId: Record.ConversationId,
    draft: Record.EntryDraft,
  ) => Effect.Effect<Record.Entry, StorageError>
  readonly createTask: (
    value: Omit<Record.Task, 'id'>,
  ) => Effect.Effect<Record.TaskId, StorageError>
  readonly createSubmission: (
    value: Record.SubmissionCreate,
  ) => Effect.Effect<Record.Submission, StorageError>
  readonly placeSubmission: (
    id: Record.SubmissionId,
    entry: Record.EntryId,
  ) => Effect.Effect<void, StorageError>
  readonly settleSubmission: (
    id: Record.SubmissionId,
    value:
      | { readonly status: 'done'; readonly answer: Record.EntryId }
      | { readonly status: 'unanswered'; readonly reason: string; readonly detail?: Record.Json },
  ) => Effect.Effect<void, StorageError>
  readonly write: (value: Record.Write) => Effect.Effect<void, StorageError>
  readonly doc: <T extends Record.JsonObject>(
    token: Document.Document<T>,
    target?: Document.Target,
  ) => Effect.Effect<Document.Draft<T>, StorageError>
  readonly retire: <T extends Record.JsonObject>(
    token: Document.Document<T>,
    target?: Document.Target,
  ) => Effect.Effect<void, StorageError>
}
export interface TransactionFunction {
  <A, E, R>(
    change: (tx: Transaction) => Effect.Effect<A, E, R>,
    options?: UnkeyedOptions,
  ): Effect.Effect<A, StorageError | E, R>
  <A extends Record.Json | void, E, R>(
    change: (tx: Transaction) => Effect.Effect<A, E, R>,
    options: CommitOptions,
  ): Effect.Effect<A, StorageError | E, R>
}
export interface Service {
  /** Physically committed facts, fenced from ambient SQL transaction previews. */
  readonly committed: Effect.Effect<Record.State, StorageError>
  readonly root: (
    initialize?: (tx: Transaction) => Effect.Effect<void, StorageError>,
  ) => Effect.Effect<Record.Conversation, StorageError>
  /** Apply the host's recovery initializer to an existing conversation atomically. */
  readonly initialize: (conversationId: Record.ConversationId) => Effect.Effect<void, StorageError>
  readonly transaction: TransactionFunction
  readonly snapshot: <T extends Record.JsonObject>(
    token: Document.Document<T>,
    target?: Document.Target,
  ) => Effect.Effect<Document.Snapshot<T> | undefined, StorageError>
  readonly snapshotAsOf: <T extends Record.JsonObject>(
    token: Document.Document<T>,
    conversationId: Record.ConversationId,
    at: Record.EntryId,
    target?: Omit<Document.Target, 'owner'>,
  ) => Effect.Effect<Document.Snapshot<T> | undefined, StorageError>
  readonly state: <T extends Record.JsonObject>(
    token: Document.Document<T>,
    target?: Document.Target,
  ) => Effect.Effect<Observation.State<T> | undefined, StorageError, import('effect/Scope').Scope>
  readonly watchDoc: <T extends Record.JsonObject>(
    token: Document.Document<T>,
    target?: Document.Target,
  ) => Effect.Effect<Observation.Watch<T> | undefined, StorageError, import('effect/Scope').Scope>
  readonly commits: Stream.Stream<Record.Frame, StorageError>
  readonly conversation: (
    id: Record.ConversationId,
  ) => Effect.Effect<Record.Conversation | undefined, StorageError>
  readonly entry: (
    id: Record.EntryId,
    conversationId?: Record.ConversationId,
  ) => Effect.Effect<
    { readonly entry: Record.Entry; readonly commitSeq: Record.Seq } | undefined,
    StorageError
  >
  readonly task: (id: Record.TaskId) => Effect.Effect<Record.Task | undefined, StorageError>
  readonly submission: (
    id: Record.SubmissionId,
  ) => Effect.Effect<Record.Submission | undefined, StorageError>
  readonly submissionByRequest: Transaction['submissionByRequest']
  readonly latestHeadMarker: Transaction['latestHeadMarker']
  readonly scanConversations: Transaction['scanConversations']
  readonly scanEntries: Transaction['scanEntries']
  readonly scanTasks: Transaction['scanTasks']
  readonly scanSubmissions: Transaction['scanSubmissions']
  readonly scanDocuments: (
    query: DocumentQuery,
    limit: number,
    cursor?: Record.Cursor,
  ) => Effect.Effect<Record.Page<Record.Document>, StorageError>
  readonly findDocument: (
    address: Record.Address,
    at?: Record.Point,
  ) => Effect.Effect<Record.Document | undefined, StorageError>
  readonly document: (
    id: Record.DocumentId,
    at?: Record.Point,
  ) => Effect.Effect<Document.Snapshot | undefined, StorageError>
  /** Sealed at the start of close, before handler or storage cleanup finishes. */
  readonly isClosed: Effect.Effect<boolean>
  /** Register an owner-local body cleanup; unregister when the invocation ends. */
  readonly onClose: (cleanup: Effect.Effect<void>) => Effect.Effect<() => void, StorageError>
  /** Join shared cleanup; cancelling a caller only cancels its wait. */
  readonly close: Effect.Effect<void, StorageError>
}
/** Optional atomically executed initializer for every newly created conversation. */
export class CreationHook extends Context.Service<
  CreationHook,
  {
    readonly run: (
      tx: Transaction,
      conversation: Record.Conversation,
    ) => Effect.Effect<void, StorageError>
    readonly recover?: (
      tx: Transaction,
      conversation: Record.Conversation,
    ) => Effect.Effect<void, StorageError>
  }
>()('@effect-harness/durable/CreationHook') {}
export class Session extends Context.Service<Session, Service>()(
  '@effect-harness/durable/Session',
) {}

const conversationPage = (
  state: Record.State,
  query: ConversationQuery,
  limit: number,
  cursor?: Record.Cursor,
) =>
  page(
    state.conversations.filter(
      (item) =>
        (query.ownerConversationId === undefined ||
          item.owner?.conversationId === query.ownerConversationId) &&
        (query.ownerTaskId === undefined || item.owner?.taskId === query.ownerTaskId),
    ),
    limit,
    cursor,
  )
const taskPage = (state: Record.State, query: TaskQuery, limit: number, cursor?: Record.Cursor) =>
  page(
    state.tasks.filter(
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
  query: SubmissionQuery,
  limit: number,
  cursor?: Record.Cursor,
) =>
  page(
    state.submissions.filter(
      (item) =>
        (query.conversationId === undefined || item.conversationId === query.conversationId) &&
        (query.status === undefined || item.status === query.status),
    ),
    limit,
    cursor,
  )
const entryPage = Effect.fnUntraced(function* (
  state: Record.State,
  query: EntryQuery,
  limit: number,
  cursor?: Record.Cursor,
) {
  if (
    !Number.isSafeInteger(limit) ||
    limit <= 0 ||
    (cursor !== undefined && !Number.isSafeInteger(cursor.after))
  )
    return yield* rejected('Invalid entry scan size or cursor')
  const entries = (yield* visibleEntries(
    state,
    query.conversationId,
    query.minEntryId,
    query.maxEntryId,
  )).filter((item) => cursor === undefined || item.id < cursor.after)
  const items = entries.slice(0, limit)
  const last = items.at(-1)
  return {
    items,
    ...(entries.length > limit && last !== undefined ? { next: { after: last.id } } : {}),
  }
})
class DraftMutationError extends Schema.TaggedError<DraftMutationError>(
  '@effect-harness/durable/Session/DraftMutationError',
)('DraftMutationError', { message: Schema.String, cause: Schema.optionalKey(Schema.Defect()) }) {}
// effect-review-allow P2-no-throw-in-effect-code, P1-throw-only-in-unsafe-orthrow:
// Native synchronous Proxy traps cannot return Effect/Result failures. Only this private
// sentinel is thrown for invalid draft operations and translated by transaction's
// targeted catchDefect. All unrelated defects retain their original Cause.
const draftValue = (input: unknown): Record.Json =>
  Result.match(
    Result.try({
      try: () => Schema.decodeUnknownSync(Schema.Json)(input),
      catch: (cause) => new DraftMutationError({ message: 'Document value must be JSON', cause }),
    }),
    {
      onSuccess: (value) => value,
      onFailure: (error) => {
        throw error
      },
    },
  )
const cloneDraft = <A>(value: A): A =>
  Result.match(detached(value), {
    onSuccess: (value) => value,
    onFailure: (error) => {
      throw new DraftMutationError({ message: error.message, cause: error.cause })
    },
  })
interface Acquired {
  readonly definition: {
    readonly version: number
    readonly history?: 'latest' | 'rewindable'
    readonly fork?: 'asOf' | 'current' | 'initial'
  }
  readonly prepare: Effect.Effect<Record.JsonObject, StorageError>
  readonly checkpoint: Effect.Effect<boolean, StorageError>
  readonly address: Record.Address
  readonly id: Record.DocumentId
  readonly stored?: Document.Snapshot
  readonly staged?: Record.DocumentCreate
  readonly value: Record.JsonObject
  readonly ops: Array<Record.Op>
  retire: boolean
}

const draft = <T extends Record.JsonObject>(
  value: T,
  active: () => boolean,
  ops: Array<Record.Op>,
): T => {
  const proxies = new WeakMap<object, object>()
  const wrap = (object: object, path: ReadonlyArray<string | number>): object => {
    const previous = proxies.get(object)
    if (previous !== undefined) return previous
    const check = () => {
      if (!active()) throw new DraftMutationError({ message: 'Document draft is revoked' })
    }
    const proxy = new Proxy(object, {
      get(target, key, receiver) {
        check()
        const item: unknown = Reflect.get(target, key, receiver)
        if (item !== null && typeof item === 'object') {
          if (!Object.hasOwn(target, key)) return undefined
          return wrap(item, [...path, Array.isArray(target) ? Number(key) : String(key)])
        }
        return item
      },
      set(target, key, item: unknown) {
        check()
        if (Array.isArray(target) && key === 'length') {
          if (
            typeof item !== 'number' ||
            !Number.isSafeInteger(item) ||
            item < 0 ||
            item > 4294967295
          )
            throw new DraftMutationError({ message: 'Invalid array length' })
          Reflect.set(target, key, item)
          ops.push(['replace', cloneDraft(value)])
          return true
        }
        if (typeof key === 'symbol')
          throw new DraftMutationError({ message: 'Symbol document keys are not JSON' })
        const valid = draftValue(item)
        const segment = Array.isArray(target) ? Number(key) : String(key)
        Object.defineProperty(target, key, {
          value: cloneDraft(valid),
          enumerable: true,
          configurable: true,
          writable: true,
        })
        ops.push(['set', [...path, segment], cloneDraft(valid)])
        return true
      },
      deleteProperty(target, key) {
        check()
        const segment = Array.isArray(target) ? Number(key) : String(key)
        Reflect.deleteProperty(target, key)
        ops.push(['delete', [...path, segment]])
        return true
      },
      defineProperty(target, key, descriptor) {
        check()
        if (
          typeof key === 'symbol' ||
          !('value' in descriptor) ||
          descriptor.get !== undefined ||
          descriptor.set !== undefined ||
          descriptor.enumerable === false ||
          descriptor.configurable === false ||
          descriptor.writable === false
        )
          throw new DraftMutationError({
            message: 'Document descriptors must be writable enumerable JSON data',
          })
        const valid = draftValue(descriptor.value)
        if (Array.isArray(target) && key === 'length') {
          if (
            typeof valid !== 'number' ||
            !Number.isSafeInteger(valid) ||
            valid < 0 ||
            valid > 4294967295
          )
            throw new DraftMutationError({ message: 'Invalid array length' })
          Reflect.set(target, key, valid)
          ops.push(['replace', cloneDraft(value)])
          return true
        }
        Object.defineProperty(target, key, {
          value: cloneDraft(valid),
          enumerable: true,
          configurable: true,
          writable: true,
        })
        ops.push([
          'set',
          [...path, Array.isArray(target) ? Number(key) : String(key)],
          cloneDraft(valid),
        ])
        return true
      },
      setPrototypeOf() {
        check()
        throw new DraftMutationError({ message: 'Document prototypes cannot change' })
      },
      preventExtensions() {
        check()
        throw new DraftMutationError({ message: 'Document drafts must remain mutable' })
      },
      ownKeys(target) {
        check()
        return Reflect.ownKeys(target)
      },
      getOwnPropertyDescriptor(target, key) {
        check()
        return Reflect.getOwnPropertyDescriptor(target, key)
      },
    })
    proxies.set(object, proxy)
    return proxy
  }
  // Proxy preserves the target shape. It is intentionally revoked by transaction scope.
  return wrap(value, []) as T
}

export const make = Effect.fnUntraced(function* () {
  const underlying = yield* Store
  const cleanupScope = yield* Scope.make()
  yield* Effect.addFinalizer((exit) => Scope.close(cleanupScope, exit))
  let sealed = false
  let closing: Fiber.Fiber<void, StorageError> | undefined
  const cleanups = new Set<Effect.Effect<void>>()
  const usable = Effect.suspend(() =>
    sealed ? Effect.fail(rejected('Session is closed', Closed)) : Effect.void,
  )
  const onClose: Service['onClose'] = (cleanup) =>
    Effect.sync(() => {
      if (sealed) return undefined
      cleanups.add(cleanup)
      return () => {
        cleanups.delete(cleanup)
      }
    }).pipe(
      Effect.filterOrFail(
        (unregister) => unregister !== undefined,
        () => rejected('Session is closed', Closed),
      ),
    )
  const close = Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      if (closing === undefined) {
        sealed = true
        const admitted = [...cleanups].reverse()
        cleanups.clear()
        closing = yield* Effect.gen(function* () {
          // Stop direct Store observers while retaining backend resources until
          // handler finalizers and already admitted operations have completed.
          const sealExit = yield* underlying.seal.pipe(Effect.exit)
          // Match normal Scope ordering: suspend inner Activity bodies first,
          // and join every registered resource even if another cleanup defects.
          const exits = yield* Effect.forEach(admitted, (cleanup) => Effect.exit(cleanup))
          const backendExit = yield* underlying.close.pipe(Effect.exit)
          let failure: Cause.Cause<StorageError> | undefined
          for (const exit of [sealExit, ...exits, backendExit])
            if (Exit.isFailure(exit))
              failure = failure === undefined ? exit.cause : Cause.combine(failure, exit.cause)
          if (failure !== undefined) return yield* Effect.failCause(failure)
        }).pipe(Effect.uninterruptible, Effect.forkIn(cleanupScope))
      }
      yield* restore(Fiber.join(closing))
    }),
  )
  yield* Effect.addFinalizer(() => close.pipe(Effect.orDie))
  // Seal Session admission before body cleanup; already admitted Store operations
  // retain their normal transaction/read lifetime until the underlying close.
  // The Store validates keyed JSON results; this private forwarding signature
  // preserves both public overloads without erasing callback environments.
  const underlyingTransact = underlying.transact as <A, E, R>(
    change: (state: Record.State) => Effect.Effect<import('./Store.ts').Candidate<A>, E, R>,
    options?: CommitOptions,
  ) => Effect.Effect<A, StorageError | E, R>
  const store = Store.of({
    ...underlying,
    read: usable.pipe(Effect.andThen(underlying.read)),
    committed: usable.pipe(Effect.andThen(underlying.committed)),
    transact: <A, E, R>(
      change: (state: Record.State) => Effect.Effect<import('./Store.ts').Candidate<A>, E, R>,
      options?: CommitOptions,
    ) => usable.pipe(Effect.andThen(underlyingTransact(change, options))),
    journal: (after) => usable.pipe(Effect.andThen(underlying.journal(after))),
  })
  const migrationCache = Document.makeMigrationCache()
  const creationHook = Option.getOrUndefined(yield* Effect.serviceOption(CreationHook))
  // Public overloads constrain keyed results; runtime validation is authoritative at the Store boundary.
  const transact: <A, E, R>(
    change: (state: Record.State) => Effect.Effect<import('./Store.ts').Candidate<A>, E, R>,
    options?: CommitOptions,
  ) => Effect.Effect<A, StorageError | E, R> = store.transact as <A, E, R>(
    change: (state: Record.State) => Effect.Effect<import('./Store.ts').Candidate<A>, E, R>,
    options?: CommitOptions,
  ) => Effect.Effect<A, StorageError | E, R>
  const transaction = <A, E, R>(
    change: (tx: Transaction) => Effect.Effect<A, E, R>,
    options?: CommitOptions,
  ): Effect.Effect<A, StorageError | E, R> =>
    transact(
      (original) =>
        Effect.gen(function* () {
          let active = true
          let tableWritten = false
          let nextId = original.nextId
          const mintPermit = yield* Semaphore.make(1)
          const writes: Array<Record.Write> = []
          const acquired = new Map<string, Acquired>()
          const forkDocuments = new Set<Record.DocumentId>()
          const forkParents = new Set<Record.ConversationId>()
          let pendingOperations = 0
          const track = <A, R>(effect: Effect.Effect<A, StorageError, R>) =>
            Effect.suspend(() => {
              pendingOperations++
              return effect.pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    pendingOperations--
                  }),
                ),
              )
            })
          const localConversations = new Map(original.conversations.map((item) => [item.id, item]))
          const localTasks = new Map(original.tasks.map((item) => [item.id, item]))
          const localSubmissions = new Map(original.submissions.map((item) => [item.id, item]))
          const abortingAncestor = (conversationId: Record.ConversationId): boolean => {
            const seen = new Set<Record.TaskId>()
            let owner = localConversations.get(conversationId)?.owner?.taskId
            while (owner !== undefined && !seen.has(owner)) {
              seen.add(owner)
              const task = localTasks.get(owner)
              if (task === undefined) return false
              if (task.state.status === 'terminal') return false
              if (task.abortRequested) return true
              if (task.background) return false
              owner = task.owner ?? localConversations.get(task.conversationId)?.owner?.taskId
            }
            return false
          }
          const retiredAddresses = new Set<string>()
          const acquisitions = new Map<string, Effect.Effect<Record.JsonObject, StorageError>>()
          const open = Effect.suspend(() =>
            active ? Effect.void : Effect.fail(rejected('Transaction is revoked', Revoked)),
          )
          const read = Effect.fnUntraced(function* () {
            yield* open
            if (tableWritten)
              return yield* rejected(
                'Table reads after the first table write are forbidden',
                ReadAfterWrite,
              )
          })
          const mint = Effect.fnUntraced(function* <S extends Schema.Constraint>(schema: S) {
            yield* open
            const id = yield* validate(schema, nextId)
            nextId++
            return id
          }, Semaphore.withPermit(mintPermit))
          const write = Effect.fnUntraced(function* (value: Record.Write) {
            yield* open
            let valid = yield* validate(Record.Write, value)
            if (valid.type === 'task' && valid.value.state.status === 'terminal') {
              const { memos: _memos, ...task } = valid.value
              valid = { type: 'task', value: task }
            }
            if (valid.type === 'task') {
              const previous = localTasks.get(valid.value.id)
              if (
                previous !== undefined &&
                (previous.state.status === 'terminal' ||
                  previous.conversationId !== valid.value.conversationId)
              )
                return yield* rejected('Task is terminal or cannot change conversations')
            }
            writes.push(yield* detachedEffect(valid))
            if (valid.type === 'conversation') localConversations.set(valid.value.id, valid.value)
            if (valid.type === 'task') localTasks.set(valid.value.id, valid.value)
            if (valid.type === 'submission') localSubmissions.set(valid.value.id, valid.value)
            if (
              valid.type === 'conversation' ||
              valid.type === 'entry' ||
              valid.type === 'task' ||
              valid.type === 'submission'
            )
              tableWritten = true
          })
          const owner = Effect.fnUntraced(function* (ownership: Ownership) {
            if (ownership.kind === 'ownerless') return {}
            const task = localTasks.get(ownership.taskId)
            if (
              task === undefined ||
              task.abortRequested ||
              task.state.status === 'terminal' ||
              task.state.status === 'completing'
            )
              return yield* rejected('Conversation owner must be a live task')
            return { owner: { taskId: task.id, conversationId: task.conversationId } }
          })
          const doc = <T extends Record.JsonObject>(
            token: Document.Document<T>,
            target: Document.Target = {},
          ): Effect.Effect<Document.Draft<T>, StorageError> =>
            Effect.gen(function* () {
              yield* open
              const logical = yield* address(token, target)
              const key = Record.addressKey(logical)
              const cached = acquisitions.get(key)
              if (cached !== undefined) return (yield* cached) as Document.Draft<T>
              const acquisition = yield* Effect.cached(
                Effect.gen(function* () {
                  yield* open
                  if (
                    logical.scope.kind === 'conversation' &&
                    !localConversations.has(logical.scope.conversationId)
                  )
                    return yield* rejected('Document conversation is absent', NotFound)
                  if (logical.scope.kind === 'task') {
                    const task = localTasks.get(logical.scope.taskId)
                    if (task === undefined || task.state.status === 'terminal')
                      return yield* rejected('Document task is absent or settled', NotFound)
                  }
                  const staged = retiredAddresses.has(key)
                    ? undefined
                    : writes.findLast(
                        (write) =>
                          (write.type === 'document.create' || write.type === 'document.copy') &&
                          Record.addressKey(write.record) === key,
                      )
                  const persisted = retiredAddresses.has(key)
                    ? undefined
                    : findDocument(original, logical, 'current')
                  let stored =
                    persisted === undefined ? undefined : yield* materialize(persisted, 'current')
                  let stagedRecord: Record.DocumentCreate | undefined
                  if (staged?.type === 'document.create' || staged?.type === 'document.copy') {
                    stagedRecord = staged.record
                    let content: Record.Content
                    if (staged.type === 'document.create') content = staged.content
                    else {
                      const source = original.documents.find(
                        (item) => item.record.id === staged.source.id,
                      )
                      if (source === undefined)
                        return yield* rejected('Staged copy source is absent', NotFound)
                      const sourceValue = yield* materialize(source, staged.source.at)
                      if (sourceValue === undefined)
                        return yield* rejected('Staged copy source is not alive', NotFound)
                      content = {
                        kind: 'base',
                        version: sourceValue.version,
                        value: sourceValue.value,
                      }
                    }
                    if (content.kind !== 'base')
                      return yield* rejected('Staged document creation requires a base')
                    stored = {
                      record: {
                        ...staged.record,
                        createdAt: yield* validate(Record.Seq, original.nextSeq),
                      },
                      version: content.version,
                      value: yield* detachedEffect(content.value),
                      deltasSinceBase: 0,
                    }
                  }
                  let value: T
                  let id: Record.DocumentId
                  if (stored === undefined) {
                    value = yield* Effect.try({
                      try: () => token.definition.initial(target.seed),
                      catch: (cause) => rejected('Document initializer failed', Invalid, cause),
                    })
                    value = yield* validate(token.definition.schema, value)
                    yield* validate(Schema.JsonObject, value)
                    id = yield* mint(Record.DocumentId)
                  } else {
                    value = (yield* typed(token, stored, migrationCache)).value
                    id = stored.record.id
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
                    prepare: validate(token.definition.schema, mutable).pipe(
                      Effect.flatMap((valid) => validate(Schema.JsonObject, valid)),
                    ),
                    checkpoint: Effect.try({
                      try: () =>
                        token.definition.checkpointWhen?.(mutable, ops, {
                          deltasSinceBase: stored?.deltasSinceBase ?? 0,
                        }) ?? false,
                      catch: (cause) => rejected('Checkpoint predicate failed', Invalid, cause),
                    }),
                    ...(stored === undefined ? {} : { stored }),
                    ...(stagedRecord === undefined ? {} : { staged: stagedRecord }),
                  }
                  acquired.set(key, record)
                  return draft(record.value, () => active, ops)
                }),
              )
              acquisitions.set(key, acquisition)
              return (yield* acquisition) as Document.Draft<T>
            })
          const rawTx: Transaction = {
            mint,
            write,
            doc,
            ensureRoot: Effect.suspend(() => {
              const existing = localConversations.get(Record.ROOT_CONVERSATION_ID)
              if (existing !== undefined) return detachedEffect(existing)
              const root = { id: Record.ROOT_CONVERSATION_ID }
              return write({ type: 'conversation', value: root }).pipe(
                Effect.andThen(creationHook?.run(tx, root) ?? Effect.void),
                Effect.as(root),
              )
            }),
            conversation: Effect.fnUntraced(function* (id) {
              yield* read()
              return yield* detachedEffect(original.conversations.find((item) => item.id === id))
            }),
            entry: Effect.fnUntraced(function* (id) {
              yield* read()
              return yield* detachedEffect(
                original.entries.find((item) => item.entry.id === id)?.entry,
              )
            }),
            task: Effect.fnUntraced(function* (id) {
              yield* read()
              return yield* detachedEffect(original.tasks.find((item) => item.id === id))
            }),
            submission: Effect.fnUntraced(function* (id) {
              yield* read()
              return yield* detachedEffect(original.submissions.find((item) => item.id === id))
            }),
            scanConversations: Effect.fnUntraced(function* (query, limit, cursor) {
              yield* read()
              return yield* conversationPage(original, query, limit, cursor)
            }),
            scanEntries: Effect.fnUntraced(function* (query, limit, cursor) {
              yield* read()
              return yield* entryPage(original, query, limit, cursor)
            }),
            scanTasks: Effect.fnUntraced(function* (query, limit, cursor) {
              yield* read()
              return yield* taskPage(original, query, limit, cursor)
            }),
            scanSubmissions: Effect.fnUntraced(function* (query, limit, cursor) {
              yield* read()
              return yield* submissionPage(original, query, limit, cursor)
            }),
            latestHeadMarker: Effect.fnUntraced(function* (conversationId, atOrBefore) {
              yield* read()
              return (yield* visibleEntries(original, conversationId, 0, atOrBefore)).find(
                (item) => item.head !== undefined,
              )
            }),
            submissionByRequest: Effect.fnUntraced(function* (conversationId, requestId) {
              yield* read()
              return yield* detachedEffect(
                original.submissions.find(
                  (item) => item.conversationId === conversationId && item.requestId === requestId,
                ),
              )
            }),
            createConversation: Effect.fnUntraced(function* (options) {
              const ownership = yield* owner(options.ownership)
              const id = yield* mint(Record.ConversationId)
              const value = { id, ...ownership }
              yield* write({ type: 'conversation', value })
              if (creationHook !== undefined) yield* creationHook.run(tx, value)
              return yield* detachedEffect(value)
            }),
            forkConversation: Effect.fnUntraced(function* (parent, at, options) {
              yield* open
              forkParents.add(parent)
              const visible = yield* visibleEntries(original, parent)
              const entry = visible.find((item) => item.id === at)
              const committed = original.entries.find((item) => item.entry.id === at)
              if (entry === undefined || committed === undefined)
                return yield* rejected('Fork cutoff is not visible', NotFound)
              const ownership = yield* owner(options.ownership)
              const id = yield* mint(Record.ConversationId)
              const selected = new Map<
                string,
                { readonly document: Record.StoredDocument; readonly at: Record.Point }
              >()
              for (const document of original.documents) {
                const record = document.record
                if (record.scope.kind !== 'conversation') continue
                if (
                  record.fork === 'asOf' &&
                  record.scope.conversationId === entry.conversationId &&
                  Record.isAlive(record, committed.commitSeq)
                )
                  selected.set(
                    Record.addressKey({
                      ...record,
                      scope: { kind: 'conversation', conversationId: id },
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
                    scope: { kind: 'conversation', conversationId: id },
                  })
                  if (selected.has(key))
                    return yield* rejected('Fork selects ambiguous document source')
                  selected.set(key, { document, at: 'current' })
                }
              }
              for (const source of selected.values()) {
                forkDocuments.add(source.document.record.id)
                const documentId = yield* mint(Record.DocumentId)
                const {
                  createdAt: _created,
                  retiredAt: _retired,
                  ...record
                } = source.document.record
                yield* write({
                  type: 'document.copy',
                  record: {
                    ...record,
                    id: documentId,
                    scope: { kind: 'conversation', conversationId: id },
                  },
                  source: { id: source.document.record.id, at: source.at },
                })
              }
              const value = { id, parent: { conversationId: parent, at }, ...ownership }
              yield* write({ type: 'conversation', value })
              if (creationHook !== undefined) yield* creationHook.run(tx, value)
              return yield* detachedEffect(value)
            }),
            appendEntry: Effect.fnUntraced(function* (conversationId, input) {
              yield* open
              if (!localConversations.has(conversationId))
                return yield* rejected('Entry conversation is absent', NotFound)
              const id = yield* mint(Record.EntryId)
              const { head, ...draftValue } = input
              const value = {
                ...draftValue,
                id,
                conversationId,
                ...(head === undefined ? {} : { head: head === 'self' ? id : head }),
              }
              yield* write({ type: 'entry', value })
              return yield* detachedEffect(value)
            }),
            createTask: Effect.fnUntraced(function* (input) {
              yield* open
              if (!localConversations.has(input.conversationId))
                return yield* rejected('Task conversation is absent', NotFound)
              if (abortingAncestor(input.conversationId))
                return yield* rejected('Task conversation has an aborting ancestor')
              if (input.owner !== undefined) {
                const parent = localTasks.get(input.owner)
                if (
                  parent === undefined ||
                  parent.abortRequested ||
                  parent.conversationId !== input.conversationId ||
                  parent.state.status === 'terminal' ||
                  parent.state.status === 'completing' ||
                  input.background
                )
                  return yield* rejected('Invalid task owner')
              }
              const id = yield* mint(Record.TaskId)
              yield* write({ type: 'task', value: { ...input, id } })
              return id
            }),
            createSubmission: Effect.fnUntraced(function* (input) {
              yield* open
              if (!localConversations.has(input.conversationId))
                return yield* rejected('Submission conversation is absent', NotFound)
              if (abortingAncestor(input.conversationId))
                return yield* rejected('Submission conversation has an aborting ancestor')
              const id = yield* mint(Record.SubmissionId)
              const value = { ...input, id }
              yield* write({ type: 'submission', value })
              return yield* detachedEffect(value)
            }),
            placeSubmission: Effect.fnUntraced(function* (id, entry) {
              yield* open
              const current = localSubmissions.get(id)
              if (current === undefined) return yield* rejected('Submission is absent', NotFound)
              if (current.status === 'done' || current.status === 'unanswered') return
              if (current.status !== 'queued')
                return yield* rejected('Only queued submissions may be placed')
              const value: Record.Submission =
                current.type === 'input'
                  ? { ...current, entry, status: 'placed' }
                  : { ...current, entry, status: 'done' }
              yield* write({ type: 'submission', value })
            }),
            settleSubmission: Effect.fnUntraced(function* (id, settlement) {
              yield* open
              const current = localSubmissions.get(id)
              if (current === undefined) return yield* rejected('Submission is absent', NotFound)
              if (current.status === 'done' || current.status === 'unanswered') return
              if (
                settlement.status === 'done' &&
                (current.type !== 'input' || current.status !== 'placed')
              )
                return yield* rejected('Only placed input may be answered')
              const value = yield* validate(Record.Submission, { ...current, ...settlement })
              yield* write({ type: 'submission', value })
            }),
            retire: Effect.fnUntraced(function* (token, target = {}) {
              yield* open
              const logical = yield* address(token, target)
              const key = Record.addressKey(logical)
              retiredAddresses.add(key)
              const item = acquired.get(key)
              if (item !== undefined) {
                item.retire = true
                acquisitions.delete(key)
                acquired.delete(key)
                acquired.set(`${key}\0retired:${item.id}`, item)
                return
              }
              const stored = findDocument(original, logical, 'current')
              if (
                stored !== undefined &&
                !writes.some(
                  (write) => write.type === 'document.retire' && write.id === stored.record.id,
                )
              )
                yield* write({ type: 'document.retire', id: stored.record.id })
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
                ? Effect.fail(rejected(defect.message, Invalid, defect.cause))
                : Effect.die(defect),
            ),
            Effect.ensuring(
              Effect.sync(() => {
                active = false
              }),
            ),
          )
          if (pendingOperations !== 0)
            return yield* rejected('Transaction callback settled before its pending operations')
          for (const item of acquired.values()) {
            const definition = item.definition
            const value = yield* item.prepare
            if (item.staged !== undefined) {
              const previousIndex = writes.findIndex(
                (write) =>
                  (write.type === 'document.create' || write.type === 'document.copy') &&
                  write.record.id === item.id,
              )
              if (previousIndex >= 0) writes.splice(previousIndex, 1)
              writes.push({
                type: 'document.create',
                record: item.staged,
                content: { kind: 'base', version: definition.version, value },
              })
            } else if (item.stored === undefined) {
              yield* validate(Record.DocumentId, item.id)
              writes.push({
                type: 'document.create',
                record: {
                  ...item.address,
                  id: item.id,
                  ...(definition.history === undefined ? {} : { history: definition.history }),
                  ...(definition.fork === undefined ? {} : { fork: definition.fork }),
                },
                content: { kind: 'base', version: definition.version, value },
              })
            } else if (item.stored.version !== definition.version)
              writes.push({
                type: 'document.change',
                id: item.id,
                content: { kind: 'base', version: definition.version, value },
              })
            else if (item.ops.length > 0) {
              const checkpoint = yield* item.checkpoint
              // Preserve the exact draft operation batch unless array methods changed index structure.
              const hasArrayMutation = item.ops.some(
                (op) =>
                  op[0] === 'replace' || (op[0] === 'delete' && typeof op[1].at(-1) === 'number'),
              )
              const ops: ReadonlyArray<Record.Op> = hasArrayMutation
                ? [['replace', value]]
                : item.ops
              writes.push({
                type: 'document.change',
                id: item.id,
                content: checkpoint
                  ? { kind: 'base', version: definition.version, value }
                  : { kind: 'delta', version: definition.version, ops },
                ...(checkpoint ? { publicationOps: ops } : {}),
              })
            }
            if (item.retire) writes.push({ type: 'document.retire', id: item.id })
          }
          for (const write of writes) {
            if (
              ((write.type === 'task' &&
                !original.tasks.some((task) => task.id === write.value.id)) ||
                (write.type === 'submission' &&
                  !original.submissions.some((item) => item.id === write.value.id))) &&
              abortingAncestor(write.value.conversationId)
            )
              return yield* rejected('New work cannot enter an aborting owned conversation')
            let affected: Record.DocumentCreate | undefined
            if (write.type === 'document.create') affected = write.record
            else if (write.type === 'document.change' || write.type === 'document.retire')
              affected = original.documents.find((item) => item.record.id === write.id)?.record
            if (
              (write.type === 'document.change' || write.type === 'document.retire') &&
              forkDocuments.has(write.id)
            )
              return yield* rejected(
                'Cannot change a copied fork source document in its fork transaction',
              )
            if (
              affected?.scope.kind === 'conversation' &&
              affected.fork === 'current' &&
              forkParents.has(affected.scope.conversationId)
            )
              return yield* rejected(
                'Cannot change current-policy documents in their fork transaction',
              )
            let ownerId: Record.TaskId | undefined
            if (write.type === 'conversation') ownerId = write.value.owner?.taskId
            else if (
              write.type === 'task' &&
              !original.tasks.some((task) => task.id === write.value.id)
            )
              ownerId = write.value.owner
            if (ownerId !== undefined) {
              const task = localTasks.get(ownerId)
              if (
                task === undefined ||
                task.abortRequested ||
                task.state.status === 'terminal' ||
                task.state.status === 'completing'
              )
                return yield* rejected('New owned work requires a live non-aborting owner')
            }
          }
          for (const task of localTasks.values())
            if (task.state.status === 'terminal') {
              for (const document of original.documents)
                if (
                  document.record.scope.kind === 'task' &&
                  document.record.scope.taskId === task.id &&
                  document.record.retiredAt === undefined &&
                  !writes.some(
                    (write) => write.type === 'document.retire' && write.id === document.record.id,
                  )
                )
                  writes.push({ type: 'document.retire', id: document.record.id })
              for (const write of writes)
                if (
                  write.type === 'document.create' &&
                  write.record.scope.kind === 'task' &&
                  write.record.scope.taskId === task.id &&
                  !writes.some(
                    (item) => item.type === 'document.retire' && item.id === write.record.id,
                  )
                )
                  writes.push({ type: 'document.retire', id: write.record.id })
            }
          return { state: { ...original, nextId }, writes, result }
        }),
      options,
    )
  const snapshot: Service['snapshot'] = Effect.fnUntraced(function* (token, target = {}) {
    const logical = yield* address(token, target)
    const state = yield* store.read
    const document = findDocument(state, logical, 'current')
    const snapshotValue =
      document === undefined ? undefined : yield* materialize(document, 'current')
    return snapshotValue === undefined
      ? undefined
      : yield* typed(token, snapshotValue, migrationCache)
  })
  const service = Session.of({
    committed: store.committed,
    initialize: (conversationId) =>
      transaction(
        Effect.fnUntraced(function* (tx) {
          const conversation = yield* tx.conversation(conversationId)
          if (conversation === undefined) return yield* rejected('Conversation is absent', NotFound)
          if (creationHook?.recover !== undefined) yield* creationHook.recover(tx, conversation)
        }),
      ),
    transaction,
    snapshot,
    root: (initialize) =>
      transaction(
        Effect.fnUntraced(function* (tx) {
          const existing = yield* tx.conversation(Record.ROOT_CONVERSATION_ID)
          if (existing !== undefined) return existing
          const root = yield* tx.ensureRoot
          if (initialize !== undefined) yield* initialize(tx)
          return root
        }),
      ),
    snapshotAsOf: Effect.fnUntraced(function* (token, conversationId, at, target = {}) {
      if (token.definition.scope !== 'conversation' || token.definition.history !== 'rewindable')
        return yield* rejected('Historical snapshot requires rewindable conversation document')
      const state = yield* store.read
      const visible = yield* visibleEntries(state, conversationId)
      const entry = visible.find((item) => item.id === at)
      const persisted = state.entries.find((item) => item.entry.id === at)
      if (entry === undefined || persisted === undefined)
        return yield* rejected('Historical entry is not visible', NotFound)
      const logical = yield* address(token, { ...target, owner: entry.conversationId })
      const document = findDocument(state, logical, persisted.commitSeq)
      const snapshotValue =
        document === undefined ? undefined : yield* materialize(document, persisted.commitSeq)
      return snapshotValue === undefined
        ? undefined
        : yield* typed(token, snapshotValue, migrationCache)
    }),
    state: (token, target) => Observation.state(store, token, target, migrationCache),
    watchDoc: (token, target) => Observation.watch(store, token, target, migrationCache),
    commits: Observation.commits(store),
    conversation: (id) =>
      store.read.pipe(
        Effect.flatMap((state) =>
          detachedEffect(state.conversations.find((item) => item.id === id)),
        ),
      ),
    entry: Effect.fnUntraced(function* (id, conversationId) {
      const state = yield* store.read
      if (
        conversationId !== undefined &&
        !(yield* visibleEntries(state, conversationId)).some((item) => item.id === id)
      )
        return undefined
      return yield* detachedEffect(state.entries.find((item) => item.entry.id === id))
    }),
    task: (id) =>
      store.read.pipe(
        Effect.flatMap((state) => detachedEffect(state.tasks.find((item) => item.id === id))),
      ),
    submission: (id) =>
      store.read.pipe(
        Effect.flatMap((state) => detachedEffect(state.submissions.find((item) => item.id === id))),
      ),
    submissionByRequest: (conversationId, requestId) =>
      store.read.pipe(
        Effect.flatMap((state) =>
          detachedEffect(
            state.submissions.find(
              (item) => item.conversationId === conversationId && item.requestId === requestId,
            ),
          ),
        ),
      ),
    latestHeadMarker: Effect.fnUntraced(function* (conversationId, atOrBefore) {
      const state = yield* store.read
      return (yield* visibleEntries(state, conversationId, 0, atOrBefore)).find(
        (item) => item.head !== undefined,
      )
    }),
    scanConversations: (query, limit, cursor) =>
      store.read.pipe(Effect.flatMap((state) => conversationPage(state, query, limit, cursor))),
    scanEntries: (query, limit, cursor) =>
      store.read.pipe(Effect.flatMap((state) => entryPage(state, query, limit, cursor))),
    scanTasks: (query, limit, cursor) =>
      store.read.pipe(Effect.flatMap((state) => taskPage(state, query, limit, cursor))),
    scanSubmissions: (query, limit, cursor) =>
      store.read.pipe(Effect.flatMap((state) => submissionPage(state, query, limit, cursor))),
    scanDocuments: (query, limit, cursor) =>
      store.read.pipe(
        Effect.flatMap((state) =>
          page(
            documentsInScope(state, query.scope, query.at)
              .map((item) => item.record)
              .filter((item) => query.kind === undefined || item.kind === query.kind),
            limit,
            cursor,
          ),
        ),
      ),
    findDocument: (logical, at = 'current') =>
      store.read.pipe(
        Effect.flatMap((state) => detachedEffect(findDocument(state, logical, at)?.record)),
      ),
    document: Effect.fnUntraced(function* (id, at = 'current') {
      const state = yield* store.read
      const document = state.documents.find((item) => item.record.id === id)
      return document === undefined ? undefined : yield* materialize(document, at)
    }),
    isClosed: Effect.sync(() => sealed),
    onClose,
    close,
  })
  return service
})
export const layer = Layer.effect(Session, make())
