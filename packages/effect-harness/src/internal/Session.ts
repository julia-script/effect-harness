/** Serialized domain transactions over record-oriented persistence. */
import * as Context from 'effect/Context'
import * as Data from 'effect/Data'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as PubSub from 'effect/PubSub'
import * as Predicate from 'effect/Predicate'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import * as Semaphore from 'effect/Semaphore'
import * as Stream from 'effect/Stream'
import * as Arr from 'effect/Array'
import * as Result from 'effect/Result'
import * as Function from 'effect/Function'
import * as Record from '../Record.ts'
import * as Document from '../Document.ts'
import * as Persistence from '../Persistence.ts'
import {
  ClosedError,
  ConflictError,
  InvalidError,
  NotFoundError,
  RevokedError,
  rejected,
  isStorageError,
  type StorageError,
} from '../StorageError.ts'
import { applyOps, detached, detachedEffect, validate } from '../internal/records.ts'

export type Ownership =
  | { readonly _tag: 'none' }
  | { readonly _tag: 'task'; readonly taskId: Record.TaskId }
export const Ownership = Data.taggedEnum<Ownership>()
export interface Transaction {
  readonly ensureRoot: Effect.Effect<Record.Conversation, StorageError>
  readonly mint: <S extends Schema.Constraint>(
    schema: S,
  ) => Effect.Effect<S['Type'], StorageError, S['DecodingServices']>
  readonly conversation: Persistence.Service['conversation']
  readonly entry: Persistence.Service['entry']
  readonly task: Persistence.Service['task']
  readonly submission: Persistence.Service['submission']
  readonly submissionByRequest: Persistence.Service['submissionByRequest']
  readonly scanConversations: Persistence.Service['scanConversations']
  readonly scanEntries: Persistence.Service['scanEntries']
  readonly scanTasks: Persistence.Service['scanTasks']
  readonly scanSubmissions: Persistence.Service['scanSubmissions']
  readonly scanDocuments: Persistence.Service['scanDocuments']
  readonly tasks: (
    query?: Persistence.TaskQuery,
  ) => Effect.Effect<ReadonlyArray<Record.Task>, StorageError>
  readonly latestHeadMarker: (
    conversationId: Record.ConversationId,
    atOrBefore?: Record.EntryId,
  ) => Effect.Effect<Option.Option<Record.Entry>, StorageError>
  readonly createConversation: (options: {
    readonly ownership: Ownership
  }) => Effect.Effect<Record.Conversation, StorageError>
  readonly forkConversation: (
    parent: Record.ConversationId,
    at: Record.EntryId,
    options: { readonly ownership: Ownership },
  ) => Effect.Effect<Record.Conversation, StorageError>
  readonly appendEntry: (
    id: Record.ConversationId,
    draft: Record.Entry.Draft,
  ) => Effect.Effect<Record.Entry, StorageError>
  readonly createTask: (
    value: Omit<Record.Task, 'id'>,
  ) => Effect.Effect<Record.TaskId, StorageError>
  readonly createSubmission: (
    value: Record.Submission.Create,
  ) => Effect.Effect<Record.Submission, StorageError>
  readonly placeSubmission: (
    id: Record.SubmissionId,
    entry: Record.EntryId,
  ) => Effect.Effect<void, StorageError>
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
  readonly write: (value: Record.Write) => Effect.Effect<void, StorageError>
  readonly putTask: (value: Record.Task) => Effect.Effect<void, StorageError>
  readonly allocateTaskId: Effect.Effect<Record.TaskId, StorageError>
  readonly doc: <T extends object>(
    token: Document.Document<T>,
    target?: Document.Document.Target,
  ) => Effect.Effect<Document.Document.Draft<T>, StorageError>
  readonly retire: <T extends object>(
    token: Document.Document<T>,
    target?: Document.Document.Target,
  ) => Effect.Effect<void, StorageError>
}
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
>()('effect-harness/internal/Session/CreationHook') {}
export class Session extends Context.Service<Session, Service>()(
  'effect-harness/internal/Session',
) {}
export interface Service extends Omit<Persistence.Service, 'commit'> {
  readonly transaction: <A, E, R>(
    change: (tx: Transaction) => Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | StorageError, R>
  readonly root: Effect.Effect<Record.Conversation, StorageError>
  readonly initialize: Effect.Effect<void, StorageError>
  readonly commits: Stream.Stream<Record.Frame, StorageError>
  readonly snapshot: <T extends object>(
    token: Document.Document<T>,
    target?: Document.Document.Target,
  ) => Effect.Effect<Option.Option<Document.Document.Snapshot<T>>, StorageError>
  readonly snapshotAsOf: <T extends object>(
    token: Document.Document<T>,
    at: Record.EntryId,
    target?: Document.Document.Target,
  ) => Effect.Effect<Option.Option<Document.Document.Snapshot<T>>, StorageError>
  /** Acquires the subscription and snapshot on the same mutation line. */
  readonly observe: <A, E, R>(
    read: Effect.Effect<A, E, R>,
  ) => Effect.Effect<
    {
      readonly snapshot: A
      readonly revision: Record.Seq | 0
      readonly frames: Stream.Stream<Record.Frame>
    },
    E | StorageError,
    R | Scope.Scope
  >
}
export declare namespace Session {
  export type Service = import('./Session.ts').Service
}

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

interface Acquired {
  readonly id: Record.DocumentId
  readonly address: Record.Address
  readonly prepare: Effect.Effect<Schema.JsonObject, StorageError>
  readonly value: object
  readonly ops: Array<Record.Op>
  readonly version: number
  readonly stored: Option.Option<Document.Document.Snapshot>
  readonly record: Record.DocumentCreate
  readonly staged: boolean
  readonly checkpoint: Effect.Effect<boolean, StorageError>
  retire: boolean
}

const owners = new WeakMap<Persistence.Service, object>()

export const make: Effect.Effect<Service, StorageError, Persistence.Persistence | Scope.Scope> =
  Effect.gen(function* () {
    const store = yield* Persistence.Persistence
    if (owners.has(store))
      return yield* rejected('Persistence already has an owning Session', ConflictError)
    const ownerToken = {}
    owners.set(store, ownerToken)
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        if (owners.get(store) === ownerToken) owners.delete(store)
      }),
    )
    const lock = yield* Semaphore.make(1)
    // Sliding delivery never lets a slow observer hold the persistence mutation line.
    // Revision gaps tell consumers to rebuild their view from the committed records.
    const changes = yield* PubSub.sliding<Record.Frame>(128)
    const hook = yield* Effect.serviceOption(CreationHook)
    let closed = false
    const open = Effect.suspend(() =>
      closed ? Effect.fail(rejected('Session is closed', ClosedError)) : Effect.void,
    )
    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        closed = true
        yield* PubSub.shutdown(changes)
      }),
    )

    const transaction: Service['transaction'] = (change) =>
      lock.withPermit(
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            yield* open
            const metadata = yield* store.metadata
            const expectedSeq = yield* validate(Record.Seq, metadata.revision + 1)
            const mintPermit = yield* Semaphore.make(1)
            const documentPermit = yield* Semaphore.make(1)
            let nextId = metadata.nextId
            let active = true
            const writes: Array<Record.Write> = []
            const acquired = new Map<string, Acquired>()
            const acquiredOrder: Array<Acquired> = []
            const copiedSources = new Set<Record.DocumentId>()
            const valid = Effect.suspend(() =>
              active ? open : Effect.fail(rejected('Transaction has ended', RevokedError)),
            )
            const write: Transaction['write'] = Effect.fn('Session.transaction.write')(
              function* (input) {
                yield* valid
                const value = yield* validate(Record.Write, input)
                if (value._tag === 'document.retire') {
                  const item = acquiredOrder.find((item) => item.id === value.id)
                  if (item !== undefined) {
                    item.retire = true
                    return
                  }
                  if (
                    writes.some((item) => item._tag === 'document.retire' && item.id === value.id)
                  )
                    return
                }
                writes.push(yield* detachedEffect(value))
              },
            )
            const mint: Transaction['mint'] = (schema) =>
              mintPermit.withPermit(
                Effect.gen(function* () {
                  yield* valid
                  if (!Number.isSafeInteger(nextId)) return yield* rejected('ID space is exhausted')
                  const id = yield* validate(schema, nextId)
                  yield* valid
                  nextId += 1
                  return id
                }),
              )
            const conversation: Transaction['conversation'] = (id) =>
              valid.pipe(
                Effect.andThen(
                  Effect.suspend(() => {
                    const saved = writes.findLast(
                      (item) => item._tag === 'conversation' && item.value.id === id,
                    )
                    return saved?._tag === 'conversation'
                      ? detachedEffect(saved.value).pipe(Effect.asSome)
                      : store.conversation(id)
                  }),
                ),
              )
            const entry: Transaction['entry'] = (id) =>
              valid.pipe(
                Effect.andThen(
                  Effect.suspend(() => {
                    const saved = writes.findLast(
                      (item) => item._tag === 'entry' && item.value.id === id,
                    )
                    return saved?._tag === 'entry'
                      ? detachedEffect(saved.value).pipe(Effect.asSome)
                      : store.entry(id)
                  }),
                ),
              )
            const task: Transaction['task'] = (id) =>
              valid.pipe(
                Effect.andThen(
                  Effect.suspend(() => {
                    const saved = writes.findLast(
                      (item) => item._tag === 'task' && item.value.id === id,
                    )
                    return saved?._tag === 'task'
                      ? detachedEffect(saved.value).pipe(Effect.asSome)
                      : store.task(id)
                  }),
                ),
              )
            const submission: Transaction['submission'] = (id) =>
              valid.pipe(
                Effect.andThen(
                  Effect.suspend(() => {
                    const saved = writes.findLast(
                      (item) => item._tag === 'submission' && item.value.id === id,
                    )
                    return saved?._tag === 'submission'
                      ? detachedEffect(saved.value).pipe(Effect.asSome)
                      : store.submission(id)
                  }),
                ),
              )
            const scan = <A extends { readonly id: number }>(
              persisted: Stream.Stream<A, StorageError>,
              staged: () => ReadonlyArray<A>,
              matches: (value: A) => boolean,
            ): Stream.Stream<A, StorageError> =>
              Stream.unwrap(
                valid.pipe(
                  Effect.map(() => {
                    const values = new Map(staged().map((value) => [value.id, value]))
                    return persisted.pipe(
                      Stream.filter((value) => !values.has(value.id)),
                      Stream.concat(Stream.fromIterable([...values.values()].filter(matches))),
                      Stream.mapEffect((value) =>
                        valid.pipe(Effect.andThen(detachedEffect(value))),
                      ),
                    )
                  }),
                ),
              )
            const taskMatches = (value: Record.Task, query: Persistence.TaskQuery) =>
              Object.entries(query).every(
                ([key, expected]) =>
                  expected === undefined ||
                  (key === 'status'
                    ? value.state.status === expected
                    : Reflect.get(value, key) === expected),
              )
            const scanTasks: Transaction['scanTasks'] = (query = {}) =>
              scan(
                store.scanTasks(query),
                () =>
                  writes
                    .filter(
                      (item): item is Extract<Record.Write, { _tag: 'task' }> =>
                        item._tag === 'task',
                    )
                    .map((item) => item.value),
                (value) => taskMatches(value, query),
              )
            const scanConversations: Transaction['scanConversations'] = (query = {}) =>
              scan(
                store.scanConversations(query),
                () =>
                  writes
                    .filter(
                      (item): item is Extract<Record.Write, { _tag: 'conversation' }> =>
                        item._tag === 'conversation',
                    )
                    .map((item) => item.value),
                (value) =>
                  (query.ownerTaskId === undefined || value.owner?.taskId === query.ownerTaskId) &&
                  (query.ownerConversationId === undefined ||
                    value.owner?.conversationId === query.ownerConversationId),
              )
            const scanSubmissions: Transaction['scanSubmissions'] = (query = {}) =>
              scan(
                store.scanSubmissions(query),
                () =>
                  writes
                    .filter(
                      (item): item is Extract<Record.Write, { _tag: 'submission' }> =>
                        item._tag === 'submission',
                    )
                    .map((item) => item.value),
                (value) =>
                  Object.entries(query).every(
                    ([key, expected]) =>
                      expected === undefined || Reflect.get(value, key) === expected,
                  ),
              )
            const scanEntries: Transaction['scanEntries'] = (query) => {
              const persisted = Stream.unwrap(
                store.conversation(query.conversationId).pipe(
                  Effect.map((found) => {
                    if (Option.isSome(found)) return store.scanEntries(query)
                    const staged = writes.findLast(
                      (item) =>
                        item._tag === 'conversation' && item.value.id === query.conversationId,
                    )
                    if (staged?._tag !== 'conversation') return store.scanEntries(query)
                    if (staged.value.parent === undefined) return Stream.empty
                    return store.scanEntries({
                      ...(query.minEntryId === undefined ? {} : { minEntryId: query.minEntryId }),
                      conversationId: staged.value.parent.conversationId,
                      maxEntryId:
                        query.maxEntryId !== undefined && query.maxEntryId < staged.value.parent.at
                          ? query.maxEntryId
                          : staged.value.parent.at,
                    })
                  }),
                ),
              )
              return scan(
                persisted,
                () =>
                  writes
                    .filter(
                      (item): item is Extract<Record.Write, { _tag: 'entry' }> =>
                        item._tag === 'entry',
                    )
                    .map((item) => item.value),
                (value) =>
                  value.conversationId === query.conversationId &&
                  (query.minEntryId === undefined || value.id >= query.minEntryId) &&
                  (query.maxEntryId === undefined || value.id <= query.maxEntryId),
              )
            }
            const scanDocuments: Transaction['scanDocuments'] = (query) =>
              Stream.unwrap(
                valid.pipe(
                  Effect.map(() => {
                    const retired = new Set(
                      writes
                        .filter(
                          (item): item is Extract<Record.Write, { _tag: 'document.retire' }> =>
                            item._tag === 'document.retire',
                        )
                        .map((item) => item.id),
                    )
                    const staged: Array<Record.Document> = writes
                      .filter(
                        (
                          item,
                        ): item is Extract<
                          Record.Write,
                          { _tag: 'document.create' | 'document.copy' }
                        > => item._tag === 'document.create' || item._tag === 'document.copy',
                      )
                      .map((item) => ({ ...item.record, createdAt: expectedSeq }))
                    for (const item of acquiredOrder)
                      if (!item.retire) staged.push({ ...item.record, createdAt: expectedSeq })
                    const stagedById = new Map(staged.map((item) => [item.id, item]))
                    return store.scanDocuments(query).pipe(
                      Stream.filter((item) => !retired.has(item.id) && !stagedById.has(item.id)),
                      Stream.concat(
                        Stream.fromIterable(
                          [...stagedById.values()].filter(
                            (item) =>
                              !retired.has(item.id) &&
                              Record.scopeKey(item.scope) === Record.scopeKey(query.scope) &&
                              (query.kind === undefined || item.kind === query.kind),
                          ),
                        ),
                      ),
                      Stream.mapEffect((value) => valid.pipe(Effect.as(value))),
                    )
                  }),
                ),
              )
            const owner = Effect.fn('Session.transaction.owner')(function* (ownership: Ownership) {
              if (ownership._tag === 'none') return {}
              const found = yield* task(ownership.taskId)
              if (
                Option.isNone(found) ||
                found.value.abortRequested ||
                found.value.state.status === 'terminal' ||
                found.value.state.status === 'completing'
              )
                return yield* rejected('Conversation owner is absent or ended')
              return {
                owner: { conversationId: found.value.conversationId, taskId: found.value.id },
              }
            })
            const ensureConversation = Effect.fn('Session.transaction.ensureConversation')(
              function* (id: Record.ConversationId) {
                if (Option.isNone(yield* conversation(id)))
                  return yield* rejected('Conversation is absent', NotFoundError)
              },
            )
            let tx: Transaction
            const createConversation: Transaction['createConversation'] = Effect.fn(
              'Session.transaction.createConversation',
            )(function* (options) {
              const value = {
                id: yield* mint(Record.ConversationId),
                ...(yield* owner(options.ownership)),
              }
              yield* write({ _tag: 'conversation', value })
              if (Option.isSome(hook)) yield* hook.value.run(tx, value)
              return value
            })
            const forkConversation: Transaction['forkConversation'] = Effect.fn(
              'Session.transaction.forkConversation',
            )(function* (parent, at, options) {
              yield* valid
              const visible = yield* store
                .scanEntries({ conversationId: parent, minEntryId: at, maxEntryId: at })
                .pipe(Stream.runHead)
              const cutoff = yield* store.entryRecord(at)
              if (Option.isNone(visible) || Option.isNone(cutoff))
                return yield* rejected('Fork cutoff is not visible', NotFoundError)
              const id = yield* mint(Record.ConversationId)
              const selected = new Map<string, { record: Record.Document; at: Record.Point }>()
              const historical = yield* store
                .scanDocuments({
                  scope: { _tag: 'conversation', conversationId: visible.value.conversationId },
                  at: cutoff.value.commitSeq,
                })
                .pipe(Stream.runCollect)
              const current = yield* store
                .scanDocuments({ scope: { _tag: 'conversation', conversationId: parent } })
                .pipe(Stream.runCollect)
              for (const record of historical)
                if (record.fork === 'asOf')
                  selected.set(
                    Record.addressKey({
                      ...record,
                      scope: { _tag: 'conversation', conversationId: id },
                    }),
                    { record, at: cutoff.value.commitSeq },
                  )
              for (const record of current)
                if (record.fork === 'current') {
                  const key = Record.addressKey({
                    ...record,
                    scope: { _tag: 'conversation', conversationId: id },
                  })
                  if (selected.has(key))
                    return yield* rejected('Fork selects ambiguous document sources')
                  selected.set(key, { record, at: 'current' })
                }
              for (const source of selected.values()) {
                copiedSources.add(source.record.id)
                const { createdAt: _created, retiredAt: _retired, ...metadata } = source.record
                yield* write({
                  _tag: 'document.copy',
                  record: {
                    ...metadata,
                    id: yield* mint(Record.DocumentId),
                    scope: { _tag: 'conversation', conversationId: id },
                  },
                  source: { id: source.record.id, at: source.at },
                })
              }
              const value = {
                id,
                parent: { conversationId: parent, at },
                ...(yield* owner(options.ownership)),
              }
              yield* write({ _tag: 'conversation', value })
              if (Option.isSome(hook)) yield* hook.value.run(tx, value)
              return value
            })
            const docImpl: Transaction['doc'] = Effect.fn('Session.transaction.doc')(function* <
              T extends object,
            >(token: Document.Document<T>, target: Document.Document.Target = {}) {
              yield* valid
              const address = yield* Document.address(token, target)
              const key = Record.addressKey(address)
              const existing = acquired.get(key)
              if (existing !== undefined && !existing.retire)
                return existing.value as Document.Document.Draft<T>
              const staged = writes.findLast(
                (item) =>
                  (item._tag === 'document.create' || item._tag === 'document.copy') &&
                  Record.addressKey(item.record) === key &&
                  !writes.some(
                    (write) => write._tag === 'document.retire' && write.id === item.record.id,
                  ),
              )
              let stored = existing?.retire
                ? Option.none<Document.Document.Snapshot>()
                : yield* store.findDocument(address)
              const storedId = Option.isSome(stored) ? stored.value.record.id : undefined
              if (
                storedId !== undefined &&
                writes.some((write) => write._tag === 'document.retire' && write.id === storedId)
              )
                stored = Option.none()
              if (staged?._tag === 'document.copy') {
                copiedSources.add(staged.source.id)
                const source = yield* store.document(staged.source.id, staged.source.at)
                if (Option.isNone(source))
                  return yield* rejected('Fork source is absent', NotFoundError)
                stored = Option.some(
                  Document.makeSnapshot({
                    version: source.value.version,
                    value: source.value.value,
                    deltasSinceBase: source.value.deltasSinceBase,
                    record: { ...staged.record, createdAt: expectedSeq },
                  }),
                )
              } else if (staged?._tag === 'document.create') {
                if (staged.content._tag !== 'base')
                  return yield* rejected('Document creation requires a base')
                stored = Option.some(
                  Document.makeSnapshot({
                    record: { ...staged.record, createdAt: expectedSeq },
                    version: staged.content.version,
                    value: staged.content.value,
                    deltasSinceBase: 0,
                  }),
                )
              }
              const typed = Option.isSome(stored)
                ? yield* Document.typed(token, stored.value)
                : undefined
              const value =
                typed === undefined
                  ? yield* Effect.try({
                      try: () => token.definition.initial(target.seed),
                      catch: (cause) =>
                        rejected('Document initializer failed', InvalidError, cause),
                    })
                  : typed.value
              const mutable = yield* Document.copyEffect(value)
              const id = Option.isSome(stored)
                ? stored.value.record.id
                : yield* mint(Record.DocumentId)
              const ops: Array<Record.Op> = []
              const proxy = draft(mutable, () => active, ops)
              const record = {
                ...address,
                id,
                ...(token.definition.history === undefined
                  ? {}
                  : { history: token.definition.history }),
                ...(token.definition.fork === undefined ? {} : { fork: token.definition.fork }),
              }
              const item: Acquired = {
                id,
                address,
                record,
                version: token.definition.version,
                stored,
                value: proxy,
                ops,
                retire: false,
                staged: staged !== undefined,
                prepare: Effect.suspend(() => Document.encode(token, mutable as T)),
                checkpoint: Effect.try({
                  try: () =>
                    token.definition.checkpointWhen?.(mutable as T, ops, {
                      deltasSinceBase: typed?.deltasSinceBase ?? 0,
                    }) ?? false,
                  catch: (cause) =>
                    rejected('Document checkpoint policy failed', InvalidError, cause),
                }),
              }
              acquired.set(key, item)
              acquiredOrder.push(item)
              return proxy as Document.Document.Draft<T>
            })
            const doc: Transaction['doc'] = (token, target) =>
              documentPermit.withPermit(docImpl(token, target))
            tx = {
              ensureRoot: Effect.gen(function* () {
                const root = yield* conversation(Record.ROOT_CONVERSATION_ID)
                if (Option.isSome(root)) return root.value
                const value = { id: Record.ROOT_CONVERSATION_ID }
                yield* write({ _tag: 'conversation', value })
                if (Option.isSome(hook)) yield* hook.value.run(tx, value)
                return value
              }),
              mint,
              conversation,
              entry,
              task,
              submission,
              scanTasks,
              scanConversations,
              scanEntries,
              scanSubmissions,
              scanDocuments,
              doc,
              write,
              createConversation,
              forkConversation,
              tasks: (query) => scanTasks(query).pipe(Stream.runCollect),
              putTask: (value) => write({ _tag: 'task', value }),
              allocateTaskId: mint(Record.TaskId),
              submissionByRequest: (id, requestId) =>
                scanSubmissions({ conversationId: id }).pipe(
                  Stream.filter((item) => item.requestId === requestId),
                  Stream.runHead,
                ),
              latestHeadMarker: (id, at) =>
                scanEntries({
                  conversationId: id,
                  ...(at === undefined ? {} : { maxEntryId: at }),
                }).pipe(
                  Stream.filter((item) => item.head !== undefined),
                  Stream.runFold(
                    () => Option.none<Record.Entry>(),
                    (_, item) => Option.some(item),
                  ),
                ),
              appendEntry: Effect.fn('Session.transaction.appendEntry')(
                function* (conversationId, input) {
                  yield* ensureConversation(conversationId)
                  const id = yield* mint(Record.EntryId)
                  const { head, ...rest } = input
                  const value = yield* validate(Record.Entry, {
                    ...rest,
                    id,
                    conversationId,
                    ...(head === undefined ? {} : { head: head === 'self' ? id : head }),
                  })
                  yield* write({ _tag: 'entry', value })
                  return value
                },
              ),
              createTask: Effect.fn('Session.transaction.createTask')(function* (input) {
                yield* ensureConversation(input.conversationId)
                if (input.owner !== undefined) {
                  const found = yield* task(input.owner)
                  if (
                    Option.isNone(found) ||
                    found.value.abortRequested ||
                    found.value.state.status === 'terminal' ||
                    found.value.state.status === 'completing' ||
                    input.background ||
                    found.value.conversationId !== input.conversationId
                  )
                    return yield* rejected('Invalid task owner')
                }
                const id = yield* mint(Record.TaskId)
                yield* write({ _tag: 'task', value: { ...input, id } })
                return id
              }),
              createSubmission: Effect.fn('Session.transaction.createSubmission')(
                function* (input) {
                  yield* ensureConversation(input.conversationId)
                  const value = yield* validate(Record.Submission, {
                    ...input,
                    id: yield* mint(Record.SubmissionId),
                  })
                  yield* write({ _tag: 'submission', value })
                  return value
                },
              ),
              placeSubmission: Effect.fn('Session.transaction.placeSubmission')(
                function* (id, entry) {
                  const found = yield* submission(id)
                  if (Option.isNone(found))
                    return yield* rejected('Submission is absent', NotFoundError)
                  const current = found.value
                  if (current.status === 'done' || current.status === 'unanswered') return
                  if (current.status !== 'queued')
                    return yield* rejected('Submission is already placed')
                  const value = yield* validate(Record.Submission, {
                    ...current,
                    entry,
                    status: current.type === 'input' ? 'placed' : 'done',
                    _tag: current.type === 'input' ? 'InputPlaced' : 'WriteDone',
                  })
                  yield* write({ _tag: 'submission', value })
                },
              ),
              settleSubmission: Effect.fn('Session.transaction.settleSubmission')(
                function* (id, settlement) {
                  const found = yield* submission(id)
                  if (Option.isNone(found))
                    return yield* rejected('Submission is absent', NotFoundError)
                  const current = found.value
                  if (current.status === 'done' || current.status === 'unanswered') return
                  if (
                    settlement.status === 'done' &&
                    (current.type !== 'input' || current.status !== 'placed')
                  )
                    return yield* rejected('Only placed input may be answered')
                  let tag = current.type === 'input' ? 'InputUnanswered' : 'WriteUnanswered'
                  if (settlement.status === 'done') tag = 'InputDone'
                  yield* write({
                    _tag: 'submission',
                    value: yield* validate(Record.Submission, {
                      ...current,
                      ...settlement,
                      _tag: tag,
                    }),
                  })
                },
              ),
              retire: Effect.fn('Session.transaction.retire')(function* (token, target = {}) {
                yield* valid
                const address = yield* Document.address(token, target)
                const key = Record.addressKey(address)
                const item = acquired.get(key)
                if (item !== undefined) {
                  item.retire = true
                  return
                }
                const found = yield* store.findDocument(address)
                if (Option.isSome(found))
                  yield* write({ _tag: 'document.retire', id: found.value.record.id })
              }),
            }
            return yield* Effect.gen(function* () {
              const result = yield* restore(change(tx)).pipe(
                Effect.catchDefect((defect) =>
                  defect instanceof DraftMutationError
                    ? Effect.fail(rejected(defect.message, InvalidError, defect.cause))
                    : Effect.die(defect),
                ),
              )
              active = false
              for (const item of acquiredOrder) {
                const value = yield* item.prepare
                if (item.staged) {
                  const index = writes.findIndex(
                    (write) =>
                      (write._tag === 'document.create' || write._tag === 'document.copy') &&
                      write.record.id === item.id,
                  )
                  if (index >= 0) writes.splice(index, 1)
                }
                if (Option.isNone(item.stored) || item.staged)
                  writes.push({
                    _tag: 'document.create',
                    record: item.record,
                    content: { _tag: 'base', version: item.version, value },
                  })
                else if (item.stored.value.version !== item.version)
                  writes.push({
                    _tag: 'document.change',
                    id: item.id,
                    content: { _tag: 'base', version: item.version, value },
                  })
                else if (item.ops.length > 0) {
                  const checkpoint = yield* item.checkpoint
                  const arrayMutation = item.ops.some(
                    (op) =>
                      op[0] === 'replace' ||
                      (op[0] === 'delete' && typeof op[1].at(-1) === 'number'),
                  )
                  const replayed = arrayMutation
                    ? undefined
                    : yield* applyOps(item.stored.value.value, item.ops)
                  const ops: ReadonlyArray<Record.Op> =
                    replayed !== undefined &&
                    Schema.toEquivalence(Schema.JsonObject)(replayed, value)
                      ? item.ops
                      : [['replace', value]]
                  writes.push({
                    _tag: 'document.change',
                    id: item.id,
                    content: checkpoint
                      ? { _tag: 'base', version: item.version, value }
                      : { _tag: 'delta', version: item.version, ops },
                    ...(checkpoint ? { publicationOps: ops } : {}),
                  })
                }
                if (item.retire) writes.push({ _tag: 'document.retire', id: item.id })
              }
              if (
                writes.some(
                  (write) =>
                    (write._tag === 'document.change' || write._tag === 'document.retire') &&
                    copiedSources.has(write.id),
                )
              )
                return yield* rejected('Cannot change a fork source in the same transaction')
              if (writes.length > 0 || nextId !== metadata.nextId) {
                const frame = yield* store.commit(writes, nextId).pipe(
                  Effect.tapError((error) => {
                    if (!isStorageError(error) || error.certainty !== 'uncertain')
                      return Effect.void
                    closed = true
                    return PubSub.shutdown(changes)
                  }),
                )
                yield* PubSub.publish(changes, frame)
              }
              return result
            }).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  active = false
                }),
              ),
            )
          }),
        ),
      )
    const snapshot: Service['snapshot'] = Effect.fn('Session.snapshot')(function* (token, target) {
      yield* open
      const address = yield* Document.address(token, target)
      const found = yield* store.findDocument(address)
      return Option.isSome(found)
        ? Option.some(yield* Document.typed(token, found.value))
        : Option.none()
    })
    return {
      ...store,
      transaction,
      snapshot,
      root: transaction((tx) => tx.ensureRoot),
      initialize: transaction(
        Effect.fn('Session.initialize')(function* (tx) {
          yield* tx.ensureRoot
          if (Option.isSome(hook) && hook.value.recover !== undefined) {
            for (const value of yield* tx.scanConversations().pipe(Stream.runCollect))
              yield* hook.value.recover(tx, value)
          }
        }),
      ),
      snapshotAsOf: Effect.fn('Session.snapshotAsOf')(function* (token, at, target) {
        yield* open
        const entry = yield* store.entryRecord(at)
        if (Option.isNone(entry))
          return yield* rejected('Historical entry is absent', NotFoundError)
        const address = yield* Document.address(token, target)
        const found = yield* store.findDocument(address, entry.value.commitSeq)
        return Option.isSome(found)
          ? Option.some(yield* Document.typed(token, found.value))
          : Option.none()
      }),
      commits: Stream.fromPubSub(changes),
      observe: (read) =>
        lock.withPermit(
          Effect.gen(function* () {
            yield* open
            const subscription = yield* PubSub.subscribe(changes)
            const snapshot = yield* read
            const metadata = yield* store.metadata
            return {
              snapshot,
              revision: metadata.revision,
              frames: Stream.fromSubscription(subscription),
            }
          }),
        ),
      seal: Effect.gen(function* () {
        closed = true
        if (owners.get(store) === ownerToken) owners.delete(store)
        yield* PubSub.shutdown(changes)
      }),
      isClosed: Effect.sync(() => closed),
    }
  })
export const layer: Layer.Layer<Session, StorageError, Persistence.Persistence> = Layer.effect(
  Session,
  make,
)
