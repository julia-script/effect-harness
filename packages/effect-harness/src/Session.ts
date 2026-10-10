/**
 * One scoped state coordinator over Storage.
 *
 * Session is an instance, not a Context service. It captures Storage,
 * lazily caches loaded documents, serializes transactions and publishes only committed changes.
 */
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import { dual } from 'effect/Function'
import * as Option from 'effect/Option'
import { pipeArguments } from 'effect/Pipeable'
import type * as Pipeable from 'effect/Pipeable'
import * as Predicate from 'effect/Predicate'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import type * as Identity from './Identity.js'
import type * as ConversationInitializer from './ConversationInitializer.js'
import * as Record from './Record.js'
import type * as Document from './Document.js'
import * as StorageRecord from './Record.js'
import * as Sequence from './Sequence.js'
import { SessionError as SessionFailure, type Failure } from './SessionError.js'
import type { Storage } from './Storage.js'
import * as Transaction from './Transaction.js'
import * as SessionRuntime from './internal/SessionRuntime.js'
import * as TxRuntime from './internal/TransactionRuntime.js'
import * as Value from './internal/DocumentValue.js'

export { SessionError } from './SessionError.js'
export type { Failure } from './SessionError.js'

export const TypeId = '~effect-harness/Session'

/** Shared state coordination for all conversations and scoped documents in one storage. */
export interface Session extends Pipeable.Pipeable {
  readonly [TypeId]: typeof TypeId
}

export const isSession = (value: unknown): value is Session => Predicate.hasProperty(value, TypeId)

/** Serializable notification of one persisted batch; it is not another storage journal. */
export const CommitSchema = Schema.Struct({
  seq: Sequence.SequenceSchema,
  changes: Schema.Array(StorageRecord.StorageWriteSchema),
})
export type Commit = typeof CommitSchema.Type

/** Defaults for task attribution when staging entries in the callback. */
export const CommitOptionsSchema = Schema.Struct({
  conversationId: Schema.optionalKey(Record.ConversationId),
  taskId: Schema.optionalKey(Record.TaskId),
})
export type CommitOptions = typeof CommitOptionsSchema.Type

/**
 * Captures Storage and registers Scope cleanup. Construction does no storage I/O.
 * One active Session coordinates a Storage instance; a second make fails with conflict.
 * Scope cleanup seals admission, settles ongoing commits and ends subscriptions.
 * Storage's supplying layer retains ownership of backend cleanup.
 * Initializers capture their services here and run in array order on each new
 * root/create/fork Transaction operation. Existing conversations skip them.
 * Callback drafts roll back to the creation boundary, even for caught failures.
 * See ConversationInitializer.make for fork inheritance and callback lifetime.
 */
export const make = Effect.fnUntraced(function* <
  const I extends ReadonlyArray<ConversationInitializer.Any> = readonly [],
>(
  options: { readonly initializers?: I } = {},
): Effect.fn.Return<
  Session,
  SessionFailure,
  Storage | Scope.Scope | ConversationInitializer.Requirements<I[number]>
> {
  // Initializer.Requirements already excludes Scope; omit only removes ambient
  // execution capabilities that must come from the callback, not construction.
  const context = (yield* Effect.context<ConversationInitializer.Requirements<I[number]>>()).pipe(
    Context.omit(Scope.Scope, SessionRuntime.Transactions),
  ) as Context.Context<ConversationInitializer.Requirements<I[number]>>
  const initializers = (options.initializers ?? []).map(
    (initializer): SessionRuntime.Initializer => {
      // The public phantom type retains callback requirements until Session captures their services.
      const execute = initializer.execute as (
        tx: Transaction.Transaction,
        record: Record.Conversation,
      ) => Effect.Effect<
        void,
        ConversationInitializer.Error<I[number]>,
        ConversationInitializer.Requirements<I[number]>
      >
      return (tx, record) =>
        Effect.suspend(() => execute(tx, record)).pipe(
          Effect.provideContext(context),
          Effect.mapError(
            (cause) =>
              new SessionFailure({
                reason: 'invalid',
                operation: 'conversation.initialize',
                message: 'Conversation initializer failed',
                cause,
              }),
          ),
        )
    },
  )
  const state = yield* SessionRuntime.make(initializers)
  const self: Session = {
    [TypeId]: TypeId,
    pipe() {
      return pipeArguments(this, arguments)
    },
  }
  SessionRuntime.register(self, state)
  return self
})

/**
 * Runs an isolated callback, persists its staged writes, adopts them, then publishes.
 * Callback failure or interruption discards drafts. Persistence/adoption/publication settle
 * together after storage admission; an uncertain outcome prevents further Session use.
 * A callback that stages no writes performs no storage commit or publication.
 * Nested Session operations must use the Transaction while this callback holds coordination.
 */
export const commit: {
  <A, E, R>(
    change: (tx: Transaction.Transaction) => Effect.Effect<A, E, R>,
    options?: CommitOptions,
  ): (self: Session) => Effect.Effect<A, E | Failure, R>
  <A, E, R>(
    self: Session,
    change: (tx: Transaction.Transaction) => Effect.Effect<A, E, R>,
    options?: CommitOptions,
  ): Effect.Effect<A, E | Failure, R>
} = dual(
  (args) => isSession(args[0]),
  <A, E, R>(
    self: Session,
    change: (tx: Transaction.Transaction) => Effect.Effect<A, E, R>,
    options?: CommitOptions,
  ) => commitImpl(self, change, options),
)

/** Reads committed records; multi-record consistent reads use a read-only commit callback. */
export const conversation: {
  (
    id: Record.ConversationId,
  ): (self: Session) => Effect.Effect<Option.Option<Record.Conversation>, Failure>
  (
    self: Session,
    id: Record.ConversationId,
  ): Effect.Effect<Option.Option<Record.Conversation>, Failure>
} = dual(
  (args) => isSession(args[0]),
  (self: Session, id: Record.ConversationId) =>
    SessionRuntime.line(self, 'session.conversation', (state) => state.storage.conversation(id)),
)

export const entry: {
  (
    id: Record.EntryId,
    options?: { readonly conversationId: Record.ConversationId },
  ): (self: Session) => Effect.Effect<Option.Option<StorageRecord.StoredEntry>, Failure>
  (
    self: Session,
    id: Record.EntryId,
    options?: { readonly conversationId: Record.ConversationId },
  ): Effect.Effect<Option.Option<StorageRecord.StoredEntry>, Failure>
} = dual(
  (args) => isSession(args[0]),
  (
    self: Session,
    id: Record.EntryId,
    options?: { readonly conversationId: Record.ConversationId },
  ) => SessionRuntime.line(self, 'session.entry', (state) => state.storage.entry(id, options)),
)

export const findLatestHeadMarker: {
  (
    conversationId: Record.ConversationId,
    atOrBeforeEntryId?: Record.EntryId,
  ): (self: Session) => Effect.Effect<Option.Option<StorageRecord.HeadMarker>, Failure>
  (
    self: Session,
    conversationId: Record.ConversationId,
    atOrBeforeEntryId?: Record.EntryId,
  ): Effect.Effect<Option.Option<StorageRecord.HeadMarker>, Failure>
} = dual(
  (args) => isSession(args[0]),
  (self: Session, conversationId: Record.ConversationId, atOrBeforeEntryId?: Record.EntryId) =>
    SessionRuntime.line(self, 'session.findLatestHeadMarker', (state) =>
      state.storage.findLatestHeadMarker(conversationId, atOrBeforeEntryId),
    ),
)

export const task: {
  (id: Record.TaskId): (self: Session) => Effect.Effect<Option.Option<Record.Task>, Failure>
  (self: Session, id: Record.TaskId): Effect.Effect<Option.Option<Record.Task>, Failure>
} = dual(
  (args) => isSession(args[0]),
  (self: Session, id: Record.TaskId) =>
    SessionRuntime.line(self, 'session.task', (state) => state.storage.task(id)),
)

export const submission: {
  (
    id: Record.SubmissionId,
  ): (self: Session) => Effect.Effect<Option.Option<Record.Submission>, Failure>
  (self: Session, id: Record.SubmissionId): Effect.Effect<Option.Option<Record.Submission>, Failure>
} = dual(
  (args) => isSession(args[0]),
  (self: Session, id: Record.SubmissionId) =>
    SessionRuntime.line(self, 'session.submission', (state) => state.storage.submission(id)),
)

export const submissionByRequest: {
  (
    conversationId: Record.ConversationId,
    requestId: Identity.RequestId,
  ): (self: Session) => Effect.Effect<Option.Option<Record.Submission>, Failure>
  (
    self: Session,
    conversationId: Record.ConversationId,
    requestId: Identity.RequestId,
  ): Effect.Effect<Option.Option<Record.Submission>, Failure>
} = dual(
  (args) => isSession(args[0]),
  (self: Session, conversationId: Record.ConversationId, requestId: Identity.RequestId) =>
    SessionRuntime.line(self, 'session.submissionByRequest', (state) =>
      state.storage.submissionByRequest(conversationId, requestId),
    ),
)

export const scanConversations: {
  (
    query?: StorageRecord.ConversationQuery,
  ): (self: Session) => Stream.Stream<Record.Conversation, Failure>
  (
    self: Session,
    query?: StorageRecord.ConversationQuery,
  ): Stream.Stream<Record.Conversation, Failure>
} = dual(
  (args) => isSession(args[0]),
  (self: Session, query?: StorageRecord.ConversationQuery) =>
    SessionRuntime.scan(self, 'session.scanConversations', (state) =>
      state.storage.scanConversations(query),
    ),
)

export const scanEntries: {
  (query: StorageRecord.EntryQuery): (self: Session) => Stream.Stream<Record.Entry, Failure>
  (self: Session, query: StorageRecord.EntryQuery): Stream.Stream<Record.Entry, Failure>
} = dual(
  (args) => isSession(args[0]),
  (self: Session, query: StorageRecord.EntryQuery) =>
    SessionRuntime.scan(self, 'session.scanEntries', (state) => state.storage.scanEntries(query)),
)

export const scanTasks: {
  (query?: StorageRecord.TaskQuery): (self: Session) => Stream.Stream<Record.Task, Failure>
  (self: Session, query?: StorageRecord.TaskQuery): Stream.Stream<Record.Task, Failure>
} = dual(
  (args) => isSession(args[0]),
  (self: Session, query?: StorageRecord.TaskQuery) =>
    SessionRuntime.scan(self, 'session.scanTasks', (state) => state.storage.scanTasks(query)),
)

export const scanSubmissions: {
  (
    query?: StorageRecord.SubmissionQuery,
  ): (self: Session) => Stream.Stream<Record.Submission, Failure>
  (self: Session, query?: StorageRecord.SubmissionQuery): Stream.Stream<Record.Submission, Failure>
} = dual(
  (args) => isSession(args[0]),
  (self: Session, query?: StorageRecord.SubmissionQuery) =>
    SessionRuntime.scan(self, 'session.scanSubmissions', (state) =>
      state.storage.scanSubmissions(query),
    ),
)

export const scanDocuments: {
  (
    query: StorageRecord.DocumentQuery,
  ): (self: Session) => Stream.Stream<StorageRecord.DocumentRecord, Failure>
  (
    self: Session,
    query: StorageRecord.DocumentQuery,
  ): Stream.Stream<StorageRecord.DocumentRecord, Failure>
} = dual(
  (args) => isSession(args[0]),
  (self: Session, query: StorageRecord.DocumentQuery) =>
    SessionRuntime.scan(self, 'session.scanDocuments', (state) =>
      state.storage.scanDocuments(query),
    ),
)

/**
 * Loads a detached committed revision on demand; never creates a missing document.
 * Automatically retired task documents return None. Legacy terminal task documents
 * remain readable until explicitly retired; reads do not migrate or sweep storage.
 */
export const snapshot: {
  <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
  ): (
    self: Session,
  ) => Effect.Effect<Option.Option<Document.Snapshot<S>>, Failure, S['DecodingServices']>
  <S extends Document.Codec>(
    self: Session,
    document: Document.Document<S>,
    target: Document.Target,
  ): Effect.Effect<Option.Option<Document.Snapshot<S>>, Failure, S['DecodingServices']>
} = dual(
  (args) => isSession(args[0]),
  <S extends Document.Codec>(
    self: Session,
    document: Document.Document<S>,
    target: Document.Target,
  ) => snapshotImpl(self, document, target),
)

/**
 * Only rewindable conversation documents permit historical reads at a visible entry.
 * Use the definition matching that historical revision's version; reads never migrate.
 */
export const snapshotAsOf: {
  <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
    at: Record.EntryId,
  ): (
    self: Session,
  ) => Effect.Effect<Option.Option<Document.Snapshot<S>>, Failure, S['DecodingServices']>
  <S extends Document.Codec>(
    self: Session,
    document: Document.Document<S>,
    target: Document.Target,
    at: Record.EntryId,
  ): Effect.Effect<Option.Option<Document.Snapshot<S>>, Failure, S['DecodingServices']>
} = dual(
  (args) => isSession(args[0]),
  <S extends Document.Codec>(
    self: Session,
    document: Document.Document<S>,
    target: Document.Target,
    at: Record.EntryId,
  ) => snapshotAsOfImpl(self, document, target, at),
)

/**
 * On stream execution, atomically subscribes and captures the initial committed snapshot.
 * Emits that snapshot and later revisions of the same incarnation; retirement ends the stream.
 * Missing documents fail with notFound. Stream termination releases the subscription.
 * Slow consumers cannot block commits; exceeding the subscriber buffer fails with overflow.
 */
export const watch: {
  <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
  ): (self: Session) => Stream.Stream<Document.Snapshot<S>, Failure, S['DecodingServices']>
  <S extends Document.Codec>(
    self: Session,
    document: Document.Document<S>,
    target: Document.Target,
  ): Stream.Stream<Document.Snapshot<S>, Failure, S['DecodingServices']>
} = dual(
  (args) => isSession(args[0]),
  <S extends Document.Codec>(
    self: Session,
    document: Document.Document<S>,
    target: Document.Target,
  ) => watchImpl(self, document, target),
)

/** Live committed batches after subscription; no initial frame or historical replay. */
export const commits: {
  (): (self: Session) => Stream.Stream<Commit, Failure>
  (self: Session): Stream.Stream<Commit, Failure>
} = dual(
  (args) => isSession(args[0]),
  (self: Session) => commitsImpl(self),
)

const commitImpl = Effect.fnUntraced(function* <A, E, R>(
  self: Session,
  change: (tx: Transaction.Transaction) => Effect.Effect<A, E, R>,
  input: CommitOptions = {},
): Effect.fn.Return<A, E | Failure, R> {
  const options = yield* Value.copy(CommitOptionsSchema, input)
  return yield* SessionRuntime.line(self, 'session.commit', (state) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const draft = yield* TxRuntime.make(state, options)
        const tx: Transaction.Transaction = {
          [Transaction.TypeId]: Transaction.TypeId,
          pipe() {
            return pipeArguments(this, arguments)
          },
        }
        TxRuntime.register(tx, draft)
        const parents = yield* SessionRuntime.Transactions
        const result = yield* restore(
          Effect.scoped(Effect.suspend(() => change(tx))).pipe(
            Effect.provideService(SessionRuntime.Transactions, new Set([...parents, state])),
            Effect.ensuring(
              Effect.sync(() => {
                draft.active = false
              }),
            ),
          ),
        )
        const changes = yield* TxRuntime.writes(draft)
        if (changes.length === 0) return result
        let persisted = false
        yield* Effect.gen(function* () {
          const sequence = yield* state.storage.commit(changes)
          persisted = true
          const seq = yield* Schema.decodeEffect(Sequence.SequenceSchema)(sequence)
          const documents = yield* TxRuntime.adopt(draft, seq)
          const committed = yield* Value.copy(CommitSchema, { seq, changes })
          yield* SessionRuntime.publish(state, committed, documents)
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.gen(function* () {
              const failure = Cause.findErrorOption(cause)
              const uncertain =
                Option.isSome(failure) &&
                failure.value._tag === 'StorageError' &&
                failure.value.reason === 'uncertain'
              if (persisted || uncertain || Cause.hasDies(cause) || Cause.hasInterrupts(cause)) {
                const poison = Option.isSome(failure)
                  ? failure.value
                  : new SessionFailure({
                      reason: 'closed',
                      operation: 'session.commit',
                      message: 'Commit settlement was uncertain; reopen Session',
                      cause,
                    })
                yield* SessionRuntime.stop(state, poison)
              }
              return yield* Effect.failCause(cause)
            }),
          ),
        )
        return result
      }),
    ),
  )
})

const snapshotImpl = Effect.fnUntraced(function* <S extends Document.Codec>(
  self: Session,
  document: Document.Document<S>,
  target: Document.Target,
) {
  const address = yield* Value.address(document, target)
  return yield* SessionRuntime.line(self, 'session.snapshot', (state) =>
    Effect.gen(function* () {
      const stored = yield* SessionRuntime.load(state, address)
      if (Option.isNone(stored)) return Option.none<Document.Snapshot<S>>()
      return Option.some(yield* Value.snapshot(document, stored.value))
    }),
  )
})

const snapshotAsOfImpl = Effect.fnUntraced(function* <S extends Document.Codec>(
  self: Session,
  document: Document.Document<S>,
  target: Document.Target,
  at: Record.EntryId,
) {
  const address = yield* Value.address(document, target)
  if (
    address.scope._tag !== 'conversation' ||
    document.definition.scope !== 'conversation' ||
    document.definition.history !== 'rewindable'
  )
    return yield* new SessionFailure({
      reason: 'invalid',
      operation: 'session.snapshotAsOf',
      message: 'Historical reads require a rewindable conversation document',
    })
  const conversationId = address.scope.conversationId
  return yield* SessionRuntime.line(self, 'session.snapshotAsOf', (state) =>
    Effect.gen(function* () {
      const entry = yield* state.storage.entry(at, { conversationId })
      if (Option.isNone(entry))
        return yield* new SessionFailure({
          reason: 'notFound',
          operation: 'session.snapshotAsOf',
          message: 'Historical entry is not visible',
        })
      const record = yield* state.storage.findDocument(
        {
          ...address,
          scope: { _tag: 'conversation', conversationId: entry.value.entry.conversationId },
        },
        entry.value.commitSeq,
      )
      if (Option.isNone(record)) return Option.none<Document.Snapshot<S>>()
      const stored = yield* state.storage.document(record.value.id, entry.value.commitSeq)
      if (Option.isNone(stored))
        return yield* new SessionFailure({
          reason: 'invalid',
          operation: 'session.snapshotAsOf',
          message: 'Historical document has no stored content',
        })
      return Option.some(yield* Value.snapshot(document, stored.value))
    }),
  )
})

const watchImpl = <S extends Document.Codec>(
  self: Session,
  document: Document.Document<S>,
  target: Document.Target,
) =>
  Stream.unwrap(
    Effect.gen(function* () {
      const address = yield* Value.address(document, target)
      const acquired = yield* SessionRuntime.line(self, 'session.watch', (state) =>
        Effect.gen(function* () {
          const stored = yield* SessionRuntime.load(state, address)
          if (Option.isNone(stored))
            return yield* new SessionFailure({
              reason: 'notFound',
              operation: 'session.watch',
              message: 'Document is absent',
            })
          const initial = yield* Value.snapshot(document, stored.value)
          const subscriber = yield* SessionRuntime.subscribe(state, stored.value.record.id)
          return { initial, subscriber }
        }),
      )
      const revisions = Stream.fromQueue(acquired.subscriber.queue).pipe(
        Stream.filter(
          (
            event,
          ): event is { readonly _tag: 'document'; readonly value: StorageRecord.StoredDocument } =>
            event._tag === 'document' && event.value !== null,
        ),
        Stream.mapEffect((event) => Value.snapshot(document, event.value)),
      )
      return Stream.concat(Stream.succeed(acquired.initial), revisions)
    }),
  )

const commitsImpl = (self: Session) =>
  Stream.unwrap(
    Effect.gen(function* () {
      const subscriber = yield* SessionRuntime.line(
        self,
        'session.commits',
        SessionRuntime.subscribe,
      )
      return Stream.fromQueue(subscriber.queue).pipe(
        Stream.filter(
          (event): event is Extract<SessionRuntime.Event, { readonly _tag: 'commit' }> =>
            event._tag === 'commit',
        ),
        Stream.mapEffect((event) => Value.copy(CommitSchema, event.value)),
      )
    }),
  )
