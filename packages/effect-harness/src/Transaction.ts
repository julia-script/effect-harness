/** Isolated transaction values. Operations stage changes; Session alone commits them. */
import * as Effect from 'effect/Effect'
import { dual } from 'effect/Function'
import type * as Option from 'effect/Option'
import type * as Pipeable from 'effect/Pipeable'
import * as Predicate from 'effect/Predicate'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Struct from 'effect/Struct'
import type * as Identity from './Identity.js'
import * as Record from './Record.js'
import type * as Document from './Document.js'
import type * as StorageRecord from './Record.js'
import type { Failure } from './SessionError.js'
import * as Runtime from './internal/TransactionRuntime.js'

export const TypeId = '~effect-harness/Transaction'

/** Temporary capability valid only during its Session.commit callback. */
export interface Transaction extends Pipeable.Pipeable {
  readonly [TypeId]: typeof TypeId
}

export const isTransaction = (value: unknown): value is Transaction =>
  Predicate.hasProperty(value, TypeId)

export const ConversationOptionsSchema = Schema.Struct({
  ownership: Schema.Union([
    Schema.TaggedStruct('ownerless', {}),
    Schema.TaggedStruct('task', { taskId: Record.TaskId }),
  ]),
})
export type ConversationOptions = typeof ConversationOptionsSchema.Type

/** Session assigns identity and task attribution before validating the complete record. */
export const EntryDraftSchema = Record.Entry.mapFields((fields) => ({
  ...Struct.omit(fields, ['id', 'conversationId', 'byTaskId', 'head']),
  head: Schema.optionalKey(Schema.Union([Record.EntryId, Schema.Literal('self')])),
}))
export type EntryDraft = typeof EntryDraftSchema.Type

export const TaskCreateSchema = Record.Task.mapFields(Struct.omit(['id']))
export type TaskCreate = typeof TaskCreateSchema.Type

export const SubmissionCreateSchema = Schema.Union([
  Record.InputQueued.mapFields(Struct.omit(['id'])),
  Record.InputPlaced.mapFields(Struct.omit(['id'])),
  Record.InputDone.mapFields(Struct.omit(['id'])),
  Record.InputUnanswered.mapFields(Struct.omit(['id'])),
  Record.WriteQueued.mapFields(Struct.omit(['id'])),
  Record.WriteDone.mapFields(Struct.omit(['id'])),
  Record.WriteUnanswered.mapFields(Struct.omit(['id'])),
])
export type SubmissionCreate = typeof SubmissionCreateSchema.Type

/** Reads include changes already staged in this transaction. */
export const conversation: {
  (
    id: Record.ConversationId,
  ): (self: Transaction) => Effect.Effect<Option.Option<Record.Conversation>, Failure>
  (
    self: Transaction,
    id: Record.ConversationId,
  ): Effect.Effect<Option.Option<Record.Conversation>, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (self: Transaction, id: Record.ConversationId) =>
    Runtime.use(self, (state) => Runtime.conversation(state, id)),
)

export const entry: {
  (
    id: Record.EntryId,
    options?: { readonly conversationId: Record.ConversationId },
  ): (self: Transaction) => Effect.Effect<Option.Option<Record.Entry>, Failure>
  (
    self: Transaction,
    id: Record.EntryId,
    options?: { readonly conversationId: Record.ConversationId },
  ): Effect.Effect<Option.Option<Record.Entry>, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (
    self: Transaction,
    id: Record.EntryId,
    options?: { readonly conversationId: Record.ConversationId },
  ) => Runtime.use(self, (state) => Runtime.entry(state, id, options)),
)

export const task: {
  (id: Record.TaskId): (self: Transaction) => Effect.Effect<Option.Option<Record.Task>, Failure>
  (self: Transaction, id: Record.TaskId): Effect.Effect<Option.Option<Record.Task>, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (self: Transaction, id: Record.TaskId) => Runtime.use(self, (state) => Runtime.task(state, id)),
)

export const submission: {
  (
    id: Record.SubmissionId,
  ): (self: Transaction) => Effect.Effect<Option.Option<Record.Submission>, Failure>
  (
    self: Transaction,
    id: Record.SubmissionId,
  ): Effect.Effect<Option.Option<Record.Submission>, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (self: Transaction, id: Record.SubmissionId) =>
    Runtime.use(self, (state) => Runtime.submission(state, id)),
)

export const submissionByRequest: {
  (
    conversationId: Record.ConversationId,
    requestId: Identity.RequestId,
  ): (self: Transaction) => Effect.Effect<Option.Option<Record.Submission>, Failure>
  (
    self: Transaction,
    conversationId: Record.ConversationId,
    requestId: Identity.RequestId,
  ): Effect.Effect<Option.Option<Record.Submission>, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (self: Transaction, conversationId: Record.ConversationId, requestId: Identity.RequestId) =>
    Runtime.use(self, (state) => Runtime.submissionByRequest(state, conversationId, requestId)),
)

export const scanConversations: {
  (
    query?: StorageRecord.ConversationQuery,
  ): (self: Transaction) => Stream.Stream<Record.Conversation, Failure>
  (
    self: Transaction,
    query?: StorageRecord.ConversationQuery,
  ): Stream.Stream<Record.Conversation, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (self: Transaction, query?: StorageRecord.ConversationQuery) =>
    Stream.unwrap(
      Runtime.use(self, (state) => Runtime.scanConversations(state, query)).pipe(
        Effect.map(Stream.fromArray),
      ),
    ).pipe(Stream.mapEffect((value) => Runtime.use(self, () => Effect.succeed(value)))),
)

export const scanEntries: {
  (query: StorageRecord.EntryQuery): (self: Transaction) => Stream.Stream<Record.Entry, Failure>
  (self: Transaction, query: StorageRecord.EntryQuery): Stream.Stream<Record.Entry, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (self: Transaction, query: StorageRecord.EntryQuery) =>
    Stream.unwrap(
      Runtime.use(self, (state) => Runtime.scanEntries(state, query)).pipe(
        Effect.map(Stream.fromArray),
      ),
    ).pipe(Stream.mapEffect((value) => Runtime.use(self, () => Effect.succeed(value)))),
)

export const scanTasks: {
  (query?: StorageRecord.TaskQuery): (self: Transaction) => Stream.Stream<Record.Task, Failure>
  (self: Transaction, query?: StorageRecord.TaskQuery): Stream.Stream<Record.Task, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (self: Transaction, query?: StorageRecord.TaskQuery) =>
    Stream.unwrap(
      Runtime.use(self, (state) => Runtime.scanTasks(state, query)).pipe(
        Effect.map(Stream.fromArray),
      ),
    ).pipe(Stream.mapEffect((value) => Runtime.use(self, () => Effect.succeed(value)))),
)

export const scanSubmissions: {
  (
    query?: StorageRecord.SubmissionQuery,
  ): (self: Transaction) => Stream.Stream<Record.Submission, Failure>
  (
    self: Transaction,
    query?: StorageRecord.SubmissionQuery,
  ): Stream.Stream<Record.Submission, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (self: Transaction, query?: StorageRecord.SubmissionQuery) =>
    Stream.unwrap(
      Runtime.use(self, (state) => Runtime.scanSubmissions(state, query)).pipe(
        Effect.map(Stream.fromArray),
      ),
    ).pipe(Stream.mapEffect((value) => Runtime.use(self, () => Effect.succeed(value)))),
)

export const scanDocuments: {
  (
    query: StorageRecord.DocumentQuery,
  ): (self: Transaction) => Stream.Stream<StorageRecord.DocumentRecord, Failure>
  (
    self: Transaction,
    query: StorageRecord.DocumentQuery,
  ): Stream.Stream<StorageRecord.DocumentRecord, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (self: Transaction, query: StorageRecord.DocumentQuery) =>
    Stream.unwrap(
      Runtime.use(self, (state) => Runtime.scanDocuments(state, query)).pipe(
        Effect.map(Stream.fromArray),
      ),
    ).pipe(Stream.mapEffect((value) => Runtime.use(self, () => Effect.succeed(value)))),
)

/** Stages the reserved root conversation when absent; initializes no Harness documents. */
export const ensureRoot: {
  (): (self: Transaction) => Effect.Effect<Record.Conversation, Failure>
  (self: Transaction): Effect.Effect<Record.Conversation, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (self: Transaction) => Runtime.use(self, (state) => Runtime.ensureRoot(state)),
)

export const createConversation: {
  (options: ConversationOptions): (self: Transaction) => Effect.Effect<Record.Conversation, Failure>
  (self: Transaction, options: ConversationOptions): Effect.Effect<Record.Conversation, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (self: Transaction, options: ConversationOptions) =>
    Runtime.use(self, (state) => Runtime.createConversation(state, options)),
)

/** Creates a new conversation with inherited history and the source documents' fork policies. */
export const forkConversation: {
  (
    parent: Record.ConversationId,
    at: Record.EntryId,
    options: ConversationOptions,
  ): (self: Transaction) => Effect.Effect<Record.Conversation, Failure>
  (
    self: Transaction,
    parent: Record.ConversationId,
    at: Record.EntryId,
    options: ConversationOptions,
  ): Effect.Effect<Record.Conversation, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (
    self: Transaction,
    parent: Record.ConversationId,
    at: Record.EntryId,
    options: ConversationOptions,
  ) => Runtime.use(self, (state) => Runtime.forkConversation(state, parent, at, options)),
)

export const appendEntry: {
  (
    conversationId: Record.ConversationId,
    draft: EntryDraft,
  ): (self: Transaction) => Effect.Effect<Record.Entry, Failure>
  (
    self: Transaction,
    conversationId: Record.ConversationId,
    draft: EntryDraft,
  ): Effect.Effect<Record.Entry, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (self: Transaction, conversationId: Record.ConversationId, draft: EntryDraft) =>
    Runtime.use(self, (state) => Runtime.appendEntry(state, conversationId, draft)),
)

/** Stages task data; does not schedule or execute it. */
export const createTask: {
  (value: TaskCreate): (self: Transaction) => Effect.Effect<Record.Task, Failure>
  (self: Transaction, value: TaskCreate): Effect.Effect<Record.Task, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (self: Transaction, value: TaskCreate) =>
    Runtime.use(self, (state) => Runtime.createTask(state, value)),
)

export const putTask: {
  (value: Record.Task): (self: Transaction) => Effect.Effect<void, Failure>
  (self: Transaction, value: Record.Task): Effect.Effect<void, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (self: Transaction, value: Record.Task) =>
    Runtime.use(self, (state) => Runtime.putTask(state, value)),
)

export const createSubmission: {
  (value: SubmissionCreate): (self: Transaction) => Effect.Effect<Record.Submission, Failure>
  (self: Transaction, value: SubmissionCreate): Effect.Effect<Record.Submission, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (self: Transaction, value: SubmissionCreate) =>
    Runtime.use(self, (state) => Runtime.createSubmission(state, value)),
)

export const putSubmission: {
  (value: Record.Submission): (self: Transaction) => Effect.Effect<void, Failure>
  (self: Transaction, value: Record.Submission): Effect.Effect<void, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  (self: Transaction, value: Record.Submission) =>
    Runtime.use(self, (state) => Runtime.putSubmission(state, value)),
)

/** Reads a detached staged or committed value; does not create a missing document. */
export const snapshot: {
  <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
  ): (
    self: Transaction,
  ) => Effect.Effect<Option.Option<Document.Snapshot<S>>, Failure, S['DecodingServices']>
  <S extends Document.Codec>(
    self: Transaction,
    document: Document.Document<S>,
    target: Document.Target,
  ): Effect.Effect<Option.Option<Document.Snapshot<S>>, Failure, S['DecodingServices']>
} = dual(
  (args) => isTransaction(args[0]),
  <S extends Document.Codec>(
    self: Transaction,
    document: Document.Document<S>,
    target: Document.Target,
  ) => Runtime.use(self, (state) => Runtime.snapshot(state, document, target)),
)

/**
 * Stages the initial value when missing. Seeds apply only to a new incarnation.
 * Existing older versions upgrade through a declared direct migration, atomically
 * with this commit. Failed migrations stage no replacement, even when caught.
 * New drafts have createdAt = 0 until commit assigns their persisted sequence.
 * Returned snapshots remain detached values; read the Session for committed metadata.
 */
export const ensureDocument: {
  <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
    options?: { readonly seed?: Schema.Json },
  ): (
    self: Transaction,
  ) => Effect.Effect<Document.Snapshot<S>, Failure, S['DecodingServices'] | S['EncodingServices']>
  <S extends Document.Codec>(
    self: Transaction,
    document: Document.Document<S>,
    target: Document.Target,
    options?: { readonly seed?: Schema.Json },
  ): Effect.Effect<Document.Snapshot<S>, Failure, S['DecodingServices'] | S['EncodingServices']>
} = dual(
  (args) => isTransaction(args[0]),
  <S extends Document.Codec>(
    self: Transaction,
    document: Document.Document<S>,
    target: Document.Target,
    options?: { readonly seed?: Schema.Json },
  ) => Runtime.use(self, (state) => Runtime.ensureDocument(state, document, target, options)),
)

/** Replaces an existing document with a value encoded through its definition's schema. */
export const setDocument: {
  <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
    value: S['Type'],
  ): (self: Transaction) => Effect.Effect<void, Failure, S['EncodingServices']>
  <S extends Document.Codec>(
    self: Transaction,
    document: Document.Document<S>,
    target: Document.Target,
    value: S['Type'],
  ): Effect.Effect<void, Failure, S['EncodingServices']>
} = dual(
  (args) => isTransaction(args[0]),
  <S extends Document.Codec>(
    self: Transaction,
    document: Document.Document<S>,
    target: Document.Target,
    value: S['Type'],
  ) => Runtime.use(self, (state) => Runtime.setDocument(state, document, target, value)),
)

/**
 * Applies a pure replacement function to a detached value of an existing document.
 * Declared migrations upgrade older values before calling the replacement function.
 * Migration and replacement validate together before staging one current-version base.
 */
export const updateDocument: {
  <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
    update: (value: S['Type']) => S['Type'],
  ): (
    self: Transaction,
  ) => Effect.Effect<void, Failure, S['DecodingServices'] | S['EncodingServices']>
  <S extends Document.Codec>(
    self: Transaction,
    document: Document.Document<S>,
    target: Document.Target,
    update: (value: S['Type']) => S['Type'],
  ): Effect.Effect<void, Failure, S['DecodingServices'] | S['EncodingServices']>
} = dual(
  (args) => isTransaction(args[0]),
  <S extends Document.Codec>(
    self: Transaction,
    document: Document.Document<S>,
    target: Document.Target,
    update: (value: S['Type']) => S['Type'],
  ) => Runtime.use(self, (state) => Runtime.updateDocument(state, document, target, update)),
)

/** Retires the current incarnation; a later ensureDocument allocates a fresh identity. */
export const retireDocument: {
  <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
  ): (self: Transaction) => Effect.Effect<void, Failure>
  <S extends Document.Codec>(
    self: Transaction,
    document: Document.Document<S>,
    target: Document.Target,
  ): Effect.Effect<void, Failure>
} = dual(
  (args) => isTransaction(args[0]),
  <S extends Document.Codec>(
    self: Transaction,
    document: Document.Document<S>,
    target: Document.Target,
  ) => Runtime.use(self, (state) => Runtime.retireDocument(state, document, target)),
)
