import type * as Types from 'effect/Types'
/**
 * Durable facts, identifiers, journal frames and tagged codecs.
 */
import { dual } from 'effect/Function'
import * as handle from './internal/handle.ts'
const EntryTokenProto = handle.prototype({
  id: '@effect-harness/durable/Record/EntryToken',
  fields: ['kind'],
})
import type * as Pipeable from 'effect/Pipeable'
import type * as Inspectable from 'effect/Inspectable'
import { identity } from 'effect/Function'
import * as Predicate from 'effect/Predicate'
import * as Schema from 'effect/Schema'
import * as Result from 'effect/Result'
import * as Struct from 'effect/Struct'
import { RequestId } from './Identity.ts'
import * as Identity from 'effect-harness/Identity'
/**
 * Schema for positive identity of a conversation.
 *
 * @category schemas
 */
export const ConversationId = Identity.ConversationId
/**
 * Schema for positive identity of a conversation entry.
 *
 * @category schemas
 */
export const EntryId = Identity.EntryId
/**
 * Positive identity of a conversation.
 *
 * @category models
 */
export type ConversationId = typeof ConversationId.Type
/**
 * Positive identity of a conversation entry.
 *
 * @category models
 */
export type EntryId = typeof EntryId.Type

const safe = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }))
/**
 * Schema for positive identity of a domain task.
 *
 * @category schemas
 */
export const TaskId = safe.pipe(Schema.brand('@effect-harness/durable/Record/TaskId'))
/**
 * Positive identity of a domain task.
 *
 * @category models
 */
export type TaskId<A = Schema.Json> = typeof TaskId.Type & { readonly __result?: A | undefined }
/**
 * Schema for positive identity of an admitted input or passive write.
 *
 * @category schemas
 */
export const SubmissionId = safe.pipe(Schema.brand('@effect-harness/durable/Record/SubmissionId'))
/**
 * Positive identity of an admitted input or passive write.
 *
 * @category models
 */
export type SubmissionId = typeof SubmissionId.Type
/**
 * Schema for positive identity of a document incarnation.
 *
 * @category schemas
 */
export const DocumentId = safe.pipe(Schema.brand('@effect-harness/durable/Record/DocumentId'))
/**
 * Positive identity of a document incarnation.
 *
 * @category models
 */
export type DocumentId = typeof DocumentId.Type
/**
 * Schema for positive commit sequence assigned by storage.
 *
 * @category schemas
 */
export const Seq = safe.pipe(Schema.brand('@effect-harness/durable/Record/Seq'))
/**
 * Positive commit sequence assigned by storage.
 *
 * @category models
 */
export type Seq = typeof Seq.Type
/**
 * Schema for commit sequence from which retained observation frames are read.
 *
 * @category schemas
 */
export const JournalCursor = Schema.Union([Seq, Schema.Literal(0)])
/**
 * Commit sequence from which retained observation frames are read.
 *
 * @category models
 */
export type JournalCursor = typeof JournalCursor.Type
/**
 * ROOT_CONVERSATION_ID schema.
 *
 * @category schemas
 */
export const ROOT_CONVERSATION_ID = Schema.decodeSync(ConversationId)(1)
/**
 * Schema for conversation identity, parent history and ownership recorded in a session.
 *
 * @category schemas
 */
export const Conversation = Schema.Struct({
  id: ConversationId,
  parent: Schema.optionalKey(Schema.Struct({ conversationId: ConversationId, at: EntryId })),
  owner: Schema.optionalKey(Schema.Struct({ conversationId: ConversationId, taskId: TaskId })),
})
/**
 * Conversation identity, parent history and ownership recorded in a session.
 *
 * @category models
 */
export type Conversation = typeof Conversation.Type
/**
 * Schema for context reset, omission or replacement applied to visible entry history.
 *
 * @category schemas
 */
export const ContextEdit = Schema.Union([
  Schema.TaggedStruct('omit', { target: EntryId }),
  Schema.TaggedStruct('replace', {
    target: EntryId,
    messages: Schema.Array(Schema.Json),
  }),
])
/**
 * Context reset, omission or replacement applied to visible entry history.
 *
 * @category models
 */
export type ContextEdit = typeof ContextEdit.Type
/**
 * Schema for committed conversation entry with identity, timestamp and stored content.
 *
 * @category schemas
 */
export const Entry = Schema.Struct({
  id: EntryId,
  conversationId: ConversationId,
  kind: Schema.String,
  model: Schema.optionalKey(Schema.Array(Schema.Json)),
  data: Schema.optionalKey(Schema.Json),
  head: Schema.optionalKey(EntryId),
  edits: Schema.optionalKey(Schema.Array(ContextEdit)),
  byTaskId: Schema.optionalKey(TaskId),
})
/**
 * Committed conversation entry with identity, timestamp and stored content.
 *
 * @category models
 */
export type Entry = typeof Entry.Type
/** Persisted task outcomes; checkpoint data never substitutes for an outcome receipt. */
export const TaskOutcome = Schema.Union([
  Schema.Struct({ status: Schema.tag('completed'), result: Schema.Json }),
  Schema.Struct({
    status: Schema.tag('failed'),
    error: Schema.Struct({ message: Schema.String }),
    result: Schema.optionalKey(Schema.Json),
  }),
  Schema.Struct({
    status: Schema.tag('aborted'),
    reason: Schema.optionalKey(Schema.String),
    result: Schema.optionalKey(Schema.Json),
  }),
  Schema.Struct({ status: Schema.tag('orphaned'), reason: Schema.String }),
  Schema.Struct({
    status: Schema.tag('faulted'),
    error: Schema.Struct({ message: Schema.String }),
  }),
])
export type TaskOutcome = typeof TaskOutcome.Type

const hasCheckpoint = Schema.is(Schema.Struct({ phase: Schema.NonEmptyString }))
const hasOutcome = Schema.is(TaskOutcome)
/**
 * Durable phase state with state-specific required and forbidden fields.
 *
 * The shared shape preserves convenient property access while refinements reject
 * invalid combinations at persistence and transport decoding boundaries.
 */
export const TaskState = Schema.Struct({
  status: Schema.Literals(['pending', 'running', 'waiting', 'completing', 'terminal']),
  checkpoint: Schema.optionalKey(Schema.Json),
  on: Schema.optionalKey(Schema.Array(TaskId)),
  policy: Schema.optionalKey(Schema.Literals(['failFast', 'allSettled'])),
  outcome: Schema.optionalKey(Schema.Json),
}).check(
  Schema.makeFilter(
    (state) => {
      switch (state.status) {
        case 'pending':
        case 'running':
          return (
            hasCheckpoint(state.checkpoint) &&
            state.on === undefined &&
            state.policy === undefined &&
            state.outcome === undefined
          )
        case 'waiting':
          return (
            hasCheckpoint(state.checkpoint) &&
            state.on !== undefined &&
            state.policy !== undefined &&
            state.outcome === undefined
          )
        case 'completing':
        case 'terminal':
          return (
            hasOutcome(state.outcome) &&
            state.checkpoint === undefined &&
            state.on === undefined &&
            state.policy === undefined
          )
      }
    },
    { message: 'Task state fields do not match its status' },
  ),
)
export type TaskState = typeof TaskState.Type

/** Schema for owned checkpoint-based work and its terminal receipt. */
export const Task = Schema.Struct({
  id: TaskId,
  conversationId: ConversationId,
  kind: Schema.NonEmptyString,
  version: safe,
  input: Schema.Json,
  owner: Schema.optionalKey(TaskId),
  background: Schema.Boolean,
  abortRequested: Schema.Boolean,
  state: TaskState,
  memos: Schema.optionalKey(Schema.JsonObject),
}).check(
  Schema.makeFilter(
    (task) =>
      (task.state.status !== 'terminal' && task.state.status !== 'completing') ||
      task.memos === undefined,
    { message: 'Completed tasks cannot retain invocation memos' },
  ),
)
/** Owned checkpoint-based work with immutable identity and ownership fields. */
export type Task = typeof Task.Type
const submissionIdentity = {
  id: SubmissionId,
  conversationId: ConversationId,
  requestId: Schema.optionalKey(RequestId),
}
const noSettlement = {
  answer: Schema.optionalKey(Schema.Never),
  reason: Schema.optionalKey(Schema.Never),
  detail: Schema.optionalKey(Schema.Never),
}
/**
 * Schema for input admitted to a busy conversation and awaiting placement.
 *
 * @category schemas
 */
export const InputQueued = Schema.TaggedStruct('InputQueued', {
  ...submissionIdentity,
  type: Schema.tag('input'),
  status: Schema.tag('queued'),
  entry: Schema.optionalKey(Schema.Never),
  ...noSettlement,
})
/**
 * Input admitted to a busy conversation and awaiting placement.
 *
 * @category models
 */
export type InputQueued = typeof InputQueued.Type

/**
 * Schema for input attached to an entry and awaiting its settled answer.
 *
 * @category schemas
 */
export const InputPlaced = Schema.TaggedStruct('InputPlaced', {
  ...submissionIdentity,
  type: Schema.tag('input'),
  status: Schema.tag('placed'),
  entry: EntryId,
  ...noSettlement,
})
/**
 * Input attached to an entry and awaiting its settled answer.
 *
 * @category models
 */
export type InputPlaced = typeof InputPlaced.Type

/**
 * Schema for input settled with a committed answer entry.
 *
 * @category schemas
 */
export const InputDone = Schema.TaggedStruct('InputDone', {
  ...submissionIdentity,
  type: Schema.tag('input'),
  status: Schema.tag('done'),
  entry: EntryId,
  answer: EntryId,
  reason: Schema.optionalKey(Schema.Never),
  detail: Schema.optionalKey(Schema.Never),
})
/**
 * Input settled with a committed answer entry.
 *
 * @category models
 */
export type InputDone = typeof InputDone.Type

/**
 * Schema for input settled without an answer, with a reason and optional detail.
 *
 * @category schemas
 */
export const InputUnanswered = Schema.TaggedStruct('InputUnanswered', {
  ...submissionIdentity,
  type: Schema.tag('input'),
  status: Schema.tag('unanswered'),
  entry: Schema.optionalKey(EntryId),
  answer: Schema.optionalKey(Schema.Never),
  reason: Schema.String,
  detail: Schema.optionalKey(Schema.Json),
})
/**
 * Input settled without an answer, with a reason and optional detail.
 *
 * @category models
 */
export type InputUnanswered = typeof InputUnanswered.Type

/**
 * Schema for passive write awaiting an admission boundary.
 *
 * @category schemas
 */
export const WriteQueued = Schema.TaggedStruct('WriteQueued', {
  ...submissionIdentity,
  type: Schema.tag('write'),
  status: Schema.tag('queued'),
  entry: Schema.optionalKey(Schema.Never),
  ...noSettlement,
})
/**
 * Passive write awaiting an admission boundary.
 *
 * @category models
 */
export type WriteQueued = typeof WriteQueued.Type

/**
 * Schema for passive write settled with its committed entry.
 *
 * @category schemas
 */
export const WriteDone = Schema.TaggedStruct('WriteDone', {
  ...submissionIdentity,
  type: Schema.tag('write'),
  status: Schema.tag('done'),
  entry: EntryId,
  ...noSettlement,
})
/**
 * Passive write settled with its committed entry.
 *
 * @category models
 */
export type WriteDone = typeof WriteDone.Type

/**
 * Schema for passive write settled without placement.
 *
 * @category schemas
 */
export const WriteUnanswered = Schema.TaggedStruct('WriteUnanswered', {
  ...submissionIdentity,
  type: Schema.tag('write'),
  status: Schema.tag('unanswered'),
  entry: Schema.optionalKey(Schema.Never),
  answer: Schema.optionalKey(Schema.Never),
  reason: Schema.String,
  detail: Schema.optionalKey(Schema.Json),
})
/**
 * Passive write settled without placement.
 *
 * @category models
 */
export type WriteUnanswered = typeof WriteUnanswered.Type

/**
 * Schema for terminal input or passive-write receipt.
 *
 * @category schemas
 */
export const SettledSubmission = Schema.Union([
  InputDone,
  InputUnanswered,
  WriteDone,
  WriteUnanswered,
])
/**
 * Terminal input or passive-write receipt.
 *
 * @category models
 */
export type SettledSubmission = typeof SettledSubmission.Type
/**
 * Schema for persisted input or passive write progressing from admission to settlement.
 *
 * @category schemas
 */
export const Submission = Schema.Union([
  InputQueued,
  InputPlaced,
  InputDone,
  InputUnanswered,
  WriteQueued,
  WriteDone,
  WriteUnanswered,
])
/**
 * Persisted input or passive write progressing from admission to settlement.
 *
 * @category models
 */
export type Submission = typeof Submission.Type
/**
 * Schema for session, conversation or task ownership of a document.
 *
 * @category schemas
 */
export const Scope = Schema.Union([
  Schema.TaggedStruct('session', {}),
  Schema.TaggedStruct('conversation', { conversationId: ConversationId }),
  Schema.TaggedStruct('task', { taskId: TaskId }),
])
/**
 * Session, conversation or task ownership of a document.
 *
 * @category models
 */
export type Scope = typeof Scope.Type
/**
 * Schema for document incarnation and its schema, scope, history and fork policies.
 *
 * @category schemas
 */
export const Document = Schema.Struct({
  id: DocumentId,
  kind: Schema.String,
  scope: Scope,
  key: Schema.optionalKey(Schema.String),
  createdAt: Seq,
  retiredAt: Schema.optionalKey(Seq),
  history: Schema.optionalKey(Schema.Literals(['latest', 'rewindable'])),
  fork: Schema.optionalKey(Schema.Literals(['asOf', 'current', 'initial'])),
})
/**
 * Document incarnation and its schema, scope, history and fork policies.
 *
 * @category models
 */
export type Document = typeof Document.Type
/**
 * Initial persisted document metadata before identity allocation.
 *
 * @category combinators
 */
export const DocumentCreate = Document.mapFields(Struct.omit(['createdAt', 'retiredAt']))
/**
 * Initial persisted document metadata before identity allocation.
 *
 * @category models
 */
export type DocumentCreate = typeof DocumentCreate.Type
/**
 * Logical document address determined by kind, scope and optional family key.
 *
 * @category models
 */
export type Address = Pick<Document, 'kind' | 'scope' | 'key'>
/**
 * Latest or historical entry cutoff for a document read.
 *
 * @category models
 */
export type Point = Seq | 'current'
/**
 * Serializable operations keep exact structural no-ops and root replacements observable.
 *
 * @category schemas
 */
export const Op = Schema.Union([
  Schema.Tuple([
    Schema.Literal('set'),
    Schema.NonEmptyArray(Schema.Union([Schema.String, Schema.Finite])),
    Schema.Json,
  ]),
  Schema.Tuple([
    Schema.Literal('delete'),
    Schema.NonEmptyArray(Schema.Union([Schema.String, Schema.Finite])),
  ]),
  Schema.Tuple([Schema.Literal('replace'), Schema.JsonObject]),
])
/**
 * Persisted object/array mutation or full document replacement.
 *
 * @category models
 */
export type Op = typeof Op.Type
/**
 * Schema for full document checkpoint or incremental mutation payload.
 *
 * @category schemas
 */
export const Content = Schema.Union([
  Schema.TaggedStruct('base', { version: safe, value: Schema.JsonObject }),
  Schema.TaggedStruct('delta', { version: safe, ops: Schema.Array(Op) }),
])
/**
 * Full document checkpoint or incremental mutation payload.
 *
 * @category models
 */
export type Content = typeof Content.Type
/**
 * Schema for domain mutation included in an atomic Persistence commit.
 *
 * @category schemas
 */
export const Write = Schema.Union([
  Schema.TaggedStruct('conversation', { value: Conversation }),
  Schema.TaggedStruct('entry', { value: Entry }),
  Schema.TaggedStruct('task', { value: Task }),
  Schema.TaggedStruct('submission', { value: Submission }),
  Schema.TaggedStruct('document.create', {
    record: DocumentCreate,
    content: Content,
  }),
  Schema.TaggedStruct('document.copy', {
    record: DocumentCreate,
    source: Schema.Struct({ id: DocumentId, at: Schema.Union([Seq, Schema.Literal('current')]) }),
  }),
  Schema.TaggedStruct('document.change', {
    id: DocumentId,
    content: Content,
    publicationOps: Schema.optionalKey(Schema.Array(Op)),
  }),
  Schema.TaggedStruct('document.retire', { id: DocumentId }),
])
/**
 * Domain mutation included in an atomic Persistence commit.
 *
 * @category models
 */
export type Write = typeof Write.Type
/**
 * Schema for document version and content saved at one commit sequence.
 *
 * @category schemas
 */
export const Revision = Schema.Struct({ seq: Seq, content: Content })
/**
 * Document version and content saved at one commit sequence.
 *
 * @category models
 */
export type Revision = typeof Revision.Type
/**
 * Schema for document metadata together with its retained revision history.
 *
 * @category schemas
 */
export const StoredDocument = Schema.Struct({ record: Document, revisions: Schema.Array(Revision) })
/**
 * Document metadata together with its retained revision history.
 *
 * @category models
 */
export type StoredDocument = typeof StoredDocument.Type
/**
 * Schema for committed sequence and affected domain categories for observation.
 *
 * @category schemas
 */
export const Publication = Schema.Struct({
  record: Document,
  version: Schema.optionalKey(Schema.Finite),
  value: Schema.NullOr(Schema.JsonObject),
  ops: Schema.Array(Op),
})
/**
 * Committed sequence and affected domain categories for observation.
 *
 * @category models
 */
export type Publication = typeof Publication.Type
/**
 * Schema for atomic commit frame containing writes and publication metadata.
 *
 * @category schemas
 */
export const Frame = Schema.Struct({
  seq: Seq,
  writes: Schema.Array(Write),
  documents: Schema.Array(Publication),
})
/**
 * Atomic commit frame containing writes and publication metadata.
 *
 * @category models
 */
export type Frame = typeof Frame.Type
/**
 * Returns the stable serialized key for a document address.
 *
 * @category combinators
 */
export const addressKey = (self: Address): string =>
  JSON.stringify([
    self.kind,
    scopeKey(self.scope),
    self.key === undefined ? ['singleton'] : ['family', self.key],
  ])
/**
 * Returns the stable serialized key for a document scope.
 *
 * @category combinators
 */
export const scopeKey = (self: Scope): string => {
  switch (self._tag) {
    case 'session':
      return 'session'
    case 'conversation':
      return `conversation:${self.conversationId}`
    case 'task':
      return `task:${self.taskId}`
  }
}
const isAliveImpl = (self: Document, at: Point): boolean =>
  at === 'current'
    ? self.retiredAt === undefined
    : self.createdAt <= at && (self.retiredAt === undefined || at < self.retiredAt)
/**
 * Returns whether the value satisfies CurrentOnly.
 *
 * @category guards
 */
export const isCurrentOnly = (self: DocumentCreate): boolean =>
  self.scope._tag !== 'conversation' || self.history === 'latest'
const EntryTokenTypeId = '~@effect-harness/durable/Record/EntryToken'
/**
 * Failure reporting an invalid typed entry kind or definition.
 *
 * @category errors
 */
export class EntryDefinitionError extends Schema.TaggedError<EntryDefinitionError>(
  '@effect-harness/durable/Record/EntryDefinitionError',
)('EntryDefinitionError', { message: Schema.String }) {}
/**
 * Entry token with its decoding service requirements retained.
 *
 * @category models
 */
export interface DecodedEntryToken<
  K extends string,
  out S extends Schema.Constraint,
> extends Entry.Token<K> {
  readonly schema: S
  readonly decode: (
    input: unknown,
  ) => import('effect/Effect').Effect<S['Type'], Schema.SchemaError, S['DecodingServices']>
}
/**
 * Validates an entry kind and creates its typed token.
 *
 * @category constructors
 */
export const defineEntry = <const K extends string, S extends Schema.Constraint>(
  kind: K,
  schema: S,
): Result.Result<DecodedEntryToken<K, S>, EntryDefinitionError> =>
  kind.length === 0
    ? Result.fail(new EntryDefinitionError({ message: 'Entry kind must be nonempty' }))
    : Result.succeed(
        makeEntryToken({
          kind,
          is: (entry: Entry | undefined): entry is Entry & { readonly kind: K } =>
            entry?.kind === kind,
          schema,
          decode: Schema.decodeUnknownEffect(schema),
        }),
      )
/**
 * Creates a typed entry token or throws for an invalid kind.
 *
 * @category constructors
 */
export const defineEntryUnsafe = <const K extends string, S extends Schema.Constraint>(
  kind: K,
  schema: S,
): DecodedEntryToken<K, S> => Result.getOrThrow(defineEntry(kind, schema))
/**
 * Schema for admission, placement and terminal settlement states of a submission.
 *
 * @category schemas
 */
export const SubmissionStatus = Schema.Literals(['queued', 'placed', 'done', 'unanswered'])

/**
 * Creates a typed entry token without reading input accessors.
 *
 * @category constructors
 */
export const makeEntryToken = <K extends string, S extends Schema.Constraint>(
  input: handle.Input<DecodedEntryToken<K, S>, typeof EntryTokenTypeId>,
): DecodedEntryToken<K, S> => {
  const value = handle.make(
    EntryTokenProto,
    handle.marked(input, EntryTokenTypeId, { _K: identity }),
  )
  return value
}
/**
 * Checks whether a value carries the nominal `EntryToken` marker.
 *
 * **Gotchas**
 *
 * This checks library identity, not the validity of arbitrary fields or stored JSON.
 *
 * @category guards
 */
export const isEntryToken = (u: unknown): u is Entry.Token =>
  Predicate.hasProperty(u, EntryTokenTypeId)

/**
 * Admission, placement and terminal settlement states of a submission.
 *
 * @category models
 */
export type SubmissionStatus = typeof SubmissionStatus.Type

/**
 * Returns whether the value satisfies Alive.
 *
 * @category guards
 */
export const isAlive: {
  (at: Point): (self: Document) => boolean
  (self: Document, at: Point): boolean
} = dual(2, isAliveImpl)

/**
 * Type-level contracts for `Entry`.
 *
 */
export declare namespace Entry {
  /**
   * Entry fields supplied before a committed identity is assigned.
   *
   * @category models
   */
  export type Draft = Omit<Entry, 'id' | 'conversationId' | 'head'> & {
    readonly head?: EntryId | 'self' | undefined
  }
  /**
   * Committed entry whose data has a caller-selected decoded type.
   *
   * @category models
   */
  export type WithData<D extends Schema.Json> = Omit<Entry, 'data'> &
    ([D] extends [never] ? { readonly data?: undefined } : { readonly data: D })
  /**
   * Uncommitted entry draft with caller-selected decoded data.
   *
   * @category models
   */
  export type DraftWithData<D extends Schema.Json> = Omit<Entry.Draft, 'kind' | 'data'> &
    ([D] extends [never] ? { readonly data?: undefined } : { readonly data: D })
  /**
   * Nominal kind token used to identify typed entry records.
   *
   * @category models
   */
  export interface Token<out K extends string = string>
    extends Pipeable.Pipeable, Inspectable.Inspectable {
    readonly [EntryTokenTypeId]: { readonly _K: Types.Covariant<K> }
    readonly kind: K
    readonly is: (entry: Entry | undefined) => entry is Entry & { readonly kind: K }
  }
}

/**
 * Type-level contracts for `Submission`.
 *
 */
export declare namespace Submission {
  /**
   * Record fields supplied before durable identity allocation.
   *
   * @category models
   */
  export type Create = Submission extends infer A
    ? A extends Submission
      ? Omit<A, 'id'>
      : never
    : never
}

/**
 * Type-level contracts for `Document`.
 *
 */
export declare namespace Document {
  /**
   * Record fields supplied before durable identity allocation.
   *
   * @category models
   */
  export type Create = DocumentCreate
}

/** Checks the decoded EntryDefinitionError contract without decoding or coercing input.
 * @category guards
 */
export const isEntryDefinitionError: (u: unknown) => u is EntryDefinitionError = Schema.is(
  Schema.toType(EntryDefinitionError),
)

/** Checks the decoded ConversationId contract without decoding or coercing input.
 * @category guards
 */
export const isConversationId: (u: unknown) => u is ConversationId = Schema.is(
  Schema.toType(ConversationId),
)

/** Checks the decoded EntryId contract without decoding or coercing input.
 * @category guards
 */
export const isEntryId: (u: unknown) => u is EntryId = Schema.is(Schema.toType(EntryId))

/** Checks the decoded SubmissionId contract without decoding or coercing input.
 * @category guards
 */
export const isSubmissionId: (u: unknown) => u is SubmissionId = Schema.is(
  Schema.toType(SubmissionId),
)

/** Checks the decoded DocumentId contract without decoding or coercing input.
 * @category guards
 */
export const isDocumentId: (u: unknown) => u is DocumentId = Schema.is(Schema.toType(DocumentId))

/** Checks the decoded Seq contract without decoding or coercing input.
 * @category guards
 */
export const isSeq: (u: unknown) => u is Seq = Schema.is(Schema.toType(Seq))

/** Checks the decoded JournalCursor contract without decoding or coercing input.
 * @category guards
 */
export const isJournalCursor: (u: unknown) => u is JournalCursor = Schema.is(
  Schema.toType(JournalCursor),
)

/** Checks the decoded Conversation contract without decoding or coercing input.
 * @category guards
 */
export const isConversation: (u: unknown) => u is Conversation = Schema.is(
  Schema.toType(Conversation),
)

/** Checks the decoded ContextEdit contract without decoding or coercing input.
 * @category guards
 */
export const isContextEdit: (u: unknown) => u is ContextEdit = Schema.is(Schema.toType(ContextEdit))

/** Checks the decoded Entry contract without decoding or coercing input.
 * @category guards
 */
export const isEntry: (u: unknown) => u is Entry = Schema.is(Schema.toType(Entry))

/** Checks the decoded Task contract without decoding or coercing input.
 * @category guards
 */
export const isTask: (u: unknown) => u is Task = Schema.is(Schema.toType(Task))

/** Checks the decoded InputQueued contract without decoding or coercing input.
 * @category guards
 */
export const isInputQueued: (u: unknown) => u is InputQueued = Schema.is(Schema.toType(InputQueued))

/** Checks the decoded InputPlaced contract without decoding or coercing input.
 * @category guards
 */
export const isInputPlaced: (u: unknown) => u is InputPlaced = Schema.is(Schema.toType(InputPlaced))

/** Checks the decoded InputDone contract without decoding or coercing input.
 * @category guards
 */
export const isInputDone: (u: unknown) => u is InputDone = Schema.is(Schema.toType(InputDone))

/** Checks the decoded InputUnanswered contract without decoding or coercing input.
 * @category guards
 */
export const isInputUnanswered: (u: unknown) => u is InputUnanswered = Schema.is(
  Schema.toType(InputUnanswered),
)

/** Checks the decoded WriteQueued contract without decoding or coercing input.
 * @category guards
 */
export const isWriteQueued: (u: unknown) => u is WriteQueued = Schema.is(Schema.toType(WriteQueued))

/** Checks the decoded WriteDone contract without decoding or coercing input.
 * @category guards
 */
export const isWriteDone: (u: unknown) => u is WriteDone = Schema.is(Schema.toType(WriteDone))

/** Checks the decoded WriteUnanswered contract without decoding or coercing input.
 * @category guards
 */
export const isWriteUnanswered: (u: unknown) => u is WriteUnanswered = Schema.is(
  Schema.toType(WriteUnanswered),
)

/** Checks the decoded SettledSubmission contract without decoding or coercing input.
 * @category guards
 */
export const isSettledSubmission: (u: unknown) => u is SettledSubmission = Schema.is(
  Schema.toType(SettledSubmission),
)

/** Checks the decoded Submission contract without decoding or coercing input.
 * @category guards
 */
export const isSubmission: (u: unknown) => u is Submission = Schema.is(Schema.toType(Submission))

/** Checks the decoded Scope contract without decoding or coercing input.
 * @category guards
 */
export const isScope: (u: unknown) => u is Scope = Schema.is(Schema.toType(Scope))

/** Checks the decoded Document contract without decoding or coercing input.
 * @category guards
 */
export const isDocument: (u: unknown) => u is Document = Schema.is(Schema.toType(Document))

/** Checks the decoded DocumentCreate contract without decoding or coercing input.
 * @category guards
 */
export const isDocumentCreate: (u: unknown) => u is DocumentCreate = Schema.is(
  Schema.toType(DocumentCreate),
)

/** Checks the decoded Op contract without decoding or coercing input.
 * @category guards
 */
export const isOp: (u: unknown) => u is Op = Schema.is(Schema.toType(Op))

/** Checks the decoded Content contract without decoding or coercing input.
 * @category guards
 */
export const isContent: (u: unknown) => u is Content = Schema.is(Schema.toType(Content))

/** Checks the decoded Write contract without decoding or coercing input.
 * @category guards
 */
export const isWrite: (u: unknown) => u is Write = Schema.is(Schema.toType(Write))

/** Checks the decoded Revision contract without decoding or coercing input.
 * @category guards
 */
export const isRevision: (u: unknown) => u is Revision = Schema.is(Schema.toType(Revision))

/** Checks the decoded StoredDocument contract without decoding or coercing input.
 * @category guards
 */
export const isStoredDocument: (u: unknown) => u is StoredDocument = Schema.is(
  Schema.toType(StoredDocument),
)

/** Checks the decoded Publication contract without decoding or coercing input.
 * @category guards
 */
export const isPublication: (u: unknown) => u is Publication = Schema.is(Schema.toType(Publication))

/** Checks the decoded Frame contract without decoding or coercing input.
 * @category guards
 */
export const isFrame: (u: unknown) => u is Frame = Schema.is(Schema.toType(Frame))

/** Checks the decoded SubmissionStatus contract without decoding or coercing input.
 * @category guards
 */
export const isSubmissionStatus: (u: unknown) => u is SubmissionStatus = Schema.is(
  Schema.toType(SubmissionStatus),
)

/** Checks a task identity without recovering its phantom result type.
 * @category guards
 */
export const isTaskId: (u: unknown) => u is TaskId = Schema.is(TaskId)
