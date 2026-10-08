/**
 * Durable facts, identifiers, journal frames and legacy-compatible codecs.
 */
import { dual } from 'effect/Function'
import * as handle from './internal/handle.ts'
const EntryTokenProto = handle.prototype('@effect-harness/durable/Record/EntryToken')
import type * as Pipeable from 'effect/Pipeable'
import type * as Inspectable from 'effect/Inspectable'
import { tagged } from './internal/legacyTag.ts'
import { identity } from 'effect/Function'
import type * as Types from 'effect/Types'
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
 * JSON value accepted by domain storage and replay receipts.
 *
 * @category models
 */
export type Json = Schema.Json
/**
 * JSON object used as a document storage representation.
 *
 * @category models
 */
export type JsonObject = Schema.JsonObject
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
  tagged('omit', { target: EntryId, action: Schema.tag('omit') }),
  tagged('replace', {
    target: EntryId,
    action: Schema.tag('replace'),
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
/**
 * Entry content supplied before the session assigns its durable identity.
 *
 * @category models
 */
export type EntryDraft = Entry.Draft
/**
 * Schema for owned work with its native execution binding, abort mark and domain completion
 * state.
 *
 * @category schemas
 */
export const Task = Schema.Struct({
  id: TaskId,
  conversationId: ConversationId,
  kind: Schema.String,
  version: safe,
  input: Schema.Json,
  owner: Schema.optionalKey(TaskId),
  background: Schema.Boolean,
  abortRequested: Schema.Boolean,
  state: Schema.Struct({
    status: Schema.Literals(['pending', 'running', 'waiting', 'completing', 'terminal']),
    checkpoint: Schema.optionalKey(Schema.Json),
    on: Schema.optionalKey(Schema.Array(TaskId)),
    policy: Schema.optionalKey(Schema.Literals(['failFast', 'allSettled'])),
    outcome: Schema.optionalKey(Schema.Json),
  }),
  memos: Schema.optionalKey(Schema.JsonObject),
})
/**
 * Owned work with its native execution binding, abort mark and domain completion state.
 *
 * @category models
 */
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
export const InputQueued = tagged('InputQueued', {
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
export const InputPlaced = tagged('InputPlaced', {
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
export const InputDone = tagged('InputDone', {
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
export const InputUnanswered = tagged('InputUnanswered', {
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
export const WriteQueued = tagged('WriteQueued', {
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
export const WriteDone = tagged('WriteDone', {
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
export const WriteUnanswered = tagged('WriteUnanswered', {
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
 * Submission content supplied before durable identity allocation.
 *
 * @category models
 */
export type SubmissionCreate = Submission.Create
/**
 * Schema for session, conversation or task ownership of a document.
 *
 * @category schemas
 */
export const Scope = Schema.Union([
  tagged('session', { kind: Schema.tag('session') }),
  tagged('conversation', { kind: Schema.tag('conversation'), conversationId: ConversationId }),
  tagged('task', { kind: Schema.tag('task'), taskId: TaskId }),
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
  tagged('base', { kind: Schema.tag('base'), version: safe, value: Schema.JsonObject }),
  tagged('delta', { kind: Schema.tag('delta'), version: safe, ops: Schema.Array(Op) }),
])
/**
 * Full document checkpoint or incremental mutation payload.
 *
 * @category models
 */
export type Content = typeof Content.Type
/**
 * Schema for domain mutation included in an atomic Store commit.
 *
 * @category schemas
 */
export const Write = Schema.Union([
  tagged('conversation', { type: Schema.tag('conversation'), value: Conversation }),
  tagged('entry', { type: Schema.tag('entry'), value: Entry }),
  tagged('task', { type: Schema.tag('task'), value: Task }),
  tagged('submission', { type: Schema.tag('submission'), value: Submission }),
  tagged('document.create', {
    type: Schema.tag('document.create'),
    record: DocumentCreate,
    content: Content,
  }),
  tagged('document.copy', {
    type: Schema.tag('document.copy'),
    record: DocumentCreate,
    source: Schema.Struct({ id: DocumentId, at: Schema.Union([Seq, Schema.Literal('current')]) }),
  }),
  tagged('document.change', {
    type: Schema.tag('document.change'),
    id: DocumentId,
    content: Content,
    publicationOps: Schema.optionalKey(Schema.Array(Op)),
  }),
  tagged('document.retire', { type: Schema.tag('document.retire'), id: DocumentId }),
])
/**
 * Domain mutation included in an atomic Store commit.
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
 * Schema for idempotency key, optional fingerprint and saved JSON or void result.
 *
 * @category schemas
 */
export const Receipt = Schema.Struct({
  key: Schema.String,
  fingerprint: Schema.String,
  result: Schema.Json,
  resultIsVoid: Schema.optionalKey(Schema.Boolean),
  seq: Seq,
})
/**
 * Idempotency key, optional fingerprint and saved JSON or void result.
 *
 * @category models
 */
export type Receipt = typeof Receipt.Type
/**
 * Schema for authoritative session records, document histories, receipts and allocation
 * counters.
 *
 * @category schemas
 */
export const State = Schema.Struct({
  format: Schema.Literal(1),
  nextId: Schema.Finite.check(Schema.makeFilter((n: number) => Number.isInteger(n))).check(
    Schema.isBetween({ minimum: 2, maximum: Number.MAX_SAFE_INTEGER + 1 }),
  ),
  nextSeq: Schema.Finite.check(Schema.makeFilter((n: number) => Number.isInteger(n))).check(
    Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER + 1 }),
  ),
  conversations: Schema.Array(Conversation),
  entries: Schema.Array(Schema.Struct({ entry: Entry, commitSeq: Seq })),
  tasks: Schema.Array(Task),
  submissions: Schema.Array(Submission),
  documents: Schema.Array(StoredDocument),
  receipts: Schema.Array(Receipt),
})
/**
 * Authoritative session records, document histories, receipts and allocation counters.
 *
 * @category models
 */
export type State = typeof State.Type
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
const PageTypeId = '~@effect-harness/durable/Record/Page'
/**
 * Bounded scan results with an optional continuation cursor.
 *
 * @category models
 */
export interface Page<out A> {
  readonly [PageTypeId]: { readonly _A: Types.Covariant<A> }

  readonly items: ReadonlyArray<A>
  readonly next?: { readonly after: number } | undefined
}
/**
 * Continuation position for a paginated record scan.
 *
 * @category models
 */
export interface Cursor {
  readonly after: number
}
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
  if (self.kind === 'session') return 'session'
  if (self.kind === 'conversation') return `conversation:${self.conversationId}`
  return `task:${self.taskId}`
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
  self.scope.kind !== 'conversation' || self.history === 'latest'
/**
 * Creates an empty durable state with initial allocation counters.
 *
 * @category combinators
 */
export const emptyState = (): State => ({
  format: 1,
  nextId: 2,
  nextSeq: 1,
  conversations: [],
  entries: [],
  tasks: [],
  submissions: [],
  documents: [],
  receipts: [],
})

/**
 * Committed entry whose data is decoded by an entry token.
 *
 * @category models
 */
export type TypedEntry<D extends Json> = Entry.WithData<D>
/**
 * Decoded entry data supplied before durable identity allocation.
 *
 * @category models
 */
export type TypedEntryDraft<D extends Json> = Entry.DraftWithData<D>
const EntryTokenTypeId = '~@effect-harness/durable/Record/EntryToken'
/**
 * Entry-kind token with identity guard and schema-based decoding.
 *
 * @category models
 */
export type EntryToken<K extends string = string> = Entry.Token<K>
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
  S extends Schema.Constraint,
> extends EntryToken<K> {
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
 * Creates a page without changing its input.
 *
 * @category constructors
 */
export const makePage = <A>(input: Omit<Page<A>, typeof PageTypeId>): Page<A> => {
  const value = Object.assign({}, input, { [PageTypeId]: { _A: identity } })
  Object.defineProperties(value, Object.getOwnPropertyDescriptors(input))
  Object.defineProperty(value, PageTypeId, { enumerable: false })
  return value
}
/**
 * Checks whether a value carries the nominal `Page` marker.
 *
 * **Gotchas**
 *
 * This checks library identity, not the validity of arbitrary fields or stored JSON.
 *
 * @category guards
 */
export const isPage = (input: unknown): input is Page<unknown> =>
  Predicate.hasProperty(input, PageTypeId)

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
export const isEntryToken = (input: unknown): input is EntryToken =>
  Predicate.hasProperty(input, EntryTokenTypeId)

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
 * @category utility types
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
  export type WithData<D extends Json> = Omit<Entry, 'data'> &
    ([D] extends [never] ? { readonly data?: undefined } : { readonly data: D })
  /**
   * Uncommitted entry draft with caller-selected decoded data.
   *
   * @category models
   */
  export type DraftWithData<D extends Json> = Omit<EntryDraft, 'kind' | 'data'> &
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
 * @category utility types
 */
export declare namespace Submission {
  /**
   * Record fields supplied before durable identity allocation.
   *
   * @category models
   */
  export type Create = Submission extends infer A
    ? A extends Submission
      ? Omit<A, 'id' | '_tag'> & { readonly _tag?: A['_tag'] | undefined }
      : never
    : never
}

/**
 * Type-level contracts for `Document`.
 *
 * @category utility types
 */
export declare namespace Document {
  /**
   * Record fields supplied before durable identity allocation.
   *
   * @category models
   */
  export type Create = DocumentCreate
}
