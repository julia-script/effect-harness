/**
 * Durable facts, identifiers, journal frames and legacy-compatible codecs.
 *
 * @since 0.0.0
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
import * as Identity from '@effect-harness/harness/Identity'
/**
 * Canonical conversation identifier schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ConversationId = Identity.ConversationId
/**
 * Canonical entry identifier schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const EntryId = Identity.EntryId
/**
 * Canonical conversation identifier schema.
 *
 * @category models
 * @since 0.0.0
 */
export type ConversationId = typeof ConversationId.Type
/**
 * Canonical entry identifier schema.
 *
 * @category models
 * @since 0.0.0
 */
export type EntryId = typeof EntryId.Type

const safe = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }))
/**
 * TaskId schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const TaskId = safe.pipe(Schema.brand('@effect-harness/durable/Record/TaskId'))
/**
 * TaskId contract.
 *
 * @category models
 * @since 0.0.0
 */
export type TaskId<A = Schema.Json> = typeof TaskId.Type & { readonly __result?: A | undefined }
/**
 * SubmissionId schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const SubmissionId = safe.pipe(Schema.brand('@effect-harness/durable/Record/SubmissionId'))
/**
 * SubmissionId contract.
 *
 * @category models
 * @since 0.0.0
 */
export type SubmissionId = typeof SubmissionId.Type
/**
 * DocumentId schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const DocumentId = safe.pipe(Schema.brand('@effect-harness/durable/Record/DocumentId'))
/**
 * DocumentId contract.
 *
 * @category models
 * @since 0.0.0
 */
export type DocumentId = typeof DocumentId.Type
/**
 * Seq schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Seq = safe.pipe(Schema.brand('@effect-harness/durable/Record/Seq'))
/**
 * Seq contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Seq = typeof Seq.Type
/**
 * JournalCursor schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const JournalCursor = Schema.Union([Seq, Schema.Literal(0)])
/**
 * JournalCursor contract.
 *
 * @category models
 * @since 0.0.0
 */
export type JournalCursor = typeof JournalCursor.Type
/**
 * ROOT_CONVERSATION_ID schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ROOT_CONVERSATION_ID = Schema.decodeSync(ConversationId)(1)
/**
 * Json contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Json = Schema.Json
/**
 * JsonObject contract.
 *
 * @category models
 * @since 0.0.0
 */
export type JsonObject = Schema.JsonObject
/**
 * Conversation schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Conversation = Schema.Struct({
  id: ConversationId,
  parent: Schema.optionalKey(Schema.Struct({ conversationId: ConversationId, at: EntryId })),
  owner: Schema.optionalKey(Schema.Struct({ conversationId: ConversationId, taskId: TaskId })),
})
/**
 * Conversation contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Conversation = typeof Conversation.Type
/**
 * ContextEdit schema.
 *
 * @category schemas
 * @since 0.0.0
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
 * ContextEdit contract.
 *
 * @category models
 * @since 0.0.0
 */
export type ContextEdit = typeof ContextEdit.Type
/**
 * Entry schema.
 *
 * @category schemas
 * @since 0.0.0
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
 * Entry contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Entry = typeof Entry.Type
/**
 * Compatibility alias for Entry.Draft.
 *
 * @category models
 * @since 0.0.0
 */
export type EntryDraft = Entry.Draft
/**
 * Task schema.
 *
 * @category schemas
 * @since 0.0.0
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
 * Task contract.
 *
 * @category models
 * @since 0.0.0
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
 * InputQueued schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const InputQueued = tagged('InputQueued', {
  ...submissionIdentity,
  type: Schema.tag('input'),
  status: Schema.tag('queued'),
  entry: Schema.optionalKey(Schema.Never),
  ...noSettlement,
})
/**
 * Decoded InputQueued values.
 *
 * @category models
 * @since 0.0.0
 */
export type InputQueued = typeof InputQueued.Type

/**
 * InputPlaced schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const InputPlaced = tagged('InputPlaced', {
  ...submissionIdentity,
  type: Schema.tag('input'),
  status: Schema.tag('placed'),
  entry: EntryId,
  ...noSettlement,
})
/**
 * Decoded InputPlaced values.
 *
 * @category models
 * @since 0.0.0
 */
export type InputPlaced = typeof InputPlaced.Type

/**
 * InputDone schema.
 *
 * @category schemas
 * @since 0.0.0
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
 * Decoded InputDone values.
 *
 * @category models
 * @since 0.0.0
 */
export type InputDone = typeof InputDone.Type

/**
 * InputUnanswered schema.
 *
 * @category schemas
 * @since 0.0.0
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
 * Decoded InputUnanswered values.
 *
 * @category models
 * @since 0.0.0
 */
export type InputUnanswered = typeof InputUnanswered.Type

/**
 * WriteQueued schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const WriteQueued = tagged('WriteQueued', {
  ...submissionIdentity,
  type: Schema.tag('write'),
  status: Schema.tag('queued'),
  entry: Schema.optionalKey(Schema.Never),
  ...noSettlement,
})
/**
 * Decoded WriteQueued values.
 *
 * @category models
 * @since 0.0.0
 */
export type WriteQueued = typeof WriteQueued.Type

/**
 * WriteDone schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const WriteDone = tagged('WriteDone', {
  ...submissionIdentity,
  type: Schema.tag('write'),
  status: Schema.tag('done'),
  entry: EntryId,
  ...noSettlement,
})
/**
 * Decoded WriteDone values.
 *
 * @category models
 * @since 0.0.0
 */
export type WriteDone = typeof WriteDone.Type

/**
 * WriteUnanswered schema.
 *
 * @category schemas
 * @since 0.0.0
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
 * Decoded WriteUnanswered values.
 *
 * @category models
 * @since 0.0.0
 */
export type WriteUnanswered = typeof WriteUnanswered.Type

/**
 * SettledSubmission schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const SettledSubmission = Schema.Union([
  InputDone,
  InputUnanswered,
  WriteDone,
  WriteUnanswered,
])
/**
 * SettledSubmission contract.
 *
 * @category models
 * @since 0.0.0
 */
export type SettledSubmission = typeof SettledSubmission.Type
/**
 * Submission schema.
 *
 * @category schemas
 * @since 0.0.0
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
 * Submission contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Submission = typeof Submission.Type
/**
 * Compatibility alias for Submission.Create.
 *
 * @category models
 * @since 0.0.0
 */
export type SubmissionCreate = Submission.Create
/**
 * Scope schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Scope = Schema.Union([
  tagged('session', { kind: Schema.tag('session') }),
  tagged('conversation', { kind: Schema.tag('conversation'), conversationId: ConversationId }),
  tagged('task', { kind: Schema.tag('task'), taskId: TaskId }),
])
/**
 * Scope contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Scope = typeof Scope.Type
/**
 * Document schema.
 *
 * @category schemas
 * @since 0.0.0
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
 * Document contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Document = typeof Document.Type
/**
 * Document creation record schema.
 *
 * @category combinators
 * @since 0.0.0
 */
export const DocumentCreate = Document.mapFields(Struct.omit(['createdAt', 'retiredAt']))
/**
 * Document creation record schema.
 *
 * @category models
 * @since 0.0.0
 */
export type DocumentCreate = typeof DocumentCreate.Type
/**
 * Address contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Address = Pick<Document, 'kind' | 'scope' | 'key'>
/**
 * Point contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Point = Seq | 'current'
/**
 * Serializable operations keep exact structural no-ops and root replacements observable.
 *
 * @category schemas
 * @since 0.0.0
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
 * Op contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Op = typeof Op.Type
/**
 * Content schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Content = Schema.Union([
  tagged('base', { kind: Schema.tag('base'), version: safe, value: Schema.JsonObject }),
  tagged('delta', { kind: Schema.tag('delta'), version: safe, ops: Schema.Array(Op) }),
])
/**
 * Content contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Content = typeof Content.Type
/**
 * Write schema.
 *
 * @category schemas
 * @since 0.0.0
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
 * Write contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Write = typeof Write.Type
/**
 * Revision schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Revision = Schema.Struct({ seq: Seq, content: Content })
/**
 * Revision contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Revision = typeof Revision.Type
/**
 * StoredDocument schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const StoredDocument = Schema.Struct({ record: Document, revisions: Schema.Array(Revision) })
/**
 * StoredDocument contract.
 *
 * @category models
 * @since 0.0.0
 */
export type StoredDocument = typeof StoredDocument.Type
/**
 * Receipt schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Receipt = Schema.Struct({
  key: Schema.String,
  fingerprint: Schema.String,
  result: Schema.Json,
  resultIsVoid: Schema.optionalKey(Schema.Boolean),
  seq: Seq,
})
/**
 * Receipt contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Receipt = typeof Receipt.Type
/**
 * State schema.
 *
 * @category schemas
 * @since 0.0.0
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
 * State contract.
 *
 * @category models
 * @since 0.0.0
 */
export type State = typeof State.Type
/**
 * Publication schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Publication = Schema.Struct({
  record: Document,
  version: Schema.optionalKey(Schema.Finite),
  value: Schema.NullOr(Schema.JsonObject),
  ops: Schema.Array(Op),
})
/**
 * Publication contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Publication = typeof Publication.Type
/**
 * Frame schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Frame = Schema.Struct({
  seq: Seq,
  writes: Schema.Array(Write),
  documents: Schema.Array(Publication),
})
/**
 * Frame contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Frame = typeof Frame.Type
const PageTypeId = '~@effect-harness/durable/Record/Page'
/**
 * Page contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface Page<out A> {
  readonly [PageTypeId]: { readonly _A: Types.Covariant<A> }

  readonly items: ReadonlyArray<A>
  readonly next?: { readonly after: number } | undefined
}
/**
 * Cursor contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface Cursor {
  readonly after: number
}
/**
 * Returns the stable serialized key for a document address.
 *
 * @category combinators
 * @since 0.0.0
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
 * @since 0.0.0
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
 * @since 0.0.0
 */
export const isCurrentOnly = (self: DocumentCreate): boolean =>
  self.scope.kind !== 'conversation' || self.history === 'latest'
/**
 * Creates an empty durable state with initial allocation counters.
 *
 * @category combinators
 * @since 0.0.0
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
 * Compatibility alias for Entry.WithData.
 *
 * @category models
 * @since 0.0.0
 */
export type TypedEntry<D extends Json> = Entry.WithData<D>
/**
 * Compatibility alias for Entry.DraftWithData.
 *
 * @category models
 * @since 0.0.0
 */
export type TypedEntryDraft<D extends Json> = Entry.DraftWithData<D>
const EntryTokenTypeId = '~@effect-harness/durable/Record/EntryToken'
/**
 * Compatibility alias for Entry.Token.
 *
 * @category models
 * @since 0.0.0
 */
export type EntryToken<K extends string = string> = Entry.Token<K>
/**
 * EntryDefinitionError schema.
 *
 * @category errors
 * @since 0.0.0
 */
export class EntryDefinitionError extends Schema.TaggedError<EntryDefinitionError>(
  '@effect-harness/durable/Record/EntryDefinitionError',
)('EntryDefinitionError', { message: Schema.String }) {}
/**
 * DecodedEntryToken contract.
 *
 * @category models
 * @since 0.0.0
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
 * @since 0.0.0
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
 * @since 0.0.0
 */
export const defineEntryUnsafe = <const K extends string, S extends Schema.Constraint>(
  kind: K,
  schema: S,
): DecodedEntryToken<K, S> => Result.getOrThrow(defineEntry(kind, schema))
/**
 * SubmissionStatus schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const SubmissionStatus = Schema.Literals(['queued', 'placed', 'done', 'unanswered'])

/**
 * Creates a page without changing its input.
 *
 * @category constructors
 * @since 0.0.0
 */
export const makePage = <A>(input: Omit<Page<A>, typeof PageTypeId>): Page<A> => {
  const value = Object.assign({}, input, { [PageTypeId]: { _A: identity } })
  Object.defineProperties(value, Object.getOwnPropertyDescriptors(input))
  Object.defineProperty(value, PageTypeId, { enumerable: false })
  return value
}
/**
 * Returns whether the value satisfies Page.
 *
 * @category guards
 * @since 0.0.0
 */
export const isPage = (input: unknown): input is Page<unknown> =>
  Predicate.hasProperty(input, PageTypeId)

/**
 * Creates a typed entry token without reading input accessors.
 *
 * @category constructors
 * @since 0.0.0
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
 * Returns whether the value satisfies EntryToken.
 *
 * @category guards
 * @since 0.0.0
 */
export const isEntryToken = (input: unknown): input is EntryToken =>
  Predicate.hasProperty(input, EntryTokenTypeId)

/**
 * SubmissionStatus contract.
 *
 * @category models
 * @since 0.0.0
 */
export type SubmissionStatus = typeof SubmissionStatus.Type

/**
 * Returns whether the value satisfies Alive.
 *
 * @category guards
 * @since 0.0.0
 */
export const isAlive: {
  (at: Point): (self: Document) => boolean
  (self: Document, at: Point): boolean
} = dual(2, isAliveImpl)

/**
 * Entry contract.
 *
 * @category models
 * @since 0.0.0
 */
export declare namespace Entry {
  /**
   * Draft contract.
   *
   * @category models
   * @since 0.0.0
   */
  export type Draft = Omit<Entry, 'id' | 'conversationId' | 'head'> & {
    readonly head?: EntryId | 'self' | undefined
  }
  /**
   * WithData contract.
   *
   * @category models
   * @since 0.0.0
   */
  export type WithData<D extends Json> = Omit<Entry, 'data'> &
    ([D] extends [never] ? { readonly data?: undefined } : { readonly data: D })
  /**
   * DraftWithData contract.
   *
   * @category models
   * @since 0.0.0
   */
  export type DraftWithData<D extends Json> = Omit<EntryDraft, 'kind' | 'data'> &
    ([D] extends [never] ? { readonly data?: undefined } : { readonly data: D })
  /**
   * Token contract.
   *
   * @category models
   * @since 0.0.0
   */
  export interface Token<out K extends string = string>
    extends Pipeable.Pipeable, Inspectable.Inspectable {
    readonly [EntryTokenTypeId]: { readonly _K: Types.Covariant<K> }
    readonly kind: K
    readonly is: (entry: Entry | undefined) => entry is Entry & { readonly kind: K }
  }
}

/**
 * Submission contract.
 *
 * @category models
 * @since 0.0.0
 */
export declare namespace Submission {
  /**
   * Create contract.
   *
   * @category models
   * @since 0.0.0
   */
  export type Create = Submission extends infer A
    ? A extends Submission
      ? Omit<A, 'id' | '_tag'> & { readonly _tag?: A['_tag'] | undefined }
      : never
    : never
}

/**
 * Document contract.
 *
 * @category models
 * @since 0.0.0
 */
export declare namespace Document {
  /**
   * Create contract.
   *
   * @category models
   * @since 0.0.0
   */
  export type Create = DocumentCreate
}
