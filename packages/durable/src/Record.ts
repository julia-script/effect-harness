import { identity } from 'effect/Function'
import * as Types from 'effect/Types'
import * as Predicate from 'effect/Predicate'
import * as Schema from 'effect/Schema'
import * as Result from 'effect/Result'
import * as Struct from 'effect/Struct'
import * as Identity from './Identity.ts'
import * as SharedIdentity from '@effect-harness/harness/Identity'
export const ConversationId = SharedIdentity.ConversationId
export const EntryId = SharedIdentity.EntryId
export type ConversationId = typeof ConversationId.Type
export type EntryId = typeof EntryId.Type

const safe = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }))
export const TaskId = safe.pipe(Schema.brand('@effect-harness/durable/Record/TaskId'))
export type TaskId<A = Schema.Json> = typeof TaskId.Type & { readonly __result?: A }
export const SubmissionId = safe.pipe(Schema.brand('@effect-harness/durable/Record/SubmissionId'))
export type SubmissionId = typeof SubmissionId.Type
export const DocumentId = safe.pipe(Schema.brand('@effect-harness/durable/Record/DocumentId'))
export type DocumentId = typeof DocumentId.Type
export const Seq = safe.pipe(Schema.brand('@effect-harness/durable/Record/Seq'))
export type Seq = typeof Seq.Type
export const JournalCursor = Schema.Union([Seq, Schema.Literal(0)])
export type JournalCursor = typeof JournalCursor.Type
export const ROOT_CONVERSATION_ID = Schema.decodeSync(ConversationId)(1)
export type Json = Schema.Json
export type JsonObject = Schema.JsonObject
export const Conversation = Schema.Struct({
  id: ConversationId,
  parent: Schema.optionalKey(Schema.Struct({ conversationId: ConversationId, at: EntryId })),
  owner: Schema.optionalKey(Schema.Struct({ conversationId: ConversationId, taskId: TaskId })),
})
export type Conversation = typeof Conversation.Type
export const ContextEdit = Schema.Union([
  Schema.Struct({ target: EntryId, action: Schema.Literal('omit') }),
  Schema.Struct({
    target: EntryId,
    action: Schema.Literal('replace'),
    messages: Schema.Array(Schema.Json),
  }),
])
export type ContextEdit = typeof ContextEdit.Type
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
export type Entry = typeof Entry.Type
export type EntryDraft = Omit<Entry, 'id' | 'conversationId' | 'head'> & {
  readonly head?: EntryId | 'self'
}
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
export type Task = typeof Task.Type
const submissionIdentity = {
  id: SubmissionId,
  conversationId: ConversationId,
  requestId: Schema.optionalKey(Identity.RequestId),
}
const noSettlement = {
  answer: Schema.optionalKey(Schema.Never),
  reason: Schema.optionalKey(Schema.Never),
  detail: Schema.optionalKey(Schema.Never),
}
export const InputQueued = Schema.Struct({
  ...submissionIdentity,
  type: Schema.Literal('input'),
  status: Schema.Literal('queued'),
  entry: Schema.optionalKey(Schema.Never),
  ...noSettlement,
})
export const InputPlaced = Schema.Struct({
  ...submissionIdentity,
  type: Schema.Literal('input'),
  status: Schema.Literal('placed'),
  entry: EntryId,
  ...noSettlement,
})
export const InputDone = Schema.Struct({
  ...submissionIdentity,
  type: Schema.Literal('input'),
  status: Schema.Literal('done'),
  entry: EntryId,
  answer: EntryId,
  reason: Schema.optionalKey(Schema.Never),
  detail: Schema.optionalKey(Schema.Never),
})
export const InputUnanswered = Schema.Struct({
  ...submissionIdentity,
  type: Schema.Literal('input'),
  status: Schema.Literal('unanswered'),
  entry: Schema.optionalKey(EntryId),
  answer: Schema.optionalKey(Schema.Never),
  reason: Schema.String,
  detail: Schema.optionalKey(Schema.Json),
})
export const WriteQueued = Schema.Struct({
  ...submissionIdentity,
  type: Schema.Literal('write'),
  status: Schema.Literal('queued'),
  entry: Schema.optionalKey(Schema.Never),
  ...noSettlement,
})
export const WriteDone = Schema.Struct({
  ...submissionIdentity,
  type: Schema.Literal('write'),
  status: Schema.Literal('done'),
  entry: EntryId,
  ...noSettlement,
})
export const WriteUnanswered = Schema.Struct({
  ...submissionIdentity,
  type: Schema.Literal('write'),
  status: Schema.Literal('unanswered'),
  entry: Schema.optionalKey(Schema.Never),
  answer: Schema.optionalKey(Schema.Never),
  reason: Schema.String,
  detail: Schema.optionalKey(Schema.Json),
})
export const SettledSubmission = Schema.Union([
  InputDone,
  InputUnanswered,
  WriteDone,
  WriteUnanswered,
])
export type SettledSubmission = typeof SettledSubmission.Type
export const Submission = Schema.Union([
  InputQueued,
  InputPlaced,
  InputDone,
  InputUnanswered,
  WriteQueued,
  WriteDone,
  WriteUnanswered,
])
export type Submission = typeof Submission.Type
export type SubmissionCreate = Submission extends infer A
  ? A extends Submission
    ? Omit<A, 'id'>
    : never
  : never
export const Scope = Schema.Union([
  Schema.Struct({ kind: Schema.Literal('session') }),
  Schema.Struct({ kind: Schema.Literal('conversation'), conversationId: ConversationId }),
  Schema.Struct({ kind: Schema.Literal('task'), taskId: TaskId }),
])
export type Scope = typeof Scope.Type
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
export type Document = typeof Document.Type
export const DocumentCreate = Document.mapFields(Struct.omit(['createdAt', 'retiredAt']))
export type DocumentCreate = typeof DocumentCreate.Type
export type Address = Pick<Document, 'kind' | 'scope' | 'key'>
export type Point = Seq | 'current'
/** Serializable operations keep exact structural no-ops and root replacements observable. */
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
export type Op = typeof Op.Type
export const Content = Schema.Union([
  Schema.Struct({ kind: Schema.Literal('base'), version: safe, value: Schema.JsonObject }),
  Schema.Struct({ kind: Schema.Literal('delta'), version: safe, ops: Schema.Array(Op) }),
])
export type Content = typeof Content.Type
export const Write = Schema.Union([
  Schema.Struct({ type: Schema.Literal('conversation'), value: Conversation }),
  Schema.Struct({ type: Schema.Literal('entry'), value: Entry }),
  Schema.Struct({ type: Schema.Literal('task'), value: Task }),
  Schema.Struct({ type: Schema.Literal('submission'), value: Submission }),
  Schema.Struct({
    type: Schema.Literal('document.create'),
    record: DocumentCreate,
    content: Content,
  }),
  Schema.Struct({
    type: Schema.Literal('document.copy'),
    record: DocumentCreate,
    source: Schema.Struct({ id: DocumentId, at: Schema.Union([Seq, Schema.Literal('current')]) }),
  }),
  Schema.Struct({
    type: Schema.Literal('document.change'),
    id: DocumentId,
    content: Content,
    publicationOps: Schema.optionalKey(Schema.Array(Op)),
  }),
  Schema.Struct({ type: Schema.Literal('document.retire'), id: DocumentId }),
])
export type Write = typeof Write.Type
export const Revision = Schema.Struct({ seq: Seq, content: Content })
export type Revision = typeof Revision.Type
export const StoredDocument = Schema.Struct({ record: Document, revisions: Schema.Array(Revision) })
export type StoredDocument = typeof StoredDocument.Type
export const Receipt = Schema.Struct({
  key: Schema.String,
  fingerprint: Schema.String,
  result: Schema.Json,
  resultIsVoid: Schema.optionalKey(Schema.Boolean),
  seq: Seq,
})
export type Receipt = typeof Receipt.Type
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
export type State = typeof State.Type
export const Publication = Schema.Struct({
  record: Document,
  version: Schema.optionalKey(Schema.Finite),
  value: Schema.Union([Schema.JsonObject, Schema.Null]),
  ops: Schema.Array(Op),
})
export type Publication = typeof Publication.Type
export const Frame = Schema.Struct({
  seq: Seq,
  writes: Schema.Array(Write),
  documents: Schema.Array(Publication),
})
export type Frame = typeof Frame.Type
const PageTypeId = '~@effect-harness/durable/Record/Page'
export interface Page<out A> {
  readonly [PageTypeId]: { readonly _A: Types.Covariant<A> }

  readonly items: ReadonlyArray<A>
  readonly next?: { readonly after: number }
}
export type Cursor = { readonly after: number }
export const addressKey = (address: Address): string =>
  JSON.stringify([
    address.kind,
    scopeKey(address.scope),
    address.key === undefined ? ['singleton'] : ['family', address.key],
  ])
export const scopeKey = (scope: Scope): string => {
  if (scope.kind === 'session') return 'session'
  if (scope.kind === 'conversation') return `conversation:${scope.conversationId}`
  return `task:${scope.taskId}`
}
export const isAlive = (record: Document, at: Point): boolean =>
  at === 'current'
    ? record.retiredAt === undefined
    : record.createdAt <= at && (record.retiredAt === undefined || at < record.retiredAt)
export const currentOnly = (record: DocumentCreate): boolean =>
  record.scope.kind !== 'conversation' || record.history === 'latest'
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

export type TypedEntry<D extends Json> = Omit<Entry, 'data'> &
  ([D] extends [never] ? { readonly data?: never } : { readonly data: D })
export type TypedEntryDraft<D extends Json> = Omit<EntryDraft, 'kind' | 'data'> &
  ([D] extends [never] ? { readonly data?: never } : { readonly data: D })
const EntryTokenTypeId = '~@effect-harness/durable/Record/EntryToken'
export interface EntryToken<out K extends string = string> {
  readonly [EntryTokenTypeId]: { readonly _K: Types.Covariant<K> }
  readonly kind: K
  readonly is: (entry: Entry | undefined) => entry is Entry & { readonly kind: K }
}
export class EntryDefinitionError extends Schema.TaggedError<EntryDefinitionError>(
  '@effect-harness/durable/Record/EntryDefinitionError',
)('EntryDefinitionError', { message: Schema.String }) {}
export interface DecodedEntryToken<
  K extends string,
  S extends Schema.Constraint,
> extends EntryToken<K> {
  readonly schema: S
  readonly decode: (
    input: unknown,
  ) => import('effect/Effect').Effect<S['Type'], Schema.SchemaError, S['DecodingServices']>
}
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
export const defineEntryUnsafe = <const K extends string, S extends Schema.Constraint>(
  kind: K,
  schema: S,
): DecodedEntryToken<K, S> => Result.getOrThrow(defineEntry(kind, schema))
export const SubmissionStatus = Schema.Literals(['queued', 'placed', 'done', 'unanswered'])

export const makePage = <A>(input: Omit<Page<A>, typeof PageTypeId>): Page<A> => {
  const value = Object.assign({}, input, { [PageTypeId]: { _A: identity } })
  Object.defineProperties(value, Object.getOwnPropertyDescriptors(input))
  Object.defineProperty(value, PageTypeId, { enumerable: false })
  return value
}
export const isPage = (input: unknown): input is Page<unknown> =>
  Predicate.hasProperty(input, PageTypeId)

export const makeEntryToken = <K extends string, S extends Schema.Constraint>(
  input: Omit<DecodedEntryToken<K, S>, typeof EntryTokenTypeId>,
): DecodedEntryToken<K, S> => {
  const value = Object.assign({}, input, { [EntryTokenTypeId]: { _K: identity } })
  Object.defineProperties(value, Object.getOwnPropertyDescriptors(input))
  Object.defineProperty(value, EntryTokenTypeId, { enumerable: false })
  return value
}
export const isEntryToken = (input: unknown): input is EntryToken =>
  Predicate.hasProperty(input, EntryTokenTypeId)
