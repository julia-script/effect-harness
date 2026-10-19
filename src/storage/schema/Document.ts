import { Schema } from 'effect'
import { ConversationId, DocumentId, NonNegativeInt, Seq, TaskId, Version } from './Ids.ts'
import { JsonObject } from './Json.ts'

const PathSegment = Schema.Union([Schema.String, Schema.Int])
const Path = Schema.Array(PathSegment)
const NonEmptyPath = Schema.NonEmptyArray(PathSegment)

/** One operation of a document delta batch, encoded as a tuple. Only `r` replaces the root. */
export const Op = Schema.Union([
  /** Replace the whole value. */
  Schema.Tuple([Schema.Literal('r'), Schema.Json]),
  /** Set the value at a path. */
  Schema.Tuple([Schema.Literal('s'), NonEmptyPath, Schema.Json]),
  /** Delete the value at a path. */
  Schema.Tuple([Schema.Literal('d'), NonEmptyPath]),
  /** Append text to the string at a path. */
  Schema.Tuple([Schema.Literal('a'), NonEmptyPath, Schema.String]),
  /** Drop a number of leading characters from the string at a path. */
  Schema.Tuple([Schema.Literal('t'), NonEmptyPath, NonNegativeInt]),
  /** Splice an array: start, delete count, inserted items. */
  Schema.Tuple([Schema.Literal('p'), Path, Schema.Int, Schema.Int, Schema.Array(Schema.Json)]),
  /** Reorder an array in place: `new[i] = old[permutation[i]]`. */
  Schema.Tuple([Schema.Literal('m'), Path, Schema.Array(Schema.Int)]),
])
export type Op = typeof Op.Type
export const SessionScope = Schema.Struct({ kind: Schema.Literal('session') })
export const ConversationScope = Schema.Struct({
  kind: Schema.Literal('conversation'),
  conversationId: ConversationId,
})
export const TaskScope = Schema.Struct({ kind: Schema.Literal('task'), taskId: TaskId })

/** Who owns a document and bounds its lifetime. */
export const DocumentScope = Schema.Union([SessionScope, ConversationScope, TaskScope])
export type DocumentScope = typeof DocumentScope.Type

/** Scope plus the history and fork behavior only conversation documents declare. */
const DocumentSemantics = {
  session: { scope: SessionScope },
  conversationLatest: {
    scope: ConversationScope,
    /** Retain only current state. */
    history: Schema.Literal('latest'),
    /** Initialize a fork from current source state or the definition's initial value. */
    fork: Schema.Literals(['current', 'initial']),
  },
  conversationRewindable: {
    scope: ConversationScope,
    /** Retain history needed for as-of reads. */
    history: Schema.Literal('rewindable'),
    /** Initialize a fork at its cutoff, from current state, or from the initial value. */
    fork: Schema.Literals(['asOf', 'current', 'initial']),
  },
  task: { scope: TaskScope },
}

const DocumentCreateFields = {
  /** Unique incarnation ID; never reused when the same logical document is recreated. */
  id: DocumentId,
  /** Stable document definition kind. */
  kind: Schema.String,
  /** Family member key; absent for singleton documents. */
  key: Schema.optionalKey(Schema.String),
}

const DocumentRecordFields = {
  ...DocumentCreateFields,
  /** Commit that created the incarnation, stamped by storage. */
  createdAt: Seq,
  /** Commit that retired the incarnation; absent while it is current. */
  retiredAt: Schema.optionalKey(Seq),
}

/** Persisted lifecycle record for one create-to-retire document incarnation. */
export const DocumentRecord = Schema.Union([
  Schema.Struct({ ...DocumentRecordFields, ...DocumentSemantics.session }),
  Schema.Struct({ ...DocumentRecordFields, ...DocumentSemantics.conversationLatest }),
  Schema.Struct({ ...DocumentRecordFields, ...DocumentSemantics.conversationRewindable }),
  Schema.Struct({ ...DocumentRecordFields, ...DocumentSemantics.task }),
])
export type DocumentRecord = typeof DocumentRecord.Type

/** Fields supplied when storage creates and stamps a new `DocumentRecord`. */
export const DocumentCreate = Schema.Union([
  Schema.Struct({ ...DocumentCreateFields, ...DocumentSemantics.session }),
  Schema.Struct({ ...DocumentCreateFields, ...DocumentSemantics.conversationLatest }),
  Schema.Struct({ ...DocumentCreateFields, ...DocumentSemantics.conversationRewindable }),
  Schema.Struct({ ...DocumentCreateFields, ...DocumentSemantics.task }),
])
export type DocumentCreate = typeof DocumentCreate.Type

/** Complete checkpoint of a document value. */
export const DocumentBase = Schema.Struct({
  version: Version,
  kind: Schema.Literal('base'),
  value: JsonObject,
})
export type DocumentBase = typeof DocumentBase.Type

/** Operation batch applied on top of the previous revision. */
export const DocumentDelta = Schema.Struct({
  version: Version,
  kind: Schema.Literal('delta'),
  ops: Schema.Array(Op),
})
export type DocumentDelta = typeof DocumentDelta.Type

/** One stored document revision: a complete checkpoint or an operation batch. */
export const DocumentContent = Schema.Union([DocumentBase, DocumentDelta])
export type DocumentContent = typeof DocumentContent.Type

/** Current state, or one historical commit, used for document membership and content reads. */
export const DocumentPoint = Schema.Union([Seq, Schema.Literal('current')])
export type DocumentPoint = typeof DocumentPoint.Type

/** Exact logical identity of a singleton or one keyed family member. */
export const DocumentAddress = Schema.Struct({
  kind: Schema.String,
  scope: DocumentScope,
  /** Absent selects the singleton; present selects one family member. */
  key: Schema.optionalKey(Schema.String),
})
export type DocumentAddress = typeof DocumentAddress.Type

/** Exact persisted source selected for a definition-free document copy. */
export const DocumentCopySource = Schema.Struct({ id: DocumentId, at: DocumentPoint })
export type DocumentCopySource = typeof DocumentCopySource.Type

/** Detached materialized value and stored definition version at a selected point. */
export const StoredDocument = Schema.Struct({
  record: DocumentRecord,
  version: Version,
  value: JsonObject,
  /** Deltas replayed after the selected base to materialize `value`. */
  deltasSinceBase: NonNegativeInt,
})
export type StoredDocument = typeof StoredDocument.Type
