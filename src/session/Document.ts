/**
 * Typed document definitions, ported from pi's `documents.ts`.
 *
 * A definition names a stable persisted `kind` and the `version` of its value shape, and says who
 * owns the document and how long it lives. A token wraps a definition and carries its value type;
 * `Tx.doc` and `Session.snapshot` take a token and the owner it is addressed under. A family is
 * keyed: each member has its own key and starts from a seed.
 */
import { Schema } from 'effect'
import type { Effect } from 'effect'
import { cast } from 'effect/Function'
import type { Draft } from '../delta/Tracker.ts'
import { DocumentCopySource, DocumentRecord, Op } from '../storage/schema/Document.ts'
import { ConversationId, NonNegativeInt } from '../storage/schema/Ids.ts'
import type { EntryId, TaskId } from '../storage/schema/Ids.ts'
import { JsonObject } from '../storage/schema/Json.ts'
import type { SnapshotError, TxDocumentError } from './SessionError.ts'

export const SessionSemantics = Schema.Struct({ scope: Schema.Literal('session') })
export type SessionSemantics = typeof SessionSemantics.Type

/** A conversation document that retains only its current state. */
export const LatestSemantics = Schema.Struct({
  scope: Schema.Literal('conversation'),
  history: Schema.Literal('latest'),
  /** A fork starts from the parent's current state, or from the initial value. */
  fork: Schema.Literals(['current', 'initial']),
})
export type LatestSemantics = typeof LatestSemantics.Type

/** A conversation document whose history stays addressable for as-of reads. */
export const RewindableSemantics = Schema.Struct({
  scope: Schema.Literal('conversation'),
  history: Schema.Literal('rewindable'),
  /** A fork starts as of its fork entry, from the parent's current state, or from the initial value. */
  fork: Schema.Literals(['asOf', 'current', 'initial']),
})
export type RewindableSemantics = typeof RewindableSemantics.Type

export const TaskSemantics = Schema.Struct({ scope: Schema.Literal('task') })
export type TaskSemantics = typeof TaskSemantics.Type

/** Who owns a document; only conversation documents declare history and fork behavior. */
export const Semantics = Schema.Union([
  SessionSemantics,
  LatestSemantics,
  RewindableSemantics,
  TaskSemantics,
])
export type Semantics = typeof Semantics.Type

/** Committed change of one document incarnation, as a commit publishes it. */
export const CommitChange = Schema.Union([
  Schema.Struct({
    type: Schema.Literal('document'),
    record: DocumentRecord,
    /** Conversation owning the document; task documents take their task's. */
    conversationId: Schema.UndefinedOr(ConversationId),
    /** Definition version of `value`; absent when this commit retired the incarnation. */
    version: Schema.UndefinedOr(Schema.Int),
    /** Adopted revision, or `null` when this commit retired the incarnation. */
    value: Schema.NullOr(JsonObject),
    /** Adopted operations of an ordinary update; empty for creation and retirement. */
    ops: Schema.Array(Op),
  }),
  Schema.Struct({
    /** Definition-free child initialization of a fork. */
    type: Schema.Literal('document.copy'),
    record: DocumentRecord,
    conversationId: ConversationId,
    source: DocumentCopySource,
  }),
])
export type CommitChange = typeof CommitChange.Type

/** Stored replay state a checkpoint predicate decides on. */
export const CheckpointInfo = Schema.Struct({
  /** Deltas stored after the newest base, not counting the change being decided. */
  deltasSinceBase: NonNegativeInt,
})
export type CheckpointInfo = typeof CheckpointInfo.Type

/**
 * Fields shared by singletons and families. Callbacks return Effects that need no services, and
 * whatever they fail with, `E`, fails the access, snapshot or commit that ran them.
 */
export interface Common<T extends JsonObject, E> {
  /** Stable persisted kind; part of the public protocol. */
  readonly kind: string
  /** Positive integer version of the stored value shape. */
  readonly version: number
  /** Upgrade a value stored under an older `version`. Its result must be strict JSON. */
  readonly migrate?: (value: JsonObject, fromVersion: number) => Effect.Effect<T, E>
  /** Succeed with true to store an ordinary change as a complete base instead of a delta. */
  readonly checkpointWhen?: (
    value: Readonly<T>,
    ops: ReadonlyArray<Op>,
    info: CheckpointInfo,
  ) => Effect.Effect<boolean, E>
}

/** A singleton document definition. */
export type Definition<T extends JsonObject, E = never, S extends Semantics = Semantics> = Common<
  T,
  E
> &
  S & { readonly initial: () => Effect.Effect<T, E> }

/** A keyed family definition; `initial(seed)` runs only when a member is absent. */
export type FamilyDefinition<
  T extends JsonObject,
  I extends Schema.Json,
  E = never,
  S extends Semantics = Semantics,
> = Common<T, E> & S & { readonly family: true; readonly initial: (seed: I) => Effect.Effect<T, E> }

declare const valueType: unique symbol

/** A typed singleton document whose callbacks can fail with `E`. */
export interface Token<T extends JsonObject, E, S extends Semantics> {
  readonly definition: Definition<T, E, S>
  readonly [valueType]?: T
}

/** A typed document family whose callbacks can fail with `E`. */
export interface FamilyToken<T extends JsonObject, I extends Schema.Json, E, S extends Semantics> {
  readonly definition: FamilyDefinition<T, I, E, S>
  readonly [valueType]?: T
}

type ConversationSemantics = LatestSemantics | RewindableSemantics

export type SessionToken<T extends JsonObject, E = never> = Token<T, E, SessionSemantics>
export type ConversationToken<T extends JsonObject, E = never> = Token<T, E, ConversationSemantics>
export type RewindableToken<T extends JsonObject, E = never> = Token<T, E, RewindableSemantics>
export type TaskToken<T extends JsonObject, E = never> = Token<T, E, TaskSemantics>

export type SessionFamilyToken<
  T extends JsonObject,
  I extends Schema.Json,
  E = never,
> = FamilyToken<T, I, E, SessionSemantics>
export type ConversationFamilyToken<
  T extends JsonObject,
  I extends Schema.Json,
  E = never,
> = FamilyToken<T, I, E, ConversationSemantics>
export type RewindableFamilyToken<
  T extends JsonObject,
  I extends Schema.Json,
  E = never,
> = FamilyToken<T, I, E, RewindableSemantics>
export type TaskFamilyToken<T extends JsonObject, I extends Schema.Json, E = never> = FamilyToken<
  T,
  I,
  E,
  TaskSemantics
>

/**
 * Define a singleton document. Its `version` is checked where the token is used: a version that
 * is not a positive integer fails every access with `DocumentError`.
 */
export const make: {
  <T extends JsonObject, E = never>(
    definition: Definition<T, E, SessionSemantics>,
  ): SessionToken<T, E>
  <T extends JsonObject, E = never>(
    definition: Definition<T, E, LatestSemantics>,
  ): ConversationToken<T, E>
  <T extends JsonObject, E = never>(
    definition: Definition<T, E, RewindableSemantics>,
  ): RewindableToken<T, E>
  <T extends JsonObject, E = never>(definition: Definition<T, E, TaskSemantics>): TaskToken<T, E>
} = cast(<T extends JsonObject, E>(definition: Definition<T, E>): Token<T, E, Semantics> => ({
  definition,
}))

/** Define a keyed document family. */
export const makeFamily: {
  <T extends JsonObject, I extends Schema.Json, E = never>(
    definition: FamilyDefinition<T, I, E, SessionSemantics>,
  ): SessionFamilyToken<T, I, E>
  <T extends JsonObject, I extends Schema.Json, E = never>(
    definition: FamilyDefinition<T, I, E, LatestSemantics>,
  ): ConversationFamilyToken<T, I, E>
  <T extends JsonObject, I extends Schema.Json, E = never>(
    definition: FamilyDefinition<T, I, E, RewindableSemantics>,
  ): RewindableFamilyToken<T, I, E>
  <T extends JsonObject, I extends Schema.Json, E = never>(
    definition: FamilyDefinition<T, I, E, TaskSemantics>,
  ): TaskFamilyToken<T, I, E>
} = cast(
  <T extends JsonObject, I extends Schema.Json, E>(
    definition: FamilyDefinition<T, I, E>,
  ): FamilyToken<T, I, E, Semantics> => ({ definition }),
)

type DraftOf<T extends JsonObject, E> = Effect.Effect<Draft<T>, TxDocumentError | E>

/**
 * Acquire a document's draft for this transaction, creating the incarnation from the
 * definition's `initial` when the address has none. A family member takes a seed, used only when
 * it is created. Edits to the draft commit with the transaction.
 */
export interface Acquire {
  <T extends JsonObject, E>(token: SessionToken<T, E>): DraftOf<T, E>
  <T extends JsonObject, E>(
    token: ConversationToken<T, E>,
    conversationId: ConversationId,
  ): DraftOf<T, E>
  <T extends JsonObject, E>(token: TaskToken<T, E>, taskId: TaskId): DraftOf<T, E>
  <T extends JsonObject, I extends Schema.Json, E>(
    token: SessionFamilyToken<T, I, E>,
    key: string,
    seed: I,
  ): DraftOf<T, E>
  <T extends JsonObject, I extends Schema.Json, E>(
    token: ConversationFamilyToken<T, I, E>,
    conversationId: ConversationId,
    key: string,
    seed: I,
  ): DraftOf<T, E>
  <T extends JsonObject, I extends Schema.Json, E>(
    token: TaskFamilyToken<T, I, E>,
    taskId: TaskId,
    key: string,
    seed: I,
  ): DraftOf<T, E>
}

type Retired = Effect.Effect<void, TxDocumentError>

/**
 * Retire a document's current incarnation in this commit; an acquired draft's final content is
 * written first. Acquiring the address again creates a new incarnation.
 */
export interface Retire {
  <T extends JsonObject, E>(token: SessionToken<T, E>): Retired
  <T extends JsonObject, E>(token: ConversationToken<T, E>, conversationId: ConversationId): Retired
  <T extends JsonObject, E>(token: TaskToken<T, E>, taskId: TaskId): Retired
  <T extends JsonObject, I extends Schema.Json, E>(
    token: SessionFamilyToken<T, I, E>,
    key: string,
  ): Retired
  <T extends JsonObject, I extends Schema.Json, E>(
    token: ConversationFamilyToken<T, I, E>,
    conversationId: ConversationId,
    key: string,
  ): Retired
  <T extends JsonObject, I extends Schema.Json, E>(
    token: TaskFamilyToken<T, I, E>,
    taskId: TaskId,
    key: string,
  ): Retired
}

type ReadOf<T extends JsonObject, X, E> = Effect.Effect<Readonly<T> | undefined, X | E>
/** Committed document reads failing with `X`, overloaded by token kind. */
export interface Reads<X> {
  <T extends JsonObject, E>(token: SessionToken<T, E>): ReadOf<T, X, E>
  <T extends JsonObject, E>(
    token: ConversationToken<T, E>,
    conversationId: ConversationId,
  ): ReadOf<T, X, E>
  <T extends JsonObject, E>(token: TaskToken<T, E>, taskId: TaskId): ReadOf<T, X, E>
  <T extends JsonObject, I extends Schema.Json, E>(
    token: SessionFamilyToken<T, I, E>,
    key: string,
  ): ReadOf<T, X, E>
  <T extends JsonObject, I extends Schema.Json, E>(
    token: ConversationFamilyToken<T, I, E>,
    conversationId: ConversationId,
    key: string,
  ): ReadOf<T, X, E>
  <T extends JsonObject, I extends Schema.Json, E>(
    token: TaskFamilyToken<T, I, E>,
    taskId: TaskId,
    key: string,
  ): ReadOf<T, X, E>
}

/** Read a document's committed value; never creates one. Absent documents read `undefined`. */
export interface Snapshot extends Reads<SnapshotError> {}

/** A rewindable document as of an entry visible from `conversationId`, which must be visible. */
export interface SnapshotAsOf {
  <T extends JsonObject, E>(
    token: RewindableToken<T, E>,
    conversationId: ConversationId,
    at: EntryId,
  ): ReadOf<T, SnapshotError, E>
  <T extends JsonObject, I extends Schema.Json, E>(
    token: RewindableFamilyToken<T, I, E>,
    conversationId: ConversationId,
    key: string,
    at: EntryId,
  ): ReadOf<T, SnapshotError, E>
}
