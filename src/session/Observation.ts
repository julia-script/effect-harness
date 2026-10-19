/**
 * Committed document observation, ported from pi's `session/observation.ts`.
 *
 * A watch delivers every committed change of one document incarnation, with its operations, in
 * commit order. A state holds the latest committed value of one incarnation. Both see committed
 * state only, never create a document, and keep their incarnation cached while their scope is
 * open. Neither follows a later incarnation at the same address: retirement is final.
 */
import type { Effect, Schema, Scope, Stream } from 'effect'
import type { Op } from '../storage/schema/Document.ts'
import type { ConversationId, TaskId } from '../storage/schema/Ids.ts'
import type { JsonObject } from '../storage/schema/Json.ts'
import type {
  ConversationFamilyToken,
  ConversationToken,
  SessionFamilyToken,
  SessionToken,
  TaskFamilyToken,
  TaskToken,
} from './Document.ts'
import type { SessionFailed, SnapshotError } from './SessionError.ts'

/** One committed change of a watched incarnation; a `null` value retires it. */
export interface Change<T extends JsonObject> {
  readonly value: Readonly<T> | null
  /**
   * Operations that turn the previous value into this one. A change committed under another
   * definition version, or one that replaces pending changes the watch fell behind on, is a root
   * replacement, `[['r', value]]`. Retirement is `[['r', null]]`.
   */
  readonly ops: ReadonlyArray<Op>
}

/**
 * An exact, ordered observation of one committed incarnation.
 *
 * `changes` can be run once. It ends after the retirement change and when the Session closes, and
 * fails with `SessionFailed` when the Session fails; changes not yet pulled are then dropped. When
 * more than 100 changes are pending, they collapse into one root replacement.
 */
export interface Watch<T extends JsonObject> {
  /** The committed value when the watch was acquired. */
  readonly value: Readonly<T>
  readonly changes: Stream.Stream<Change<T>, SessionFailed>
}

/**
 * The latest committed value of one incarnation; `null` once it retired. After the Session
 * closes or fails, it keeps its last value.
 */
export interface State<T extends JsonObject> {
  readonly get: Effect.Effect<Readonly<T> | null>
  /**
   * The current value, then each later one; a slow reader skips to the newest. Ends after
   * retirement and when the Session closes or fails. Every run is independent.
   */
  readonly changes: Stream.Stream<Readonly<T> | null>
}

/** An observation of a document, or `undefined` when the address has none. */
type Observed<O, E> = Effect.Effect<O | undefined, SnapshotError | E, Scope.Scope>

/**
 * Watch the current incarnation of a document; its scope keeps the watch attached. Absent
 * documents are not created and watch as `undefined`.
 */
export interface WatchDoc {
  <T extends JsonObject, E>(token: SessionToken<T, E>): Observed<Watch<T>, E>
  <T extends JsonObject, E>(
    token: ConversationToken<T, E>,
    conversationId: ConversationId,
  ): Observed<Watch<T>, E>
  <T extends JsonObject, E>(token: TaskToken<T, E>, taskId: TaskId): Observed<Watch<T>, E>
  <T extends JsonObject, I extends Schema.Json, E>(
    token: SessionFamilyToken<T, I, E>,
    key: string,
  ): Observed<Watch<T>, E>
  <T extends JsonObject, I extends Schema.Json, E>(
    token: ConversationFamilyToken<T, I, E>,
    conversationId: ConversationId,
    key: string,
  ): Observed<Watch<T>, E>
  <T extends JsonObject, I extends Schema.Json, E>(
    token: TaskFamilyToken<T, I, E>,
    taskId: TaskId,
    key: string,
  ): Observed<Watch<T>, E>
}

/**
 * Hold the latest committed value of a document's current incarnation while the scope is open.
 * Absent documents are not created and read as `undefined`.
 */
export interface DocumentState {
  <T extends JsonObject, E>(token: SessionToken<T, E>): Observed<State<T>, E>
  <T extends JsonObject, E>(
    token: ConversationToken<T, E>,
    conversationId: ConversationId,
  ): Observed<State<T>, E>
  <T extends JsonObject, E>(token: TaskToken<T, E>, taskId: TaskId): Observed<State<T>, E>
  <T extends JsonObject, I extends Schema.Json, E>(
    token: SessionFamilyToken<T, I, E>,
    key: string,
  ): Observed<State<T>, E>
  <T extends JsonObject, I extends Schema.Json, E>(
    token: ConversationFamilyToken<T, I, E>,
    conversationId: ConversationId,
    key: string,
  ): Observed<State<T>, E>
  <T extends JsonObject, I extends Schema.Json, E>(
    token: TaskFamilyToken<T, I, E>,
    taskId: TaskId,
    key: string,
  ): Observed<State<T>, E>
}
