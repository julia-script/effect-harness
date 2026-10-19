/**
 * Immutable JSON revisions and the exact op batches between them, ported from Chord's
 * `delta/tracker.ts` in pi.
 *
 * A tracker owns one revision sequence. `beginChange` opens a draft over the current revision:
 * a proxy that reads through to it and records each write, without copying or mutating it.
 * `prepare` emits the ops of the draft's edits and materializes the next revision from them,
 * sharing every untouched container with the base. `adopt` swaps the tracker's root to a
 * prepared revision and stales every competing change.
 *
 * Immutability is an ownership contract. Nothing is frozen, so mutating a value the tracker owns
 * or published (`value`, `base`, `ops` and their payloads, any root passed in) silently corrupts
 * state. Roots passed to `make` and `prepareReplace` are trusted to be alias-free strict JSON;
 * values placed into a draft are validated and copied.
 */
import type { Effect } from 'effect'
import type { Op } from '../storage/schema/Document.ts'
import type { AdoptRejected, ChangeSettled, InvalidWrite } from './DeltaError.ts'
import * as internal from './internal/tracker.ts'

/** A mutable view of a JSON value scoped to one open change, preserving tuple positions. */
export type Draft<T, Depth extends ReadonlyArray<unknown> = []> = Depth['length'] extends 8
  ? T
  : T extends null | boolean | number | string
    ? T
    : T extends (...args: Array<never>) => unknown
      ? T
      : T extends object
        ? { -readonly [Key in keyof T]: Draft<T[Key], [...Depth, unknown]> }
        : T

/** A materialized candidate revision, ready to adopt. */
export interface Prepared<T extends object> {
  /** The revision the change started from. */
  readonly base: T
  /** The candidate. It is `base` itself when `ops` is empty. */
  readonly value: T
  /** Replaying these on `base` yields `value`. Exact, but not canonical. */
  readonly ops: ReadonlyArray<Op>
  readonly baseRevision: number
  /** Prevent adoption. The candidate stays readable. */
  readonly abort: Effect.Effect<void>
}

/** An open draft over one revision. Settle it with `prepare` or `abort`. */
export interface Change<T extends object> {
  /**
   * The draft. Writable only while the change is open; afterwards every handle read from it reads
   * as empty and refuses writes. Writes through a handle whose element left the draft are ignored.
   *
   * Placed values are validated and copied, so the caller keeps its object. A write the draft
   * cannot represent leaves it unchanged and makes `prepare` fail with `InvalidWrite`.
   */
  readonly state: Draft<T>
  /** Emit the draft's ops and materialize the candidate. Draft handles are unusable afterwards. */
  readonly prepare: Effect.Effect<Prepared<T>, InvalidWrite | ChangeSettled>
  /** Discard the change, or prevent adoption of its preparation. Idempotent. */
  readonly abort: Effect.Effect<void>
}

export interface Tracker<T extends object> {
  /** The latest adopted revision. */
  readonly value: Effect.Effect<T>
  /** How many preparations were adopted, no-ops included. */
  readonly revision: Effect.Effect<number>
  readonly beginChange: Effect.Effect<Change<T>>
  /**
   * Prepare a whole-root replacement, taking ownership of `value`. A deeply equal value prepares
   * no ops and keeps the current root; any other prepares `['r', value]`.
   */
  readonly prepareReplace: (value: T) => Effect.Effect<Prepared<T>>
  /**
   * Make a preparation the tracker's revision: an infallible pointer swap once it is accepted.
   * Every other open or prepared change of the tracker becomes stale.
   */
  readonly adopt: (prepared: Prepared<T>) => Effect.Effect<void, AdoptRejected>
}

/** Track `initial`, taking immutable ownership of it in O(1). */
export const make: <T extends object>(initial: T) => Effect.Effect<Tracker<T>> = internal.make
