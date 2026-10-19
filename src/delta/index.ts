/**
 * Immutable JSON revisions and the op batches between them, ported from Chord's `delta` in pi.
 */

/** The revision tracker, its changes, and their preparations. */
export * as Tracker from './Tracker.ts'

/** Errors a `Tracker` reports. */
export * as DeltaError from './DeltaError.ts'

/** Replaying op batches onto immutable values. */
export * as Ops from './Ops.ts'
