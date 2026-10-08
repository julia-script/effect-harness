/**
 * Scoped storage service, allocation accessors and memory acquisition.
 */
import * as Layer from 'effect/Layer'
import type * as Scope from 'effect/Scope'
import * as memoryStore from './internal/memoryStore.ts'
import { identity } from 'effect/Function'
import type * as Types from 'effect/Types'
import * as Predicate from 'effect/Predicate'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Schema from 'effect/Schema'
import type * as Record from './Record.ts'
import { rejected, type StorageError } from './StorageError.ts'
import { validate } from './storage/internal/state.ts'

/**
 * Transaction options without receipt-based replay.
 *
 * @category models
 */
export type UnkeyedOptions = Store.UnkeyedOptions
/**
 * Persisted idempotency key and optional request fingerprint.
 *
 * @category models
 */
export type ReceiptOptions = Store.ReceiptOptions
/**
 * Choice between an ordinary commit and a persisted replay receipt.
 *
 * @category models
 */
export type CommitOptions = Store.CommitOptions
/**
 * Atomic callback that publishes a validated storage candidate.
 *
 * @category models
 */
export type Transact = Store.Transact
const CandidateTypeId = '~@effect-harness/durable/Store/Candidate'
/**
 * Next state, staged writes and callback result proposed for one atomic commit.
 *
 * @category models
 */
export type Candidate<A> = Store.Candidate<A>
/**
 * Coherent saved state and retained frames after a requested sequence.
 *
 * @category models
 */
export type Journal = Store.Journal
/**
 * Storage adapter contract for commit serialization and lifecycle.
 *
 * @category models
 */
export type Service = Store.Service
/**
 * Service for atomic domain state, receipts and retained commit frames.
 *
 * **When to use**
 *
 * Use with Session or supply an adapter implementing the same commit and lifecycle contract.
 *
 * **Details**
 *
 * A candidate becomes visible only after validation and successful publication.
 * Receipt-based commits save writes and their replay result together. Observers read
 * committed state rather than candidates.
 *
 * **Gotchas**
 *
 * An uncertain persistence outcome poisons the open Store. Reopen the backend and inspect
 * saved receipts before continuing.
 *
 * @category services
 */
export class Store extends Context.Service<Store, Service>()('@effect-harness/durable/Store') {}

/**
 * Allocates through the transaction callback, validating before publication and again after allocation.
 *
 * @category combinators
 */
export const mintId = Effect.fnUntraced(function* <S extends Schema.Constraint>(
  schema: S,
): Effect.fn.Return<S['Type'], StorageError, Store | S['DecodingServices']> {
  const store = yield* Store
  return yield* store
    .transact((state) =>
      Effect.gen(function* () {
        if (!Number.isSafeInteger(state.nextId)) return yield* rejected('ID space is exhausted')
        yield* validate(schema, state.nextId)
        return makeCandidate({
          state: { ...state, nextId: state.nextId + 1 },
          writes: [],
          result: state.nextId,
        })
      }),
    )
    .pipe(Effect.flatMap((id) => validate(schema, id)))
})

/**
 * Creates a nominal transaction candidate from the supplied fields.
 *
 * **Details**
 *
 * Copies the carrier and its property descriptors. Nested state, writes and result values
 * are retained by reference; this is not a deep clone.
 *
 * @category constructors
 */
export const makeCandidate = <A>(
  input: Omit<Candidate<A>, typeof CandidateTypeId>,
): Candidate<A> => {
  const value = Object.assign({}, input, { [CandidateTypeId]: { _A: identity } })
  Object.defineProperties(value, Object.getOwnPropertyDescriptors(input))
  Object.defineProperty(value, CandidateTypeId, { enumerable: false })
  return value
}
/**
 * Checks whether a value carries the nominal `Candidate` marker.
 *
 * **Gotchas**
 *
 * This checks library identity, not the validity of arbitrary fields or stored JSON.
 *
 * @category guards
 */
export const isCandidate = (input: unknown): input is Candidate<unknown> =>
  Predicate.hasProperty(input, CandidateTypeId)

/**
 * Acquires a fresh scoped in-memory Store.
 *
 * **When to use**
 *
 * Use when tests or sessions need no recovery across process restarts.
 *
 * **Gotchas**
 *
 * Every acquisition starts with empty state. A persistent WorkflowEngine does not make this
 * domain Store persistent.
 *
 * @category constructors
 */
export const makeMemory: Effect.Effect<Store['Service'], never, Scope.Scope> = Effect.suspend(
  () => memoryStore.make,
)
/**
 * Provides scoped, process-local domain storage.
 *
 * **Details**
 *
 * Share this Layer value to share one Store; rebuilding it independently creates independent
 * session data.
 *
 * @category layers
 */
export const layerMemory: Layer.Layer<Store> = Layer.effect(Store, makeMemory)
/**
 * Alias of layerMemory for scoped in-memory domain storage.
 *
 * @see {@link layerMemory} for acquisition and sharing semantics.
 * @category layers
 */
export const layerStoreMemory: Layer.Layer<Store> = layerMemory

/**
 * Type-level contracts for `Store`.
 *
 * @category utility types
 */
export declare namespace Store {
  /**
   * Transaction options without receipt-based replay.
   *
   * @category models
   */
  export interface UnkeyedOptions {
    readonly key?: undefined
    readonly fingerprint?: undefined
  }
  /**
   * Replay identity for a transaction result saved with its domain writes.
   *
   * **Details**
   *
   * key identifies the operation within the Store. fingerprint, when supplied, must match the
   * original receipt when the key is reused.
   *
   * **Gotchas**
   *
   * The result must be JSON-safe or void; undefined is recorded separately from JSON null.
   *
   * @category models
   */
  export interface ReceiptOptions {
    readonly key: string
    readonly fingerprint?: string | undefined
  }
  /**
   * Choice between an ordinary commit and a persisted replay receipt.
   *
   * @category models
   */
  export type CommitOptions = UnkeyedOptions | ReceiptOptions
  /**
   * Atomic callback that publishes a validated storage candidate.
   *
   * @category models
   */
  export interface Transact {
    <A, E, R>(
      change: (state: Record.State) => Effect.Effect<Candidate<A>, E, R>,
      options?: UnkeyedOptions,
    ): Effect.Effect<A, StorageError | E, R>
    <A extends Record.Json | void, E, R>(
      change: (state: Record.State) => Effect.Effect<Candidate<A>, E, R>,
      options: CommitOptions,
    ): Effect.Effect<A, StorageError | E, R>
  }
  /**
   * Proposed next state, writes and callback result for a Store transaction.
   *
   * **Details**
   *
   * Construct with makeCandidate so the nominal guard recognizes the value. Supplying a
   * candidate does not itself persist or validate its contents.
   *
   * @category models
   */
  export interface Candidate<out A> {
    readonly [CandidateTypeId]: { readonly _A: Types.Covariant<A> }

    readonly state: Record.State
    readonly writes: ReadonlyArray<Record.Write>
    readonly result: A
  }
  /**
   * Coherent saved state and retained frames after a requested sequence.
   *
   * @category models
   */
  export interface Journal {
    readonly frames: ReadonlyArray<Record.Frame>
    readonly reset: boolean
    readonly state: Record.State
  }
  /**
   * Storage adapter contract for authoritative snapshots and serialized commits.
   *
   * **Details**
   *
   * read supports transaction construction; committed exposes saved state independently of
   * staged candidates. journal pairs retained frames with a coherent saved snapshot.
   *
   * **Gotchas**
   *
   * Scope owns release. seal stops admission and observers; awaitClosed only waits for cleanup
   * and never initiates it.
   *
   * @category models
   */
  export interface Service {
    /**
     * Reads authoritative state for transaction construction.
     */
    readonly read: Effect.Effect<Record.State, StorageError>
    /** Optional stable key for batching reads that share the same storage view. */
    readonly readContext?: Effect.Effect<object> | undefined
    /** Reads the saved snapshot without exposing a transaction candidate. */
    readonly committed: Effect.Effect<Record.State, StorageError>
    /**
     * Serializes a candidate callback, validates the result and publishes it atomically, with
     * optional receipt replay.
     */
    readonly transact: Transact
    /**
     * Publishes the supplied domain writes atomically and returns the commit sequence.
     */
    readonly commit: (
      writes: ReadonlyArray<Record.Write>,
      options?: CommitOptions,
    ) => Effect.Effect<Record.Seq, StorageError>
    /**
     * Reads retained frames after a sequence together with a coherent saved state; reset signals
     * a retention gap.
     */
    readonly journal: (after: Record.Seq | 0) => Effect.Effect<Journal, StorageError>
    /** Stop admission and committed observers while retaining resources for admitted-operation cleanup. */
    readonly seal: Effect.Effect<void>
    /** Observe the persistent cleanup receipt after the owning Scope releases; never initiates release. */
    readonly awaitClosed: Effect.Effect<void, StorageError>
  }
}
