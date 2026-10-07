/**
 * Scoped storage service, allocation accessors and memory acquisition.
 *
 * @since 0.0.0
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
 * Compatibility alias for Store.UnkeyedOptions.
 *
 * @category models
 * @since 0.0.0
 */
export type UnkeyedOptions = Store.UnkeyedOptions
/**
 * Compatibility alias for Store.ReceiptOptions.
 *
 * @category models
 * @since 0.0.0
 */
export type ReceiptOptions = Store.ReceiptOptions
/**
 * Compatibility alias for Store.CommitOptions.
 *
 * @category models
 * @since 0.0.0
 */
export type CommitOptions = Store.CommitOptions
/**
 * Compatibility alias for Store.Transact.
 *
 * @category models
 * @since 0.0.0
 */
export type Transact = Store.Transact
const CandidateTypeId = '~@effect-harness/durable/Store/Candidate'
/**
 * Compatibility alias for Store.Candidate.
 *
 * @category models
 * @since 0.0.0
 */
export type Candidate<A> = Store.Candidate<A>
/**
 * Compatibility alias for Store.Journal.
 *
 * @category models
 * @since 0.0.0
 */
export type Journal = Store.Journal
/**
 * Compatibility alias for Store.Service.
 *
 * @category models
 * @since 0.0.0
 */
export type Service = Store.Service
/**
 * Store service.
 *
 * @category services
 * @since 0.0.0
 */
export class Store extends Context.Service<Store, Service>()('@effect-harness/durable/Store') {}

/**
 * Allocates through the transaction callback, validating before publication and again after allocation.
 *
 * @category combinators
 * @since 0.0.0
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
 * Creates a detached transaction candidate without changing its input.
 *
 * @category constructors
 * @since 0.0.0
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
 * Returns whether the value satisfies Candidate.
 *
 * @category guards
 * @since 0.0.0
 */
export const isCandidate = (input: unknown): input is Candidate<unknown> =>
  Predicate.hasProperty(input, CandidateTypeId)

/**
 * Creates a scoped memory Store with fresh state on each acquisition.
 *
 * @category constructors
 * @since 0.0.0
 */
export const makeMemory: Effect.Effect<Store['Service'], never, Scope.Scope> = Effect.suspend(
  () => memoryStore.make,
)
/**
 * Scoped in-memory Store layer.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerMemory: Layer.Layer<Store> = Layer.effect(Store, makeMemory)
/**
 * Scoped in-memory Store implementation layer.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerStoreMemory: Layer.Layer<Store> = layerMemory

/**
 * Store contract.
 *
 * @category models
 * @since 0.0.0
 */
export declare namespace Store {
  /**
   * UnkeyedOptions contract.
   *
   * @category models
   * @since 0.0.0
   */
  export interface UnkeyedOptions {
    readonly key?: undefined
    readonly fingerprint?: undefined
  }
  /**
   * ReceiptOptions contract.
   *
   * @category models
   * @since 0.0.0
   */
  export interface ReceiptOptions {
    readonly key: string
    readonly fingerprint?: string | undefined
  }
  /**
   * CommitOptions contract.
   *
   * @category models
   * @since 0.0.0
   */
  export type CommitOptions = UnkeyedOptions | ReceiptOptions
  /**
   * Transact contract.
   *
   * @category models
   * @since 0.0.0
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
   * Candidate contract.
   *
   * @category models
   * @since 0.0.0
   */
  export interface Candidate<out A> {
    readonly [CandidateTypeId]: { readonly _A: Types.Covariant<A> }

    readonly state: Record.State
    readonly writes: ReadonlyArray<Record.Write>
    readonly result: A
  }
  /**
   * Journal contract.
   *
   * @category models
   * @since 0.0.0
   */
  export interface Journal {
    readonly frames: ReadonlyArray<Record.Frame>
    readonly reset: boolean
    readonly state: Record.State
  }
  /**
   * Service contract.
   *
   * @category models
   * @since 0.0.0
   */
  export interface Service {
    readonly read: Effect.Effect<Record.State, StorageError>
    /** Actual ambient read lease; absent custom drivers are isolated by their complete caller Context. */
    readonly readContext?: Effect.Effect<object> | undefined
    /** Reads outside an inherited SQL transaction, waiting for physical settlement. */
    readonly committed: Effect.Effect<Record.State, StorageError>
    readonly transact: Transact
    readonly commit: (
      writes: ReadonlyArray<Record.Write>,
      options?: CommitOptions,
    ) => Effect.Effect<Record.Seq, StorageError>
    readonly journal: (after: Record.Seq | 0) => Effect.Effect<Journal, StorageError>
    /** Stop admission and committed observers while retaining resources for admitted-operation cleanup. */
    readonly seal: Effect.Effect<void>
    /** Observe the persistent cleanup receipt after the owning Scope releases; never initiates release. */
    readonly awaitClosed: Effect.Effect<void, StorageError>
  }
}
