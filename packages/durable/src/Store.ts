import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Record from './Record.ts'
import { rejected, type StorageError } from './StorageError.ts'
import { validate } from './storage/State.ts'

export interface UnkeyedOptions {
  readonly key?: never
  readonly fingerprint?: never
}
export interface ReceiptOptions {
  readonly key: string
  readonly fingerprint?: string
}
export type CommitOptions = UnkeyedOptions | ReceiptOptions
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
export interface Candidate<A> {
  readonly state: Record.State
  readonly writes: ReadonlyArray<Record.Write>
  readonly result: A
}
export interface Journal {
  readonly frames: ReadonlyArray<Record.Frame>
  readonly reset: boolean
  readonly state: Record.State
}
export interface Service {
  readonly read: Effect.Effect<Record.State, StorageError>
  /** Actual ambient read lease; absent custom drivers are isolated by their complete caller Context. */
  readonly readContext?: Effect.Effect<object>
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
export class Store extends Context.Service<Store, Service>()('@effect-harness/durable/Store') {}

/** Allocate through the transaction callback, validating before publication and again after allocation. */
export const mintId = Effect.fnUntraced(function* <S extends Schema.Constraint>(
  schema: S,
): Effect.fn.Return<S['Type'], StorageError, Store | S['DecodingServices']> {
  const store = yield* Store
  return yield* store
    .transact((state) =>
      Effect.gen(function* () {
        if (!Number.isSafeInteger(state.nextId)) return yield* rejected('ID space is exhausted')
        yield* validate(schema, state.nextId)
        return { state: { ...state, nextId: state.nextId + 1 }, writes: [], result: state.nextId }
      }),
    )
    .pipe(Effect.flatMap((id) => validate(schema, id)))
})
