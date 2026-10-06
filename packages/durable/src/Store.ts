import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Record from './Record.ts'
import type { StorageError } from './StorageError.ts'

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
  readonly mintId: <S extends Schema.Constraint>(
    schema: S,
  ) => Effect.Effect<S['Type'], StorageError, S['DecodingServices']>
  readonly read: Effect.Effect<Record.State, StorageError>
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
