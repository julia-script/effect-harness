/** Transactional creation callbacks, separate from post-commit notification hooks. */
import type * as Effect from 'effect/Effect'
import type * as Scope from 'effect/Scope'
import type * as Record from './Record.js'
import type * as Transaction from './Transaction.js'

export const TypeId = '~effect-harness/ConversationInitializer'

export interface Any {
  readonly [TypeId]: {
    readonly _Requirements: (_: never) => unknown
    readonly _Error: (_: never) => unknown
  }
  readonly execute: (tx: Transaction.Transaction, conversation: Record.Conversation) => unknown
}

export interface ConversationInitializer<E = never, R = never> extends Any {
  readonly [TypeId]: {
    readonly _Requirements: (_: never) => R
    readonly _Error: (_: never) => E
  }
  readonly execute: (
    tx: Transaction.Transaction,
    conversation: Record.Conversation,
  ) => Effect.Effect<void, E, R>
}

export type Requirements<I extends Any> = I extends {
  readonly [TypeId]: { readonly _Requirements: (_: never) => infer R }
}
  ? Exclude<R, Scope.Scope>
  : never

export type Error<I extends Any> = I extends {
  readonly [TypeId]: { readonly _Error: (_: never) => infer E }
}
  ? E
  : never

/**
 * Register with Session.make or HarnessRuntime options in array order. Every new
 * root, independent conversation and fork runs the callbacks once, including raw
 * Transaction creation. Existing roots and reopened conversations skip them.
 * Harness first ensures harness.agent; forks inherit documents by their policies
 * before callbacks run. Explicit Harness.create agent overrides apply afterward.
 *
 * Use only the supplied Transaction for staged writes and reads. It includes
 * prior drafts and earlier initializer writes; it is revoked after initialization.
 * Failure or interruption discards creation, copied documents and all callback
 * drafts, even when the caller catches the failure and commits other work.
 * Typed callback failures become SessionError(operation: 'conversation.initialize') at Session,
 * and HarnessError at the runtime boundary. External effects cannot roll back;
 * callbacks should express required durable effects through Transaction.
 * Services are captured when Session/runtime is built; Scope is callback-owned.
 * Notification hooks retain their separate post-commit behavior.
 */
export const make = <E, R>(definition: {
  readonly execute: (
    tx: Transaction.Transaction,
    conversation: Record.Conversation,
  ) => Effect.Effect<void, E, R>
}): ConversationInitializer<E, R> => ({
  ...definition,
  [TypeId]: { _Requirements: (_: never) => _, _Error: (_: never) => _ },
})
