/**
 * The loop side of a named `init`: what the Harness runs, in the commit that creates a
 * conversation, when a client names the `Init` it implements.
 */
import { Effect, Schema } from 'effect'
import type { Init, Payload } from '../protocol/Init.ts'
import type { Tx } from '../session/Session.ts'
import type { ConversationId } from '../storage/schema/Ids.ts'
import { InitFailed } from '../protocol/HarnessError.ts'

export interface Initializer {
  readonly name: string
  /** Decode `payload` and run; any failure is `InitFailed`. */
  readonly run: (
    tx: Tx,
    conversationId: ConversationId,
    payload: Schema.Json,
  ) => Effect.Effect<void, InitFailed>
}

/** Implement `init`: `run` gets the decoded payload, and fails the creation by failing. */
export const make = <S extends Payload, E>(
  init: Init<S>,
  run: (tx: Tx, conversationId: ConversationId, payload: S['Type']) => Effect.Effect<void, E>,
): Initializer => ({
  name: init.name,
  run: (tx, conversationId, payload) =>
    Schema.decodeEffect(init.payload)(payload).pipe(
      Effect.flatMap((decoded) => run(tx, conversationId, decoded)),
      Effect.mapError(
        (cause) => new InitFailed({ message: `Init ${init.name} failed`, init: init.name, cause }),
      ),
    ),
})
