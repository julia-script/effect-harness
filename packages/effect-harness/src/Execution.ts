import type * as Effect from 'effect/Effect'
import type * as Option from 'effect/Option'
import type * as Stream from 'effect/Stream'
import type * as Record from './Record.js'
import type * as Document from './Document.js'
import type { Failure } from './ExecutionError.js'
import type * as Transaction from './Transaction.js'

/** Invocation access to the Harness's existing Session; this does not create another Session. */
export interface Access {
  readonly conversationId: Record.ConversationId
  readonly snapshot: <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
  ) => Effect.Effect<Option.Option<Document.Snapshot<S>>, Failure, S['DecodingServices']>
  readonly watch: <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
  ) => Stream.Stream<Document.Snapshot<S>, Failure, S['DecodingServices']>
  readonly commit: <A, E, R>(
    change: (tx: Transaction.Transaction) => Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | Failure, R>
}
