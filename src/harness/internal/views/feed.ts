import { Effect, Queue, Stream } from 'effect'
import type { Cause } from 'effect'
import type { SessionFailed } from '../../../session/SessionError.ts'

/** Items a feed can fall behind on before they collapse into one. */
const MAX_PENDING = 100

/** What a view observer delivers to its reader, in commit order. */
export interface Feed<A> {
  /** Deliver `item`; past `MAX_PENDING` pending items, drop them all for `collapse()`. */
  readonly offer: (item: A) => Effect.Effect<void>
  /** End delivery: the loop closed, or its Session failed with `failure`. */
  readonly end: (failure?: SessionFailed) => Effect.Effect<void>
  /** Stop delivery when the reader's scope closes. */
  readonly close: Effect.Effect<void>
  readonly stream: Stream.Stream<A, SessionFailed>
}

/** A feed whose overflow is replaced with `collapse()`, read when it overflows. */
export const make = <A>(collapse: () => A): Effect.Effect<Feed<A>> =>
  Effect.map(Queue.unbounded<A, SessionFailed | Cause.Done>(), (queue) => ({
    offer: (item) =>
      Queue.sizeUnsafe(queue) >= MAX_PENDING
        ? Effect.andThen(Effect.ignore(Queue.clear(queue)), Queue.offer(queue, collapse()))
        : Effect.asVoid(Queue.offer(queue, item)),
    // Pending items are dropped: a closed or failed loop delivers nothing more.
    end: (failure) =>
      Effect.andThen(
        Effect.ignore(Queue.clear(queue)),
        failure === undefined ? Queue.end(queue) : Queue.fail(queue, failure),
      ),
    close: Effect.asVoid(Queue.shutdown(queue)),
    stream: Stream.fromQueue(queue),
  }))
