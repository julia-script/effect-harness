/**
 * Watches over the transport: the first frame and the changes after it, opened again when the
 * connection drops.
 */
import { Effect, Option, Sink, Stream } from 'effect'
import type { Schema } from 'effect'
import { RpcClientDefect, RpcClientError } from 'effect/rpc/RpcClientError'
import * as Ops from '../../delta/Ops.ts'
import type * as AgentEvent from '../../harness/AgentEvent.ts'
import type { Change, Watch } from '../../harness/ViewState.ts'
import type { Op } from '../../storage/schema/Document.ts'

/** The loop broke the protocol of a watch. */
export const broken = (message: string) =>
  new RpcClientError({ reason: new RpcClientDefect({ message, cause: undefined }) })

/** The first element of `stream`, and the rest, while the scope is open. */
const first = <A, E>(stream: Stream.Stream<A, E>) =>
  Effect.flatMap(Stream.peel(stream, Sink.head<A>()), ([head, rest]) =>
    Option.isNone(head)
      ? Effect.fail(broken('The loop ended a watch before its first frame'))
      : Effect.succeed([head.value, rest] as const),
  )

type Frame<T> =
  | { readonly type: 'value'; readonly value: T }
  | { readonly type: 'change'; readonly ops: ReadonlyArray<Op> }

/** The next value and change of a watch after one frame. */
const replay = <T extends Schema.Json>(value: T, frame: Frame<T>): readonly [T, Change<T>] => {
  if (frame.type === 'value')
    return [frame.value, { value: frame.value, ops: [['r', frame.value]] }]
  const next = Ops.apply(value, frame.ops)
  return [next, { value: next, ops: frame.ops }]
}

/** Wait before opening a watch again: as the transport waits before reconnecting. */
const backoff = (failures: number) => Effect.sleep(Math.min(500 * 1.5 ** failures, 5000))

/**
 * A watch's stream that opens the watch again when its connection drops, such as when the loop
 * restarts. The new watch starts from the loop's state then: a fresh value, or a fresh snapshot.
 */
export const resubscribing = <A, E>(open: Stream.Stream<A, E | RpcClientError>) =>
  Stream.suspend(() => {
    let failures = 0
    const attempt = (): Stream.Stream<A, E | RpcClientError> =>
      open.pipe(
        Stream.tap(() => Effect.sync(() => (failures = 0))),
        Stream.catchTag('RpcClientError', () =>
          Stream.unwrap(Effect.as(backoff(failures++), Stream.suspend(attempt))),
        ),
      )
    return attempt()
  })

/** A watch from its frames: the value they start with, then each change replayed onto it. */
export const watched = <T extends Schema.Json, E>(frames: Stream.Stream<Frame<T>, E>) =>
  Effect.flatMap(first(frames), ([head, rest]) =>
    head.type === 'value'
      ? Effect.succeed<Watch<T, E>>({
          value: head.value,
          changes: Stream.mapAccum(
            rest,
            () => head.value,
            (value, frame) => {
              const [next, change] = replay(value, frame)
              return [next, [change]]
            },
          ),
        })
      : Effect.fail(broken('The loop started a watch without its value')),
  )

/** Events from their batches: the first holds the snapshot. */
export const eventsFrom = <E>(batches: Stream.Stream<ReadonlyArray<AgentEvent.AgentEvent>, E>) =>
  Effect.flatMap(first(batches), ([head, rest]) => {
    const snapshot = head[0]
    return snapshot?.type === 'snapshot'
      ? Effect.succeed<AgentEvent.Events<E>>({ snapshot, batches: rest })
      : Effect.fail(broken('The loop started events without a snapshot'))
  })
