/**
 * Watches over the transport: the first frame and the changes after it, opened again when the
 * connection drops.
 */
import { Effect, Option, Sink, Stream, SubscriptionRef } from 'effect'
import type { Schema } from 'effect'
import { RpcClientDefect, RpcClientError } from 'effect/rpc/RpcClientError'
import * as Ops from '../../delta/Ops.ts'
import type * as AgentEvent from '../../protocol/AgentEvent.ts'
import type { Change, Watch } from '../../protocol/ViewState.ts'
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

/** Open a watch again: `open` again when its connection drops, from the loop's state then. */
export type Reopen = <A, E>(
  open: Stream.Stream<A, E | RpcClientError>,
) => Stream.Stream<A, E | RpcClientError>

/**
 * Open a watch again when its connection drops, such as when the loop restarts. The new watch
 * starts from the loop's state then: a fresh value, or a fresh snapshot. With the count of the
 * transport's connections, it opens as soon as the connection is back; otherwise, or when the
 * connection did not drop, after a backoff.
 */
export const resubscribing =
  (connections: Option.Option<SubscriptionRef.SubscriptionRef<number>>): Reopen =>
  <A, E>(open: Stream.Stream<A, E | RpcClientError>) =>
    Stream.suspend(() => {
      let failures = 0
      const opened = Option.match(connections, {
        onNone: () => Effect.succeed(0),
        onSome: SubscriptionRef.get,
      })
      /** After the backoff, or once the connection has opened again since the `seen`th time. */
      const retry = (seen: number) =>
        Option.match(connections, {
          onNone: () => backoff(failures++),
          onSome: (count) =>
            Effect.raceFirst(
              backoff(failures++),
              SubscriptionRef.changes(count).pipe(
                Stream.filter((n) => n > seen),
                Stream.runHead,
              ),
            ),
        })
      const attempt = (): Stream.Stream<A, E | RpcClientError> =>
        Stream.unwrap(
          Effect.map(opened, (seen) =>
            open.pipe(
              Stream.tap(() => Effect.sync(() => (failures = 0))),
              Stream.catchTag('RpcClientError', () =>
                Stream.unwrap(Effect.as(retry(seen), Stream.suspend(attempt))),
              ),
            ),
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
