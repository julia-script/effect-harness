/**
 * Transports between a client and the loop. Both sides take their RPC protocol from context: the
 * loop an `RpcServer.Protocol`, the client an `RpcClient.Protocol`. In one process they share
 * `layerMemory`; split, the loop serves `layerWebSocketServer` on an HTTP server and clients
 * connect with `layerWebSocket`.
 *
 * The protocols carry any RPC group, so a credential store served remotely (`Credentials.
 * layerServer`, `Credentials.layerRpc`) uses the same transports at its own path.
 */
import { Context, Effect, Layer, Queue, Schema, SubscriptionRef } from 'effect'
import type { Scope } from 'effect'
import type { HttpRouter } from 'effect/http'
import { RpcClient, RpcSerialization, RpcServer } from 'effect/rpc'
import type { RpcMessage } from 'effect/rpc'
import { Socket } from 'effect/socket'

/** Both ends of a transport. */
export type Protocols = RpcServer.Protocol | RpcClient.Protocol

/** Forward every message of `queue`, in order, through `write`. */
const forward = <A>(
  queue: Queue.Dequeue<readonly [number, A]>,
  write: (clientId: number, message: A) => Effect.Effect<void>,
) =>
  Effect.forkScoped(
    Effect.forever(Effect.flatMap(Queue.take(queue), ([id, message]) => write(id, message))),
  )

/** Both protocols over two queues, with `codecFor` filling the holes of each message. */
const pair = (codecFor: RpcSerialization.CodecFor) =>
  Effect.gen(function* () {
    const toServer = yield* Queue.unbounded<readonly [number, RpcMessage.FromClientEncoded]>()
    const toClient = yield* Queue.unbounded<readonly [number, RpcMessage.FromServerEncoded]>()
    const disconnects = yield* Queue.unbounded<number>()
    const clientIds = new Set<number>()
    const server = yield* RpcServer.Protocol.make((write) =>
      Effect.as(forward(toServer, write), {
        disconnects,
        send: (clientId: number, response: RpcMessage.FromServerEncoded) =>
          Queue.offer(toClient, [clientId, response]).pipe(Effect.asVoid),
        end: () => Effect.void,
        clientIds: Effect.succeed(clientIds),
        initialMessage: Effect.succeedNone,
        supportsAck: true,
        supportsTransferables: false,
        supportsSpanPropagation: true,
        supportsNotifications: false,
        codecFor,
      }),
    )
    const client = yield* RpcClient.Protocol.make((write) =>
      Effect.as(forward(toClient, write), {
        send: (clientId: number, request: RpcMessage.FromClientEncoded) =>
          Effect.suspend(() => {
            clientIds.add(clientId)
            return Queue.offer(toServer, [clientId, request]).pipe(Effect.asVoid)
          }),
        supportsAck: true,
        supportsTransferables: false,
        codecFor,
      }),
    )
    return Context.make(RpcServer.Protocol, server).pipe(Context.add(RpcClient.Protocol, client))
  })

/**
 * A server and a client protocol connected in memory, through queues between fibers. Nothing is
 * encoded or decoded: requests, results and errors pass as the values they are, checked against
 * their Schemas' types. Each end buffers what arrives before it runs.
 */
export const makeMemory: Effect.Effect<Context.Context<Protocols>, never, Scope.Scope> = pair(
  Schema.toType,
)

/**
 * `makeMemory`, but every message is encoded to JSON values and decoded on the other side, as
 * across a process; nothing is written to strings. Use it to check that a protocol can be split.
 */
export const makeMemoryEncoded: Effect.Effect<
  Context.Context<Protocols>,
  never,
  Scope.Scope
> = pair(Schema.toCodecJson)

/** `makeMemory` as a Layer: provide it once to both the loop and its clients. */
export const layerMemory: Layer.Layer<Protocols> = Layer.effectContext(makeMemory)

/**
 * How many times a client's connection has opened. `layerWebSocket` counts its socket's, so a
 * watch that dropped with the connection opens again as soon as it is back, instead of waiting
 * out its own backoff.
 */
export class Connections extends Context.Service<
  Connections,
  SubscriptionRef.SubscriptionRef<number>
>()('effect-harness/client/HarnessTransport/Connections') {}

/**
 * A client protocol over a WebSocket to `url`, such as `ws://localhost:3000/harness`, with JSON
 * frames. It uses the global `WebSocket`, so it runs in browsers, Bun and Deno alike, and
 * reconnects after transient failures, counting each connection in `Connections`.
 */
export const layerWebSocket = (url: string): Layer.Layer<RpcClient.Protocol | Connections> =>
  Layer.unwrap(
    Effect.map(SubscriptionRef.make(0), (opened) =>
      RpcClient.layerProtocolSocket({ retryTransientErrors: true }).pipe(
        Layer.provide(Socket.layerWebSocket(url)),
        Layer.provide(Socket.layerWebSocketConstructorGlobal),
        Layer.provide(RpcSerialization.layerJson),
        Layer.provide(
          Layer.succeed(RpcClient.ConnectionHooks, {
            onConnect: SubscriptionRef.update(opened, (count) => count + 1),
            onDisconnect: Effect.void,
          }),
        ),
        Layer.merge(Layer.succeed(Connections, opened)),
      ),
    ),
  )

/**
 * A server protocol that accepts WebSockets at `path` of the `HttpRouter` in context, with JSON
 * frames. Serve the router with `HttpRouter.serve` and the platform's HTTP server.
 */
export const layerWebSocketServer = (
  path: HttpRouter.PathInput,
): Layer.Layer<RpcServer.Protocol, never, HttpRouter.HttpRouter> =>
  RpcServer.layerProtocolWebsocket({ path }).pipe(Layer.provide(RpcSerialization.layerJson))
