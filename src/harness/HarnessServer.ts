/**
 * The loop serving its clients: `HarnessRpcs` on the `Harness`, over the `RpcServer.Protocol` in
 * context. `layer` builds the default loop and serves it; `layerRpcs` serves a Harness built
 * elsewhere. Run it with `Layer.launch`; clients reach it through the matching transport. In its own
 * process, give it `HarnessTransport.layerWebSocketServer(path)` and serve it with
 * `HttpRouter.serve` on the platform's HTTP server.
 */
import { Layer } from 'effect'
import { RpcServer } from 'effect/rpc'
import type { Rpc, RpcGroup } from 'effect/rpc'
import { HarnessRpcs } from '../client/HarnessRpcs.ts'
import type { Model } from '../model/Model.ts'
import type { CommitError } from '../session/SessionError.ts'
import type { Storage } from '../storage/Storage.ts'
import type { Extension } from './Extension.ts'
import type { Extensions } from './Extensions.ts'
import type { InvalidExtension } from './HarnessError.ts'
import * as Harness from './Harness.ts'
import type { Initializer } from './Initializer.ts'
import { handlers } from './internal/server.ts'

export interface ServeOptions {
  /** The named inits clients may call. */
  readonly initializers?: ReadonlyArray<Initializer>
}

export interface Options<Installs extends ReadonlyArray<Extension> = ReadonlyArray<Extension>>
  extends Harness.LayerOptions<Installs>, ServeOptions {}

/**
 * The handlers of `HarnessRpcs` over the `Harness` in context, without a server: for a host that
 * serves an `RpcGroup` itself, such as a Cloudflare Durable Object.
 */
export const layerHandlers = (
  options: ServeOptions = {},
): Layer.Layer<Rpc.ToHandler<RpcGroup.Rpcs<typeof HarnessRpcs>>, never, Harness.Harness> =>
  handlers(options.initializers ?? [])

/** Serve the `Harness` in context. */
export const layerRpcs = (
  options: ServeOptions = {},
): Layer.Layer<never, never, Harness.Harness | RpcServer.Protocol> =>
  RpcServer.layer(HarnessRpcs).pipe(Layer.provide(layerHandlers(options)))

/**
 * The default loop, `Harness.layer`, served over the `RpcServer.Protocol` in context. It needs the
 * `Storage`, the `Model`, and the implementations of the given extensions' tools.
 */
export const layer = <const Installs extends ReadonlyArray<Extension> = readonly []>(
  options: Options<Installs> = {},
): Layer.Layer<
  Harness.Harness | Extensions,
  CommitError | InvalidExtension,
  Storage | Model | Harness.Requires<Installs[number]> | RpcServer.Protocol
> => layerRpcs(options).pipe(Layer.provideMerge(Harness.layer(options)))
