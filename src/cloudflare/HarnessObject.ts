/**
 * The harness in a Cloudflare Durable Object, deployed with Alchemy. One object is one loop and
 * its store: name the object after a user or a workspace. The object is single-threaded, so the
 * loop is always the store's only writer.
 *
 * `layer` is for the inner Effect of an Alchemy `Cloudflare.RpcDurableObject` with
 * `{ schema: HarnessObject.Rpcs }`: Alchemy serves the RPCs over hibernatable WebSockets, with the
 * same protocol as `HarnessTransport.layerWebSocket`, so a `HarnessClient` connects unchanged.
 * Conversations, accounts and credentials live in the object's SQLite, and the object stays awake
 * while a task runs. When Cloudflare evicts or resets it anyway, the next activation resumes the
 * tasks from storage, and clients' watches open again.
 *
 * `route` is the Worker in front: it checks a token and forwards `/<prefix>/<name>` to the object
 * of that name.
 *
 * The coding tools and code mode do not run in a Worker, which has no file system or processes.
 */
import type { RuntimeContext } from 'alchemy/RuntimeContext'
import { DurableObjectState } from 'alchemy/Cloudflare/Workers/DurableObjectState'
import { SqliteClient } from '@effect/sql-sqlite-do'
import { Effect, Layer } from 'effect'
import type { Duration } from 'effect'
import { KeyValueStore } from 'effect/persistence'
import type { Rpc, RpcGroup } from 'effect/rpc'
import * as Accounts from '../accounts/Accounts.ts'
import { AccountsRpcs } from '../accounts/AccountsRpcs.ts'
import type * as Adapter from '../accounts/Adapter.ts'
import { HarnessRpcs } from '../client/HarnessRpcs.ts'
import * as CredentialStore from '../credentials/CredentialStore.ts'
import * as Harness from '../harness/Harness.ts'
import type { Extension } from '../harness/Extension.ts'
import * as HarnessServer from '../harness/HarnessServer.ts'
import * as Storage from '../storage/Storage.ts'
import type { StorageError } from '../storage/StorageError.ts'
import * as keepAwake from './internal/keepAwake.ts'
import * as routing from './internal/route.ts'

/** What an object serves: the loop's requests and the account registry's. */
export const Rpcs = HarnessRpcs.merge(AccountsRpcs)

/** The handlers of every request in `Rpcs`. */
export type Handlers = Rpc.ToHandler<RpcGroup.Rpcs<typeof Rpcs>>

/**
 * The loop's `Storage` and a `KeyValueStore`, for accounts and credentials, in the SQLite of the
 * Durable Object in context.
 */
export const layerStorage: Layer.Layer<
  Storage.Storage | KeyValueStore.KeyValueStore,
  StorageError,
  DurableObjectState
> = Layer.unwrap(
  Effect.map(DurableObjectState, (state) =>
    Layer.mergeAll(Storage.layerSql(), KeyValueStore.layerSql()).pipe(
      Layer.provide(SqliteClient.layer({ storage: state.raw.storage })),
    ),
  ),
)

/**
 * Keep the object awake while the `Harness` in context has a live task, checking `every` interval
 * (default 10 seconds). It needs the object's runtime, so build it in the inner Effect.
 */
export const layerKeepAwake = (
  every: Duration.Input = '10 seconds',
): Layer.Layer<never, never, Harness.Harness | RuntimeContext> => keepAwake.layer(every)

export interface Options<
  Adapters extends Record<string, Adapter.Any>,
  Installs extends ReadonlyArray<Extension> = ReadonlyArray<Extension>,
>
  extends Harness.LayerOptions<Installs>, HarnessServer.ServeOptions, Accounts.Options<Adapters> {
  /** The provider types people can add accounts of. */
  readonly adapters: Adapter.Group<Adapters>
  /** How often to check for live tasks while the object runs. Default 10 seconds. */
  readonly keepAwake?: Duration.Input
}

/**
 * The handlers of `Rpcs`: the default loop over the accounts' providers, the account registry,
 * and both stored in the object's SQLite, kept awake while tasks run. It needs each adapter's
 * implementation, an `HttpClient`, and the given extensions' tools.
 */
export const layer = <
  Adapters extends Record<string, Adapter.Any>,
  const Installs extends ReadonlyArray<Extension> = readonly [],
>(
  options: Options<Adapters, Installs>,
): Layer.Layer<
  Handlers,
  never,
  | Adapter.Implemented<Adapter.Ids<Adapters>>
  | Exclude<Adapter.Requires, CredentialStore.CredentialStore>
  | Harness.Requires<Installs[number]>
  | DurableObjectState
  | RuntimeContext
> => {
  const loop = Harness.layer(options)
  const served = Layer.mergeAll(
    HarnessServer.layerHandlers(options),
    Accounts.layerHandlers,
    layerKeepAwake(options.keepAwake),
  ).pipe(Layer.provide(loop))
  return served.pipe(
    Layer.provide(Accounts.layerModel),
    Layer.provideMerge(Accounts.layer(options.adapters, options)),
    Layer.provideMerge(CredentialStore.layerKeyValueStore()),
    Layer.provide(layerStorage),
    Layer.orDie,
  )
}

export interface RouteOptions {
  /** The path before the object's name. Default `/agents`. */
  readonly prefix?: string
  /**
   * The token every request must carry, as `Authorization: Bearer <token>` or, since browsers
   * cannot set headers on a WebSocket, a `token` query parameter. Without it, the route is open:
   * anyone who knows a name reaches that object.
   */
  readonly token?: routing.Token
}

/**
 * A Worker's `fetch` that forwards `<prefix>/<name>` to the object of that name, once the request
 * carries the token. Names are 1 to 64 letters, digits, `-` or `_`; anything else is a 404.
 */
export const route: (objects: routing.Objects, options?: RouteOptions) => routing.Handler =
  routing.make
