/**
 * The account registry: the accounts of a host's adapters, added and removed while it runs. It
 * stores accounts in the `KeyValueStore` in context, their secrets in `Credentials`, and builds one
 * provider per account, which `layerModel` routes to. Accounts can also be declared in code, for a
 * fixed account beside the dynamic ones.
 *
 * An account is added in one step, with its credential, and only once the provider accepts it. An
 * account that has lost its credential stays, as `needsAuth`, until `setKey` or `signIn` fixes it.
 */
import { Context, Effect, Layer, Stream, SubscriptionRef } from 'effect'
import type { Redacted } from 'effect'
import type { KeyValueStore } from 'effect/persistence'
import { RpcServer } from 'effect/rpc'
import type { Rpc, RpcGroup } from 'effect/rpc'
import { layerDynamic } from '../model/Model.ts'
import type { Model, Provider } from '../model/Model.ts'
import type { Add, Models, SignIn, SignInEvent, Status } from './Account.ts'
import type { AccountError } from './AccountError.ts'
import { AccountsRpcs } from './AccountsRpcs.ts'
import type { AccountOf, Any, Group, Ids, Implemented, Requires } from './Adapter.ts'
import * as operations from './internal/operations.ts'
import * as registry from './internal/registry.ts'

export interface Service {
  readonly list: Effect.Effect<ReadonlyArray<Status>, AccountError>
  /** The accounts now, then again at each change. */
  readonly changes: Stream.Stream<ReadonlyArray<Status>, AccountError>
  /** Add an account of an `apiKey` or `none` adapter, once listing models with it works. */
  readonly add: (input: Add) => Effect.Effect<Status, AccountError>
  /** Replace an account's API key, once listing models with the new one works. */
  readonly setKey: (name: string, apiKey: Redacted.Redacted) => Effect.Effect<Status, AccountError>
  /** Sign in with a device code, adding the account or renewing an existing one. */
  readonly signIn: (input: SignIn) => Stream.Stream<SignInEvent, AccountError>
  readonly relabel: (name: string, label: string) => Effect.Effect<Status, AccountError>
  /** Forget the account and its credential. */
  readonly remove: (name: string) => Effect.Effect<void, AccountError>
  /** Every account's models, listed live; a refused listing marks the account `needsAuth`. */
  readonly models: Effect.Effect<ReadonlyArray<Models>, AccountError>
  /** The providers of the accounts that are `ready` now. */
  readonly providers: Effect.Effect<ReadonlyArray<Provider<Requires>>>
}

export class Accounts extends Context.Service<Accounts, Service>()(
  'effect-harness/accounts/Accounts',
) {}

export interface Options<Adapters extends Record<string, Any>> {
  /** Accounts declared in code: never stored, relabelled or removed. */
  readonly accounts?: ReadonlyArray<AccountOf<Adapters>>
  /** The `KeyValueStore` key the accounts are stored under. Default `accounts`. */
  readonly key?: string
}

/** A registry of `group`'s accounts; it needs each adapter's implementation, from `toLayer`. */
export const make = <Adapters extends Record<string, Any>>(
  group: Group<Adapters>,
  options: Options<Adapters> = {},
): Effect.Effect<
  Service,
  AccountError,
  KeyValueStore.KeyValueStore | Requires | Implemented<Ids<Adapters>>
> =>
  Effect.map(registry.open(group, options), (state): Service => {
    const statuses = Effect.flatMap(registry.current(state), (all) => registry.statuses(state, all))
    return {
      list: statuses,
      changes: Stream.merge(
        SubscriptionRef.changes(state.live),
        SubscriptionRef.changes(state.refused),
      ).pipe(Stream.mapEffect(() => statuses)),
      add: operations.add(state),
      setKey: operations.setKey(state),
      signIn: operations.signIn(state),
      relabel: operations.relabel(state),
      remove: operations.remove(state),
      models: operations.models(state),
      providers: registry.providers(state),
    }
  })

export const layer = <Adapters extends Record<string, Any>>(
  group: Group<Adapters>,
  options: Options<Adapters> = {},
): Layer.Layer<
  Accounts,
  AccountError,
  KeyValueStore.KeyValueStore | Requires | Implemented<Ids<Adapters>>
> => Layer.effect(Accounts, make(group, options))

/** A `Model` over the providers of the accounts that are `ready`, as they are at each request. */
export const layerModel: Layer.Layer<Model, never, Accounts | Requires> = Layer.unwrap(
  Effect.map(Accounts, (accounts) => layerDynamic(accounts.providers)),
)

/**
 * The handlers of `AccountsRpcs` over the `Accounts` in context, without a server: for a host that
 * serves an `RpcGroup` itself, such as a Cloudflare Durable Object.
 */
export const layerHandlers: Layer.Layer<
  Rpc.ToHandler<RpcGroup.Rpcs<typeof AccountsRpcs>>,
  never,
  Accounts
> = AccountsRpcs.toLayer(
  Effect.map(Accounts, (accounts) =>
    AccountsRpcs.of({
      List: () => accounts.list,
      Changes: () => accounts.changes,
      Add: (input) => accounts.add(input),
      SetKey: ({ name, apiKey }) => accounts.setKey(name, apiKey),
      SignIn: (input) => accounts.signIn(input),
      Relabel: ({ name, label }) => accounts.relabel(name, label),
      Remove: ({ name }) => accounts.remove(name),
      Models: () => accounts.models,
    }),
  ),
)

/** Serve the `Accounts` in context over the `RpcServer.Protocol` in context. */
export const layerServer: Layer.Layer<never, never, Accounts | RpcServer.Protocol> =
  RpcServer.layer(AccountsRpcs).pipe(Layer.provide(layerHandlers))
