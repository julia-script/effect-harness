/**
 * Adapters: the provider types a host supports, declared once, in code. An adapter names its
 * settings Schema and how its accounts authenticate. It holds no code and imports no SDK, so the
 * client can use it to draw a form. Accounts are its instances, added while the host runs.
 *
 * The implementation turns an account into a `Model` provider, and signs it in when the adapter
 * takes OAuth. It runs where the loop runs, as a layer: `layer` for one adapter, or
 * `Group.toLayer` for several, the way tool handlers are given to `Toolkit.toLayer`. The registry
 * needs one for every adapter of its group, so a missing one is a type error.
 */
import { Context, Effect, Layer, Schema } from 'effect'
import type { Scope } from 'effect'
import type { HttpClient } from 'effect/http'
import type { Credentials } from '../credentials/Credentials.ts'
import type { Listable } from '../model/Model.ts'
import type { DeviceCode } from './Account.ts'

/** A Schema of settings that needs no services: settings are stored and sent encoded. */
export type Settings = Schema.ConstraintCodec<unknown>

/** The account holds an API key, checked by listing models with it. */
export const apiKey = 'apiKey' as const
/** The account signs in with a device code and holds OAuth tokens. */
export const deviceCode = 'deviceCode' as const
/** The account signs in through a browser and returns an authorization callback or pasted code. */
export const browserOAuth = 'browserOAuth' as const
/** The account needs no credential, such as a local server. */
export const none = 'none' as const

export type Auth = typeof apiKey | typeof deviceCode | typeof browserOAuth | typeof none

export const TypeId = '~effect-harness/accounts/Adapter'
export type TypeId = typeof TypeId

/** A declared adapter. */
export interface Adapter<Id extends string, S extends Settings, A extends Auth> {
  readonly [TypeId]: TypeId
  readonly id: Id
  /** What people read, such as `ChatGPT (Codex)`. */
  readonly label: string
  readonly settings: S
  readonly auth: A
}

export type Any = Adapter<string, Settings, Auth>

const NoSettings = Schema.Struct({})

/** Declare an adapter whose accounts take no settings. */
export function make<const Id extends string, const A extends Auth>(
  id: Id,
  options: { readonly label: string; readonly auth: A },
): Adapter<Id, typeof NoSettings, A>
/** Declare an adapter whose accounts take `settings`. */
export function make<const Id extends string, const A extends Auth, S extends Settings>(
  id: Id,
  options: { readonly label: string; readonly auth: A; readonly settings: S },
): Adapter<Id, S, A>
export function make(
  id: string,
  options: { readonly label: string; readonly auth: Auth; readonly settings?: Settings },
): Any {
  return {
    [TypeId]: TypeId,
    id,
    label: options.label,
    settings: options.settings ?? NoSettings,
    auth: options.auth,
  }
}

/** An account of `A`, with its settings decoded. */
export interface Account<A extends Any = Any> {
  readonly adapter: A['id']
  readonly name: string
  readonly label: string
  readonly settings: A['settings']['Type']
  /** The model IDs the account listed when its credential was last checked. */
  readonly models?: ReadonlyArray<string>
}

/** What the providers and sign-ins of accounts need to answer. */
export type Requires = HttpClient.HttpClient | Credentials

/** What an implementation's sign-in fails with: anything with a message. */
export interface LoginFailure {
  readonly message: string
}

/** The host-facing callbacks and completion of a scoped OAuth sign-in. */
export interface LoginFlow {
  readonly onCode: (code: DeviceCode) => Effect.Effect<void>
  readonly onAuthorize: (url: string) => Effect.Effect<void>
  readonly completion: Effect.Effect<string>
  readonly redirectUri?: string
}

/** The implementation of `A`: its provider, and its sign-in when it takes OAuth. */
export interface Base<A extends Any> {
  /** The account's provider, named after the account. Build it once; the registry keeps it. */
  readonly provider: (account: Account<A>) => Listable<Requires>
  /** Sign in, staging tokens in `Credentials` under the account's name until success. */
  readonly login?: (
    account: Account<A>,
    flow: LoginFlow,
  ) => Effect.Effect<void, LoginFailure, Requires>
}

export type Implementation<A extends Any> = A['auth'] extends
  | typeof deviceCode
  | typeof browserOAuth
  ? Base<A> & { readonly login: NonNullable<Base<A>['login']> }
  : Base<A>

/** One implementation per adapter, by ID. */
export type Implementations<Adapters extends Record<string, Any>> = {
  readonly [Id in keyof Adapters]: Implementation<Adapters[Id]>
}

/** The service a group's layer provides for the adapter `Id`. */
export interface Implemented<Id extends string> {
  readonly id: Id
  readonly implemented: unique symbol
}

/** The adapters of a list, by ID. */
export type ById<Adapters extends ReadonlyArray<Any>> = {
  readonly [A in Adapters[number] as A['id']]: A
}

/** The IDs of a group's adapters. */
export type Ids<Adapters extends Record<string, Any>> = Adapters[keyof Adapters]['id']

/** An account of any adapter of a group, with its settings decoded. */
export type AccountOf<Adapters extends Record<string, Any>> = {
  readonly [Id in keyof Adapters]: Account<Adapters[Id]>
}[keyof Adapters]

export interface Group<Adapters extends Record<string, Any>> {
  readonly adapters: Adapters
  /** The adapter `id`, if the group lists it. */
  readonly get: (id: string) => Any | undefined
  /** Type `implementations` against the group. */
  readonly of: (implementations: Implementations<Adapters>) => Implementations<Adapters>
  /** Implement every adapter, with implementations or an Effect that builds them. */
  readonly toLayer: <E = never, R = never>(
    build: Implementations<Adapters> | Effect.Effect<Implementations<Adapters>, E, R>,
  ) => Layer.Layer<Implemented<Ids<Adapters>>, E, Exclude<R, Scope.Scope>>
}

/** The key of adapter `id`'s implementation. */
export const implementation = (id: string) =>
  Context.Service<Implemented<string>, Base<Any>>(`effect-harness/accounts/Adapter/${id}`)

/** Implement `adapter`, with its implementation or an Effect that builds it. */
export const layer = <A extends Any, E = never, R = never>(
  adapter: A,
  build: Implementation<A> | Effect.Effect<Implementation<A>, E, R>,
): Layer.Layer<Implemented<A['id']>, E, Exclude<R, Scope.Scope>> =>
  Layer.effectContext(
    Effect.map(Effect.isEffect(build) ? build : Effect.succeed(build), (built) =>
      Context.makeUnsafe<Implemented<A['id']>>(new Map([[implementation(adapter.id).key, built]])),
    ),
  )

/** A group of `adapters`: what a host supports. */
export function group<const Adapters extends ReadonlyArray<Any>>(
  ...adapters: Adapters
): Group<ById<Adapters>>
export function group(...adapters: ReadonlyArray<Any>): Group<Record<string, Any>> {
  const byId = new Map(adapters.map((adapter) => [adapter.id, adapter]))
  const implementations = (built: Readonly<Record<string, Base<Any>>>) => {
    const services = new Map<string, Base<Any>>()
    for (const id of byId.keys()) {
      const found = built[id]
      if (found !== undefined) services.set(implementation(id).key, found)
    }
    return Context.makeUnsafe<Implemented<string>>(services)
  }
  return {
    adapters: Object.fromEntries(byId),
    get: (id) => byId.get(id),
    of: (built) => built,
    toLayer: (build) =>
      Layer.effectContext(
        Effect.map(Effect.isEffect(build) ? build : Effect.succeed(build), implementations),
      ),
  }
}
