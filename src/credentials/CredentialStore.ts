/**
 * The `CredentialStore` service: where provider secrets live. The loop reads a credential before each
 * model request; a client sets, lists and removes them; an OAuth refresh writes back with a
 * compare-and-set. The layer decides where they are kept: in memory, in `Config` such as a `.env`
 * file, or in a remote store reached over `CredentialsRpcs`. A client and its loops share a store
 * by providing the same layer.
 */
import { Context, Layer } from 'effect'
import type { Duration, Scope, Effect } from 'effect'
import { RpcServer } from 'effect/rpc'
import type { RpcClient } from 'effect/rpc'
import type { Credential, Info, OAuth } from './Credential.ts'
import type { CredentialChanged, CredentialsError, ReadOnly } from './CredentialsError.ts'
import { CredentialsRpcs } from './CredentialsRpcs.ts'
import type { KeyValueStore } from 'effect/persistence'
import * as config from './internal/config.ts'
import * as keyValueStore from './internal/keyValueStore.ts'
import * as memory from './internal/memory.ts'
import * as resolver from './internal/resolve.ts'
import * as rpc from './internal/rpc.ts'

export interface Service {
  /** The stored credential, possibly expired; `resolve` refreshes it. */
  readonly get: (provider: string) => Effect.Effect<Credential | undefined, CredentialsError>
  /** What is stored, without secrets. */
  readonly list: Effect.Effect<ReadonlyArray<Info>, CredentialsError>
  readonly set: (
    provider: string,
    credential: Credential,
  ) => Effect.Effect<void, CredentialsError | ReadOnly>
  readonly remove: (provider: string) => Effect.Effect<void, CredentialsError | ReadOnly>
  /** Replace `expected` with `next`; fails `CredentialChanged` when `expected` is not stored. */
  readonly update: (
    provider: string,
    expected: Credential,
    next: Credential,
  ) => Effect.Effect<void, CredentialChanged | CredentialsError | ReadOnly>
}

export class CredentialStore extends Context.Service<CredentialStore, Service>()(
  'effect-harness/credentials/CredentialStore',
) {}

/** A store in memory, for tests and single-process hosts. */
export const makeMemory = (
  initial: Iterable<readonly [string, Credential]> = [],
): Effect.Effect<Service> => memory.make(initial)

export const layerMemory = (initial: Iterable<readonly [string, Credential]> = []) =>
  Layer.effect(CredentialStore, makeMemory(initial))

/**
 * A writable store that survives restarts: every credential as JSON under `key` of the
 * `KeyValueStore` in context, such as `KeyValueStore.layerFileSystem(dir)`. Secrets are stored as
 * plain strings, as pi's `auth.json` is, so keep the store private. Writes run one at a time in
 * this process; two processes must not share one store.
 */
export const makeKeyValueStore = (
  key = 'credentials',
): Effect.Effect<Service, never, KeyValueStore.KeyValueStore> => keyValueStore.make(key)

export const layerKeyValueStore = (
  key = 'credentials',
): Layer.Layer<CredentialStore, never, KeyValueStore.KeyValueStore> =>
  Layer.effect(CredentialStore, makeKeyValueStore(key))

/** Config names to read, per provider: the first that is set is its API key. */
export type Names = Readonly<Record<string, ReadonlyArray<string>>>

/** The variables pi reads for the common providers. */
export const defaultNames: Names = {
  anthropic: ['ANTHROPIC_OAUTH_TOKEN', 'ANTHROPIC_API_KEY'],
  openai: ['OPENAI_API_KEY'],
  google: ['GEMINI_API_KEY'],
  openrouter: ['OPENROUTER_API_KEY'],
  xai: ['XAI_API_KEY'],
  groq: ['GROQ_API_KEY'],
  mistral: ['MISTRAL_API_KEY'],
  deepseek: ['DEEPSEEK_API_KEY'],
}

/**
 * API keys from the `ConfigProvider` in context, read at every `get`. Writes fail `ReadOnly`, so
 * credentials that need refreshing cannot live here.
 */
export const makeConfig = (names: Names = defaultNames): Service => config.make(names)

export const layerConfig = (names: Names = defaultNames) =>
  Layer.succeed(CredentialStore, makeConfig(names))

/** A remote store, over the `RpcClient.Protocol` in context; transport failures are `CredentialsError`. */
export const makeRpc: Effect.Effect<Service, never, RpcClient.Protocol | Scope.Scope> =
  rpc.makeClient

export const layerRpc: Layer.Layer<CredentialStore, never, RpcClient.Protocol> = Layer.effect(
  CredentialStore,
  makeRpc,
)

/** Serve the `CredentialStore` in context over the `RpcServer.Protocol` in context. */
export const layerServer: Layer.Layer<never, never, CredentialStore | RpcServer.Protocol> =
  RpcServer.layer(CredentialsRpcs).pipe(Layer.provide(rpc.handlers(CredentialStore)))

export interface ResolveOptions<E> {
  /** New tokens for `credential`, from the provider. */
  readonly refresh: (provider: string, credential: OAuth) => Effect.Effect<OAuth, E>
  /** Refresh tokens that expire sooner than this. Default: five minutes. */
  readonly minValidity?: Duration.Input
}

/**
 * A reader of fresh credentials, for one loop: OAuth tokens near expiry are refreshed, at most
 * one refresh per provider at a time, and written back with `update`. When another writer
 * refreshed first, its tokens are returned.
 */
export const makeResolver: <E>(
  options: ResolveOptions<E>,
) => Effect.Effect<
  (provider: string) => Effect.Effect<Credential | undefined, E | CredentialsError | ReadOnly>,
  never,
  CredentialStore
> = resolver.make
