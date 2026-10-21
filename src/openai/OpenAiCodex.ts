/**
 * Codex through a ChatGPT subscription, ported from pi-ai's `openai-codex` provider. Sign in once
 * with `login`, which stores OAuth tokens in `CredentialStore` under the provider's name; `provider`
 * reads them for each request, refreshing tokens near expiry and writing them back. The store must be
 * writable, so a read-only one like `CredentialStore.layerConfig` cannot hold them.
 */
import { Schema } from 'effect'
import type { Effect } from 'effect'
import type { HttpClient } from 'effect/http'
import type { Layer } from 'effect'
import * as Adapter from '../accounts/Adapter.ts'
import type { CredentialStore } from '../credentials/CredentialStore.ts'
import type { CredentialsError, ReadOnly } from '../credentials/CredentialsError.ts'
import type { Definition, Listable, Ref } from '../model/Model.ts'
import { models as catalog } from './internal/codex/catalog.ts'
import * as auth from './internal/codex/auth.ts'
import * as internal from './internal/codex/provider.ts'
import * as declaration from './adapter/OpenAiCodex.ts'

/** The provider name requests route by, and the credential its tokens are stored under, by default. */
export const name = 'openai-codex'

/** The API its responses name. */
export const api = 'openai-codex-responses'

/** The Codex models, from pi-ai's catalog. */
export const models: ReadonlyArray<Definition> = catalog

/** Signing in failed, or the tokens could not be refreshed. */
export class LoginError extends Schema.TaggedError<LoginError, { readonly brand: unique symbol }>(
  'effect-harness/openai/OpenAiCodex/LoginError',
)('LoginError', {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

/** What the person signing in opens, and the code they enter there. */
export const DeviceCode = Schema.Struct({ url: Schema.String, code: Schema.String })
export type DeviceCode = typeof DeviceCode.Type

export interface Options {
  /**
   * The name requests route by and the tokens are stored under. Default `openai-codex`; give each
   * account its own, such as `codex-work`, and sign in to each with `login({ name })`.
   */
  readonly name?: string
  /** Default `https://chatgpt.com/backend-api`. */
  readonly baseUrl?: string
  /** Default `https://auth.openai.com`, where tokens are refreshed. */
  readonly authUrl?: string
  /** Default `models`. */
  readonly models?: ReadonlyArray<Definition>
  /** How requests name this client. Default `effect-harness`. */
  readonly originator?: string
  /** The Codex CLI version `listModels` asks for, which decides what the backend lists. */
  readonly clientVersion?: string
}

/**
 * The Codex provider over the tokens `login` stored. Its `listModels` lists the account's models
 * with their efforts and service tiers, over the same tokens.
 *
 * **Details**
 *
 * Refresh retains the original identity and revision and rejects a changed account ID.
 *
 * @category constructors
 */
export const provider = (
  options: Options = {},
): Listable<HttpClient.HttpClient | CredentialStore> => internal.make(options)

export interface LoginOptions {
  /** Show the person the URL and code; the login waits for them to approve it. */
  readonly onCode: (code: DeviceCode) => Effect.Effect<void>
  /** The provider name to store the tokens under. Default `openai-codex`. */
  readonly name?: string
  /** Default `https://auth.openai.com`. */
  readonly authUrl?: string
}

/**
 * Sign in with a ChatGPT device code and store the tokens in `CredentialStore`.
 *
 * **Details**
 *
 * Every success creates a fresh revision, even for unchanged tokens or the same account.
 * Identity uses the exchanged token's ChatGPT account ID and the canonical issuer
 * https://auth.openai.com, independent of a custom authUrl.
 *
 * @category authentication
 */
export const login = (
  options: LoginOptions,
): Effect.Effect<
  void,
  LoginError | CredentialsError | ReadOnly,
  HttpClient.HttpClient | CredentialStore
> => auth.store(options.name ?? name, options.authUrl ?? auth.AUTH_URL, options.onCode)

/** The `Model.Ref` of Codex model `modelId`, served by the provider named `providerName`. */
export const ref = (modelId: string, providerName: string = name): Ref => ({
  provider: providerName,
  modelId,
})

/** The adapter's declaration; a client imports it from `effect-harness/openai/adapter`. */
export const adapter = declaration.adapter

/**
 * The adapter's implementation, as a layer for `Accounts.layer`: each account's provider over the tokens stored under its name, and
 * its sign-in. `options` apply to every account, such as a `baseUrl` for a stand-in server.
 */
export const layerAdapter = (
  options: Omit<Options, 'name'> = {},
): Layer.Layer<Adapter.Implemented<typeof adapter.id>> =>
  Adapter.layer(adapter, {
    provider: (account) => provider({ ...options, name: account.name }),
    login: (account, flow) =>
      login({
        name: account.name,
        onCode: flow.onCode,
        ...(options.authUrl === undefined ? {} : { authUrl: options.authUrl }),
      }),
  })
