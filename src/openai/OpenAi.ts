/**
 * OpenAI, as a `Model` provider over the Responses API. `provider` is a plain value: give it to
 * `Model.layer`, which then needs an `HttpClient`, and `Credentials` unless the provider was given
 * an API key. Its `listModels` lists what the key reaches, over the same key.
 */
import type { HttpClient } from 'effect/http'
import type { Layer } from 'effect'
import type { Redacted } from 'effect'
import * as Adapter from '../accounts/Adapter.ts'
import type { Credentials } from '../credentials/Credentials.ts'
import type { Definition, Listable, Ref } from '../model/Model.ts'
import { models as catalog } from './internal/catalog.ts'
import * as internal from './internal/provider.ts'
import * as declaration from './adapter/OpenAi.ts'

/** The provider name requests route by, and its API key's name in `Credentials`, by default. */
export const name = 'openai'

/** The API its responses name. */
export const api = 'openai-responses'

/** The chat models the provider serves unless told otherwise, from pi-ai's catalog. */
export const models: ReadonlyArray<Definition> = catalog

export interface Options {
  /**
   * The name requests route by and the API key is stored under. Default `openai`; give each
   * account its own, such as `openai-work`.
   */
  readonly name?: string
  /** Without it, each request reads the API key stored under `name` from `Credentials`. */
  readonly apiKey?: Redacted.Redacted
  /** Default `https://api.openai.com/v1`; set it for an OpenAI-compatible server. */
  readonly apiUrl?: string
  /** Default `models`. A model the catalog knows keeps its effort mapping. */
  readonly models?: ReadonlyArray<Definition>
}

/** The OpenAI provider, with a fixed API key. */
export function provider(
  options: Options & { readonly apiKey: Redacted.Redacted },
): Listable<HttpClient.HttpClient>
/** The OpenAI provider, reading its API key from `Credentials` for each request. */
export function provider(options?: Options): Listable<HttpClient.HttpClient | Credentials>
export function provider(options: Options = {}): Listable<HttpClient.HttpClient | Credentials> {
  return internal.make(options)
}

/** The `Model.Ref` of OpenAI model `modelId`, served by the provider named `providerName`. */
export const ref = (modelId: string, providerName: string = name): Ref => ({
  provider: providerName,
  modelId,
})

/** The adapter's declaration; a client imports it from `effect-harness/openai/adapter`. */
export const adapter = declaration.adapter

/**
 * The adapter's implementation, as a layer for `Accounts.layer`: each account's provider, named after the account and reading its
 * key from `Credentials`. An account at a custom `apiUrl` serves the models it listed when its key
 * was checked; one at the default URL serves the catalog.
 */
export const layerAdapter: Layer.Layer<Adapter.Implemented<typeof adapter.id>> = Adapter.layer(
  adapter,
  {
    provider: (account) => {
      const { apiUrl } = account.settings
      if (apiUrl === undefined) return provider({ name: account.name })
      return provider({
        name: account.name,
        apiUrl,
        ...(account.models === undefined ? {} : { models: account.models.map((id) => ({ id })) }),
      })
    },
  },
)
