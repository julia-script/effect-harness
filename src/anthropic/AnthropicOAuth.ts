/** Explicit Claude subscription OAuth. API keys continue to use `Anthropic`. */
import { Schema } from 'effect'
import type { Duration, Effect, Redacted } from 'effect'
import type { HttpClient } from 'effect/http'
import type { Credentials } from '../credentials/Credentials.ts'
import type { CredentialsError, ReadOnly } from '../credentials/CredentialsError.ts'
import type { Definition, Listable, Ref } from '../model/Model.ts'
import { models as catalog } from './internal/catalog.ts'
import * as auth from './internal/oauth/auth.ts'
import * as internal from './internal/oauth/provider.ts'

export const name = 'anthropic-oauth'
export const api = 'anthropic-messages'
export const models: ReadonlyArray<Definition> = catalog

/** Authentication errors deliberately omit HTTP requests and token response bodies. */
export class LoginError extends Schema.TaggedError<LoginError, { readonly brand: unique symbol }>(
  'effect-harness/anthropic/AnthropicOAuth/LoginError',
)('LoginError', {
  message: Schema.String,
  reason: Schema.Literals([
    'InvalidConfiguration',
    'InvalidState',
    'InvalidResponse',
    'InvalidGrant',
    'TransportFailure',
    'Timeout',
  ]),
}) {}

export interface Options {
  readonly name?: string
  readonly apiUrl?: string
  /** Token server origin; defaults to https://platform.claude.com. */
  readonly authUrl?: string
  readonly models?: ReadonlyArray<Definition>
  /** Discovered model IDs whose API metadata advertises adaptive thinking. */
  readonly adaptiveModels?: ReadonlyArray<string>
}

/** Opaque login context: keep it private and retain it only until completion. */
export interface Authorization {
  readonly name: string
  readonly url: string
  readonly state: string
  readonly verifier: Redacted.Redacted
  readonly redirectUri: string
  readonly expires: number
}

export interface BeginOptions {
  readonly name?: string
  /** Optional host-owned loopback callback; otherwise the copy-code callback is used. */
  readonly redirectUri?: string
}

/** Start PKCE authorization. The host opens `url` and retains this context for `complete`. */
export const begin: (options?: BeginOptions) => Effect.Effect<Authorization, LoginError> =
  auth.begin

/** Validate a callback URL, query string, or `code#state`, exchange it, and store the tokens. */
export const complete: (
  authorization: Authorization,
  completion: string,
  options?: Pick<Options, 'authUrl'>,
) => Effect.Effect<
  void,
  LoginError | CredentialsError | ReadOnly,
  HttpClient.HttpClient | Credentials
> = auth.complete

export interface LoginOptions extends BeginOptions {
  readonly onAuthorize: (url: string) => Effect.Effect<void>
  readonly completion: Effect.Effect<string, LoginError>
  readonly authUrl?: string
  /** Bounds authorization and exchange; defaults to ten minutes. */
  readonly timeout?: Duration.Input
}

/** Browser/CLI copy-code sign-in; not a device-code flow. Existing credentials change only on success. */
export const login: (
  options: LoginOptions,
) => Effect.Effect<
  void,
  LoginError | CredentialsError | ReadOnly,
  HttpClient.HttpClient | Credentials
> = auth.login

/** A subscription provider; credentials are refreshed with compare-and-set persistence. */
export const provider = (options: Options = {}): Listable<HttpClient.HttpClient | Credentials> =>
  internal.make(options)

export const ref = (modelId: string, providerName: string = name): Ref => ({
  provider: providerName,
  modelId,
})
