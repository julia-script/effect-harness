/**
 * Single-use Anthropic OAuth consent, refresh and scoped browser callbacks.
 */
import * as Arr from 'effect/Array'
import * as String from 'effect/String'
// effect-review-allow P9-namespace-alias-equals-module: @effect-harness/auth/Duration and effect/Duration both bind Duration; AuthDuration distinguishes the concepts.
import * as AuthDuration from '@effect-harness/auth/Duration'
import * as Config from 'effect/Config'
import {
  AuthError,
  makeAuthErrorReason,
  Secret,
  type AuthErrorCode,
  type OpaqueOAuth,
} from '@effect-harness/auth/Credential'
import { CredentialStore } from '@effect-harness/auth/CredentialStore'
import * as Pkce from '@effect-harness/auth/Pkce'
import type { Fields as TokenFields } from '@effect-harness/auth/Token'
import * as HashMap from 'effect/HashMap'
import * as Equal from 'effect/Equal'
import * as DateTime from 'effect/DateTime'
import * as Duration from 'effect/Duration'
import * as Context from 'effect/Context'
import * as Crypto from 'effect/Crypto'
import * as Effect from 'effect/Effect'
import * as Deferred from 'effect/Deferred'
import * as Exit from 'effect/Exit'
import * as Ref from 'effect/Ref'
import * as HttpServer from 'effect/http/HttpServer'
import * as HttpServerRequest from 'effect/http/HttpServerRequest'
import * as HttpServerResponse from 'effect/http/HttpServerResponse'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'

// Protocol adapted from Pi commit 636703a0 (MIT); see package NOTICE.
/**
 * OAuth client identifier used by the supported Anthropic account protocol.
 *
 * @category constants
 */
export const clientId = '9d1c250a-e61b-44d9-88ed-5944d1962f5e'
/**
 * Base URL of the Anthropic account authorization server.
 *
 * @category constants
 */
export const authorizationServer = 'https://platform.claude.com'
/**
 * Browser consent endpoint used for Anthropic account authorization.
 *
 * @category constants
 */
export const authorizeUrl = 'https://claude.ai/oauth/authorize'
/**
 * Token endpoint used for code exchange and refresh.
 *
 * @category constants
 */
export const tokenUrl = `${authorizationServer}/v1/oauth/token`
/**
 * Loopback callback URL required by browser consent mode.
 *
 * @category constants
 */
export const browserRedirectUri = 'http://localhost:53692/callback'
/**
 * Hosted callback URL used to obtain a copied authorization code.
 *
 * @category constants
 */
export const copyCodeRedirectUri = `${authorizationServer}/oauth/code/callback`
/**
 * Permission scopes requested by the account authorization protocol.
 *
 * @category constants
 */
export const scopes = [
  'org:create_api_key',
  'user:profile',
  'user:inference',
  'user:sessions:claude_code',
  'user:mcp_servers',
  'user:file_upload',
] as const
/**
 * Pending Anthropic consent URL, secret state, redirect and expiry.
 *
 * @category models
 */
export interface Authorization {
  readonly url: Redacted.Redacted<string>
  /** Pi uses the PKCE verifier as state, so this value is a secret too. */
  readonly state: Redacted.Redacted<string>
  readonly redirectUri: string
  readonly expiresAt: DateTime.Utc
}
/**
 * Type-level contracts for `OAuth`.
 *
 * @category utility types
 */
export declare namespace OAuth {
  /**
   * Explicit Anthropic consent, completion and serialized token refresh.
   *
   * @category models
   */
  export interface Service {
    /**
     * Creates a pending browser or copy-code authorization under a caller-selected account key.
     */
    readonly begin: (options: {
      readonly account: string
      readonly method?: 'browser' | 'copyCode' | undefined
    }) => Effect.Effect<Authorization, AuthError>
    /**
     * Exchanges the callback URL or copied code using the Redacted secret state and persists an
     * OpaqueOAuth grant.
     */
    readonly complete: (
      state: Redacted.Redacted<string>,
      input: string,
    ) => Effect.Effect<OpaqueOAuth, AuthError>
    /**
     * Refreshes the selected grant under credential serialization; force bypasses its normal
     * refresh deadline.
     */
    readonly refresh: (
      account: string,
      options?: { readonly force?: boolean | undefined },
    ) => Effect.Effect<OpaqueOAuth, AuthError>
    /**
     * Returns a Redacted fresh bearer token for the selected account.
     */
    readonly accessToken: (account: string) => Effect.Effect<Redacted.Redacted<string>, AuthError>
    /**
     * Removes the selected account grant from application-owned storage.
     */
    readonly signOut: (account: string) => Effect.Effect<void, AuthError>
    /**
     * Cancels the pending authorization identified by its Redacted state.
     */
    readonly cancel: (state: Redacted.Redacted<string>) => Effect.Effect<void>
  }
}
/**
 * Explicit Anthropic consent, completion and serialized token refresh.
 *
 * @category models
 */
export type Service = OAuth.Service
/**
 * Service for explicit Anthropic account consent and serialized token refresh.
 *
 * **Details**
 *
 * begin selects browser or copy-code consent. The host passes the callback URL or copied
 * code to complete. The resulting OpaqueOAuth grant uses a caller-selected account key.
 *
 * **Gotchas**
 *
 * The protocol uses the PKCE verifier as state, so both the URL and state are Redacted
 * secrets. It does not establish a verified OIDC identity or read CLI credentials.
 *
 * @category services
 */
export class OAuth extends Context.Service<OAuth, Service>()(
  '@effect-harness/provider-anthropic/OAuth',
) {}
interface Pending {
  readonly account: string
  readonly authorization: Authorization
  readonly challenge: Pkce.Challenge
}
const failure = (reason: AuthErrorCode, message: string, status?: number, cause?: unknown) =>
  new AuthError({
    reason: makeAuthErrorReason({
      reason,
      message,
      ...(status === undefined ? {} : { status }),
      ...(cause === undefined ? {} : { cause }),
    }),
  })
const Token = Schema.Struct({
  access_token: Secret,
  refresh_token: Secret,
  expires_in: Schema.Finite.check(Schema.isGreaterThan(0)),
  scope: Schema.optionalKey(Schema.String),
  token_type: Schema.optionalKey(Schema.String),
})
const permission = (value: ReadonlyArray<string>) =>
  value.includes('user:inference')
    ? Effect.void
    : Effect.fail(failure('permission', 'Anthropic inference scope was not granted'))
const splitScopes = (value: string): ReadonlyArray<string> => [
  ...Arr.dedupe(value.split(/\s+/).filter(String.isNonEmpty)),
]
const matches = (value: OpaqueOAuth) =>
  value.provider === 'anthropic' &&
  value.authorizationServer === authorizationServer &&
  value.clientId === clientId

/**
 * Provides Anthropic consent and refresh with application-owned credentials.
 *
 * **Details**
 *
 * Consumes CredentialStore, HttpClient and Crypto. Completion requires the inference scope;
 * token refresh is serialized by CredentialStore.
 *
 * **Gotchas**
 *
 * Consent must be initiated by the user. Account access remains subject to the provider’s
 * runtime authorization.
 *
 * @category layers
 */
export const layer = (options?: {
  readonly authorizationLifetimeMs?: Duration.Input | undefined
  readonly refreshSkewMs?: Duration.Input | undefined
}): Layer.Layer<OAuth, AuthError, CredentialStore | Crypto.Crypto | HttpClient.HttpClient> =>
  Layer.effect(OAuth)(
    Effect.gen(function* () {
      const message = 'Authorization and refresh durations must be finite valid durations'
      const lifetime = yield* AuthDuration.fromInput(
        options?.authorizationLifetimeMs ?? '10 minutes',
        message,
      )
      const skew = yield* AuthDuration.fromInput(options?.refreshSkewMs ?? '5 minutes', message)
      if (
        !Number.isFinite(Duration.toMillis(lifetime)) ||
        Duration.toMillis(lifetime) <= 0 ||
        !Number.isFinite(Duration.toMillis(skew)) ||
        Duration.toMillis(skew) < 0
      )
        return yield* failure('configuration', message)
      const store = yield* CredentialStore
      const http = yield* HttpClient.HttpClient
      const crypto = yield* Effect.context<Crypto.Crypto>()
      const pending = yield* Ref.make(HashMap.empty<Redacted.Redacted<string>, Pending>())
      yield* Effect.addFinalizer(() => Ref.set(pending, HashMap.empty()))
      const request = Effect.fnUntraced(
        function* (fields: TokenFields) {
          const response = yield* http
            .execute(
              HttpClientRequest.post(tokenUrl).pipe(
                HttpClientRequest.bodyJsonUnsafe(
                  Object.fromEntries(
                    Object.entries(fields).flatMap(([key, value]) =>
                      value === undefined
                        ? []
                        : [[key, Redacted.isRedacted(value) ? Redacted.value(value) : value]],
                    ),
                  ),
                ),
                HttpClientRequest.acceptJson,
              ),
            )
            .pipe(
              Effect.mapError((cause) =>
                failure(
                  'network',
                  'Anthropic token endpoint could not be reached',
                  undefined,
                  cause,
                ),
              ),
            )
          if (response.status !== 200)
            return yield* failure(
              'token',
              'Anthropic token endpoint rejected the grant',
              response.status,
            )
          const json = yield* response.json.pipe(
            Effect.mapError((cause) =>
              failure('protocol', 'Invalid Anthropic token response', undefined, cause),
            ),
          )
          const token = yield* Schema.decodeUnknownEffect(Token)(json).pipe(
            Effect.mapError((cause) =>
              failure('protocol', 'Invalid Anthropic token response', undefined, cause),
            ),
          )
          if (token.token_type !== undefined && token.token_type.toLowerCase() !== 'bearer')
            return yield* failure('protocol', 'Unsupported Anthropic token type')
          return token
        },
        Effect.timeoutOrElse({
          duration: '30 seconds',
          orElse: () => Effect.fail(failure('network', 'Anthropic token request timed out')),
        }),
      )
      const credential = Effect.fnUntraced(function* (
        token: typeof Token.Type,
        previousScopes: ReadonlyArray<string>,
        redirectUri?: string,
      ) {
        const granted = token.scope === undefined ? previousScopes : splitScopes(token.scope)
        yield* permission(granted)
        const expiresAt = DateTime.addDuration(
          yield* DateTime.now,
          Duration.seconds(token.expires_in),
        )
        if (!Number.isSafeInteger(DateTime.toEpochMillis(expiresAt)))
          return yield* failure('protocol', 'Invalid Anthropic token lifetime')
        return {
          _tag: 'opaqueOAuth',
          provider: 'anthropic',
          authorizationServer,
          clientId,
          accessToken: token.access_token,
          refreshToken: token.refresh_token,
          scopes: granted,
          expiresAt,
          ...(redirectUri === undefined ? {} : { redirectUri }),
        } satisfies OpaqueOAuth
      })
      const refresh: Service['refresh'] = Effect.fnUntraced(function* (account, refreshOptions) {
        const updated = yield* store.modify(
          account,
          Effect.fnUntraced(function* (current) {
            const previous = yield* Effect.fromOption(current, () =>
              failure('missing', 'Anthropic account credential was not found'),
            )
            if (previous._tag !== 'opaqueOAuth' || !matches(previous))
              return yield* failure('missing', 'Anthropic account credential was not found')
            yield* permission(previous.scopes)
            if (
              !refreshOptions?.force &&
              DateTime.isGreaterThan(
                previous.expiresAt,
                DateTime.addDuration(yield* DateTime.now, skew),
              )
            )
              return previous
            const token = yield* request({
              grant_type: 'refresh_token',
              client_id: clientId,
              refresh_token: previous.refreshToken,
            })
            return yield* credential(token, previous.scopes, previous.redirectUri)
          }),
        )
        if (updated?._tag !== 'opaqueOAuth')
          return yield* failure('missing', 'Anthropic account credential was not found')
        return updated
      })
      return OAuth.of({
        begin: Effect.fnUntraced(function* (beginOptions) {
          if (
            beginOptions.method !== undefined &&
            beginOptions.method !== 'browser' &&
            beginOptions.method !== 'copyCode'
          )
            return yield* failure('configuration', 'Unsupported Anthropic consent method')
          if (beginOptions.account.length === 0)
            return yield* failure('configuration', 'Supply a nonempty account storage key')
          const challenge = yield* Pkce.make.pipe(Effect.provideContext(crypto))
          const state = challenge.verifier
          const redirectUri =
            beginOptions.method === 'copyCode' ? copyCodeRedirectUri : browserRedirectUri
          const now = yield* DateTime.now
          const expiresAt = DateTime.addDuration(now, lifetime)
          const params = new URLSearchParams({
            code: 'true',
            client_id: clientId,
            response_type: 'code',
            redirect_uri: redirectUri,
            scope: scopes.join(' '),
            code_challenge: challenge.challenge,
            code_challenge_method: 'S256',
            state: Redacted.value(state),
          })
          const authorization = {
            url: Redacted.make(`${authorizeUrl}?${params.toString()}`),
            state: challenge.verifier,
            redirectUri,
            expiresAt,
          }
          yield* Ref.update(pending, (attempts) =>
            HashMap.set(
              HashMap.filter(attempts, (attempt) =>
                DateTime.isGreaterThan(attempt.authorization.expiresAt, now),
              ),
              state,
              { account: beginOptions.account, authorization, challenge },
            ),
          )
          return authorization
        }),
        complete: Effect.fnUntraced(function* (secretState, input) {
          const state = secretState
          const attempt = yield* Effect.fromOption(
            HashMap.get(yield* Ref.get(pending), state),
            () => failure('callback', 'Unknown or consumed Anthropic authorization'),
          )
          if (DateTime.isLessThanOrEqualTo(attempt.authorization.expiresAt, yield* DateTime.now)) {
            yield* Ref.update(pending, (attempts) =>
              Option.exists(HashMap.get(attempts, state), (current) => current === attempt)
                ? HashMap.remove(attempts, state)
                : attempts,
            )
            return yield* failure('expired', 'Anthropic authorization expired')
          }
          const value = input.trim()
          let code = value
          let receivedState: Redacted.Redacted<string> | undefined
          if (/^https?:\/\//.test(value)) {
            const url = yield* Effect.try({
              try: () => new URL(value),
              catch: (cause) => failure('callback', 'Invalid Anthropic callback', undefined, cause),
            })
            const redirect = new URL(attempt.authorization.redirectUri)
            if (
              url.origin !== redirect.origin ||
              url.pathname !== redirect.pathname ||
              url.username !== '' ||
              url.password !== ''
            )
              return yield* failure('callback', 'Unexpected Anthropic callback address')
            if (url.searchParams.has('error'))
              return yield* failure('denied', 'Anthropic authorization was denied')
            if (
              url.searchParams.getAll('code').length !== 1 ||
              url.searchParams.getAll('state').length !== 1
            )
              return yield* failure('callback', 'Incomplete or ambiguous Anthropic callback')
            code = url.searchParams.get('code') ?? ''
            receivedState = Redacted.make(url.searchParams.get('state') ?? '')
          } else if (value.includes('#')) {
            const pieces = value.split('#')
            if (pieces.length !== 2)
              return yield* failure('callback', 'Invalid Anthropic authorization input')
            code = pieces[0] ?? ''
            receivedState = pieces[1] === undefined ? undefined : Redacted.make(pieces[1])
          } else if (value.includes('code=')) {
            const params = new URLSearchParams(value)
            if (params.getAll('code').length !== 1 || params.getAll('state').length > 1)
              return yield* failure('callback', 'Ambiguous Anthropic authorization input')
            code = params.get('code') ?? ''
            const parsedState = params.get('state')
            receivedState = parsedState === null ? undefined : Redacted.make(parsedState)
          }
          if (receivedState !== undefined && !Equal.equals(receivedState, state))
            return yield* failure('callback', 'Anthropic authorization state mismatch')
          if (code.length === 0)
            return yield* failure('callback', 'Missing Anthropic authorization code')
          // Consume before yielding: an attempt cannot exchange twice, even concurrently or after failure.
          const consumed = yield* Ref.modify(pending, (attempts) =>
            Option.exists(HashMap.get(attempts, state), (current) => current === attempt)
              ? ([true, HashMap.remove(attempts, state)] as const)
              : ([false, attempts] as const),
          )
          if (!consumed)
            return yield* failure('callback', 'Consumed or cancelled Anthropic authorization')
          const token = yield* request({
            grant_type: 'authorization_code',
            client_id: clientId,
            code: Redacted.make(code),
            state,
            redirect_uri: attempt.authorization.redirectUri,
            code_verifier: attempt.challenge.verifier,
          })
          const saved = yield* credential(token, scopes, attempt.authorization.redirectUri)
          yield* store.set(attempt.account, saved)
          return saved
        }),
        refresh,
        accessToken: Effect.fnUntraced(function* (account) {
          const credential = yield* refresh(account)
          return credential.accessToken
        }),
        signOut: (account) => store.remove(account),
        cancel: (state) => Ref.update(pending, HashMap.remove(state)),
      })
    }),
  )

/**
 * Identifies the Callback service in the Effect context.
 *
 * @category services
 */
export class Callback extends Context.Service<
  Callback,
  {
    readonly authorization: Authorization
    readonly await: Effect.Effect<OpaqueOAuth, AuthError>
  }
>()('@effect-harness/provider-anthropic/OAuth/Callback') {}

/**
 * Installs a scoped browser callback at the account protocol’s loopback address.
 *
 * **Details**
 *
 * Consumes an application-supplied HttpServer and OAuth. The listener owns one authorization
 * attempt and closes with its Scope.
 *
 * **Gotchas**
 *
 * Copy-code mode does not require this server. The server must be bound to the protocol’s
 * expected address and port.
 *
 * @category layers
 */
export const layerCallback = (options: {
  readonly account: string
}): Layer.Layer<Callback, AuthError, HttpServer.HttpServer | OAuth> =>
  Layer.effect(Callback)(
    Effect.gen(function* () {
      const server = yield* HttpServer.HttpServer
      if (
        server.address._tag !== 'InetAddressV4' ||
        server.address.address.toString() !== '127.0.0.1' ||
        server.address.port !== 53692
      )
        return yield* failure(
          'configuration',
          'Anthropic browser callback must bind 127.0.0.1:53692',
        )
      const auth = yield* OAuth
      const result = yield* Deferred.make<OpaqueOAuth, AuthError>()
      const claimed = yield* Ref.make(false)
      const authorization = yield* Effect.acquireRelease(
        auth.begin({ account: options.account, method: 'browser' }),
        (authorization) => auth.cancel(authorization.state),
      )
      yield* server.serve(
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest
          const parsed = yield* Effect.try({
            try: () => new URL(request.url, browserRedirectUri),
            catch: (cause) => failure('callback', 'Invalid Anthropic callback', undefined, cause),
          }).pipe(Effect.option)
          const url = yield* Option.match(parsed, {
            onNone: () => Effect.void,
            onSome: Effect.succeed,
          })
          if (url === undefined || request.method !== 'GET' || url.pathname !== '/callback')
            return HttpServerResponse.empty({ status: 404 })
          if (
            !Equal.equals(Redacted.make(url.searchParams.get('state') ?? ''), authorization.state)
          )
            return HttpServerResponse.text('Invalid sign-in attempt', { status: 400 })
          const exit = yield* Effect.uninterruptibleMask((restore) =>
            Ref.getAndSet(claimed, true).pipe(
              Effect.flatMap((alreadyClaimed) =>
                alreadyClaimed
                  ? restore(Deferred.await(result)).pipe(Effect.exit)
                  : restore(auth.complete(authorization.state, url.href)).pipe(
                      Effect.exit,
                      Effect.tap((exit) => Deferred.done(result, exit)),
                    ),
              ),
            ),
          )
          return HttpServerResponse.text(
            Exit.isSuccess(exit)
              ? 'Sign-in complete. You may close this window.'
              : 'Sign-in failed. Restart sign-in in the application.',
            {
              status: Exit.isSuccess(exit) ? 200 : 400,
              headers: {
                'cache-control': 'no-store',
                'content-security-policy': "default-src 'none'",
                'referrer-policy': 'no-referrer',
              },
            },
          )
        }),
      )
      return Callback.of({
        authorization,
        await: Effect.gen(function* () {
          const now = yield* DateTime.now
          const remaining = DateTime.isGreaterThan(authorization.expiresAt, now)
            ? DateTime.distance(now, authorization.expiresAt)
            : Duration.zero
          return yield* Deferred.await(result).pipe(
            Effect.timeoutOrElse({
              duration: remaining,
              orElse: () =>
                auth
                  .cancel(authorization.state)
                  .pipe(
                    Effect.andThen(
                      Effect.fail(failure('expired', 'Anthropic authorization expired')),
                    ),
                  ),
            }),
          )
        }).pipe(Effect.withSpan('Callback.await')),
      })
    }),
  )

/**
 * Resolves all layer options through the caller's ConfigProvider.
 *
 * @category layers
 */
export const layerConfig = (
  config: Config.Wrap<NonNullable<Parameters<typeof layer>[0]>>,
): Layer.Layer<
  OAuth,
  AuthError | Config.ConfigError,
  CredentialStore | Crypto.Crypto | HttpClient.HttpClient
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layer(yield* Config.unwrap(config))
    }),
  )

/**
 * Resolves all layerCallback options through the caller's ConfigProvider.
 *
 * @category layers
 */
export const layerCallbackConfig = (
  config: Config.Wrap<NonNullable<Parameters<typeof layerCallback>[0]>>,
): Layer.Layer<Callback, AuthError | Config.ConfigError, OAuth | HttpServer.HttpServer> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layerCallback(yield* Config.unwrap(config))
    }),
  )
