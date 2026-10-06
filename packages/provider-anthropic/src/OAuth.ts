import {
  AuthError,
  makeAuthErrorReason,
  Secret,
  type AuthErrorCode,
  type OpaqueOAuth,
} from '@effect-harness/auth/Credential'
import { CredentialStore } from '@effect-harness/auth/CredentialStore'
import * as Pkce from '@effect-harness/auth/Pkce'
import * as Clock from 'effect/Clock'
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
export const clientId = '9d1c250a-e61b-44d9-88ed-5944d1962f5e'
export const authorizationServer = 'https://platform.claude.com'
export const authorizeUrl = 'https://claude.ai/oauth/authorize'
export const tokenUrl = `${authorizationServer}/v1/oauth/token`
export const browserRedirectUri = 'http://localhost:53692/callback'
export const copyCodeRedirectUri = `${authorizationServer}/oauth/code/callback`
export const scopes = [
  'org:create_api_key',
  'user:profile',
  'user:inference',
  'user:sessions:claude_code',
  'user:mcp_servers',
  'user:file_upload',
] as const
export interface Authorization {
  readonly url: Redacted.Redacted<string>
  /** Pi uses the PKCE verifier as state, so this value is a secret too. */
  readonly state: Redacted.Redacted<string>
  readonly redirectUri: string
  readonly expiresAt: number
}
export interface Service {
  readonly begin: (options: {
    readonly account: string
    readonly method?: 'browser' | 'copyCode'
  }) => Effect.Effect<Authorization, AuthError>
  readonly complete: (
    state: Redacted.Redacted<string>,
    input: string,
  ) => Effect.Effect<OpaqueOAuth, AuthError>
  readonly refresh: (
    account: string,
    options?: { readonly force?: boolean },
  ) => Effect.Effect<OpaqueOAuth, AuthError>
  readonly accessToken: (account: string) => Effect.Effect<Redacted.Redacted<string>, AuthError>
  readonly signOut: (account: string) => Effect.Effect<void, AuthError>
  readonly cancel: (state: Redacted.Redacted<string>) => Effect.Effect<void>
}
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
  ...new Set(value.split(/\s+/).filter(Boolean)),
]
const matches = (value: OpaqueOAuth) =>
  value.provider === 'anthropic' &&
  value.authorizationServer === authorizationServer &&
  value.clientId === clientId

/** Portable explicit consent service. It never opens a browser or reads another application's credentials. */
export const layer = (options?: {
  readonly authorizationLifetimeMs?: number
  readonly refreshSkewMs?: number
}) =>
  Layer.effect(OAuth)(
    Effect.gen(function* () {
      const lifetime = options?.authorizationLifetimeMs ?? 600_000
      const skew = options?.refreshSkewMs ?? 300_000
      if (!Number.isFinite(lifetime) || lifetime <= 0 || !Number.isFinite(skew) || skew < 0)
        return yield* failure(
          'configuration',
          'Authorization and refresh durations must be finite valid durations',
        )
      const store = yield* CredentialStore
      const http = yield* HttpClient.HttpClient
      const crypto = yield* Effect.context<Crypto.Crypto>()
      const pending = new Map<string, Pending>()
      yield* Effect.addFinalizer(() => Effect.sync(() => pending.clear()))
      const request = Effect.fnUntraced(
        function* (fields: Readonly<Record<string, string>>) {
          const response = yield* http
            .execute(
              HttpClientRequest.post(tokenUrl).pipe(
                HttpClientRequest.bodyJsonUnsafe(fields),
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
        const expiresAt = (yield* Clock.currentTimeMillis) + token.expires_in * 1000
        if (!Number.isSafeInteger(expiresAt))
          return yield* failure('protocol', 'Invalid Anthropic token lifetime')
        return {
          kind: 'opaqueOAuth',
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
            if (
              Option.isNone(current) ||
              current.value.kind !== 'opaqueOAuth' ||
              !matches(current.value)
            )
              return yield* failure('missing', 'Anthropic account credential was not found')
            const previous = current.value
            yield* permission(previous.scopes)
            if (
              !refreshOptions?.force &&
              previous.expiresAt > (yield* Clock.currentTimeMillis) + skew
            )
              return previous
            const token = yield* request({
              grant_type: 'refresh_token',
              client_id: clientId,
              refresh_token: Redacted.value(previous.refreshToken),
            })
            return yield* credential(token, previous.scopes, previous.redirectUri)
          }),
        )
        if (updated?.kind !== 'opaqueOAuth')
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
          const challenge = yield* Pkce.make().pipe(Effect.provideContext(crypto))
          const state = Redacted.value(challenge.verifier)
          const redirectUri =
            beginOptions.method === 'copyCode' ? copyCodeRedirectUri : browserRedirectUri
          const expiresAt = (yield* Clock.currentTimeMillis) + lifetime
          const params = new URLSearchParams({
            code: 'true',
            client_id: clientId,
            response_type: 'code',
            redirect_uri: redirectUri,
            scope: scopes.join(' '),
            code_challenge: challenge.challenge,
            code_challenge_method: 'S256',
            state,
          })
          const authorization = {
            url: Redacted.make(`${authorizeUrl}?${params.toString()}`),
            state: challenge.verifier,
            redirectUri,
            expiresAt,
          }
          for (const [key, value] of pending)
            if (value.authorization.expiresAt <= (yield* Clock.currentTimeMillis))
              pending.delete(key)
          pending.set(state, { account: beginOptions.account, authorization, challenge })
          return authorization
        }),
        complete: Effect.fnUntraced(function* (secretState, input) {
          const state = Redacted.value(secretState)
          const attempt = pending.get(state)
          if (attempt === undefined)
            return yield* failure('callback', 'Unknown or consumed Anthropic authorization')
          if (attempt.authorization.expiresAt <= (yield* Clock.currentTimeMillis)) {
            pending.delete(state)
            return yield* failure('expired', 'Anthropic authorization expired')
          }
          const value = input.trim()
          let code = value
          let receivedState: string | undefined
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
            receivedState = url.searchParams.get('state') ?? undefined
          } else if (value.includes('#')) {
            const pieces = value.split('#')
            if (pieces.length !== 2)
              return yield* failure('callback', 'Invalid Anthropic authorization input')
            code = pieces[0] ?? ''
            receivedState = pieces[1]
          } else if (value.includes('code=')) {
            const params = new URLSearchParams(value)
            if (params.getAll('code').length !== 1 || params.getAll('state').length > 1)
              return yield* failure('callback', 'Ambiguous Anthropic authorization input')
            code = params.get('code') ?? ''
            receivedState = params.get('state') ?? undefined
          }
          if (receivedState !== undefined && receivedState !== state)
            return yield* failure('callback', 'Anthropic authorization state mismatch')
          if (code.length === 0)
            return yield* failure('callback', 'Missing Anthropic authorization code')
          // Consume before yielding: an attempt cannot exchange twice, even concurrently or after failure.
          if (pending.get(state) !== attempt)
            return yield* failure('callback', 'Consumed or cancelled Anthropic authorization')
          pending.delete(state)
          const token = yield* request({
            grant_type: 'authorization_code',
            client_id: clientId,
            code,
            state,
            redirect_uri: attempt.authorization.redirectUri,
            code_verifier: Redacted.value(attempt.challenge.verifier),
          })
          const saved = yield* credential(token, scopes, attempt.authorization.redirectUri)
          yield* store.set(attempt.account, saved)
          return saved
        }),
        refresh,
        accessToken: (account) => refresh(account).pipe(Effect.map((value) => value.accessToken)),
        signOut: (account) => store.remove(account),
        cancel: (state) =>
          Effect.sync(() => {
            pending.delete(Redacted.value(state))
          }),
      })
    }),
  )

export class Callback extends Context.Service<
  Callback,
  {
    readonly authorization: Authorization
    readonly await: Effect.Effect<OpaqueOAuth, AuthError>
  }
>()('@effect-harness/provider-anthropic/OAuth/Callback') {}

/** Opt-in browser listener. Caller provides a scoped native HttpServer bound to 127.0.0.1:53692. */
export const layerCallback = (options: { readonly account: string }) =>
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
      const authorization = yield* auth.begin({ account: options.account, method: 'browser' })
      yield* Effect.addFinalizer(() => auth.cancel(authorization.state))
      yield* server.serve(
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest
          const parsed = yield* Effect.try({
            try: () => new URL(request.url, browserRedirectUri),
            catch: (cause) => failure('callback', 'Invalid Anthropic callback', undefined, cause),
          }).pipe(Effect.option)
          if (
            Option.isNone(parsed) ||
            request.method !== 'GET' ||
            parsed.value.pathname !== '/callback'
          )
            return HttpServerResponse.empty({ status: 404 })
          if (parsed.value.searchParams.get('state') !== Redacted.value(authorization.state))
            return HttpServerResponse.text('Invalid sign-in attempt', { status: 400 })
          const exit = yield* Effect.uninterruptibleMask((restore) =>
            Ref.getAndSet(claimed, true).pipe(
              Effect.flatMap((alreadyClaimed) =>
                alreadyClaimed
                  ? restore(Deferred.await(result)).pipe(Effect.exit)
                  : restore(auth.complete(authorization.state, parsed.value.href)).pipe(
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
          const remaining = Math.max(0, authorization.expiresAt - (yield* Clock.currentTimeMillis))
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
        }),
      })
    }),
  )
