/**
 * Single-use ChatGPT OAuth authorization and persisted account credentials.
 *
 * @since 0.0.0
 */
import * as Arr from 'effect/Array'
import * as String from 'effect/String'
import * as Time from '@effect-harness/auth/Time'
// effect-review-allow P9-namespace-alias-equals-module: @effect-harness/auth/Duration and effect/Duration both bind Duration; AuthDuration distinguishes the concepts.
import * as AuthDuration from '@effect-harness/auth/Duration'
import * as Config from 'effect/Config'
import {
  AuthBusyError,
  AuthCallbackError,
  AuthConfigurationError,
  AuthDeniedError,
  AuthExpiredError,
  AuthIdentityError,
  AuthMissingError,
  AuthNetworkError,
  AuthPermissionError,
  AuthProtocolError,
  AuthError,
  accountKey,
  OAuth,
  type Registration,
} from '@effect-harness/auth/Credential'
import { CredentialStore } from '@effect-harness/auth/CredentialStore'
import { Jwt } from '@effect-harness/auth/Jwt'
import * as Pkce from '@effect-harness/auth/Pkce'
import * as Token from '@effect-harness/auth/Token'
import * as DateTime from 'effect/DateTime'
import * as Duration from 'effect/Duration'
import * as Ref from 'effect/Ref'
import * as HashMap from 'effect/HashMap'
import * as Context from 'effect/Context'
import type * as Crypto from 'effect/Crypto'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'

/**
 * Tests whether an unknown value satisfies the decoded suer schema.
 *
 * @category guards
 * @since 0.0.0
 */
export const issuer = 'https://auth.openai.com'
/**
 * Defines resource for the ChatGpt boundary.
 *
 * @category models
 * @since 0.0.0
 */
export const resource = 'https://api.openai.com/v1'
/**
 * Defines directScope for the ChatGpt boundary.
 *
 * @category models
 * @since 0.0.0
 */
export const directScope = 'chatgpt.tokens.use.direct'
const tokenEndpoint = `${issuer}/api/accounts/oauth/token`
const jwksUrl = `${issuer}/.well-known/jwks.json`
const requestedScopes = [
  'openid',
  'profile',
  'email',
  'offline_access',
  'resource.invoke',
  directScope,
]

/**
 * Describes the Authorization contract.
 *
 * @category types
 * @since 0.0.0
 */
export interface Authorization {
  readonly url: Redacted.Redacted<string>
  readonly state: string
  readonly redirectUri: string
  readonly expiresAt: DateTime.Utc
}
interface Pending {
  readonly authorization: Authorization
  readonly challenge: Pkce.Challenge
  readonly hostId: string
  readonly returning?: OAuth | Registration | undefined
}
/**
 * Defines Model for the ChatGpt boundary.
 *
 * @category models
 * @since 0.0.0
 */
export const Model = Schema.Struct({
  slug: Schema.NonEmptyString,
  display_name: Schema.String,
  visibility: Schema.String,
})
export type Model = typeof Model.Type
/**
 * Tests whether an unknown value satisfies the decoded Model schema.
 *
 * @category guards
 * @since 0.0.0
 */
export const isModel: (u: unknown) => u is Model = Schema.is(Model)
const ModelList = Schema.Struct({ models: Schema.Array(Model) })

/**
 * Types owned by the ChatGpt concept.
 *
 * @category types
 * @since 0.0.0
 */
export declare namespace ChatGpt {
  /**
   * Describes the Service contract.
   *
   * @category types
   * @since 0.0.0
   */
  export interface Service {
    readonly begin: (options: {
      readonly redirectUri: string
      readonly account?: string | undefined
    }) => Effect.Effect<Authorization, AuthError>
    readonly complete: (callbackUrl: string) => Effect.Effect<OAuth, AuthError>
    readonly refresh: (
      account: string,
      options?: { readonly force?: boolean | undefined },
    ) => Effect.Effect<OAuth, AuthError>
    readonly accessToken: (account: string) => Effect.Effect<Redacted.Redacted<string>, AuthError>
    readonly models: (account: string) => Effect.Effect<ReadonlyArray<Model>, AuthError>
    readonly signOut: (account: string) => Effect.Effect<void, AuthError>
    readonly cancel: (state: string) => Effect.Effect<void>
  }
}
/**
 * Describes the Service contract.
 *
 * @category types
 * @since 0.0.0
 */
export type Service = ChatGpt.Service
/**
 * Identifies the ChatGpt service in the Effect context.
 *
 * @category services
 * @since 0.0.0
 */
export class ChatGpt extends Context.Service<ChatGpt, Service>()(
  '@effect-harness/provider-openai/ChatGpt',
) {}

const parseUrl = (input: string) =>
  Effect.try({
    try: () => new URL(input),
    catch: (cause) =>
      new AuthError({ reason: new AuthCallbackError({ cause, message: 'Invalid callback URL' }) }),
  })
const scopeList = (scope: string): ReadonlyArray<string> => [
  ...Arr.dedupe(scope.split(/\s+/).filter(String.isNonEmpty)),
]
const requireDirect = (scopes: ReadonlyArray<string>) =>
  scopes.includes(directScope)
    ? Effect.void
    : Effect.fail(
        new AuthError({
          reason: new AuthPermissionError({ message: 'ChatGPT plan permission was not granted' }),
        }),
      )

/** Validate arithmetic against the same finite timestamp codecs used by persisted OAuth grants. */
const deadlines = (
  now: DateTime.Utc,
  token: { readonly expires_in: number; readonly earliest_refresh_at?: number | undefined },
) =>
  Schema.decodeEffect(
    Schema.Struct({
      expiresAt: Time.EpochMillis,
      earliestRefreshAt: Schema.optional(Time.EpochMillis),
    }),
  )({
    expiresAt: DateTime.toEpochMillis(
      DateTime.addDuration(now, Duration.seconds(token.expires_in)),
    ),
    ...(token.earliest_refresh_at === undefined
      ? {}
      : { earliestRefreshAt: token.earliest_refresh_at * 1000 }),
  }).pipe(
    Effect.map((value) => ({
      expiresAt: value.expiresAt,
      ...(value.earliestRefreshAt === undefined
        ? {}
        : { earliestRefreshAt: value.earliestRefreshAt }),
    })),
    Effect.mapError(
      (cause) =>
        new AuthError({
          reason: new AuthProtocolError({ cause, message: 'Invalid ChatGPT token lifetime' }),
        }),
    ),
  )

/**
 * Provides ChatGpt services with the declared native dependencies.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer = (options: {
  readonly appName: string
  readonly authorizationLifetimeMs?: Duration.Input | undefined
  readonly refreshSkewMs?: Duration.Input | undefined
}): Layer.Layer<
  ChatGpt,
  AuthError,
  CredentialStore | Jwt | Crypto.Crypto | HttpClient.HttpClient
> =>
  Layer.effect(ChatGpt)(
    Effect.gen(function* () {
      if (options.appName.trim().length === 0)
        return yield* new AuthError({
          reason: new AuthConfigurationError({
            message: 'An actual application name is required',
          }),
        })
      const message = 'Authorization lifetime and refresh skew must be finite valid durations'
      const lifetime = yield* AuthDuration.fromInput(
        options.authorizationLifetimeMs ?? '10 minutes',
        message,
      )
      const skew = yield* AuthDuration.fromInput(options.refreshSkewMs ?? '1 minute', message)
      if (
        !Number.isFinite(Duration.toMillis(lifetime)) ||
        Duration.toMillis(lifetime) <= 0 ||
        !Number.isFinite(Duration.toMillis(skew)) ||
        Duration.toMillis(skew) < 0
      )
        return yield* new AuthError({ reason: new AuthConfigurationError({ message }) })
      const store = yield* CredentialStore
      const jwt = yield* Jwt
      const client = yield* HttpClient.HttpClient
      const cryptoContext = yield* Effect.context<Crypto.Crypto>()
      const pending = yield* Ref.make(HashMap.empty<string, Pending>())
      yield* Effect.addFinalizer(() => Ref.set(pending, HashMap.empty()))
      const load = Effect.fnUntraced(function* (key: string) {
        const current = yield* store.get(key)
        const credential = yield* Effect.fromOption(
          current,
          () =>
            new AuthError({
              reason: new AuthMissingError({
                message: 'ChatGPT account registration was not found',
              }),
            }),
        )
        if (
          (credential._tag !== 'oauth' && credential._tag !== 'registration') ||
          credential.provider !== 'openai' ||
          credential.issuer !== issuer
        )
          return yield* new AuthError({
            reason: new AuthMissingError({
              message: 'ChatGPT account registration was not found',
            }),
          })
        return credential
      })
      const refresh: Service['refresh'] = Effect.fnUntraced(function* (key, refreshOptions) {
        const updated = yield* store.modify(
          key,
          Effect.fnUntraced(function* (current) {
            const credential = yield* Effect.fromOption(
              current,
              () =>
                new AuthError({
                  reason: new AuthMissingError({
                    message: 'ChatGPT account is signed out',
                  }),
                }),
            )
            if (
              credential._tag !== 'oauth' ||
              credential.provider !== 'openai' ||
              credential.issuer !== issuer
            )
              return yield* new AuthError({
                reason: new AuthMissingError({
                  message: 'ChatGPT account is signed out',
                }),
              })

            if (credential.clientId === 'dynamic_agent_client')
              return yield* new AuthError({
                reason: new AuthProtocolError({
                  message: 'An issued account client ID is required',
                }),
              })
            yield* requireDirect(credential.scopes)
            const now = yield* DateTime.now
            if (
              !refreshOptions?.force &&
              DateTime.isGreaterThan(credential.expiresAt, DateTime.addDuration(now, skew))
            )
              return credential
            if (
              credential.earliestRefreshAt !== undefined &&
              DateTime.isLessThan(now, credential.earliestRefreshAt)
            ) {
              if (DateTime.isGreaterThan(credential.expiresAt, now)) return credential
              return yield* new AuthError({
                reason: new AuthExpiredError({
                  message: 'Credential cannot yet be refreshed',
                }),
              })
            }
            const token = yield* Token.request(tokenEndpoint, {
              grant_type: 'refresh_token',
              client_id: credential.clientId,
              refresh_token: credential.refreshToken,
              resource,
            }).pipe(Effect.provideService(HttpClient.HttpClient, client))
            const scopes = token.scope === undefined ? credential.scopes : scopeList(token.scope)
            yield* requireDirect(scopes)
            if (token.id_token !== undefined) {
              const identity = yield* jwt.verify(token.id_token, {
                issuer,
                audience: credential.clientId,
                jwksUrl,
                algorithms: ['RS256'],
              })
              if (identity.sub !== credential.subject)
                return yield* new AuthError({
                  reason: new AuthIdentityError({
                    message: 'Refreshed credential belongs to another account',
                  }),
                })
            }
            return {
              ...credential,
              accessToken: token.access_token,
              refreshToken: token.refresh_token,
              idToken: token.id_token ?? credential.idToken,
              scopes,
              ...(yield* deadlines(now, token)),
            }
          }),
        )
        if (updated?._tag !== 'oauth')
          return yield* new AuthError({
            reason: new AuthMissingError({
              message: 'ChatGPT account is signed out',
            }),
          })
        return updated
      })
      const accessToken: Service['accessToken'] = Effect.fnUntraced(function* (account) {
        const credential = yield* refresh(account)
        return credential.accessToken
      })
      return ChatGpt.of({
        begin: Effect.fnUntraced(function* (beginOptions) {
          const redirect = yield* parseUrl(beginOptions.redirectUri)
          if (
            redirect.protocol !== 'http:' ||
            redirect.hostname !== '127.0.0.1' ||
            redirect.pathname !== '/auth/callback' ||
            redirect.search !== '' ||
            redirect.hash !== '' ||
            redirect.username !== '' ||
            redirect.password !== ''
          )
            return yield* new AuthError({
              reason: new AuthConfigurationError({
                message: 'Use an HTTP 127.0.0.1 loopback callback at /auth/callback',
              }),
            })
          const returning =
            beginOptions.account === undefined ? undefined : yield* load(beginOptions.account)
          if (returning !== undefined) {
            if (
              returning.clientId === 'dynamic_agent_client' ||
              returning.redirectUri === undefined
            )
              return yield* new AuthError({
                reason: new AuthProtocolError({
                  message: 'Incomplete account registration',
                }),
              })
            const previous = yield* parseUrl(returning.redirectUri)
            if (
              previous.protocol !== redirect.protocol ||
              previous.hostname !== redirect.hostname ||
              previous.pathname !== redirect.pathname
            )
              return yield* new AuthError({
                reason: new AuthConfigurationError({
                  message: 'Callback scheme, host and path must match registration',
                }),
              })
          }
          const hostId = yield* store.hostId('openai')
          if (returning !== undefined && returning.hostId !== hostId)
            return yield* new AuthError({
              reason: new AuthConfigurationError({
                message: 'Account registration belongs to another host',
              }),
            })
          const challenge = yield* Pkce.make.pipe(Effect.provideContext(cryptoContext))
          const now = yield* DateTime.now
          const query = new URLSearchParams({
            client_id: returning?.clientId ?? 'dynamic_agent_client',
            ext_agent_host_id: hostId,
            response_type: 'code',
            redirect_uri: beginOptions.redirectUri,
            scope: requestedScopes.join(' '),
            resource,
            state: challenge.state,
            nonce: challenge.nonce,
            code_challenge_method: 'S256',
            code_challenge: challenge.challenge,
          })
          if (returning === undefined) query.set('agent_name_hint', options.appName)
          else {
            if (returning._tag === 'oauth')
              query.set('id_token_hint', Redacted.value(returning.idToken))
            if (returning.email !== undefined) query.set('login_hint', returning.email)
          }
          const authorization = {
            url: Redacted.make(`${issuer}/api/accounts/authorize?${query.toString()}`),
            state: challenge.state,
            redirectUri: beginOptions.redirectUri,
            expiresAt: DateTime.addDuration(now, lifetime),
          }
          const admitted = yield* Ref.modify(pending, (attempts) => {
            const fresh = HashMap.filter(attempts, (attempt) =>
              DateTime.isGreaterThan(attempt.authorization.expiresAt, now),
            )
            return HashMap.size(fresh) >= 32
              ? ([false, fresh] as const)
              : ([
                  true,
                  HashMap.set(fresh, challenge.state, {
                    authorization,
                    challenge,
                    hostId,
                    returning,
                  }),
                ] as const)
          })
          if (!admitted)
            return yield* new AuthError({
              reason: new AuthBusyError({ message: 'Too many pending authorization attempts' }),
            })
          return authorization
        }),
        complete: Effect.fnUntraced(function* (callbackUrl) {
          const callback = yield* parseUrl(callbackUrl)
          const state = callback.searchParams.get('state')
          const current =
            state === null ? Option.none<Pending>() : HashMap.get(yield* Ref.get(pending), state)
          const attempt = yield* Effect.fromOption(
            current,
            () =>
              new AuthError({
                reason: new AuthCallbackError({
                  message: 'Authorization state does not match a pending attempt',
                }),
              }),
          )
          if (state === null)
            return yield* new AuthError({
              reason: new AuthCallbackError({
                message: 'Authorization state does not match a pending attempt',
              }),
            })

          const expected = yield* parseUrl(attempt.authorization.redirectUri)
          if (
            callback.origin !== expected.origin ||
            callback.pathname !== expected.pathname ||
            callback.hash !== '' ||
            callback.username !== '' ||
            callback.password !== ''
          )
            return yield* new AuthError({
              reason: new AuthCallbackError({
                message: 'Callback does not match the authorization redirect',
              }),
            })
          for (const key of ['state', 'code', 'client_id', 'error'])
            if (callback.searchParams.getAll(key).length > 1)
              return yield* new AuthError({
                reason: new AuthCallbackError({
                  message: 'Duplicate authorization callback parameter',
                }),
              })
          const consumed = yield* Ref.modify(pending, (attempts) =>
            Option.exists(HashMap.get(attempts, state), (current) => current === attempt)
              ? ([true, HashMap.remove(attempts, state)] as const)
              : ([false, attempts] as const),
          )
          if (!consumed)
            return yield* new AuthError({
              reason: new AuthCallbackError({
                message: 'Authorization was already consumed or cancelled',
              }),
            })
          if (DateTime.isGreaterThanOrEqualTo(yield* DateTime.now, attempt.authorization.expiresAt))
            return yield* new AuthError({
              reason: new AuthExpiredError({
                message: 'Authorization attempt expired',
              }),
            })
          if (callback.searchParams.has('error'))
            return yield* new AuthError({
              reason: new AuthDeniedError({
                message: 'Authorization was declined or failed',
              }),
            })
          const code = callback.searchParams.get('code')
          const suppliedId = callback.searchParams.get('client_id')
          const clientId = suppliedId ?? attempt.returning?.clientId
          if (
            code === null ||
            code.length === 0 ||
            clientId === undefined ||
            clientId === null ||
            clientId.length === 0 ||
            clientId === 'dynamic_agent_client'
          )
            return yield* new AuthError({
              reason: new AuthCallbackError({
                message: 'Authorization code and issued client ID are required',
              }),
            })
          if (attempt.returning !== undefined && clientId !== attempt.returning.clientId)
            return yield* new AuthError({
              reason: new AuthIdentityError({
                message: 'Callback client ID differs from selected account',
              }),
            })
          const token = yield* Token.request(tokenEndpoint, {
            grant_type: 'authorization_code',
            client_id: clientId,
            code: Redacted.make(code),
            code_verifier: attempt.challenge.verifier,
            redirect_uri: attempt.authorization.redirectUri,
            resource,
          }).pipe(Effect.provideService(HttpClient.HttpClient, client))
          if (token.id_token === undefined || token.scope === undefined)
            return yield* new AuthError({
              reason: new AuthProtocolError({
                message: 'Sign-in response requires identity and granted scopes',
              }),
            })
          const identity = yield* jwt.verify(token.id_token, {
            issuer,
            audience: clientId,
            jwksUrl,
            nonce: attempt.challenge.nonce,
            algorithms: ['RS256'],
          })
          if (attempt.returning !== undefined && identity.sub !== attempt.returning.subject)
            return yield* new AuthError({
              reason: new AuthIdentityError({
                message: 'Signed-in identity differs from selected account',
              }),
            })
          const scopes = scopeList(token.scope)
          yield* requireDirect(scopes)
          const now = yield* DateTime.now
          const credential: OAuth = {
            _tag: 'oauth',
            provider: 'openai',
            issuer,
            subject: identity.sub,
            clientId,
            hostId: attempt.hostId,
            redirectUri: attempt.authorization.redirectUri,
            ...(identity.email === undefined ? {} : { email: identity.email }),
            accessToken: token.access_token,
            refreshToken: token.refresh_token,
            idToken: token.id_token,
            scopes,
            ...(yield* deadlines(now, token)),
          }
          yield* store.set(accountKey(credential), credential)
          return credential
        }),
        refresh,
        accessToken,
        models: Effect.fnUntraced(function* (account) {
          // P5-request-resolver-batching: this endpoint has no multi-account bulk read. Each call
          // refreshes this account first, then independently samples current plan visibility with
          // its provider/resource and current grant. Dedup/cache would hide authorization changes
          // or replay a failed catalogue; no reusable HTTP response/result crosses calls.
          const access = yield* accessToken(account)
          const response = yield* client
            .execute(
              HttpClientRequest.get(`${resource}/models`).pipe(
                HttpClientRequest.bearerToken(Redacted.value(access)),
              ),
            )
            .pipe(
              Effect.mapError(
                (cause) =>
                  new AuthError({
                    reason: new AuthNetworkError({
                      cause,
                      message: 'Model catalog could not be loaded',
                    }),
                  }),
              ),
            )
          if (response.status !== 200)
            return yield* new AuthError({
              reason: new AuthPermissionError({
                message: 'Account model catalog unavailable',
                status: response.status,
              }),
            })
          const body = yield* response.json.pipe(
            Effect.mapError(
              (cause) =>
                new AuthError({
                  reason: new AuthProtocolError({ cause, message: 'Invalid model catalog' }),
                }),
            ),
          )
          const catalog = yield* Schema.decodeUnknownEffect(ModelList)(body).pipe(
            Effect.mapError(
              (cause) =>
                new AuthError({
                  reason: new AuthProtocolError({ cause, message: 'Invalid model catalog' }),
                }),
            ),
          )
          return catalog.models.filter((model) => model.visibility === 'list')
        }),
        signOut: Effect.fnUntraced(function* (account) {
          yield* store.modify(
            account,
            Effect.fnUntraced(function* (current) {
              const credential = yield* Effect.fromOption(
                current,
                () =>
                  new AuthError({
                    reason: new AuthMissingError({
                      message: 'ChatGPT registration was not found',
                    }),
                  }),
              )
              if (
                (credential._tag !== 'oauth' && credential._tag !== 'registration') ||
                credential.provider !== 'openai' ||
                credential.issuer !== issuer
              )
                return yield* new AuthError({
                  reason: new AuthMissingError({
                    message: 'ChatGPT registration was not found',
                  }),
                })

              if (credential._tag === 'registration') return credential
              yield* Token.revoke(`${issuer}/api/accounts/oauth/revoke`, {
                token: credential.refreshToken,
                token_type_hint: 'refresh_token',
                client_id: credential.clientId,
              }).pipe(Effect.provideService(HttpClient.HttpClient, client))
              return {
                _tag: 'registration',
                provider: credential.provider,
                issuer: credential.issuer,
                subject: credential.subject,
                clientId: credential.clientId,
                hostId: credential.hostId,
                ...(credential.email === undefined ? {} : { email: credential.email }),
                ...(credential.redirectUri === undefined
                  ? {}
                  : { redirectUri: credential.redirectUri }),
              }
            }),
          )
        }),
        cancel: (state) => Ref.update(pending, HashMap.remove(state)),
      })
    }),
  )

/**
 * Resolves all layer options through the caller's ConfigProvider.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerConfig = (
  config: Config.Wrap<NonNullable<Parameters<typeof layer>[0]>>,
): Layer.Layer<
  ChatGpt,
  AuthError | Config.ConfigError,
  CredentialStore | Crypto.Crypto | HttpClient.HttpClient | Jwt
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layer(yield* Config.unwrap(config))
    }),
  )
