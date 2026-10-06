import { AuthError, accountKey, OAuth, type Registration } from '@effect-harness/auth/Credential'
import { CredentialStore } from '@effect-harness/auth/CredentialStore'
import { Jwt } from '@effect-harness/auth/Jwt'
import * as Pkce from '@effect-harness/auth/Pkce'
import * as Token from '@effect-harness/auth/Token'
import * as Clock from 'effect/Clock'
import * as Context from 'effect/Context'
import * as Crypto from 'effect/Crypto'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'

export const issuer = 'https://auth.openai.com'
export const resource = 'https://api.openai.com/v1'
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

export interface Authorization {
  readonly url: Redacted.Redacted<string>
  readonly state: string
  readonly redirectUri: string
  readonly expiresAt: number
}
interface Pending {
  readonly authorization: Authorization
  readonly challenge: Pkce.Challenge
  readonly hostId: string
  readonly returning?: OAuth | Registration | undefined
}
export const Model = Schema.Struct({
  slug: Schema.NonEmptyString,
  display_name: Schema.String,
  visibility: Schema.String,
})
export type Model = typeof Model.Type
const ModelList = Schema.Struct({ models: Schema.Array(Model) })

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
export class ChatGpt extends Context.Service<ChatGpt, Service>()(
  '@effect-harness/provider-openai/ChatGpt',
) {}

const parseUrl = (input: string) =>
  Effect.try({
    try: () => new URL(input),
    catch: () => new AuthError({ reason: 'callback', message: 'Invalid callback URL' }),
  })
const scopeList = (scope: string): ReadonlyArray<string> => [
  ...new Set(scope.split(/\s+/).filter((item) => item.length > 0)),
]
const requireDirect = (scopes: ReadonlyArray<string>) =>
  scopes.includes(directScope)
    ? Effect.void
    : Effect.fail(
        new AuthError({ reason: 'permission', message: 'ChatGPT plan permission was not granted' }),
      )

/** Validate arithmetic against the same finite timestamp codecs used by persisted OAuth grants. */
const deadlines = (
  now: number,
  token: { readonly expires_in: number; readonly earliest_refresh_at?: number | undefined },
) =>
  Schema.decodeEffect(
    Schema.Struct({
      expiresAt: OAuth.fields.expiresAt,
      earliestRefreshAt: OAuth.fields.earliestRefreshAt,
    }),
  )({
    expiresAt: now + token.expires_in * 1000,
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
      () => new AuthError({ reason: 'protocol', message: 'Invalid ChatGPT token lifetime' }),
    ),
  )

export const layer = (options: {
  readonly appName: string
  readonly authorizationLifetimeMs?: number | undefined
  readonly refreshSkewMs?: number | undefined
}) =>
  Layer.effect(ChatGpt)(
    Effect.gen(function* () {
      if (options.appName.trim().length === 0)
        return yield* new AuthError({
          reason: 'configuration',
          message: 'An actual application name is required',
        })
      if (
        (options.authorizationLifetimeMs !== undefined &&
          (!Number.isFinite(options.authorizationLifetimeMs) ||
            options.authorizationLifetimeMs <= 0)) ||
        (options.refreshSkewMs !== undefined &&
          (!Number.isFinite(options.refreshSkewMs) || options.refreshSkewMs < 0))
      )
        return yield* new AuthError({
          reason: 'configuration',
          message: 'Authorization lifetime and refresh skew must be finite valid durations',
        })
      const store = yield* CredentialStore
      const jwt = yield* Jwt
      const client = yield* HttpClient.HttpClient
      const cryptoContext = yield* Effect.context<Crypto.Crypto>()
      const pending = new Map<string, Pending>()
      const load = Effect.fnUntraced(function* (key: string) {
        const current = yield* store.get(key)
        if (
          Option.isNone(current) ||
          (current.value.kind !== 'oauth' && current.value.kind !== 'registration') ||
          current.value.provider !== 'openai' ||
          current.value.issuer !== issuer
        )
          return yield* new AuthError({
            reason: 'missing',
            message: 'ChatGPT account registration was not found',
          })
        return current.value
      })
      const refresh: Service['refresh'] = Effect.fnUntraced(function* (key, refreshOptions) {
        const updated = yield* store.modify(
          key,
          Effect.fnUntraced(function* (current) {
            if (
              Option.isNone(current) ||
              current.value.kind !== 'oauth' ||
              current.value.provider !== 'openai' ||
              current.value.issuer !== issuer
            )
              return yield* new AuthError({
                reason: 'missing',
                message: 'ChatGPT account is signed out',
              })
            const credential = current.value
            if (credential.clientId === 'dynamic_agent_client')
              return yield* new AuthError({
                reason: 'protocol',
                message: 'An issued account client ID is required',
              })
            yield* requireDirect(credential.scopes)
            const now = yield* Clock.currentTimeMillis
            if (
              !refreshOptions?.force &&
              credential.expiresAt > now + (options.refreshSkewMs ?? 60_000)
            )
              return credential
            if (credential.earliestRefreshAt !== undefined && now < credential.earliestRefreshAt) {
              if (credential.expiresAt > now) return credential
              return yield* new AuthError({
                reason: 'expired',
                message: 'Credential cannot yet be refreshed',
              })
            }
            const token = yield* Token.request(tokenEndpoint, {
              grant_type: 'refresh_token',
              client_id: credential.clientId,
              refresh_token: Redacted.value(credential.refreshToken),
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
                  reason: 'identity',
                  message: 'Refreshed credential belongs to another account',
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
        if (updated?.kind !== 'oauth')
          return yield* new AuthError({
            reason: 'missing',
            message: 'ChatGPT account is signed out',
          })
        return updated
      })
      const accessToken: Service['accessToken'] = (account) =>
        refresh(account).pipe(Effect.map((credential) => credential.accessToken))
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
              reason: 'configuration',
              message: 'Use an HTTP 127.0.0.1 loopback callback at /auth/callback',
            })
          const returning =
            beginOptions.account === undefined ? undefined : yield* load(beginOptions.account)
          if (returning !== undefined) {
            if (
              returning.clientId === 'dynamic_agent_client' ||
              returning.redirectUri === undefined
            )
              return yield* new AuthError({
                reason: 'protocol',
                message: 'Incomplete account registration',
              })
            const previous = yield* parseUrl(returning.redirectUri)
            if (
              previous.protocol !== redirect.protocol ||
              previous.hostname !== redirect.hostname ||
              previous.pathname !== redirect.pathname
            )
              return yield* new AuthError({
                reason: 'configuration',
                message: 'Callback scheme, host and path must match registration',
              })
          }
          const hostId = yield* store.hostId('openai')
          if (returning !== undefined && returning.hostId !== hostId)
            return yield* new AuthError({
              reason: 'configuration',
              message: 'Account registration belongs to another host',
            })
          const challenge = yield* Pkce.make().pipe(Effect.provideContext(cryptoContext))
          const now = yield* Clock.currentTimeMillis
          for (const [state, attempt] of pending)
            if (attempt.authorization.expiresAt <= now) pending.delete(state)
          if (pending.size >= 32)
            return yield* new AuthError({
              reason: 'busy',
              message: 'Too many pending authorization attempts',
            })
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
            if (returning.kind === 'oauth')
              query.set('id_token_hint', Redacted.value(returning.idToken))
            if (returning.email !== undefined) query.set('login_hint', returning.email)
          }
          const authorization = {
            url: Redacted.make(`${issuer}/api/accounts/authorize?${query.toString()}`),
            state: challenge.state,
            redirectUri: beginOptions.redirectUri,
            expiresAt: now + (options.authorizationLifetimeMs ?? 600_000),
          }
          pending.set(challenge.state, { authorization, challenge, hostId, returning })
          return authorization
        }),
        complete: Effect.fnUntraced(function* (callbackUrl) {
          const callback = yield* parseUrl(callbackUrl)
          const state = callback.searchParams.get('state')
          const attempt = state === null ? undefined : pending.get(state)
          if (attempt === undefined || state === null)
            return yield* new AuthError({
              reason: 'callback',
              message: 'Authorization state does not match a pending attempt',
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
              reason: 'callback',
              message: 'Callback does not match the authorization redirect',
            })
          for (const key of ['state', 'code', 'client_id', 'error'])
            if (callback.searchParams.getAll(key).length > 1)
              return yield* new AuthError({
                reason: 'callback',
                message: 'Duplicate authorization callback parameter',
              })
          pending.delete(state)
          if ((yield* Clock.currentTimeMillis) >= attempt.authorization.expiresAt)
            return yield* new AuthError({
              reason: 'expired',
              message: 'Authorization attempt expired',
            })
          if (callback.searchParams.has('error'))
            return yield* new AuthError({
              reason: 'denied',
              message: 'Authorization was declined or failed',
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
              reason: 'callback',
              message: 'Authorization code and issued client ID are required',
            })
          if (attempt.returning !== undefined && clientId !== attempt.returning.clientId)
            return yield* new AuthError({
              reason: 'identity',
              message: 'Callback client ID differs from selected account',
            })
          const token = yield* Token.request(tokenEndpoint, {
            grant_type: 'authorization_code',
            client_id: clientId,
            code,
            code_verifier: Redacted.value(attempt.challenge.verifier),
            redirect_uri: attempt.authorization.redirectUri,
            resource,
          }).pipe(Effect.provideService(HttpClient.HttpClient, client))
          if (token.id_token === undefined || token.scope === undefined)
            return yield* new AuthError({
              reason: 'protocol',
              message: 'Sign-in response requires identity and granted scopes',
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
              reason: 'identity',
              message: 'Signed-in identity differs from selected account',
            })
          const scopes = scopeList(token.scope)
          yield* requireDirect(scopes)
          const now = yield* Clock.currentTimeMillis
          const credential: OAuth = {
            kind: 'oauth',
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
          const access = yield* accessToken(account)
          const response = yield* client
            .execute(
              HttpClientRequest.get(`${resource}/models`).pipe(
                HttpClientRequest.bearerToken(Redacted.value(access)),
              ),
            )
            .pipe(
              Effect.mapError(
                () =>
                  new AuthError({
                    reason: 'network',
                    message: 'Model catalog could not be loaded',
                  }),
              ),
            )
          if (response.status !== 200)
            return yield* new AuthError({
              reason: 'permission',
              message: 'Account model catalog unavailable',
              status: response.status,
            })
          const body = yield* response.json.pipe(
            Effect.mapError(
              () => new AuthError({ reason: 'protocol', message: 'Invalid model catalog' }),
            ),
          )
          const catalog = yield* Schema.decodeUnknownEffect(ModelList)(body).pipe(
            Effect.mapError(
              () => new AuthError({ reason: 'protocol', message: 'Invalid model catalog' }),
            ),
          )
          return catalog.models.filter((model) => model.visibility === 'list')
        }),
        signOut: Effect.fnUntraced(function* (account) {
          yield* store.modify(
            account,
            Effect.fnUntraced(function* (current) {
              if (
                Option.isNone(current) ||
                (current.value.kind !== 'oauth' && current.value.kind !== 'registration') ||
                current.value.provider !== 'openai' ||
                current.value.issuer !== issuer
              )
                return yield* new AuthError({
                  reason: 'missing',
                  message: 'ChatGPT registration was not found',
                })
              const credential = current.value
              if (credential.kind === 'registration') return credential
              yield* Token.revoke(`${issuer}/api/accounts/oauth/revoke`, {
                token: Redacted.value(credential.refreshToken),
                token_type_hint: 'refresh_token',
                client_id: credential.clientId,
              }).pipe(Effect.provideService(HttpClient.HttpClient, client))
              return {
                kind: 'registration',
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
        cancel: (state) =>
          Effect.sync(() => {
            pending.delete(state)
          }),
      })
    }),
  )
