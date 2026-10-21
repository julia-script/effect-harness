/**
 * ChatGPT sign-in for Codex, ported from pi-ai's `auth/oauth/openai-codex.ts`: the device code
 * login, token refresh, and the account id the access token carries.
 */
import { Clock, Duration, Effect, Option, Redacted, Result, Schema } from 'effect'
import { Base64Url } from 'effect/encoding'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/http'
import { Identity } from '../../../credentials/Credential.ts'
import type { OAuth } from '../../../credentials/Credential.ts'
import { CredentialStore } from '../../../credentials/CredentialStore.ts'
import { LoginError } from '../../OpenAiCodex.ts'
import type { DeviceCode } from '../../OpenAiCodex.ts'

export const AUTH_URL = 'https://auth.openai.com'
const CLIENT_ID = 'app_EMoamEEZ73f0CkXaXp7hrann'
const DEVICE_TIMEOUT = Duration.minutes(15)
const MIN_INTERVAL_MS = 1000
const SLOW_DOWN_MS = 5000
const ACCOUNT_CLAIM = 'https://api.openai.com/auth'

const Token = Schema.Struct({
  access_token: Schema.String,
  refresh_token: Schema.String,
  expires_in: Schema.Finite,
})

const UserCode = Schema.Struct({
  device_auth_id: Schema.String,
  user_code: Schema.String,
  interval: Schema.Union([Schema.Finite, Schema.FiniteFromString]),
})

const Authorization = Schema.Struct({
  authorization_code: Schema.String,
  code_verifier: Schema.String,
})

const PollError = Schema.Struct({
  error: Schema.optionalKey(
    Schema.Union([Schema.String, Schema.Struct({ code: Schema.optionalKey(Schema.String) })]),
  ),
})

const Claims = Schema.fromJsonString(
  Schema.Struct({
    [ACCOUNT_CLAIM]: Schema.optionalKey(
      Schema.Struct({ chatgpt_account_id: Schema.optionalKey(Schema.String) }),
    ),
  }),
)
const decodeClaims = Schema.decodeUnknownOption(Claims)

/** The ChatGPT account id in an access token's claims. */
export const accountId = (access: string): string | undefined => {
  const payload = access.split('.')[1]
  if (payload === undefined) return undefined
  const json = Base64Url.decodeString(payload)
  if (Result.isFailure(json)) return undefined
  const claims = decodeClaims(json.success)
  const id = Option.isSome(claims) ? claims.value[ACCOUNT_CLAIM]?.chatgpt_account_id : undefined
  return id === undefined || id === '' ? undefined : id
}

const fail = (message: string) => (cause: unknown) => new LoginError({ message, cause })

/** Tokens as a stored credential, with the account id they carry. */
const credential = (token: typeof Token.Type) =>
  Effect.gen(function* () {
    const id = accountId(token.access_token)
    if (id === undefined) return yield* new LoginError({ message: 'The token has no account id' })
    const now = yield* Clock.currentTimeMillis
    const stored: OAuth = {
      _tag: 'OAuth',
      access: Redacted.make(token.access_token),
      refresh: Redacted.make(token.refresh_token),
      expires: now + token.expires_in * 1000,
      // These claims come from the successful token exchange, not a caller-supplied JWT.
      identity: Identity.make({ issuer: AUTH_URL, subject: id }),
      extra: { accountId: id },
    }
    return stored
  })

const tokens = (authUrl: string, params: Record<string, string>, operation: string) =>
  Effect.flatMap(HttpClient.HttpClient, (client) =>
    client
      .execute(
        HttpClientRequest.post(`${authUrl}/oauth/token`).pipe(
          HttpClientRequest.bodyUrlParams({ client_id: CLIENT_ID, ...params }),
        ),
      )
      .pipe(
        Effect.flatMap(HttpClientResponse.filterStatusOk),
        Effect.flatMap(HttpClientResponse.schemaBodyJson(Token)),
        Effect.mapError(fail(`Codex token ${operation} failed`)),
        Effect.flatMap(credential),
      ),
  )

/** Refresh tokens without changing the bound account or sign-in generation. */
export const refresh = Effect.fnUntraced(function* (authUrl: string, stored: OAuth) {
  const next = yield* tokens(
    authUrl,
    { grant_type: 'refresh_token', refresh_token: Redacted.value(stored.refresh) },
    'refresh',
  )
  const previousId =
    typeof stored.extra?.accountId === 'string'
      ? stored.extra.accountId
      : accountId(Redacted.value(stored.access))
  if (
    (stored.identity !== undefined &&
      (stored.identity.issuer !== next.identity?.issuer ||
        stored.identity.subject !== next.identity?.subject)) ||
    (previousId !== undefined && previousId !== next.extra?.accountId)
  )
    return yield* new LoginError({ message: 'Codex token refresh returned a different account' })
  return {
    ...stored,
    access: next.access,
    refresh: next.refresh,
    expires: next.expires,
    extra: { ...stored.extra, ...next.extra },
  } satisfies OAuth
})

const postJson = (url: string, body: Record<string, string>) =>
  Effect.flatMap(HttpClient.HttpClient, (client) =>
    client.execute(HttpClientRequest.post(url).pipe(HttpClientRequest.bodyJsonUnsafe(body))),
  )

type Poll =
  | { readonly status: 'complete'; readonly value: typeof Authorization.Type }
  | { readonly status: 'pending' | 'slow_down' }

const pollOnce = (authUrl: string, device: typeof UserCode.Type) =>
  Effect.gen(function* () {
    const response = yield* postJson(`${authUrl}/api/accounts/deviceauth/token`, {
      device_auth_id: device.device_auth_id,
      user_code: device.user_code,
    })
    if (response.status === 200) {
      const value = yield* HttpClientResponse.schemaBodyJson(Authorization)(response)
      return { status: 'complete', value } satisfies Poll
    }
    if (response.status === 403 || response.status === 404)
      return { status: 'pending' } satisfies Poll
    const body = yield* Effect.option(HttpClientResponse.schemaBodyJson(PollError)(response))
    const error = Option.isSome(body) ? body.value.error : undefined
    const code = typeof error === 'object' ? error.code : error
    if (code === 'deviceauth_authorization_pending') return { status: 'pending' } satisfies Poll
    if (code === 'slow_down') return { status: 'slow_down' } satisfies Poll
    return yield* new LoginError({ message: `Codex device login failed (${response.status})` })
  }).pipe(
    Effect.mapError((cause) =>
      cause instanceof LoginError ? cause : fail('Codex device login failed')(cause),
    ),
  )

const poll = (
  authUrl: string,
  device: typeof UserCode.Type,
  intervalMs: number,
): Effect.Effect<typeof Authorization.Type, LoginError, HttpClient.HttpClient> =>
  Effect.flatMap(pollOnce(authUrl, device), (result) => {
    if (result.status === 'complete') return Effect.succeed(result.value)
    const next = result.status === 'slow_down' ? intervalMs + SLOW_DOWN_MS : intervalMs
    return Effect.flatMap(Effect.sleep(Duration.millis(next)), () => poll(authUrl, device, next))
  })

/** Sign in with a device code: `onCode` shows the code, and the tokens come back once approved. */
export const login = (authUrl: string, onCode: (code: DeviceCode) => Effect.Effect<void>) =>
  Effect.gen(function* () {
    const response = yield* postJson(`${authUrl}/api/accounts/deviceauth/usercode`, {
      client_id: CLIENT_ID,
    }).pipe(
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      Effect.flatMap(HttpClientResponse.schemaBodyJson(UserCode)),
      Effect.mapError(fail('Codex device code request failed')),
    )
    yield* onCode({ url: `${authUrl}/codex/device`, code: response.user_code })
    const intervalMs = Math.max(MIN_INTERVAL_MS, response.interval * 1000)
    const authorization = yield* Effect.sleep(Duration.millis(intervalMs)).pipe(
      Effect.andThen(poll(authUrl, response, intervalMs)),
      Effect.timeoutOrElse({
        duration: DEVICE_TIMEOUT,
        orElse: () => Effect.fail(new LoginError({ message: 'Codex device login timed out' })),
      }),
    )
    const stored = yield* tokens(
      authUrl,
      {
        grant_type: 'authorization_code',
        code: authorization.authorization_code,
        code_verifier: authorization.code_verifier,
        redirect_uri: `${authUrl}/deviceauth/callback`,
      },
      'exchange',
    )
    const revision = yield* Effect.try({
      try: () => crypto.randomUUID(),
      catch: fail('Unable to generate credential revision'),
    })
    return { ...stored, revision } satisfies OAuth
  })

/** `login`, storing the tokens under `name`. */
export const store = (
  name: string,
  authUrl: string,
  onCode: (code: DeviceCode) => Effect.Effect<void>,
) =>
  Effect.flatMap(login(authUrl, onCode), (stored) =>
    Effect.flatMap(CredentialStore, (credentials) => credentials.set(name, stored)),
  )
