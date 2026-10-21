/** Claude's JSON token exchange and state-bound PKCE copy-code/loopback flow. */
import { Clock, Duration, Effect, Predicate, Redacted, Schema } from 'effect'
import { Base64Url } from 'effect/encoding'
import { HttpClient, HttpClientRequest } from 'effect/http'
import { Identity } from '../../../credentials/Credential.ts'
import type { OAuth } from '../../../credentials/Credential.ts'
import { CredentialStore } from '../../../credentials/CredentialStore.ts'
import { LoginError } from '../../AnthropicOAuth.ts'
import type { Authorization, BeginOptions, LoginOptions, Options } from '../../AnthropicOAuth.ts'

export const AUTH_URL = 'https://platform.claude.com'
const CLIENT_ID = '9d1c250a-e61b-44d9-88ed-5944d1962f5e'
const REDIRECT_URI = `${AUTH_URL}/oauth/code/callback`
const SCOPE =
  'org:create_api_key user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload'
const TIMEOUT = Duration.minutes(10)

const failure = (reason: LoginError['reason'], message: string) =>
  new LoginError({ reason, message })
const Token = Schema.Struct({
  access_token: Schema.NonEmptyString,
  refresh_token: Schema.NonEmptyString,
  expires_in: Schema.Finite.check(Schema.isGreaterThan(0)),
  // Claude's successful token response can include account.uuid and email_address.
  // No profile request or additional scopes are needed; omission means unknown identity.
  account: Schema.optionalKey(
    Schema.NullOr(
      Schema.Struct({
        uuid: Schema.optionalKey(Schema.NullOr(Schema.String)),
        email_address: Schema.optionalKey(Schema.NullOr(Schema.String)),
      }),
    ),
  ),
})

const isLoopback = (url: URL) =>
  url.protocol === 'http:' &&
  (url.hostname === 'localhost' || url.hostname === '127.0.0.1') &&
  url.pathname === '/callback' &&
  url.search === '' &&
  url.hash === '' &&
  url.username === '' &&
  url.password === ''

const validateRedirect = (redirectUri: string) => {
  const url = URL.parse(redirectUri)
  return redirectUri === REDIRECT_URI || (url !== null && isLoopback(url))
    ? Effect.void
    : Effect.fail(
        failure('InvalidConfiguration', 'Use the copy-code callback or a localhost /callback URL'),
      )
}

export const begin = Effect.fnUntraced(function* (
  options: BeginOptions = {},
): Effect.fn.Return<Authorization, LoginError> {
  const name = options.name ?? 'anthropic-oauth'
  if (name.trim() === '')
    return yield* failure('InvalidConfiguration', 'A credential name is required')
  const redirectUri = options.redirectUri ?? REDIRECT_URI
  yield* validateRedirect(redirectUri)
  const verifier = yield* Effect.sync(() =>
    Base64Url.encode(crypto.getRandomValues(new Uint8Array(32))),
  )
  const digest = yield* Effect.tryPromise({
    try: () => crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier)),
    catch: () => failure('InvalidConfiguration', 'Unable to generate PKCE authorization'),
  })
  // Claude requires the verifier itself as OAuth state, unlike Codex.
  const params = new URLSearchParams({
    code: 'true',
    response_type: 'code',
    client_id: CLIENT_ID,
    redirect_uri: redirectUri,
    scope: SCOPE,
    code_challenge: Base64Url.encode(new Uint8Array(digest)),
    code_challenge_method: 'S256',
    state: verifier,
  })
  return {
    name,
    url: `https://claude.ai/oauth/authorize?${params.toString()}`,
    state: verifier,
    verifier: Redacted.make(verifier),
    redirectUri,
    expires: (yield* Clock.currentTimeMillis) + Duration.toMillis(TIMEOUT),
  }
})

interface Callback {
  readonly code: string | null | undefined
  readonly state: string | null | undefined
}

/** The one `code` and `state` of a callback query; `undefined` for an error or a repeated key. */
const fromQuery = (params: URLSearchParams): Callback | undefined =>
  params.has('error') || params.getAll('code').length !== 1 || params.getAll('state').length !== 1
    ? undefined
    : { code: params.get('code'), state: params.get('state') }

/** What the person pasted: the callback URL, its query string, or `code#state`. */
const callback = (value: string, redirectUri: string): Callback | undefined => {
  if (/^https?:\/\//u.test(value)) {
    const url = URL.parse(value)
    const expected = URL.parse(redirectUri)
    return url === null ||
      expected === null ||
      url.origin !== expected.origin ||
      url.pathname !== expected.pathname
      ? undefined
      : fromQuery(url.searchParams)
  }
  if (value.includes('code=')) return fromQuery(new URLSearchParams(value.replace(/^\?/u, '')))
  const parts = value.split('#')
  return parts.length === 2 ? { code: parts[0], state: parts[1] } : undefined
}

const completionCode = (input: string, authorization: Authorization) => {
  const found = callback(input.trim(), authorization.redirectUri)
  return found !== undefined &&
    Predicate.isString(found.code) &&
    found.code !== '' &&
    found.state === authorization.state &&
    found.state === Redacted.value(authorization.verifier)
    ? Effect.succeed(found.code)
    : Effect.fail(
        failure('InvalidState', 'The authorization callback has an invalid state or code'),
      )
}

const exchange = Effect.fnUntraced(function* (authUrl: string, fields: Record<string, string>) {
  const client = yield* HttpClient.HttpClient
  const response = yield* client
    .execute(
      HttpClientRequest.post(`${authUrl.replace(/\/$/u, '')}/v1/oauth/token`).pipe(
        HttpClientRequest.bodyJsonUnsafe({ client_id: CLIENT_ID, ...fields }),
      ),
    )
    .pipe(
      Effect.mapError(() =>
        failure('TransportFailure', 'Claude token exchange could not reach the server'),
      ),
    )
  if (response.status < 200 || response.status >= 300)
    return yield* failure(
      response.status === 429 || response.status >= 500 ? 'TransportFailure' : 'InvalidGrant',
      `Claude token exchange failed (${response.status})`,
    )
  const token = yield* response.json.pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Token)),
    Effect.mapError(() => failure('InvalidResponse', 'Claude returned an invalid token response')),
  )
  const expires = (yield* Clock.currentTimeMillis) + Math.floor(token.expires_in * 1000)
  if (!Number.isSafeInteger(expires))
    return yield* failure('InvalidResponse', 'Claude returned an invalid token expiry')
  const account = token.account
  const identity =
    Predicate.isString(account?.uuid) && account.uuid !== ''
      ? Identity.make({
          issuer: AUTH_URL,
          subject: account.uuid,
          ...(Predicate.isString(account.email_address) ? { email: account.email_address } : {}),
        })
      : undefined
  return {
    _tag: 'OAuth',
    access: Redacted.make(token.access_token),
    refresh: Redacted.make(token.refresh_token),
    expires,
    ...(identity === undefined ? {} : { identity }),
  } satisfies OAuth
})

export const complete = Effect.fnUntraced(function* (
  authorization: Authorization,
  input: string,
  options: Pick<Options, 'authUrl'> = {},
) {
  if ((yield* Clock.currentTimeMillis) >= authorization.expires)
    return yield* failure('Timeout', 'Claude authorization expired')
  const code = yield* completionCode(input, authorization)
  const tokens = yield* exchange(options.authUrl ?? AUTH_URL, {
    grant_type: 'authorization_code',
    code,
    code_verifier: Redacted.value(authorization.verifier),
    state: authorization.state,
    redirect_uri: authorization.redirectUri,
  })
  const revision = yield* Effect.sync(() => crypto.randomUUID())
  const store = yield* CredentialStore
  return yield* store.set(authorization.name, { ...tokens, revision })
})

export const refresh = Effect.fnUntraced(function* (authUrl: string, stored: OAuth) {
  const tokens = yield* exchange(authUrl, {
    grant_type: 'refresh_token',
    refresh_token: Redacted.value(stored.refresh),
  })
  if (
    stored.identity !== undefined &&
    tokens.identity !== undefined &&
    (stored.identity.issuer !== tokens.identity.issuer ||
      stored.identity.subject !== tokens.identity.subject)
  )
    return yield* failure('InvalidResponse', 'Claude token refresh returned a different account')
  return {
    ...stored,
    access: tokens.access,
    refresh: tokens.refresh,
    expires: tokens.expires,
  } satisfies OAuth
})

export const login = Effect.fnUntraced(
  function* (options: LoginOptions) {
    const authorization = yield* begin(options)
    yield* options.onAuthorize(authorization.url)
    yield* complete(authorization, yield* options.completion, options)
  },
  (effect, options) =>
    Effect.timeoutOrElse(effect, {
      duration: options.timeout ?? TIMEOUT,
      orElse: () => Effect.fail(failure('Timeout', 'Claude authorization timed out')),
    }),
)
