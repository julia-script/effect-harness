/** Claude's JSON token exchange and state-bound PKCE copy-code/loopback flow. */
import { Clock, Effect, Redacted, Schema } from 'effect'
import { Base64Url } from 'effect/encoding'
import { HttpClient, HttpClientRequest } from 'effect/http'
import type { OAuth } from '../../../credentials/Credential.ts'
import { Credentials } from '../../../credentials/Credentials.ts'
import { LoginError } from '../../AnthropicOAuth.ts'
import type { Authorization, BeginOptions, LoginOptions, Options } from '../../AnthropicOAuth.ts'

export const AUTH_URL = 'https://platform.claude.com'
const CLIENT_ID = '9d1c250a-e61b-44d9-88ed-5944d1962f5e'
const REDIRECT_URI = `${AUTH_URL}/oauth/code/callback`
const SCOPE =
  'org:create_api_key user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload'
const TIMEOUT_MS = 600_000

const failure = (reason: LoginError['reason'], message: string) =>
  new LoginError({ reason, message })
const Token = Schema.Struct({
  access_token: Schema.NonEmptyString,
  refresh_token: Schema.NonEmptyString,
  expires_in: Schema.Finite.check(Schema.isGreaterThan(0)),
})

const validateRedirect = (redirectUri: string) =>
  Effect.try({
    try: () => {
      const url = new URL(redirectUri)
      const loopback =
        url.protocol === 'http:' &&
        (url.hostname === 'localhost' || url.hostname === '127.0.0.1') &&
        url.pathname === '/callback' &&
        url.search === '' &&
        url.hash === '' &&
        url.username === '' &&
        url.password === ''
      if (redirectUri !== REDIRECT_URI && !loopback) throw new Error('Invalid redirect')
    },
    catch: () =>
      failure('InvalidConfiguration', 'Use the copy-code callback or a localhost /callback URL'),
  })

export const begin = Effect.fnUntraced(function* (
  options: BeginOptions = {},
): Effect.fn.Return<Authorization, LoginError> {
  const name = options.name ?? 'anthropic-oauth'
  if (name.trim() === '')
    return yield* failure('InvalidConfiguration', 'A credential name is required')
  const redirectUri = options.redirectUri ?? REDIRECT_URI
  yield* validateRedirect(redirectUri)
  const pkce = yield* Effect.tryPromise({
    try: async () => {
      const verifier = Base64Url.encode(crypto.getRandomValues(new Uint8Array(32)))
      const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier))
      return { verifier, challenge: Base64Url.encode(new Uint8Array(digest)) }
    },
    catch: () => failure('InvalidConfiguration', 'Unable to generate PKCE authorization'),
  })
  // Claude requires the verifier itself as OAuth state, unlike Codex.
  const params = new URLSearchParams({
    code: 'true',
    response_type: 'code',
    client_id: CLIENT_ID,
    redirect_uri: redirectUri,
    scope: SCOPE,
    code_challenge: pkce.challenge,
    code_challenge_method: 'S256',
    state: pkce.verifier,
  })
  return {
    name,
    url: `https://claude.ai/oauth/authorize?${params.toString()}`,
    state: pkce.verifier,
    verifier: Redacted.make(pkce.verifier),
    redirectUri,
    expires: (yield* Clock.currentTimeMillis) + TIMEOUT_MS,
  }
})

const completionCode = (input: string, authorization: Authorization) =>
  Effect.try({
    try: () => {
      const value = input.trim()
      let code: string | null | undefined
      let state: string | null | undefined
      if (/^https?:\/\//u.test(value) || value.includes('code=')) {
        let params: URLSearchParams
        if (/^https?:\/\//u.test(value)) {
          const url = new URL(value)
          const expected = new URL(authorization.redirectUri)
          if (url.origin !== expected.origin || url.pathname !== expected.pathname)
            throw new Error('Redirect')
          params = url.searchParams
        } else params = new URLSearchParams(value.replace(/^\?/u, ''))
        if (
          params.has('error') ||
          params.getAll('code').length !== 1 ||
          params.getAll('state').length !== 1
        )
          throw new Error('Callback')
        code = params.get('code')
        state = params.get('state')
      } else {
        const parts = value.split('#')
        if (parts.length !== 2) throw new Error('Callback')
        ;[code, state] = parts
      }
      if (
        code === undefined ||
        code === null ||
        code === '' ||
        state !== authorization.state ||
        state !== Redacted.value(authorization.verifier)
      )
        throw new Error('State')
      return code
    },
    catch: () => failure('InvalidState', 'The authorization callback has an invalid state or code'),
  })

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
  return {
    type: 'oauth',
    access: Redacted.make(token.access_token),
    refresh: Redacted.make(token.refresh_token),
    expires,
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
  const identity = yield* Effect.try({
    try: () => crypto.randomUUID(),
    catch: () => failure('InvalidConfiguration', 'Unable to generate credential identity'),
  })
  const store = yield* Credentials
  return yield* store.set(authorization.name, { ...tokens, extra: { identity } })
})

export const refresh = Effect.fnUntraced(function* (authUrl: string, stored: OAuth) {
  const tokens = yield* exchange(authUrl, {
    grant_type: 'refresh_token',
    refresh_token: Redacted.value(stored.refresh),
  })
  return {
    ...tokens,
    ...(stored.extra === undefined ? {} : { extra: stored.extra }),
  } satisfies OAuth
})

export const login = (options: LoginOptions) =>
  Effect.gen(function* () {
    const authorization = yield* begin(options)
    yield* options.onAuthorize(authorization.url)
    yield* complete(authorization, yield* options.completion, options)
  }).pipe(
    Effect.timeoutOrElse({
      duration: options.timeout ?? TIMEOUT_MS,
      orElse: () => Effect.fail(failure('Timeout', 'Claude authorization timed out')),
    }),
  )
