/** OAuth protocol machinery adapted from pi b45e0eb; see NOTICE. No host imports. */
import * as Clock from 'effect/Clock'
import * as Effect from 'effect/Effect'
import * as Cause from 'effect/Cause'
import * as Scope from 'effect/Scope'
import * as Option from 'effect/Option'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import type * as Credential from '../provider-auth/Credential.js'
import * as Login from '../provider-auth/Login.js'
import { AuthError } from '../provider-auth/AuthError.js'

export interface Protocol {
  readonly provider: 'openai-codex' | 'anthropic'
  readonly clientId: string
  readonly authorizeUrl: string
  readonly tokenUrl: string
  readonly redirectUri: string
  readonly scope: string
  readonly port: number
  readonly path: string
}
const invalid = () => new AuthError({ reason: 'InvalidResponse' })
const tokenSchema = Schema.Struct({
  access_token: Schema.RedactedFromValue(Schema.NonEmptyString),
  refresh_token: Schema.RedactedFromValue(Schema.NonEmptyString),
  expires_in: Schema.Number.check(Schema.isGreaterThan(0), Schema.isFinite()),
})
const claimsSchema = Schema.Struct({
  'https://api.openai.com/auth': Schema.Struct({ chatgpt_account_id: Schema.NonEmptyString }),
})
export const accountId = (accessToken: Redacted.Redacted<string>) =>
  Effect.gen(function* () {
    const claims = yield* Effect.try({
      try: () => {
        const parts = Redacted.value(accessToken).split('.')
        if (parts.length !== 3 || parts[1] === undefined) throw new Error('JWT shape')
        return JSON.parse(atob(parts[1].replace(/-/g, '+').replace(/_/g, '/'))) as unknown
      },
      catch: invalid,
    })
    const decoded = yield* Schema.decodeUnknownEffect(claimsSchema)(claims).pipe(
      Effect.mapError(invalid),
    )
    return decoded['https://api.openai.com/auth'].chatgpt_account_id
  })

export const post = (url: string, body: Record<string, string>, form = false) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient
    const request = form
      ? HttpClientRequest.post(url).pipe(HttpClientRequest.bodyUrlParams(body))
      : HttpClientRequest.post(url).pipe(HttpClientRequest.bodyJsonUnsafe(body))
    const scope = yield* Effect.serviceOption(Scope.Scope)
    const execute =
      scope._tag === 'Some'
        ? HttpClient.withScope(client)
            .execute(request)
            .pipe(Effect.provideService(Scope.Scope, scope.value))
        : client.execute(request)
    return yield* execute.pipe(
      Effect.catchCause((cause) =>
        Cause.hasInterrupts(cause)
          ? Effect.interrupt
          : Effect.fail(new AuthError({ reason: 'TransportFailure' })),
      ),
    )
  })

export const exchange = (protocol: Protocol, fields: Record<string, string>) =>
  Effect.gen(function* () {
    const response = yield* post(
      protocol.tokenUrl,
      { client_id: protocol.clientId, ...fields },
      protocol.provider === 'openai-codex',
    )
    if (response.status < 200 || response.status >= 300)
      return yield* new AuthError({
        reason:
          response.status === 429 || response.status >= 500 ? 'TransportFailure' : 'InvalidGrant',
      })
    const token = yield* response.json.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(tokenSchema)),
      Effect.mapError(invalid),
    )
    const expiresAt = (yield* Clock.currentTimeMillis) + Math.floor(token.expires_in * 1000)
    if (!Number.isSafeInteger(expiresAt)) return yield* invalid()
    return {
      type: 'oauth',
      accessToken: token.access_token,
      refreshToken: token.refresh_token,
      expiresAt,
      ...(protocol.provider === 'openai-codex'
        ? { accountId: yield* accountId(token.access_token) }
        : {}),
    } satisfies Credential.OAuth
  }).pipe(Effect.scoped)

export const refresh = (protocol: Protocol, credential: Credential.OAuth) =>
  exchange(protocol, {
    grant_type: 'refresh_token',
    refresh_token: Redacted.value(credential.refreshToken),
  })

const base64url = (bytes: Uint8Array) =>
  btoa(String.fromCharCode(...bytes))
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '')
const pkce = Effect.tryPromise({
  try: async () => {
    const verifier = base64url(crypto.getRandomValues(new Uint8Array(32)))
    const challenge = base64url(
      new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier))),
    )
    return { verifier, challenge, state: base64url(crypto.getRandomValues(new Uint8Array(32))) }
  },
  catch: () => new AuthError({ reason: 'InvalidConfiguration' }),
})

const completion = (input: string, expected: string, allowBare: boolean) =>
  Effect.try({
    try: () => {
      const value = input.trim()
      let code: string | null | undefined
      let state: string | null | undefined
      if (/^https?:\/\//.test(value)) {
        const url = new URL(value)
        code = url.searchParams.get('code')
        state = url.searchParams.get('state')
        if (url.searchParams.has('error')) throw new Error('denied')
      } else if (value.includes('#')) {
        ;[code, state] = value.split('#')
      } else if (value.includes('code=')) {
        const params = new URLSearchParams(value)
        code = params.get('code')
        state = params.get('state')
      } else if (allowBare) {
        code = value
        state = expected
      }
      if (!code || state !== expected) throw new Error('state')
      return code
    },
    catch: () => new AuthError({ reason: 'InvalidState' }),
  })

export const bounded = <A, E, R>(effect: Effect.Effect<A, E, R>, timeoutMs = 600_000) =>
  Effect.suspend(() =>
    !Number.isSafeInteger(timeoutMs) || timeoutMs <= 0
      ? Effect.fail(new AuthError({ reason: 'InvalidConfiguration' }))
      : effect.pipe(
          Effect.timeoutOrElse({
            duration: timeoutMs,
            orElse: () => Effect.fail(new AuthError({ reason: 'Timeout' })),
          }),
        ),
  )

export const browserLogin = (protocol: Protocol, options: Login.Options, useCallback: boolean) =>
  bounded(
    Effect.scoped(
      Effect.gen(function* () {
        const { verifier, challenge, state: randomState } = yield* pkce
        const state = protocol.provider === 'anthropic' ? verifier : randomState
        const host = yield* Effect.serviceOption(Login.CallbackServer)
        const callback =
          useCallback && Option.isSome(host)
            ? yield* host.value
                .open({
                  port: protocol.port,
                  path: protocol.path,
                  state,
                  fallbackPort: protocol.provider === 'anthropic',
                })
                .pipe(Effect.option)
            : Option.none()
        const redirectUri = Option.isSome(callback)
          ? callback.value.redirectUri
          : protocol.redirectUri
        const params = new URLSearchParams({
          response_type: 'code',
          client_id: protocol.clientId,
          redirect_uri: redirectUri,
          scope: protocol.scope,
          code_challenge: challenge,
          code_challenge_method: 'S256',
          state,
        })
        if (protocol.provider === 'anthropic') params.set('code', 'true')
        else {
          params.set('id_token_add_organizations', 'true')
          params.set('codex_cli_simplified_flow', 'true')
          params.set('originator', 'effect-harness')
        }
        yield* options.interaction.authorize(`${protocol.authorizeUrl}?${params.toString()}`)
        const manual = options.interaction.complete.pipe(
          Effect.flatMap((value) => completion(value, state, options.allowBareCode ?? false)),
        )
        const code = Option.isSome(callback)
          ? yield* Effect.raceFirst(
              callback.value.completion.pipe(
                Effect.flatMap((value) => completion(value, state, false)),
              ),
              manual,
            )
          : yield* manual
        const credential = yield* exchange(protocol, {
          grant_type: 'authorization_code',
          code,
          code_verifier: verifier,
          redirect_uri: redirectUri,
          ...(protocol.provider === 'anthropic' ? { state } : {}),
        })
        return credential
      }),
    ),
    options.timeoutMs,
  )
