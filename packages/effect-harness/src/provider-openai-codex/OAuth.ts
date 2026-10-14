/** ChatGPT subscription OAuth browser and device flows. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Option from 'effect/Option'
import * as HttpClient from 'effect/http/HttpClient'
import type * as Credential from '../provider-auth/Credential.js'
import * as CredentialStore from '../provider-auth/CredentialStore.js'
import type * as Login from '../provider-auth/Login.js'
import { AuthError } from '../provider-auth/AuthError.js'
import * as OAuth from '../internal/ProviderOAuth.js'

export const protocol = {
  provider: 'openai-codex',
  clientId: 'app_EMoamEEZ73f0CkXaXp7hrann',
  authorizeUrl: 'https://auth.openai.com/oauth/authorize',
  tokenUrl: 'https://auth.openai.com/oauth/token',
  redirectUri: 'http://localhost:1455/auth/callback',
  scope: 'openid profile email offline_access',
  port: 1455,
  path: '/auth/callback',
} as const satisfies OAuth.Protocol

/** Trusted host endpoint overrides, separate from inference routing and durable options. */
export interface Endpoints {
  readonly authBaseUrl?: string
}
const selected = (options?: Endpoints): OAuth.Protocol =>
  options?.authBaseUrl === undefined
    ? protocol
    : {
        ...protocol,
        authorizeUrl: `${options.authBaseUrl.replace(/\/$/, '')}/oauth/authorize`,
        tokenUrl: `${options.authBaseUrl.replace(/\/$/, '')}/oauth/token`,
      }
export const loginBrowser = (options: Login.Options & Endpoints) =>
  OAuth.browserLogin(selected(options), options, true)
export const refresh = (credential: Credential.OAuth, endpoints?: Endpoints) =>
  OAuth.refresh(selected(endpoints), credential)

const deviceInfo = Schema.Struct({
  device_auth_id: Schema.NonEmptyString,
  user_code: Schema.NonEmptyString,
  interval: Schema.Union([Schema.Finite, Schema.String]),
  expires_in: Schema.optionalKey(Schema.Finite),
})
const deviceCode = Schema.Struct({
  authorization_code: Schema.NonEmptyString,
  code_verifier: Schema.NonEmptyString,
})

/** Headless device flow: poll for an authorization code, then exchange its returned verifier. */
export const loginDevice = (options: Login.Options & Endpoints) =>
  OAuth.bounded(
    Effect.gen(function* () {
      const base = (options.authBaseUrl ?? 'https://auth.openai.com').replace(/\/$/, '')
      if (!options.profile || options.interaction.device === undefined)
        return yield* new AuthError({ reason: 'InvalidConfiguration' })
      const response = yield* OAuth.post(`${base}/api/accounts/deviceauth/usercode`, {
        client_id: protocol.clientId,
      })
      if (response.status !== 200) return yield* new AuthError({ reason: 'AccessDenied' })
      const device = yield* response.json.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(deviceInfo)),
        Effect.mapError(() => new AuthError({ reason: 'InvalidResponse' })),
      )
      let interval = Number(device.interval)
      const expiry = device.expires_in ?? 900
      if (!Number.isFinite(interval) || interval < 0 || !Number.isFinite(expiry) || expiry <= 0)
        return yield* new AuthError({ reason: 'InvalidResponse' })
      interval = Math.max(1, interval)
      const code = yield* Effect.gen(function* () {
        yield* options.interaction.device!({ url: `${base}/codex/device`, code: device.user_code })
        while (true) {
          yield* Effect.sleep(interval * 1000)
          const poll = yield* OAuth.post(`${base}/api/accounts/deviceauth/token`, {
            device_auth_id: device.device_auth_id,
            user_code: device.user_code,
          })
          if (poll.status === 200)
            return yield* poll.json.pipe(
              Effect.flatMap(Schema.decodeUnknownEffect(deviceCode)),
              Effect.mapError(() => new AuthError({ reason: 'InvalidResponse' })),
            )
          const body = yield* poll.json.pipe(
            Effect.flatMap(
              Schema.decodeUnknownEffect(
                Schema.Struct({
                  error: Schema.Union([Schema.String, Schema.Struct({ code: Schema.String })]),
                }),
              ),
            ),
            Effect.option,
          )
          if (Option.isNone(body)) {
            if (poll.status === 403 || poll.status === 404) continue
            return yield* new AuthError({ reason: 'InvalidResponse' })
          }
          const error =
            typeof body.value.error === 'string' ? body.value.error : body.value.error.code
          if (error === 'deviceauth_authorization_pending' || error === 'authorization_pending')
            continue
          if (error === 'slow_down') {
            interval += 5
            continue
          }
          return yield* new AuthError({
            reason: error === 'expired_token' ? 'ExpiredCredential' : 'AccessDenied',
          })
        }
      }).pipe(
        Effect.timeoutOrElse({
          duration: Math.floor(expiry * 1000),
          orElse: () => Effect.fail(new AuthError({ reason: 'ExpiredCredential' })),
        }),
      )
      const credential = yield* OAuth.exchange(
        selected(options),
        { provider: 'openai-codex', profile: options.profile, identity: crypto.randomUUID() },
        {
          grant_type: 'authorization_code',
          code: code.authorization_code,
          code_verifier: code.code_verifier,
          redirect_uri: `${base}/deviceauth/callback`,
        },
      )
      return yield* CredentialStore.save(credential)
    }).pipe(Effect.scoped),
    options.timeoutMs ?? 900_000,
  )

/** Captures the injected HTTP service, never one access token. */
export const makeRefresh = Effect.fnUntraced(function* (endpoints?: Endpoints) {
  const client = yield* HttpClient.HttpClient
  return (credential: Credential.OAuth) =>
    refresh(credential, endpoints).pipe(Effect.provideService(HttpClient.HttpClient, client))
})
