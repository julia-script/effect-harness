/** Explicit Claude subscription OAuth; no automatic credential discovery or API-key fallback. */
import * as Effect from 'effect/Effect'
import * as HttpClient from 'effect/http/HttpClient'
import type * as Credential from '../provider-auth/Credential.js'
import type * as Login from '../provider-auth/Login.js'
import * as OAuth from '../internal/ProviderOAuth.js'

export const protocol = {
  provider: 'anthropic',
  clientId: '9d1c250a-e61b-44d9-88ed-5944d1962f5e',
  authorizeUrl: 'https://claude.ai/oauth/authorize',
  tokenUrl: 'https://platform.claude.com/v1/oauth/token',
  redirectUri: 'http://localhost:53692/callback',
  port: 53692,
  path: '/callback',
  scope:
    'org:create_api_key user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload',
} as const satisfies OAuth.Protocol

export interface Endpoints {
  readonly authorizeUrl?: string
  readonly tokenUrl?: string
}
const selected = (options?: Endpoints): OAuth.Protocol => ({ ...protocol, ...options })
export const loginBrowser = (options: Login.Options & Endpoints) =>
  OAuth.browserLogin(selected(options), options, true)
export const loginCopyCode = (options: Login.Options & Endpoints) =>
  OAuth.browserLogin(
    { ...selected(options), redirectUri: 'https://platform.claude.com/oauth/code/callback' },
    options,
    false,
  )
export const refresh = (credential: Credential.OAuth, endpoints?: Endpoints) =>
  OAuth.refresh(selected(endpoints), credential)
export const makeRefresh = Effect.fnUntraced(function* (endpoints?: Endpoints) {
  const client = yield* HttpClient.HttpClient
  return (credential: Credential.OAuth) =>
    refresh(credential, endpoints).pipe(Effect.provideService(HttpClient.HttpClient, client))
})
