# How to add account sign-in

Use this guide when your application lets a user authorize an account for inference. The host owns the sign-in UI, browser navigation and callback delivery. The provider services own protocol validation, application-owned credentials and token refresh.

## Install the authorization packages

```sh
npm install @effect-harness/auth @effect-harness/provider-openai @effect-harness/provider-anthropic effect @effect/platform-node
```

Choose the transport the application actually needs: direct ChatGPT authorization, direct Anthropic authorization, or the separately installed Claude Code CLI. A successful protocol exchange still depends on the provider granting the required inference permissions.

## Persist application-owned credentials

Use a dedicated credential directory. Provide native FileSystem, Path and Crypto at the application boundary:

```ts
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as CredentialStore from '@effect-harness/auth/CredentialStore'
import * as Layer from 'effect/Layer'

export const Credentials = CredentialStore.layerProtectedFile({
  path: './private-credentials/accounts.json',
}).pipe(Layer.provide(NodeServices.layer))
```

The file contains secrets protected by owner-only permissions, not encryption. Keep it outside committed source and served files. `modify` serializes refresh-token rotation. Stale locks are reported as busy; recover one only after establishing that its owning process is gone.

## ChatGPT: begin and complete consent

Build `ChatGpt.layer({ appName })` with Credentials, Jwt, HttpClient and Crypto. `JoseJwt.layer` supplies the JWT verifier; a native fetch client supplies HTTP. Reuse these Layer values when constructing the authorization and inference services:

```ts
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as CredentialStore from '@effect-harness/auth/CredentialStore'
import * as JoseJwt from '@effect-harness/auth/JoseJwt'
import * as ChatGpt from '@effect-harness/provider-openai/ChatGpt'
import * as Layer from 'effect/Layer'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'

const Credentials = CredentialStore.layerProtectedFile({
  path: './private-credentials/accounts.json',
})
const Jwt = JoseJwt.layer.pipe(Layer.provide(FetchHttpClient.layer))

export const Accounts = ChatGpt.layer({ appName: 'my-application' }).pipe(
  Layer.provide(Layer.mergeAll(Credentials, Jwt, FetchHttpClient.layer)),
  Layer.provide(NodeServices.layer),
)
```

Set `appName` to your actual application's name. After the user starts sign-in, call `begin` and open the returned URL through the UI. Return the full callback URL to `complete` on the **same scoped authorization service**:

```ts
import { ChatGpt } from '@effect-harness/provider-openai/ChatGpt'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'

export const signIn = Effect.fn('signIn')(function* (
  redirectUri: string,
  openBrowser: (url: string) => Effect.Effect<void>,
  receiveCallback: Effect.Effect<string>,
) {
  const accounts = yield* ChatGpt
  const authorization = yield* accounts.begin({ redirectUri })
  yield* openBrowser(Redacted.value(authorization.url))
  return yield* accounts.complete(yield* receiveCallback)
})
```

Expose the URL only to the intended browser/UI; keep it out of general logs. For a loopback flow, `provider-openai/Callback.layer` installs a listener before exposing its `authorization`. Supply a scoped HttpServer bound to IPv4 `127.0.0.1`, open the URL, then await `Callback.await`.

Completion checks state, PKCE, redirect, OIDC identity and direct-inference scope. Compute the account key with `Credential.accountKey` from the returned credential, then use it for `Catalog.layerChatGpt` or `ChatGptLanguageModel.layer`. `models(account)` discovers visible model IDs; supply their limits/capabilities to the catalogue separately.

`accessToken(account)` refreshes when required. `refresh(account, { force: true })` requests an explicit refresh. `signOut(account)` revokes the refresh token and retains dynamic client registration; `cancel(state)` ends a pending attempt.

## Anthropic: use copy-code or a browser callback

Provide Credentials, HttpClient and Crypto to `provider-anthropic/OAuth.layer`. Copy-code mode needs no local server:

```ts
import { OAuth } from '@effect-harness/provider-anthropic/OAuth'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'

export const signIn = Effect.fn('signIn')(function* (
  account: string,
  openBrowser: (url: string) => Effect.Effect<void>,
  receiveCode: Effect.Effect<string>,
) {
  const accounts = yield* OAuth
  const authorization = yield* accounts.begin({ account, method: 'copyCode' })
  yield* openBrowser(Redacted.value(authorization.url))
  return yield* accounts.complete(authorization.state, yield* receiveCode)
})
```

Choose a stable host account key. The saved grant is OpaqueOAuth; the key does not claim a verified OIDC subject. Both URL and state are Redacted, and state is a protocol secret. Keep pending authorization in the same service lifetime until completion or cancellation.

For browser mode, supply the scoped HttpServer required by `OAuth.layerCallback({ account })` at its protocol loopback address and port. Its authorization and await operations handle listener installation and completion. Use `AnthropicAccountClient.layer({ account })` for the authorized native client, then `Catalog.layer` for harness models; `AnthropicAccountLanguageModel.layer` provides a direct model Layer.

`accessToken` refreshes as needed; `refresh`, `cancel` and `signOut` are explicit operations. Sign-out removes the application-owned grant.

## Use an installed Claude Code CLI

For this path, install and sign in to the CLI independently, then install `@effect-harness/provider-claude-code` and `@effect/platform-node` in the application.

Build `Cli.layer({ policyTrust: 'trusted-installed-cli' })` with ChildProcessSpawner after auditing the installed executable and its managed policy. Supply `IntentServer.layer` with a scoped loopback HttpServer when exposing harness tools, or `IntentServer.layerDisabled` without tools. Provide those services to `ClaudeCodeLanguageModel.layer` or the provider catalogue.

The adapter uses the CLI's own authentication and does not copy its credentials into CredentialStore. Its default history policy rejects history it cannot import faithfully. Explicit transcript mode renders saved messages as input data; it does not restore a native CLI session. See [installed CLI constraints](../reference/packages.md#installed-claude-code) before selecting it for saved conversations.
