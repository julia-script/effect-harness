import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import type * as HttpClient from 'effect/http/HttpClient'
import type * as Catalog from 'effect-harness/ModelCatalog'
import type * as ModelInfo from 'effect-harness/ModelInfo'
import type * as Providers from 'effect-harness/Providers'
import type * as Instance from 'effect-harness/ProviderInstance'
import * as Codex from 'effect-harness/provider-openai-codex/Provider'
import * as Claude from 'effect-harness/provider-anthropic/Provider'
import * as Compatible from 'effect-harness/provider-openai-compatible/Provider'
import type * as OpenRouter from 'effect-harness/provider-openrouter/Provider'
import type * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as CodexAuth from 'effect-harness/provider-openai-codex/OAuth'
import type * as Login from 'effect-harness/provider-auth/Login'

class Listing extends Context.Service<Listing, string>()('provider-catalog/Listing') {}
class Details extends Context.Service<Details, string>()('provider-catalog/Details') {}
declare const list: Effect.Effect<Catalog.Page, 'list-error', Listing>
declare const get: Effect.Effect<ModelInfo.ModelInfo, 'get-error', Details>
const source = { list: () => list, get: () => get }
const definitions = {
  codex: Codex.definition,
  claude: Claude.definition,
  custom: Codex.define(source),
}
declare const manager: Providers.Service<typeof definitions>
declare const codexId: Instance.Id<'codex'>
declare const customId: Instance.Id<'custom'>
declare const interaction: Login.Interaction

test('manager owns credentials while each operation retains its HTTP or custom services', () => {
  const listing = manager.models.list({ instanceId: codexId })
  const selected = manager.models.resolve({ instanceId: codexId, modelId: 'model' })
  expect<Effect.Services<typeof listing>>().type.toBe<HttpClient.HttpClient>()
  expect<Effect.Services<typeof selected>>().type.toBe<HttpClient.HttpClient>()
  const customList = manager.models.list({ instanceId: customId })
  const customGet = manager.models.get({ instanceId: customId, modelId: 'model' })
  const customResolve = manager.models.resolve({ instanceId: customId, modelId: 'model' })
  expect<Effect.Services<typeof customList>>().type.toBe<Listing>()
  expect<Effect.Services<typeof customGet>>().type.toBe<Details>()
  expect<Effect.Services<typeof customResolve>>().type.toBe<Details | HttpClient.HttpClient>()
  expect<'get-error'>().type.toBeAssignableTo<Effect.Error<typeof customResolve>>()
  const login = CodexAuth.loginBrowser({ interaction })
  expect<Effect.Services<typeof login>>().type.toBe<HttpClient.HttpClient>()
})

test('gateway factory infers custom config while reusing typed model settings', () => {
  const gateway = Compatible.define({
    version: 1,
    config: Schema.Struct({ host: Schema.String, region: Schema.String }),
    connection: (config) => ({ baseUrl: config.host }),
    models: source,
  })
  type Manager = Providers.Service<{ gateway: typeof gateway }>
  expect<Parameters<Manager['add']>[0]['config']>().type.toBe<{
    readonly host: string
    readonly region: string
  }>()
})

declare const router: Providers.Service<{ openrouter: typeof OpenRouter.definition }>
declare const routerKey: Redacted.Redacted<string>
declare const routerId: Instance.Id<'openrouter'>
test('OpenRouter requires no endpoint or metadata config and retains typed API-key secrets', () => {
  expect(
    router.add({
      provider: 'openrouter',
      label: 'OpenRouter',
      config: {},
      secrets: { apiKey: routerKey },
    }),
  ).type.toBeAssignableTo<Effect.Effect<unknown, unknown>>()
  expect(router.update(routerId, { secrets: { accessToken: routerKey } })).type.toRaiseError()
  const listing = router.models.list({ instanceId: routerId })
  expect<Effect.Services<typeof listing>>().type.toBe<HttpClient.HttpClient>()
})
