import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import type * as HttpClient from 'effect/http/HttpClient'
import * as Catalog from 'effect-harness/ModelCatalog'
import type * as ModelInfo from 'effect-harness/ModelInfo'
import * as Provider from 'effect-harness/Provider'
import type { CredentialStore } from 'effect-harness/provider-auth/CredentialStore'
import * as CodexCatalog from 'effect-harness/provider-openai-codex/Catalog'
import * as Codex from 'effect-harness/provider-openai-codex/Provider'
import * as Claude from 'effect-harness/provider-anthropic/Provider'
import * as ClaudeCatalog from 'effect-harness/provider-anthropic/Catalog'
import type * as Redacted from 'effect/Redacted'

class Listing extends Context.Service<Listing, string>()('provider-catalog/Listing') {}
class Details extends Context.Service<Details, string>()('provider-catalog/Details') {}
declare const list: Effect.Effect<Provider.Page, 'list-error', Listing>
declare const get: Effect.Effect<ModelInfo.ModelInfo, 'get-error', Details>
declare const apiKey: Redacted.Redacted<string>
const codexOptions = {
  id: 'codex',
  binding: { provider: 'openai-codex', profile: 'personal', identity: 'login' },
  clientVersion: '0.99.0',
} as const
const claudeOptions = {
  id: 'claude',
  binding: { provider: 'anthropic', profile: 'personal', identity: 'login' },
} as const
const codex = Codex.make(codexOptions)
const claude = Claude.make(claudeOptions)
declare const cachedSource: Effect.Success<ReturnType<typeof CodexCatalog.cached>>
const catalog = Catalog.make({ providers: [codex, claude] })

test('concrete discovery retains only the selected provider services', () => {
  const codexList = Catalog.list(catalog, { provider: 'codex' })
  const codexGet = Catalog.get(catalog, { provider: 'codex', modelId: 'model' })
  const claudeList = Catalog.list(catalog, { provider: 'claude' })
  const claudeGet = Catalog.get(catalog, { provider: 'claude', modelId: 'model' })
  const claudeResolve = Catalog.resolve(catalog, { provider: 'claude', modelId: 'model' })
  const codexResolve = Catalog.resolve(catalog, { provider: 'codex', modelId: 'model' })
  expect<Effect.Services<typeof codexList>>().type.toBe<HttpClient.HttpClient | CredentialStore>()
  expect<Effect.Services<typeof codexGet>>().type.toBe<HttpClient.HttpClient | CredentialStore>()
  expect<Effect.Services<typeof claudeList>>().type.toBe<never>()
  expect<Effect.Services<typeof claudeGet>>().type.toBe<never>()
  expect<Effect.Services<typeof claudeResolve>>().type.toBe<
    HttpClient.HttpClient | CredentialStore
  >()
  expect<Effect.Services<typeof codexResolve>>().type.toBe<
    HttpClient.HttpClient | CredentialStore
  >()
  const apiKeyGet = ClaudeCatalog.apiKey({ apiKey }).get('model')
  expect<Effect.Services<typeof apiKeyGet>>().type.toBe<HttpClient.HttpClient>()
})

test('custom sources keep listing and detail services separate from native construction', () => {
  const source = { list: () => list, get: () => get }
  const customCodex = Codex.makeWithSource(codexOptions, source)
  const customClaude = Claude.makeWithSource(claudeOptions, source)
  const codexList = Provider.list(customCodex)
  const codexGet = Provider.get(customCodex, 'model')
  const codexResolve = Provider.resolve(customCodex, { modelId: 'model' })
  const claudeList = Provider.list(customClaude)
  const claudeGet = Provider.get(customClaude, 'model')
  const claudeResolve = Provider.resolve(customClaude, { modelId: 'model' })
  expect<Effect.Services<typeof codexList>>().type.toBe<Listing>()
  expect<Effect.Services<typeof claudeList>>().type.toBe<Listing>()
  expect<Effect.Services<typeof codexGet>>().type.toBe<Details>()
  expect<Effect.Services<typeof claudeGet>>().type.toBe<Details>()
  expect<Effect.Services<typeof codexResolve>>().type.toBe<
    Details | HttpClient.HttpClient | CredentialStore
  >()
  expect<Effect.Services<typeof claudeResolve>>().type.toBe<
    Details | HttpClient.HttpClient | CredentialStore
  >()
  expect<'get-error'>().type.toBeAssignableTo<Effect.Error<typeof codexResolve>>()
  expect<'get-error'>().type.toBeAssignableTo<Effect.Error<typeof claudeResolve>>()
})

test('Codex bulk snapshot binds discovery services without binding model construction', () => {
  const cached = CodexCatalog.cached(codexOptions, { timeToLive: '5 minutes' })
  expect<Effect.Services<typeof cached>>().type.toBe<HttpClient.HttpClient | CredentialStore>()
  declareSource(cachedSource)
})

function declareSource(source: Effect.Success<ReturnType<typeof CodexCatalog.cached>>) {
  const provider = Codex.makeWithSource(codexOptions, source)
  const list = Provider.list(provider)
  const get = Provider.get(provider, 'model')
  const resolve = Provider.resolve(provider, { modelId: 'model' })
  expect<Effect.Services<typeof list>>().type.toBe<never>()
  expect<Effect.Services<typeof get>>().type.toBe<never>()
  expect<Effect.Services<typeof resolve>>().type.toBe<HttpClient.HttpClient | CredentialStore>()
}
