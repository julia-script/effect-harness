/** Concrete provider catalogs; only the selected provider's discovery runs. */
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Catalog from 'effect-harness/ModelCatalog'
import * as Codex from 'effect-harness/provider-openai-codex/Provider'
import * as Claude from 'effect-harness/provider-anthropic/Provider'

export const catalog = Catalog.make({
  providers: [
    Codex.make({
      id: 'personal-codex',
      binding: { provider: 'openai-codex', profile: 'personal', identity: 'login' },
      // Explicit compatibility version for the Codex catalog endpoint.
      clientVersion: '0.99.0',
    }),
    Claude.make({
      id: 'personal-claude',
      binding: { provider: 'anthropic', profile: 'personal', identity: 'login' },
    }),
  ],
})

// Creating this Effect does no IO. Running it needs HttpClient and CredentialStore;
// the resulting model's durable configuration pins the selected effort.
export const selected = Catalog.resolve(catalog, {
  provider: 'personal-claude',
  modelId: 'claude-opus-5-5',
  options: { reasoning: { effort: 'high' } },
})

// Claude OAuth metadata is bundled, so this program runs offline without credentials.
// Selecting personal-codex instead retains its live discovery service requirements.
await Effect.runPromise(
  Effect.gen(function* () {
    const page = yield* Catalog.list(catalog, { provider: 'personal-claude' })
    yield* Console.log(page.items)
    const info = yield* Catalog.get(catalog, {
      provider: 'personal-claude',
      modelId: 'claude-opus-5-5',
    })
    yield* Console.log(info.reasoning)
  }),
)
