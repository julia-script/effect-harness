/** Concrete provider catalogs; only the selected provider's discovery runs. */
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Catalog from 'effect-harness/ModelCatalog'
import * as Provider from 'effect-harness/Provider'
import * as Codex from 'effect-harness/provider-openai-codex/Provider'
import * as Claude from 'effect-harness/provider-anthropic/Provider'

const claudeOptions = {
  id: 'personal-claude',
  binding: { provider: 'anthropic', profile: 'personal', identity: 'login' },
} as const

export const catalog = Catalog.make({
  providers: [
    Codex.make({
      id: 'personal-codex',
      binding: { provider: 'openai-codex', profile: 'personal', identity: 'login' },
      clientVersion: '0.99.0',
    }),
    Claude.make(claudeOptions),
  ],
})

// Both live catalogs need HttpClient and CredentialStore when executed.
// Creating this Effect performs no network requests or credential lookup.
export const selected = Catalog.resolve(catalog, {
  provider: 'personal-claude',
  modelId: 'claude-opus-5-5',
  options: { reasoning: { effort: 'high' } },
})

// An application can also supply its own metadata. This illustrative source keeps
// the executable example offline; it does not describe an actual remote model.
const offline = Claude.makeWithSource(
  claudeOptions,
  Provider.fromModels([
    {
      id: 'example-model',
      name: 'Example model',
      capabilities: { tools: true, reasoning: true, images: false, structuredOutput: false },
      reasoning: { effort: { values: ['low', 'high'] }, thinking: { modes: ['adaptive'] } },
    },
  ]),
)

await Effect.runPromise(
  Effect.gen(function* () {
    yield* Console.log((yield* Provider.list(offline)).items)
    yield* Console.log((yield* Provider.get(offline, 'example-model')).reasoning)
  }),
)
