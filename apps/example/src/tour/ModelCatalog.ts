/** Lazy catalog composition; discovery runs offline and model construction retains its live services. */
import * as Console from 'effect/Console'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Catalog from 'effect-harness/ModelCatalog'
import { ModelCatalogError } from 'effect-harness/ModelCatalogError'
import * as ModelInfo from 'effect-harness/ModelInfo'
import * as Provider from 'effect-harness/Provider'
import * as CodexModel from 'effect-harness/provider-openai-codex/Model'
import * as Codex from 'effect-harness/provider-openai-codex/CodexLanguageModel'

class Discovery extends Context.Service<Discovery, Provider.ModelSource>()(
  'example/ModelCatalog/Discovery',
) {}

// Illustrative metadata, not a declaration about a currently available remote model.
const metadata: ModelInfo.ModelInfo = {
  id: 'example-codex-model',
  name: 'Example Codex model',
  capabilities: { tools: true, reasoning: true, images: false, structuredOutput: false },
  reasoning: { effort: { values: ['low', 'high'], default: 'low' } },
}

export const provider = Provider.make({
  id: 'personal-codex',
  models: {
    list: (input) => Effect.flatMap(Discovery, (source) => source.list(input)),
    get: (modelId) => Effect.flatMap(Discovery, (source) => source.get(modelId)),
  },
  create: Effect.fnUntraced(function* ({ info, options }) {
    if (options.reasoning?.thinking !== undefined)
      return yield* new ModelCatalogError({
        reason: 'unsupportedOptions',
        modelId: info.id,
        message: 'Codex uses effort rather than thinking budgets',
      })
    // The installed adapter's schema is the final authority on supported wire options.
    const config = yield* Schema.decodeUnknownEffect(Codex.Settings)(
      options.reasoning?.effort === undefined ? {} : { reasoningEffort: options.reasoning.effort },
    )
    const supported = yield* Effect.forEach(info.reasoning?.effort?.values ?? [], (effort) =>
      Schema.decodeUnknownEffect(Codex.Settings)({ reasoningEffort: effort }),
    )
    return yield* CodexModel.make({
      model: info.id,
      definition: yield* ModelInfo.toDefinition(info, 'openai-codex'),
      binding: { provider: 'openai-codex', profile: 'personal', identity: 'login' },
      config,
      reasoningEfforts: supported.flatMap((settings) =>
        settings.reasoningEffort === undefined ? [] : [settings.reasoningEffort],
      ),
    })
  }),
})

export const catalog = Catalog.make({ providers: [provider] })

// Creating this Effect does no IO. Running it additionally requires the HTTP client
// and CredentialStore used by CodexModel.make; its configuration codec pins the effort.
export const selected = Catalog.resolve(catalog, {
  provider: 'personal-codex',
  modelId: metadata.id,
  options: { reasoning: { effort: 'high' } },
})

const program = Effect.gen(function* () {
  const page = yield* Catalog.list(catalog, { provider: 'personal-codex' })
  yield* Console.log(page.items)
  const info = yield* Catalog.get(catalog, { provider: 'personal-codex', modelId: metadata.id })
  yield* Console.log(info.reasoning?.effort)
})

await Effect.runPromise(
  program.pipe(
    Effect.provideService(Discovery, {
      list: () => Effect.succeed({ items: [{ id: metadata.id, name: metadata.name }] }),
      get: () => Effect.succeed(metadata),
    }),
  ),
)
