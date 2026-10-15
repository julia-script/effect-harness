/** Run adapter contracts through the real manager with an already provisioned credential reference. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Provider from '../../src/Provider.js'
import * as Providers from '../../src/Providers.js'
import * as ProviderStore from '../../src/ProviderStore.js'
import type * as Instance from '../../src/ProviderInstance.js'
import type * as Catalog from '../../src/ModelCatalog.js'
import type * as ModelInfo from '../../src/ModelInfo.js'
import type * as Credential from '../../src/provider-auth/Credential.js'
export const managed = Effect.fnUntraced(function* <P extends Provider.Any>(
  definition: P,
  config: P['config']['Type'],
  credential?: Credential.Binding,
) {
  const store = yield* ProviderStore.makeMemory
  const id = crypto.randomUUID()
  yield* store.modify(id, () =>
    Effect.succeed([
      undefined,
      {
        id,
        provider: 'test',
        version: definition.version,
        revision: 'initial',
        label: 'Test',
        config: Schema.encodeSync(Schema.toCodecJson(definition.config))(config),
        ...(credential === undefined ? {} : { credential }),
      },
    ] as const),
  )
  const manager = yield* Providers.make({ definitions: { test: definition } }).pipe(
    Effect.provideService(ProviderStore.ProviderStore, store),
  )
  const instanceId = id as Instance.Id<'test'>
  return {
    list: (input: Catalog.ListOptions = {}) => manager.models.list({ ...input, instanceId }),
    get: (modelId: string) => manager.models.get({ instanceId, modelId }),
    resolve: (input: { readonly modelId: string; readonly options?: ModelInfo.Options }) =>
      manager.models.resolve({ ...input, instanceId }),
  }
})
