/** Declare integrations once; create any number of user-configured instances. */
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Providers from 'effect-harness/Providers'
import * as ProviderStore from 'effect-harness/ProviderStore'
import * as Compatible from 'effect-harness/provider-openai-compatible/Provider'
import * as MemoryCredentials from 'effect-harness/provider-auth/MemoryCredentialStore'

// Static metadata is supplied in instance config when the gateway has no discovery API.
await Effect.runPromise(
  Effect.gen(function* () {
    const providers = yield* Providers.make({ definitions: { gateway: Compatible.definition } })
    const instance = yield* providers.add({
      provider: 'gateway',
      label: 'Local model server',
      config: {
        baseUrl: 'http://localhost:8080/v1',
        authentication: 'optional',
        models: [
          {
            id: 'example-model',
            name: 'Example model',
            capabilities: { tools: true, images: false, reasoning: false, structuredOutput: false },
          },
        ],
      },
    })
    yield* Console.log(yield* providers.models.list({ instanceId: instance.id }))
    yield* Console.log(
      yield* providers.models.get({ instanceId: instance.id, modelId: 'example-model' }),
    )
  }).pipe(Effect.provide(Layer.merge(ProviderStore.layerMemory, MemoryCredentials.layer))),
)
