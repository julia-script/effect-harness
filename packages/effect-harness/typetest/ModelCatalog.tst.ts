import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Data from 'effect/Data'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Model from 'effect-harness/Model'
import type * as ModelInfo from 'effect-harness/ModelInfo'
import * as Provider from 'effect-harness/Provider'
import type * as Providers from 'effect-harness/Providers'
import type * as Instance from 'effect-harness/ProviderInstance'
import * as Catalog from 'effect-harness/ModelCatalog'
import type { ModelCatalogError } from 'effect-harness/ModelCatalogError'
import type { ProviderError } from 'effect-harness/ProviderError'
import type { AuthError } from 'effect-harness/provider-auth/AuthError'

class Listing extends Context.Service<Listing, string>()('catalog/types/Listing') {}
class Details extends Context.Service<Details, string>()('catalog/types/Details') {}
class Construction extends Context.Service<Construction, string>()('catalog/types/Construction') {}
class Inference extends Context.Service<Inference, string>()('catalog/types/Inference') {}
class Other extends Context.Service<Other, string>()('catalog/types/Other') {}
class ListError extends Data.TaggedError('ListError')<{}> {}
class GetError extends Data.TaggedError('GetError')<{}> {}
class CreateError extends Data.TaggedError('CreateError')<{}> {}
const options = Schema.Struct({ temperature: Schema.Finite })
declare const model: Model.Model<typeof options, Inference>
declare const listing: Effect.Effect<Catalog.Page, ListError, Listing>
declare const details: Effect.Effect<ModelInfo.ModelInfo, GetError, Details>
declare const construction: Effect.Effect<typeof model, CreateError, Construction>
declare const otherDetails: Effect.Effect<ModelInfo.ModelInfo, never, Other>
const first = Provider.define({
  version: 1,
  config: Schema.Struct({ baseUrl: Schema.String }),
  secrets: Schema.Struct({ token: Schema.String }),
  models: { list: () => listing, get: () => details },
  createModel: () => construction,
})
const second = Provider.define({
  version: 1,
  config: Schema.Struct({ region: Schema.String }),
  secrets: Schema.Struct({ password: Schema.String }),
  models: { list: () => Effect.succeed({ items: [] }), get: () => otherDetails },
  createModel: () => Effect.succeed(model),
})
const definitions = Provider.definitions({ first, second })
declare const manager: Providers.Service<typeof definitions>
declare const firstId: Instance.Id<'first'>
declare const dynamicId: Instance.Id<'first' | 'second'>

test('instance selection retains operation-specific dependencies, errors and model option types', () => {
  expect(manager.models.list({ instanceId: firstId })).type.toBe<
    Effect.Effect<Catalog.Page, ListError | ModelCatalogError | ProviderError | AuthError, Listing>
  >()
  expect(manager.models.get({ instanceId: firstId, modelId: 'model' })).type.toBe<
    Effect.Effect<
      ModelInfo.ModelInfo,
      GetError | ModelCatalogError | ProviderError | AuthError,
      Details
    >
  >()
  const resolved = manager.models.resolve({ instanceId: firstId, modelId: 'model' })
  expect<Effect.Success<typeof resolved>>().type.toBe<typeof model>()
  expect<Effect.Services<typeof resolved>>().type.toBe<Details | Construction>()
  expect<Model.Requirements<Effect.Success<typeof resolved>>>().type.toBe<Inference>()
  expect<Effect.Services<ReturnType<typeof manager.models.get<'first' | 'second'>>>>().type.toBe<
    Details | Other
  >()
  expect(manager.models.get({ instanceId: dynamicId, modelId: 'model' })).type.toBe<
    Effect.Effect<
      ModelInfo.ModelInfo,
      GetError | ModelCatalogError | ProviderError | AuthError,
      Details | Other
    >
  >()
})

test('add and update infer config and secrets from the definition and instance ID', () => {
  const added = manager.add({ provider: 'first', label: 'First', config: { baseUrl: 'url' } })
  expect<Effect.Success<typeof added>['id']>().type.toBe<Instance.Id<'first'>>()
  expect<Effect.Success<typeof added>['config']>().type.toBe<{ readonly baseUrl: string }>()
  expect(manager.add).type.not.toBeCallableWith({ provider: 'missing', label: 'No', config: {} })
  expect(manager.add).type.not.toBeCallableWith({
    provider: 'first',
    label: 'Wrong config',
    config: { region: 'x' },
  })
  expect(manager.add).type.not.toBeCallableWith({
    provider: 'first',
    label: 'Wrong secrets',
    config: { baseUrl: 'x' },
    secrets: { password: 'x' },
  })
  expect(manager.update).type.not.toBeCallableWith(firstId, { config: { region: 'x' } })
  expect(manager.update).type.not.toBeCallableWith(firstId, { secrets: { password: 'x' } })
  expect(manager.update).type.toBeCallableWith(firstId, { secrets: null })
  expect(model.configure).type.not.toBeCallableWith(
    { temperature: 'hot' },
    { purpose: 'generation' },
  )
})

test('source caching captures discovery services and keeps independent error types', () => {
  const cached = Catalog.cached(
    { list: () => listing, get: () => details },
    { capacity: 10, timeToLive: '5 minutes' },
  )
  expect<Effect.Services<typeof cached>>().type.toBe<Listing | Details>()
  type Cached = Effect.Success<typeof cached>
  expect<Effect.Services<ReturnType<Cached['get']>>>().type.toBe<never>()
  expect<Effect.Error<ReturnType<Cached['get']>>>().type.toBe<GetError>()
})
