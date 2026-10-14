import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Data from 'effect/Data'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Model from 'effect-harness/Model'
import type * as ModelInfo from 'effect-harness/ModelInfo'
import * as Provider from 'effect-harness/Provider'
import * as Catalog from 'effect-harness/ModelCatalog'
import type { ModelCatalogError } from 'effect-harness/ModelCatalogError'

class Listing extends Context.Service<Listing, string>()('catalog/types/Listing') {}
class Details extends Context.Service<Details, string>()('catalog/types/Details') {}
class Construction extends Context.Service<Construction, string>()('catalog/types/Construction') {}
class Inference extends Context.Service<Inference, string>()('catalog/types/Inference') {}
class Other extends Context.Service<Other, string>()('catalog/types/Other') {}
class ListError extends Data.TaggedError('ListError')<{}> {}
class GetError extends Data.TaggedError('GetError')<{}> {}
class CreateError extends Data.TaggedError('CreateError')<{}> {}

const options = Schema.Struct({})
declare const model: Model.Model<typeof options, Inference>
declare const listing: Effect.Effect<Provider.Page, ListError, Listing>
declare const details: Effect.Effect<ModelInfo.ModelInfo, GetError, Details>
declare const construction: Effect.Effect<typeof model, CreateError, Construction>
declare const otherDetails: Effect.Effect<ModelInfo.ModelInfo, never, Other>
declare const dynamicId: string
declare const unionId: 'primary' | 'secondary'
declare const brandedId: string & { readonly __brand: 'ProviderId' }
declare const templateId: `f${string}`

const first = Provider.make({
  id: 'first',
  models: { list: () => listing, get: () => details },
  create: () => construction,
})
const second = Provider.make({
  id: 'second',
  models: { list: () => Effect.succeed({ items: [] }), get: () => otherDetails },
  create: () => Effect.succeed(model),
})
const catalog = Catalog.make({ providers: [first, second] })
declare const cachedFirst: Effect.Success<ReturnType<typeof Provider.cached<typeof first>>>

test('literal provider lookup retains operation-specific dependencies and errors', () => {
  expect(first.id).type.toBe<'first'>()
  expect(Provider.list(first)).type.toBe<
    Effect.Effect<Provider.Page, ListError | ModelCatalogError, Listing>
  >()
  expect(Catalog.list(catalog, { provider: 'first' })).type.toBe<
    Effect.Effect<Provider.Page, ListError | ModelCatalogError, Listing>
  >()
  expect(Catalog.get(catalog, { provider: 'first', modelId: 'model' })).type.toBe<
    Effect.Effect<ModelInfo.ModelInfo, GetError | ModelCatalogError, Details>
  >()
  expect(Catalog.get(catalog, { provider: 'second', modelId: 'model' })).type.toBe<
    Effect.Effect<ModelInfo.ModelInfo, ModelCatalogError, Other>
  >()
  const resolved = Catalog.resolve(catalog, { provider: 'first', modelId: 'model' })
  expect(resolved).type.toBe<
    Effect.Effect<typeof model, GetError | CreateError | ModelCatalogError, Details | Construction>
  >()
  expect<Model.Requirements<Effect.Success<typeof resolved>>>().type.toBe<Inference>()
})

test('dynamic selection requires the union, while unknown providers fail without dependencies', () => {
  expect(Catalog.get(catalog, { provider: brandedId, modelId: 'model' })).type.toBe<
    Effect.Effect<ModelInfo.ModelInfo, GetError | ModelCatalogError, Details | Other>
  >()
  expect(Catalog.list(catalog, { provider: brandedId })).type.toBe<
    Effect.Effect<Provider.Page, ListError | ModelCatalogError, Listing>
  >()
  expect(Catalog.resolve(catalog, { provider: brandedId, modelId: 'model' })).type.toBe<
    Effect.Effect<
      typeof model,
      GetError | CreateError | ModelCatalogError,
      Details | Other | Construction
    >
  >()
  expect(Catalog.get(catalog, { provider: templateId, modelId: 'model' })).type.toBe<
    Effect.Effect<ModelInfo.ModelInfo, GetError | ModelCatalogError, Details>
  >()
  expect(Catalog.get(catalog, { provider: dynamicId, modelId: 'model' })).type.toBe<
    Effect.Effect<ModelInfo.ModelInfo, GetError | ModelCatalogError, Details | Other>
  >()
  expect(Catalog.list(catalog, { provider: dynamicId })).type.toBe<
    Effect.Effect<Provider.Page, ListError | ModelCatalogError, Listing>
  >()
  expect(Catalog.get(catalog, { provider: 'missing', modelId: 'model' })).type.toBe<
    Effect.Effect<ModelInfo.ModelInfo, ModelCatalogError>
  >()
  const unionProvider = Provider.make({ ...first, id: unionId })
  expect(
    Catalog.get(Catalog.make({ providers: [unionProvider] }), {
      provider: 'primary',
      modelId: 'model',
    }),
  ).type.toBe<Effect.Effect<ModelInfo.ModelInfo, GetError | ModelCatalogError, Details>>()
})

test('cache construction owns discovery services; model construction and inference remain independent', () => {
  const cached = Provider.cached(first, { capacity: 10, timeToLive: '5 minutes' })
  expect<Effect.Services<typeof cached>>().type.toBe<Listing | Details>()
  expect<Effect.Success<typeof cached>>().type.toBeAssignableTo<Provider.Any>()
  declareCached(cachedFirst)
})

function declareCached(cached: Effect.Success<ReturnType<typeof Provider.cached<typeof first>>>) {
  expect(Provider.list(cached)).type.toBe<
    Effect.Effect<Provider.Page, ListError | ModelCatalogError>
  >()
  expect(Provider.get(cached, 'model')).type.toBe<
    Effect.Effect<ModelInfo.ModelInfo, GetError | ModelCatalogError>
  >()
  expect(Provider.resolve(cached, { modelId: 'model' })).type.toBe<
    Effect.Effect<typeof model, GetError | CreateError | ModelCatalogError, Construction>
  >()
}
