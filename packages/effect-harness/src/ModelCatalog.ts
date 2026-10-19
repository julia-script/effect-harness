/** Lazy metadata sources and caches with independent listing and detail requirements. */
import * as Cache from 'effect/Cache'
import * as Data from 'effect/Data'
import type * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Schema from 'effect/Schema'
import * as ModelInfo from './ModelInfo.js'
import { ModelCatalogError } from './ModelCatalogError.js'

export const ListOptionsSchema = Schema.Struct({
  query: Schema.optionalKey(Schema.String),
  cursor: Schema.optionalKey(Schema.String),
  limit: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
})
export type ListOptions = typeof ListOptionsSchema.Type

export const PageSchema = Schema.Struct({
  items: Schema.Array(ModelInfo.SummarySchema),
  nextCursor: Schema.optionalKey(Schema.String),
})
export type Page = typeof PageSchema.Type

export interface ModelSource<LE = never, LR = never, GE = never, GR = never> {
  readonly list: (input: ListOptions) => Effect.Effect<Page, LE, LR>
  readonly get: (modelId: string) => Effect.Effect<ModelInfo.ModelInfo, GE, GR>
}

/** Local discovery with the same lazy interface as a remote source. */
export const fromModels = (
  models: ReadonlyArray<ModelInfo.ModelInfo>,
): ModelSource<ModelCatalogError, never, ModelCatalogError> => {
  const entries = models.map((info) => structuredClone(info))
  return {
    list: Effect.fnUntraced(function* (input) {
      const offset = input.cursor === undefined ? 0 : Number(input.cursor)
      if (
        !Number.isSafeInteger(offset) ||
        offset < 0 ||
        (input.cursor !== undefined && String(offset) !== input.cursor)
      ) {
        return yield* new ModelCatalogError({
          reason: 'unsupportedOptions',
          message: 'Invalid model page cursor',
        })
      }
      const query = input.query?.toLowerCase() ?? ''
      const matching = entries.filter((info) =>
        `${info.id} ${info.name}`.toLowerCase().includes(query),
      )
      const end = offset + (input.limit ?? 50)
      return {
        items: matching.slice(offset, end).map(({ id, name }) => ({ id, name })),
        ...(end < matching.length ? { nextCursor: String(end) } : {}),
      }
    }),
    get: (modelId) =>
      Effect.suspend(() => {
        const info = entries.find((info) => info.id === modelId)
        return info === undefined
          ? Effect.fail(
              new ModelCatalogError({ reason: 'modelNotFound', modelId, message: 'Unknown model' }),
            )
          : ModelInfo.decode(structuredClone(info))
      }),
  }
}

/** Bind a static metadata collection selected from a provider's configuration. */
export const fromConfig = <C>(select: (config: C) => ReadonlyArray<ModelInfo.ModelInfo>) => ({
  list: (context: { readonly config: C }, input: ListOptions) =>
    fromModels(select(context.config)).list(input),
  get: (context: { readonly config: C }, modelId: string) =>
    fromModels(select(context.config)).get(modelId),
})

export interface CacheOptions {
  readonly capacity: number
  readonly timeToLive: Duration.Input
}
class PageKey extends Data.Class<ListOptions> {}

/** Cache a source for one immutable instance revision; never share it across accounts. */
export const cached = <LE, LR, GE, GR>(
  source: ModelSource<LE, LR, GE, GR>,
  options: CacheOptions,
) =>
  Effect.gen(function* () {
    const context = yield* Effect.context<LR | GR>()
    const pages = yield* Cache.makeWith(
      (key: PageKey) => source.list(key).pipe(Effect.provideContext(context)),
      {
        capacity: options.capacity,
        timeToLive: (exit) => (Exit.isSuccess(exit) ? options.timeToLive : 0),
      },
    )
    const models = yield* Cache.makeWith(
      (id: string) => source.get(id).pipe(Effect.provideContext(context)),
      {
        capacity: options.capacity,
        timeToLive: (exit) => (Exit.isSuccess(exit) ? options.timeToLive : 0),
      },
    )
    return {
      list: (input: ListOptions) =>
        Cache.get(pages, new PageKey(input)).pipe(Effect.map((value) => structuredClone(value))),
      get: (id: string) =>
        Cache.get(models, id).pipe(Effect.map((value) => structuredClone(value))),
      invalidate: Effect.all([Cache.invalidateAll(pages), Cache.invalidateAll(models)], {
        discard: true,
      }),
    }
  })
