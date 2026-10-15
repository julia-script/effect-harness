/** Lazy model discovery and construction with independent Effect errors and requirements. */
import * as Cache from 'effect/Cache'
import * as Data from 'effect/Data'
import type * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Schema from 'effect/Schema'
import type * as Model from './Model.js'
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

export interface Resolved {
  readonly info: ModelInfo.ModelInfo
  readonly options: ModelInfo.Options
}

/** Structural constraint that preserves each concrete callback through generic composition. */
export interface Any {
  readonly id: string
  readonly models: ModelSource<unknown, unknown, unknown, unknown>
  readonly create: (resolved: Resolved) => Effect.Effect<Model.Any, unknown, unknown>
}

export interface Provider<
  Id extends string,
  LE,
  LR,
  GE,
  GR,
  M extends Model.Any,
  CE,
  CR,
> extends Any {
  readonly id: Id
  readonly models: ModelSource<LE, LR, GE, GR>
  /** Translate portable controls and construct a model using the existing provider factory. */
  readonly create: (resolved: Resolved) => Effect.Effect<M, CE, CR>
}

export type ListError<P extends Any> = Effect.Error<ReturnType<P['models']['list']>>
export type ListServices<P extends Any> = Effect.Services<ReturnType<P['models']['list']>>
export type GetError<P extends Any> = Effect.Error<ReturnType<P['models']['get']>>
export type GetServices<P extends Any> = Effect.Services<ReturnType<P['models']['get']>>
export type ModelOf<P extends Any> = Effect.Success<ReturnType<P['create']>>
export type CreateError<P extends Any> = Effect.Error<ReturnType<P['create']>>
export type CreateServices<P extends Any> = Effect.Services<ReturnType<P['create']>>

/** Define a provider without invoking discovery, acquiring services, or starting a runtime. */
export const make = <const Id extends string, LE, LR, GE, GR, M extends Model.Any, CE, CR>(
  input: Provider<Id, LE, LR, GE, GR, M, CE, CR>,
): Provider<Id, LE, LR, GE, GR, M, CE, CR> => ({
  id: input.id,
  models: { ...input.models },
  create: input.create,
})

/** Fetch one page only when executed. The source owns pagination and search semantics. */
export function list<P extends Any>(
  self: P,
  input: ListOptions = {},
): Effect.Effect<Page, ListError<P> | ModelCatalogError, ListServices<P>> {
  // Preserve the selected callback's channels across the structural Any constraint.
  const source = self.models as ModelSource<
    ListError<P>,
    ListServices<P>,
    GetError<P>,
    GetServices<P>
  >
  return Effect.gen(function* () {
    const options = yield* Schema.decodeEffect(ListOptionsSchema)(input).pipe(
      Effect.mapError(
        (cause) =>
          new ModelCatalogError({
            reason: 'unsupportedOptions',
            provider: self.id,
            message: 'Invalid model list options',
            cause,
          }),
      ),
    )
    const page = yield* source.list(options)
    return yield* Schema.decodeEffect(PageSchema)(page).pipe(
      Effect.mapError(
        (cause) =>
          new ModelCatalogError({
            reason: 'invalidMetadata',
            provider: self.id,
            message: 'Invalid model page',
            cause,
          }),
      ),
    )
  })
}

/** Fetch and validate one model without enumerating the provider's catalog. */
export function get<P extends Any>(
  self: P,
  modelId: string,
): Effect.Effect<ModelInfo.ModelInfo, GetError<P> | ModelCatalogError, GetServices<P>> {
  const source = self.models as ModelSource<
    ListError<P>,
    ListServices<P>,
    GetError<P>,
    GetServices<P>
  >
  return Effect.gen(function* () {
    const info = yield* ModelInfo.decode(yield* source.get(modelId))
    if (info.id !== modelId)
      return yield* new ModelCatalogError({
        reason: 'invalidMetadata',
        provider: self.id,
        modelId,
        message: 'Discovery returned a different model',
      })
    return info
  })
}

export interface ResolveOptions {
  readonly modelId: string
  readonly options?: ModelInfo.Options
}

/** Resolve metadata, validate controls, then construct only the selected runtime model. */
export function resolve<P extends Any>(
  self: P,
  input: ResolveOptions,
): Effect.Effect<
  ModelOf<P>,
  GetError<P> | CreateError<P> | ModelCatalogError,
  GetServices<P> | CreateServices<P>
> {
  const create = self.create as (
    resolved: Resolved,
  ) => Effect.Effect<ModelOf<P>, CreateError<P>, CreateServices<P>>
  return Effect.gen(function* () {
    const info = yield* get(self, input.modelId)
    const options = yield* ModelInfo.validateOptions(info, input.options ?? {})
    const model = yield* create({ info, options })
    if (model.definition.ref.modelId !== info.id)
      return yield* new ModelCatalogError({
        reason: 'invalidMetadata',
        provider: self.id,
        modelId: info.id,
        message: 'The provider constructed a different model',
      })
    return model
  })
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

export interface CacheOptions {
  readonly capacity: number
  readonly timeToLive: Duration.Input
}

class PageKey extends Data.Class<ListOptions> {}

/**
 * Bind discovery to the current services and share bounded, lazy lookups.
 *
 * Create one cache per account/endpoint. Discovery dependencies are required here;
 * construction dependencies remain on resolve. No discovery runs during construction.
 * Failures expire immediately. Invalidate after changing account metadata or credentials.
 */
export const cached = <P extends Any>(self: P, options: CacheOptions) =>
  Effect.gen(function* () {
    const context = yield* Effect.context<ListServices<P> | GetServices<P>>()
    const pages = yield* Cache.makeWith(
      (input: PageKey) => list(self, input).pipe(Effect.provideContext(context)),
      {
        capacity: options.capacity,
        timeToLive: (exit) => (Exit.isSuccess(exit) ? options.timeToLive : 0),
      },
    )
    const models = yield* Cache.makeWith(
      (modelId: string) => get(self, modelId).pipe(Effect.provideContext(context)),
      {
        capacity: options.capacity,
        timeToLive: (exit) => (Exit.isSuccess(exit) ? options.timeToLive : 0),
      },
    )
    const provider = make({
      id: self.id as P['id'],
      models: {
        list: (input: ListOptions) =>
          Cache.get(pages, new PageKey(input)).pipe(Effect.map((page) => structuredClone(page))),
        get: (modelId: string) =>
          Cache.get(models, modelId).pipe(Effect.map((info) => structuredClone(info))),
      },
      create: self.create as (
        resolved: Resolved,
      ) => Effect.Effect<ModelOf<P>, CreateError<P>, CreateServices<P>>,
    })
    return {
      ...provider,
      invalidate: Effect.all([Cache.invalidateAll(pages), Cache.invalidateAll(models)], {
        discard: true,
      }),
    }
  })
