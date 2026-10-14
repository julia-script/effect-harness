/** Provider-indexed lazy discovery. Construction performs no IO and captures no services. */
import * as Effect from 'effect/Effect'
import * as Provider from './Provider.js'
import type * as ModelInfo from './ModelInfo.js'
import { ModelCatalogError } from './ModelCatalogError.js'

export interface ModelCatalog<Providers extends ReadonlyArray<Provider.Any>> {
  readonly providers: Providers
}

/** Preserve literal provider IDs and each operation's independent errors and requirements. */
export const make = <const Providers extends ReadonlyArray<Provider.Any>>(input: {
  readonly providers: Providers
}): ModelCatalog<Providers> => ({ providers: [...input.providers] as unknown as Providers })

/** A runtime string selects the union; a literal selects only matching providers. */
export type Selected<P extends Provider.Any, Id extends string> = P extends unknown
  ? [Extract<Id, P['id']> | Extract<P['id'], Id>] extends [never]
    ? never
    : P
  : never

const find = <P extends ReadonlyArray<Provider.Any>, Id extends string>(
  self: ModelCatalog<P>,
  id: Id,
): Effect.Effect<Selected<P[number], Id>, ModelCatalogError> =>
  Effect.suspend(() => {
    const matches = self.providers.filter((provider) => provider.id === id)
    const provider = matches[0]
    if (provider === undefined)
      return Effect.fail(
        new ModelCatalogError({
          reason: 'providerNotFound',
          provider: id,
          message: 'Unknown model provider',
        }),
      )
    if (matches.length !== 1)
      return Effect.fail(
        new ModelCatalogError({
          reason: 'invalidMetadata',
          provider: id,
          message: 'Duplicate model provider ID',
        }),
      )
    // Runtime equality narrows the heterogeneous provider union to the selected ID.
    return Effect.succeed(provider as Selected<P[number], Id>)
  })

/** List one selected provider page; never enumerates other providers or resolves model details. */
export const list = <P extends ReadonlyArray<Provider.Any>, const Id extends string>(
  self: ModelCatalog<P>,
  input: Provider.ListOptions & { readonly provider: Id },
): Effect.Effect<
  Provider.Page,
  Provider.ListError<Selected<P[number], Id>> | ModelCatalogError,
  Provider.ListServices<Selected<P[number], Id>>
> =>
  Effect.flatMap(find(self, input.provider), (provider) =>
    Provider.list(provider, {
      ...(input.query === undefined ? {} : { query: input.query }),
      ...(input.cursor === undefined ? {} : { cursor: input.cursor }),
      ...(input.limit === undefined ? {} : { limit: input.limit }),
    }),
  )

/** Resolve metadata directly by ID; no list operation is required. */
export const get = <P extends ReadonlyArray<Provider.Any>, const Id extends string>(
  self: ModelCatalog<P>,
  input: { readonly provider: Id; readonly modelId: string },
): Effect.Effect<
  ModelInfo.ModelInfo,
  Provider.GetError<Selected<P[number], Id>> | ModelCatalogError,
  Provider.GetServices<Selected<P[number], Id>>
> => Effect.flatMap(find(self, input.provider), (provider) => Provider.get(provider, input.modelId))

/** Construct only the chosen model, retaining its construction and inference requirements. */
export const resolve = <P extends ReadonlyArray<Provider.Any>, const Id extends string>(
  self: ModelCatalog<P>,
  input: Provider.ResolveOptions & { readonly provider: Id },
): Effect.Effect<
  Provider.ModelOf<Selected<P[number], Id>>,
  | Provider.GetError<Selected<P[number], Id>>
  | Provider.CreateError<Selected<P[number], Id>>
  | ModelCatalogError,
  Provider.GetServices<Selected<P[number], Id>> | Provider.CreateServices<Selected<P[number], Id>>
> => Effect.flatMap(find(self, input.provider), (provider) => Provider.resolve(provider, input))
