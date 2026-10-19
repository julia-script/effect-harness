/**
 * The `Model` service over providers: each request goes to the provider its model names. The
 * providers are read for each call, so a set that changes while the loop runs is routed as it is
 * now.
 */
import { Effect, Stream } from 'effect'
import type { Context } from 'effect'
import type { DeferredHandle } from '../Message.ts'
import type { Info, Provider, Ref, Service } from '../Model.ts'
import { ModelError, Rejected, UnknownModel } from '../ModelError.ts'

const unknown = (ref: Ref) =>
  new ModelError({
    message: `Model ${ref.provider}/${ref.modelId} is not available`,
    reason: new UnknownModel(ref),
  })

const unsupported = (model: Info) =>
  new ModelError({
    message: `Provider ${model.provider} does not support deferred responses`,
    reason: new Rejected({}),
  })

/** Providers by name; a later provider of the same name replaces an earlier one. */
export interface Routes<R> {
  readonly byName: ReadonlyMap<string, Provider<R>>
  readonly models: ReadonlyArray<Info>
}

export const routes = <R>(providers: Iterable<Provider<R>>): Routes<R> => {
  const byName = new Map<string, Provider<R>>()
  for (const provider of providers) byName.set(provider.name, provider)
  return { byName, models: [...byName.values()].flatMap((provider) => provider.models) }
}

const find = <R>(routed: Routes<R>, ref: Ref): readonly [Provider<R>, Info] | undefined => {
  const provider = routed.byName.get(ref.provider)
  const model = provider?.models.find((candidate) => candidate.id === ref.modelId)
  return provider === undefined || model === undefined ? undefined : [provider, model]
}

/** A `Model` over the routes `current` reads at each call, answering in `context`. */
export const make = <R>(
  current: Effect.Effect<Routes<R>>,
  context: Context.Context<R>,
): Service => {
  const resolved = (ref: Ref) =>
    Effect.flatMap(current, (routed) => {
      const found = find(routed, ref)
      return found === undefined ? Effect.fail(unknown(ref)) : Effect.succeed(found)
    })
  return {
    models: Effect.map(current, (routed) => routed.models),
    info: (ref) => Effect.map(current, (routed) => find(routed, ref)?.[1]),
    stream: (request) =>
      Stream.unwrap(
        Effect.map(resolved(request.model), ([provider, model]) =>
          Stream.provideContext(provider.stream(request, model), context),
        ),
      ),
    poll: (ref, handle: DeferredHandle) =>
      Effect.flatMap(resolved(ref), ([provider, model]) =>
        provider.poll === undefined
          ? Effect.fail(unsupported(model))
          : Effect.provideContext(provider.poll(handle, model), context),
      ),
    cancel: (ref, handle) =>
      Effect.flatMap(resolved(ref), ([provider, model]) =>
        provider.cancel === undefined
          ? Effect.void
          : Effect.provideContext(provider.cancel(handle, model), context),
      ),
  }
}
