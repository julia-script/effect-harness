/** Concrete Codex discovery and durable model construction. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Provider from '../Provider.js'
import * as ModelInfo from '../ModelInfo.js'
import { ModelCatalogError } from '../ModelCatalogError.js'
import * as Catalog from './Catalog.js'
import * as Native from './CodexLanguageModel.js'
import * as Model from './Model.js'

export interface Options<Id extends string> extends Omit<
  Native.Options,
  'model' | 'reasoningEfforts'
> {
  readonly id: Id
}

/** Use an application-owned discovery source while retaining its exact errors and services. */
export const makeWithSource = <const Id extends string, LE, LR, GE, GR>(
  options: Options<Id>,
  models: Provider.ModelSource<LE, LR, GE, GR>,
) =>
  Provider.make({
    id: options.id,
    models,
    create: Effect.fnUntraced(function* (resolved: Provider.Resolved) {
      const effort =
        resolved.options.reasoning?.effort ??
        options.config?.reasoningEffort ??
        resolved.info.reasoning?.effort?.default
      yield* ModelInfo.validateOptions(
        resolved.info,
        effort === undefined && resolved.options.reasoning?.thinking === undefined
          ? {}
          : {
              reasoning: {
                ...(effort === undefined ? {} : { effort }),
                ...(resolved.options.reasoning?.thinking === undefined
                  ? {}
                  : { thinking: resolved.options.reasoning.thinking }),
              },
            },
      )
      if (resolved.options.reasoning?.thinking !== undefined)
        return yield* new ModelCatalogError({
          reason: 'unsupportedOptions',
          modelId: resolved.info.id,
          message: 'Codex does not accept thinking modes or token budgets',
        })
      const config = yield* Schema.decodeUnknownEffect(Native.Settings)({
        ...options.config,
        ...(effort === undefined ? {} : { reasoningEffort: effort }),
      })
      const reasoningEfforts = yield* Schema.decodeUnknownEffect(
        Schema.Array(Native.ReasoningEffort),
      )(resolved.info.reasoning?.effort?.values ?? [])
      return yield* Model.make({
        ...options,
        model: resolved.info.id,
        definition: yield* ModelInfo.toDefinition(resolved.info, 'openai-codex'),
        config,
        reasoningEfforts,
      })
    }),
  })

/** Discover the authenticated account's models and construct only the selected model. */
export const make = <const Id extends string>(
  options: Options<Id> & Pick<Catalog.Options, 'clientVersion' | 'catalogUrl'>,
) => makeWithSource(options, Catalog.make(options))
