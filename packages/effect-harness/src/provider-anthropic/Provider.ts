/** Concrete Claude OAuth catalogs backed by the existing durable Messages adapter. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Provider from '../Provider.js'
import * as ModelInfo from '../ModelInfo.js'
import { ModelCatalogError } from '../ModelCatalogError.js'
import * as Catalog from './Catalog.js'
import * as Native from './OAuthLanguageModel.js'
import * as Model from './OAuthModel.js'
import * as ThinkingPolicy from './ThinkingPolicy.js'

export interface Options<Id extends string> extends Omit<
  Native.Options,
  'model' | 'thinkingModes' | 'efforts'
> {
  readonly id: Id
}

/** Preserve custom discovery requirements while fixing protocol construction and option translation. */
export const makeWithSource = <const Id extends string, LE, LR, GE, GR>(
  options: Options<Id>,
  models: Provider.ModelSource<LE, LR, GE, GR>,
) =>
  Provider.make({
    id: options.id,
    models,
    create: Effect.fnUntraced(function* ({ info, options: selected }: Provider.Resolved) {
      const effort =
        selected.reasoning?.effort ?? options.config?.effort ?? info.reasoning?.effort?.default
      const modes: ReadonlyArray<'disabled' | 'enabled' | 'adaptive'> =
        info.reasoning?.thinking?.modes ?? []
      const configuredThinking = options.config?.thinking
      let thinking = selected.reasoning?.thinking
      if (thinking === undefined && configuredThinking !== undefined) {
        thinking = {
          mode: configuredThinking.type,
          ...(configuredThinking.type === 'enabled'
            ? { budgetTokens: configuredThinking.budget_tokens }
            : {}),
        }
      }
      if (thinking === undefined && modes.includes('adaptive')) thinking = { mode: 'adaptive' }
      yield* ModelInfo.validateOptions(info, {
        reasoning: {
          ...(effort === undefined ? {} : { effort }),
          ...(thinking === undefined ? {} : { thinking }),
        },
      })
      if (
        (ThinkingPolicy.forModel(info.id) === 'server-drop' && thinking?.mode !== 'adaptive') ||
        (ThinkingPolicy.forModel(info.id) === 'unverified' &&
          thinking !== undefined &&
          thinking.mode !== 'disabled') ||
        (effort !== undefined && thinking?.mode !== 'adaptive')
      )
        return yield* new ModelCatalogError({
          reason: 'unsupportedOptions',
          modelId: info.id,
          message: 'Selected controls are unsupported by the OAuth thinking-history policy',
        })
      const config = yield* Schema.decodeUnknownEffect(Native.Settings)({
        ...options.config,
        ...(effort === undefined ? {} : { effort }),
        ...(thinking === undefined
          ? {}
          : {
              thinking: {
                type: thinking.mode,
                ...(thinking.budgetTokens === undefined
                  ? {}
                  : { budget_tokens: thinking.budgetTokens }),
              },
            }),
      })
      if (
        config.thinking?.type === 'enabled' &&
        config.thinking.budget_tokens >= (config.maxTokens ?? 4096)
      )
        return yield* new ModelCatalogError({
          reason: 'unsupportedOptions',
          modelId: info.id,
          message: 'Thinking budget must be smaller than maxTokens',
        })
      const efforts = yield* Schema.decodeUnknownEffect(Schema.Array(Native.Effort))(
        info.reasoning?.effort?.values ?? [],
      )
      return yield* Model.make({
        ...options,
        model: info.id,
        definition: yield* ModelInfo.toDefinition(info, 'anthropic'),
        config,
        thinkingModes: modes,
        efforts,
      })
    }),
  })

/** Bundled OAuth-compatible definitions; listing and detail lookup require no network services. */
export const make = <const Id extends string>(options: Options<Id>) =>
  makeWithSource(options, Catalog.bundled())
