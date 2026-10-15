/** Claude protocol definition; each user instance supplies configuration and a credential reference. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Provider from '../Provider.js'
import type * as ModelCatalog from '../ModelCatalog.js'
import * as ModelInfo from '../ModelInfo.js'
import { ModelCatalogError } from '../ModelCatalogError.js'
import * as Catalog from './Catalog.js'
import * as Native from './OAuthLanguageModel.js'
import * as Model from './OAuthModel.js'
import * as Credential from '../provider-auth/Credential.js'
import { AuthError } from '../provider-auth/AuthError.js'
import * as ThinkingPolicy from './ThinkingPolicy.js'

export const Config = Schema.Struct({
  apiVersion: Schema.optionalKey(Schema.NonEmptyString),
  authorizeUrl: Schema.optionalKey(Schema.NonEmptyString),
  tokenUrl: Schema.optionalKey(Schema.NonEmptyString),
  apiUrl: Schema.optionalKey(Schema.NonEmptyString),
  minimumValidityMs: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  refreshTimeoutMs: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  automaticRefresh: Schema.optionalKey(Schema.Boolean),
  config: Schema.optionalKey(Native.Settings),
})
export type Config = typeof Config.Type
export type Context = Provider.Context<Config, Credential.OAuth>
const optionsFor = Effect.fnUntraced(function* (context: Context) {
  yield* context.check
  if (context.credential === undefined) return yield* new AuthError({ reason: 'MissingCredential' })
  return { ...context.config, binding: context.credential }
})
const live = {
  list: Effect.fnUntraced(function* (context: Context, input: ModelCatalog.ListOptions) {
    return yield* Catalog.oauth(yield* optionsFor(context)).list(input)
  }),
  get: Effect.fnUntraced(function* (context: Context, modelId: string) {
    return yield* Catalog.oauth(yield* optionsFor(context)).get(modelId)
  }),
}

/** Override metadata lookup without reimplementing the inference protocol. */
export const define = <LE, LR, GE, GR>(
  models: {
    readonly list: (
      context: Context,
      input: ModelCatalog.ListOptions,
    ) => Effect.Effect<ModelCatalog.Page, LE, LR>
    readonly get: (context: Context, modelId: string) => Effect.Effect<ModelInfo.ModelInfo, GE, GR>
  },
  policy: { readonly thinkingPolicy?: (info: ModelInfo.ModelInfo) => ThinkingPolicy.Policy } = {},
) =>
  Provider.define({
    version: 1,
    config: Config,
    secrets: Credential.OAuth,
    models,
    createModel: Effect.fnUntraced(function* (context: Context, resolved: Provider.Resolved) {
      const selectedPolicy = policy.thinkingPolicy
      const options = yield* optionsFor(context)

      const { info, options: selected } = resolved
      const thinkingPolicy = selectedPolicy?.(info) ?? ThinkingPolicy.forModel(info.id)
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
        (thinkingPolicy === 'server-drop' && thinking?.mode !== 'adaptive') ||
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
        thinkingPolicy,
        definition: yield* ModelInfo.toDefinition(info, 'anthropic'),
        config,
        thinkingModes: modes,
        efforts,
      })
    }),
  })

export const definition = define(live)
