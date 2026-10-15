/** Codex protocol definition; each user instance supplies configuration and a credential reference. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Provider from '../Provider.js'
import type * as ModelCatalog from '../ModelCatalog.js'
import * as ModelInfo from '../ModelInfo.js'
import { ModelCatalogError } from '../ModelCatalogError.js'
import * as Catalog from './Catalog.js'
import * as Native from './CodexLanguageModel.js'
import * as Model from './Model.js'
import * as Credential from '../provider-auth/Credential.js'
import { AuthError } from '../provider-auth/AuthError.js'

export const Config = Schema.Struct({
  clientVersion: Schema.NonEmptyString,
  catalogUrl: Schema.optionalKey(Schema.NonEmptyString),
  authBaseUrl: Schema.optionalKey(Schema.NonEmptyString),
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
    return yield* Catalog.make(yield* optionsFor(context)).list(input)
  }),
  get: Effect.fnUntraced(function* (context: Context, modelId: string) {
    return yield* Catalog.make(yield* optionsFor(context)).get(modelId)
  }),
}

/** Override metadata lookup without reimplementing the inference protocol. */
export const define = <LE, LR, GE, GR>(models: {
  readonly list: (
    context: Context,
    input: ModelCatalog.ListOptions,
  ) => Effect.Effect<ModelCatalog.Page, LE, LR>
  readonly get: (context: Context, modelId: string) => Effect.Effect<ModelInfo.ModelInfo, GE, GR>
}) =>
  Provider.define({
    version: 1,
    config: Config,
    secrets: Credential.OAuth,
    models,
    createModel: Effect.fnUntraced(function* (context: Context, resolved: Provider.Resolved) {
      const options = yield* optionsFor(context)

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
        supportsTemperature: resolved.info.capabilities?.temperature === true,
      })
    }),
  })

export const definition = define(live)
