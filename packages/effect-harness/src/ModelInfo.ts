/** Serializable model discovery metadata. Missing capabilities mean unknown. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Model from './Model.js'
import { ModelCatalogError } from './ModelCatalogError.js'

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))
const ThinkingMode = Schema.Literals(['disabled', 'enabled', 'adaptive'])

export const SummarySchema = Schema.Struct({
  id: Schema.NonEmptyString,
  name: Schema.NonEmptyString,
})
export type Summary = typeof SummarySchema.Type

export const ModelInfoSchema = Schema.Struct({
  ...SummarySchema.fields,
  capabilities: Schema.optionalKey(
    Schema.Struct({
      tools: Schema.optionalKey(Schema.Boolean),
      reasoning: Schema.optionalKey(Schema.Boolean),
      images: Schema.optionalKey(Schema.Boolean),
      structuredOutput: Schema.optionalKey(Schema.Boolean),
    }),
  ),
  contextWindow: Model.DefinitionSchema.fields.contextWindow,
  maxOutputTokens: Model.DefinitionSchema.fields.maxOutputTokens,
  pricing: Model.DefinitionSchema.fields.pricing,
  imageLimits: Model.DefinitionSchema.fields.imageLimits,
  reasoning: Schema.optionalKey(
    Schema.Struct({
      effort: Schema.optionalKey(
        Schema.Struct({
          values: Schema.NonEmptyArray(Schema.NonEmptyString),
          default: Schema.optionalKey(Schema.NonEmptyString),
        }),
      ),
      thinking: Schema.optionalKey(
        Schema.Struct({
          modes: Schema.NonEmptyArray(ThinkingMode),
          budgetTokens: Schema.optionalKey(
            Schema.Struct({
              min: PositiveInt,
              max: Schema.optionalKey(PositiveInt),
            }),
          ),
        }),
      ),
    }),
  ),
})
export type ModelInfo = typeof ModelInfoSchema.Type

export const OptionsSchema = Schema.Struct({
  reasoning: Schema.optionalKey(
    Schema.Struct({
      effort: Schema.optionalKey(Schema.NonEmptyString),
      thinking: Schema.optionalKey(
        Schema.Struct({
          mode: ThinkingMode,
          budgetTokens: Schema.optionalKey(PositiveInt),
        }),
      ),
    }),
  ),
})
export type Options = typeof OptionsSchema.Type

/** Decode remote metadata and reject contradictory option declarations. */
export const decode = Effect.fnUntraced(function* (input: unknown) {
  const info = yield* Schema.decodeUnknownEffect(ModelInfoSchema)(input).pipe(
    Effect.mapError(
      (cause) =>
        new ModelCatalogError({
          reason: 'invalidMetadata',
          message: 'Invalid model metadata',
          cause,
        }),
    ),
  )
  const effort = info.reasoning?.effort
  const thinking = info.reasoning?.thinking
  if (
    (effort?.default !== undefined && !effort.values.includes(effort.default)) ||
    (effort !== undefined && new Set(effort.values).size !== effort.values.length) ||
    (thinking !== undefined && new Set(thinking.modes).size !== thinking.modes.length) ||
    (thinking?.budgetTokens !== undefined && !thinking.modes.includes('enabled')) ||
    (thinking?.budgetTokens?.max !== undefined &&
      thinking.budgetTokens.max < thinking.budgetTokens.min) ||
    (info.capabilities?.reasoning === false &&
      (effort !== undefined || thinking?.modes.some((mode) => mode !== 'disabled')))
  )
    return yield* new ModelCatalogError({
      reason: 'invalidMetadata',
      modelId: info.id,
      message: 'Contradictory reasoning metadata',
    })
  return info
})

/** Validate explicit controls without clamping or replacing omitted options with defaults. */
export const validateOptions = Effect.fnUntraced(function* (info: ModelInfo, input: Options) {
  const options = yield* Schema.decodeEffect(OptionsSchema, { onExcessProperty: 'error' })(
    input,
  ).pipe(
    Effect.mapError(
      (cause) =>
        new ModelCatalogError({
          reason: 'unsupportedOptions',
          modelId: info.id,
          message: 'Invalid model options',
          cause,
        }),
    ),
  )
  const effort = options.reasoning?.effort
  const thinking = options.reasoning?.thinking
  const support = info.reasoning?.thinking
  if (effort !== undefined && !info.reasoning?.effort?.values.includes(effort)) {
    return yield* new ModelCatalogError({
      reason: 'unsupportedOptions',
      modelId: info.id,
      message: `Unsupported reasoning effort: ${effort}`,
    })
  }
  if (
    thinking !== undefined &&
    (!support?.modes.includes(thinking.mode) ||
      (thinking.mode === 'enabled' &&
        ((support.budgetTokens === undefined && thinking.budgetTokens !== undefined) ||
          (support.budgetTokens !== undefined &&
            (thinking.budgetTokens === undefined ||
              thinking.budgetTokens < support.budgetTokens.min ||
              (support.budgetTokens.max !== undefined &&
                thinking.budgetTokens > support.budgetTokens.max))))) ||
      (thinking.mode !== 'enabled' && thinking.budgetTokens !== undefined))
  )
    return yield* new ModelCatalogError({
      reason: 'unsupportedOptions',
      modelId: info.id,
      message: 'Unsupported thinking mode or token budget',
    })
  return options
})

/** Convert known metadata into a runtime definition; unknown capabilities must be resolved first. */
export const toDefinition = Effect.fnUntraced(function* (info: ModelInfo, provider: string) {
  return yield* Schema.decodeUnknownEffect(Model.DefinitionSchema)({
    ...info,
    ref: { provider, modelId: info.id },
  }).pipe(
    Effect.mapError(
      (cause) =>
        new ModelCatalogError({
          reason: 'invalidMetadata',
          provider,
          modelId: info.id,
          message: 'A runtime model requires explicit capability declarations',
          cause,
        }),
    ),
  )
})
