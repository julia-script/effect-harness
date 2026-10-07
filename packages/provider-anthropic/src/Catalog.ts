import type * as HttpClient from 'effect/http/HttpClient'
import * as Config from 'effect/Config'
import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as AnthropicLanguageModel from '@effect/ai-anthropic/AnthropicLanguageModel'
import * as Generated from '@effect/ai-anthropic/Generated'
import * as Model from '@effect-harness/harness/Model'
import { ModelError, ModelNoModel, ModelUnsupported } from '@effect-harness/harness/Error'
import * as Usage from '@effect-harness/harness/Usage'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Result from 'effect/Result'
import * as Predicate from 'effect/Predicate'
import * as Schema from 'effect/Schema'
import type * as Response from 'effect/ai/Response'
import type * as Redacted from 'effect/Redacted'
import * as Prompt from './Prompt.ts'

const fail = (message: string, cause?: unknown) =>
  new ModelError({
    reason: new ModelUnsupported({ message, ...(cause === undefined ? {} : { cause }) }),
  })
const fields = Generated.BetaCreateMessageParams.fields
const Options = Schema.Struct({
  metadata: fields.metadata,
  temperature: fields.temperature,
  top_p: fields.top_p,
  top_k: fields.top_k,
  stop_sequences: fields.stop_sequences,
  service_tier: fields.service_tier,
  speed: fields.speed,
  inference_geo: fields.inference_geo,
  max_tokens: Schema.optionalKey(Schema.Int),
  thinking: fields.thinking,
  cache_control: fields.cache_control,
  output_config: Schema.optionalKey(
    Schema.Struct({
      effort: Schema.optionalKey(Schema.NullOr(Schema.Literals(['low', 'medium', 'high']))),
    }),
  ),
  disableParallelToolCalls: Schema.optionalKey(Schema.Boolean),
  structuredOutputs: Schema.optionalKey(Schema.Boolean),
  strictJsonSchema: Schema.optionalKey(Schema.Boolean),
  midConversationSystemMessages: Schema.optionalKey(Schema.Boolean),
})
const Price = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))
export const Prices = Schema.Struct({
  input: Price,
  output: Price,
  cacheRead: Price,
  cacheWrite: Price,
  cacheWrite1h: Schema.optional(Price),
})
export type Prices = typeof Prices.Type
const Limit = Schema.Int.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
)
const EntryOptions = Schema.Struct({
  ...Options.fields,
  disableParallelToolCalls: Schema.optional(Schema.Boolean),
  structuredOutputs: Schema.optional(Schema.Boolean),
  strictJsonSchema: Schema.optional(Schema.Boolean),
  midConversationSystemMessages: Schema.optional(Schema.Boolean),
})
export const Entry = Schema.Struct({
  modelId: Schema.NonEmptyString,
  contextWindow: Limit,
  maxOutputTokens: Limit,
  thinking: Schema.optional(
    Schema.Union([
      Schema.Struct({ mode: Schema.Literal('adaptive') }),
      Schema.Struct({
        mode: Schema.Literal('budget'),
        budgets: Schema.Record(Schema.String, Limit.check(Schema.isGreaterThanOrEqualTo(1024))),
      }),
    ]),
  ),
  efforts: Schema.optional(Schema.Array(Schema.Literals(['low', 'medium', 'high']))),
  cache: Schema.optional(Schema.Boolean),
  config: Schema.optional(EntryOptions),
  prices: Schema.optional(Prices),
}).check(
  Schema.makeFilter((entry) => entry.maxOutputTokens <= entry.contextWindow),
  Schema.makeFilter(
    (entry) =>
      entry.config?.max_tokens === undefined ||
      (Number.isSafeInteger(entry.config.max_tokens) &&
        entry.config.max_tokens > 0 &&
        entry.config.max_tokens <= entry.maxOutputTokens),
  ),
  Schema.makeFilter(
    (entry) =>
      entry.thinking?.mode !== 'budget' ||
      Object.values(entry.thinking.budgets).every((budget) => budget < entry.maxOutputTokens),
  ),
)
export type Entry = typeof Entry.Type
const decode = (value: unknown) =>
  Schema.decodeUnknownEffect(Options, { onExcessProperty: 'error' })(
    Predicate.isReadonlyObject(value)
      ? Object.fromEntries(
          Object.entries(value).filter(
            ([key, entry]) => entry !== undefined || !Object.hasOwn(Options.fields, key),
          ),
        )
      : value,
  ).pipe(
    Effect.mapError((cause) => fail('Unsupported or invalid Anthropic request options', cause)),
  )
const positive = (value: number) => Number.isSafeInteger(value) && value > 0
const session = Schema.String.check(Schema.isUUID(7))
const cacheMetadata = Schema.Struct({
  anthropic: Schema.Struct({
    usage: Schema.Struct({
      cache_creation: Schema.NullOr(
        Schema.Struct({
          ephemeral_1h_input_tokens: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
        }),
      ),
    }),
  }),
})
const usage = (
  value: Response.Usage,
  metadata: Response.ProviderMetadata,
  prices?: Prices,
): Usage.Usage => {
  const extended = Schema.decodeUnknownResult(cacheMetadata)(metadata)
  const cacheWrite1h = Result.isSuccess(extended)
    ? extended.success.anthropic.usage.cache_creation?.ephemeral_1h_input_tokens
    : undefined
  const result = Usage.fromResponse(value, cacheWrite1h === undefined ? {} : { cacheWrite1h })
  if (prices === undefined) return result
  const input =
    value.inputTokens.uncached === undefined ? 0 : (result.input * prices.input) / 1_000_000
  const output = (result.output * prices.output) / 1_000_000
  const cacheRead = (result.cacheRead * prices.cacheRead) / 1_000_000
  const validBreakdown = cacheWrite1h !== undefined && cacheWrite1h <= result.cacheWrite
  const uniformCachePrice = prices.cacheWrite1h === prices.cacheWrite
  const knownBreakdown = validBreakdown || uniformCachePrice || result.cacheWrite === 0
  const oneHour = validBreakdown ? cacheWrite1h : 0
  const cacheWrite = knownBreakdown
    ? ((result.cacheWrite - oneHour) * prices.cacheWrite + oneHour * (prices.cacheWrite1h ?? 0)) /
      1_000_000
    : 0
  const known =
    (value.inputTokens.uncached !== undefined || prices.input === 0) &&
    (value.outputTokens.total !== undefined || prices.output === 0) &&
    (value.inputTokens.cacheRead !== undefined || prices.cacheRead === 0) &&
    (value.inputTokens.cacheWrite !== undefined || prices.cacheWrite === 0) &&
    knownBreakdown &&
    (oneHour === 0 || prices.cacheWrite1h !== undefined)
  if (
    ![input, output, cacheRead, cacheWrite, input + output + cacheRead + cacheWrite].every(
      Number.isFinite,
    )
  )
    return result
  return {
    ...result,
    cost: {
      input,
      output,
      cacheRead,
      cacheWrite,
      total: input + output + cacheRead + cacheWrite,
      known,
      totalKnown: known,
    },
  }
}

/** Captures the standard native client; public user metadata correlates UUID7 requests without private affinity headers. */
export const descriptor = Effect.fnUntraced(function* (entry: Entry, provider = 'anthropic') {
  const defaults = yield* decode(entry.config ?? {})
  yield* Schema.decodeEffect(Entry)({ ...entry, config: defaults }).pipe(
    Effect.mapError((cause) =>
      fail('Invalid Anthropic catalogue entry, defaults or thinking budget', cause),
    ),
  )
  const model = yield* Prompt.make({
    model: entry.modelId,
    config: { ...defaults, max_tokens: defaults.max_tokens ?? entry.maxOutputTokens },
  })
  const configure = Effect.fnUntraced(function* (request: Model.RequestOptions) {
    if (request.sessionId !== undefined)
      yield* Schema.decodeEffect(session)(request.sessionId).pipe(
        Effect.mapError((cause) => fail('Conversation sessionId must be UUID7', cause)),
      )
    const supplied = yield* decode(request.options)
    const merged = { ...defaults, ...supplied }
    const max = request.maxTokens ?? merged.max_tokens ?? entry.maxOutputTokens
    if (!positive(max) || max > entry.maxOutputTokens)
      return yield* fail(
        'maxTokens must be a positive integer within the declared model output limit',
      )
    if (
      request.maxTokens !== undefined &&
      supplied.max_tokens !== undefined &&
      request.maxTokens !== supplied.max_tokens
    )
      return yield* fail('Conflicting Anthropic output token limits')
    let thinking: typeof Generated.BetaThinkingConfigParam.Encoded = { type: 'disabled' }
    let effort = merged.output_config?.effort
    if (request.thinking !== 'off') {
      if (entry.thinking?.mode === 'adaptive') {
        if (!entry.efforts?.some((value) => value === request.thinking))
          return yield* fail('Requested adaptive effort is not declared supported')
        thinking = { type: 'adaptive' }
        const decoded = yield* Schema.decodeUnknownEffect(
          Schema.Literals(['low', 'medium', 'high']),
        )(request.thinking).pipe(
          Effect.mapError((cause) =>
            fail('Adaptive effort is unsupported by the installed native provider', cause),
          ),
        )
        if (
          supplied.output_config?.effort !== undefined &&
          supplied.output_config.effort !== decoded
        )
          return yield* fail('Conflicting adaptive effort and pinned thinking')
        effort = decoded
      } else if (entry.thinking?.mode === 'budget') {
        const budget = Object.hasOwn(entry.thinking.budgets, request.thinking)
          ? entry.thinking.budgets[request.thinking]
          : undefined
        if (budget === undefined || budget >= max)
          return yield* fail('No declared thinking budget fits this output limit')
        thinking = { type: 'enabled', budget_tokens: budget }
      } else return yield* fail('This model does not declare thinking support')
    }
    if (effort !== undefined && effort !== null && !entry.efforts?.includes(effort))
      return yield* fail('Native effort option is not declared supported by this model')
    if (
      supplied.thinking !== undefined &&
      (supplied.thinking.type !== thinking.type ||
        (supplied.thinking.type === 'enabled' &&
          thinking.type === 'enabled' &&
          supplied.thinking.budget_tokens !== thinking.budget_tokens))
    )
      return yield* fail('Use the pinned thinking field and declared budgets')
    if (request.cache !== undefined && request.cache !== 'none' && entry.cache !== true)
      return yield* fail('This model does not declare prompt caching')
    if (merged.cache_control !== undefined && merged.cache_control !== null && entry.cache !== true)
      return yield* fail('Native caching is not declared supported')
    if (request.cache !== undefined && supplied.cache_control !== undefined)
      return yield* fail('Specify either cache or native cache_control')
    if (
      request.sessionId !== undefined &&
      supplied.metadata?.user_id !== undefined &&
      supplied.metadata.user_id !== request.sessionId
    )
      return yield* fail('metadata.user_id must match the pinned conversation sessionId')
    let cacheControl = merged.cache_control
    if (request.cache === 'none') cacheControl = null
    else if (request.cache === 'short') cacheControl = { type: 'ephemeral', ttl: '5m' }
    else if (request.cache === 'long') cacheControl = { type: 'ephemeral', ttl: '1h' }
    const config = yield* decode({
      ...merged,
      max_tokens: max,
      thinking,
      output_config: effort === undefined ? undefined : { effort },
      cache_control: cacheControl,
      metadata:
        request.sessionId === undefined
          ? merged.metadata
          : { ...merged.metadata, user_id: request.sessionId },
    })
    return Context.make(AnthropicLanguageModel.Config, {
      ...config,
      model: entry.modelId,
      max_tokens: max,
    })
  })
  return {
    ref: { provider, modelId: entry.modelId },
    model,
    contextWindow: entry.contextWindow,
    maxOutputTokens: entry.maxOutputTokens,
    configure,
    usage: (value, metadata) => usage(value, metadata, entry.prices),
    classify: (error) => Model.classify(error, 'anthropic'),
  } satisfies Model.Descriptor
})
export const layer = (options: {
  readonly models: ReadonlyArray<Entry>
  readonly provider?: string | undefined
}) =>
  Layer.effect(Model.Catalog)(
    Effect.gen(function* () {
      if (new Set(options.models.map((entry) => entry.modelId)).size !== options.models.length)
        return yield* fail('Duplicate Anthropic catalogue model IDs')
      const entries = yield* Effect.forEach(options.models, (entry) =>
        descriptor(entry, options.provider),
      )
      const byId = new Map(entries.map((entry) => [entry.ref.modelId, entry]))
      return Model.Catalog.of({
        resolve: (ref) => {
          const found = byId.get(ref.modelId)
          return found !== undefined && found.ref.provider === ref.provider
            ? Effect.succeed(found)
            : Effect.fail(
                new ModelError({
                  reason: new ModelNoModel({
                    message: 'Anthropic model is not available in this catalogue',
                  }),
                }),
              )
        },
      })
    }),
  )
export const layerApiKey = (
  options: AnthropicClient.Options & {
    readonly apiKey: Redacted.Redacted<string>
    readonly models: ReadonlyArray<Entry>
    readonly provider?: string | undefined
  },
) => layer(options).pipe(Layer.provideMerge(AnthropicClient.layer(options)))

/** Resolves all layer options through the caller's ConfigProvider. */
export const layerConfig = (
  config: Config.Wrap<NonNullable<Parameters<typeof layer>[0]>>,
): Layer.Layer<Model.Catalog, ModelError | Config.ConfigError, AnthropicClient.AnthropicClient> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layer(yield* Config.unwrap(config))
    }),
  )

/** Resolves all layerApiKey options through the caller's ConfigProvider. */
export const layerApiKeyConfig = (
  config: Config.Wrap<NonNullable<Parameters<typeof layerApiKey>[0]>>,
): Layer.Layer<
  Model.Catalog | AnthropicClient.AnthropicClient,
  ModelError | Config.ConfigError,
  HttpClient.HttpClient
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layerApiKey(yield* Config.unwrap(config))
    }),
  )
