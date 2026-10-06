import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as AnthropicLanguageModel from '@effect/ai-anthropic/AnthropicLanguageModel'
import * as Generated from '@effect/ai-anthropic/Generated'
import * as Model from '@effect-harness/harness/Model'
import { ModelError } from '@effect-harness/harness/Error'
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

export interface Prices {
  readonly input: number
  readonly output: number
  readonly cacheRead: number
  readonly cacheWrite: number
  readonly cacheWrite1h?: number | undefined
}
export interface Entry {
  readonly modelId: string
  readonly contextWindow: number
  readonly maxOutputTokens: number
  readonly thinking?:
    | { readonly mode: 'adaptive' }
    | { readonly mode: 'budget'; readonly budgets: Readonly<Record<string, number>> }
    | undefined
  readonly efforts?: ReadonlyArray<'low' | 'medium' | 'high'> | undefined
  readonly cache?: boolean | undefined
  readonly config?: Omit<typeof AnthropicLanguageModel.Config.Service, 'model'> | undefined
  readonly prices?: Prices | undefined
}
const fail = (message: string) => new ModelError({ reason: 'unsupported', message })
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
})
const decode = (value: unknown) =>
  Schema.decodeUnknownEffect(Options, { onExcessProperty: 'error' })(
    Predicate.isReadonlyObject(value)
      ? Object.fromEntries(
          Object.entries(value).filter(
            ([key, entry]) => entry !== undefined || !Object.hasOwn(Options.fields, key),
          ),
        )
      : value,
  ).pipe(Effect.mapError(() => fail('Unsupported or invalid Anthropic request options')))
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
export const descriptor = (entry: Entry, provider = 'anthropic') =>
  Effect.gen(function* () {
    if (
      entry.modelId.length === 0 ||
      !positive(entry.contextWindow) ||
      !positive(entry.maxOutputTokens) ||
      entry.maxOutputTokens > entry.contextWindow
    )
      return yield* fail('Supply valid model ID, context window and output limit')
    if (
      entry.prices !== undefined &&
      Object.values(entry.prices).some((value) => !Number.isFinite(value) || value < 0)
    )
      return yield* fail('Prices must be finite nonnegative USD per million tokens')
    if (
      entry.thinking?.mode === 'budget' &&
      Object.values(entry.thinking.budgets).some(
        (value) => !positive(value) || value < 1024 || value >= entry.maxOutputTokens,
      )
    )
      return yield* fail('Thinking budgets must be integers >=1024 below the declared output cap')
    const defaults = yield* decode(entry.config ?? {})
    if (
      defaults.max_tokens !== undefined &&
      (!positive(defaults.max_tokens) || defaults.max_tokens > entry.maxOutputTokens)
    )
      return yield* fail('Default output limit exceeds the declared model output cap')
    const model = yield* Prompt.make({
      model: entry.modelId,
      config: { ...defaults, max_tokens: defaults.max_tokens ?? entry.maxOutputTokens },
    })
    const configure = Effect.fnUntraced(function* (request: Model.RequestOptions) {
      if (request.sessionId !== undefined)
        yield* Schema.decodeEffect(session)(request.sessionId).pipe(
          Effect.mapError(() => fail('Conversation sessionId must be UUID7')),
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
            Effect.mapError(() =>
              fail('Adaptive effort is unsupported by the installed native provider'),
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
      if (
        merged.cache_control !== undefined &&
        merged.cache_control !== null &&
        entry.cache !== true
      )
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
      normalizePrompt: Prompt.normalize,
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
                  reason: 'no_model',
                  message: 'Anthropic model is not available in this catalogue',
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
