const DescriptorTypeId = '~effect-harness/provider-anthropic/Catalog/Descriptor'

/**
 * Validated model catalogues with pinned request configuration and usage accounting.
 */
import * as Pipeable from 'effect/Pipeable'
import * as Inspectable from 'effect/Inspectable'
import * as Predicate from 'effect/Predicate'
import { dual, constUndefined } from 'effect/Function'
import * as HashMap from 'effect/HashMap'
import * as Option from 'effect/Option'
import type * as HttpClient from 'effect/http/HttpClient'
import * as Config from 'effect/Config'
import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'

import * as Generated from '@effect/ai-anthropic/Generated'
import * as Model from 'effect-harness/Model'
import { ModelError, ModelNoModelError, ModelUnsupportedError } from 'effect-harness/ModelError'
import * as Usage from 'effect-harness/Usage'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Record from 'effect/Record'
import * as Array from 'effect/Array'
import type * as Response from 'effect/ai/Response'
import type * as Redacted from 'effect/Redacted'
import * as AnthropicLanguageModel from './AnthropicLanguageModel.ts'

const fail = (message: string, cause?: unknown) =>
  new ModelError({
    reason: new ModelUnsupportedError({ message, ...(cause === undefined ? {} : { cause }) }),
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
/**
 * Schema for USD prices per million input, output and cached tokens.
 *
 * **Gotchas**
 *
 * Prices are declarations supplied by the host. Unknown prices stay unknown in usage
 * accounting.
 *
 * @category models
 */
export const Prices = Schema.Struct({
  input: Price,
  output: Price,
  cacheRead: Price,
  cacheWrite: Price,
  cacheWrite1h: Schema.optional(Price),
})
/**
 * Declared USD prices per million input, output and cache tokens.
 *
 * @category models
 */
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
/**
 * Schema for a caller-declared model and its supported request capabilities.
 *
 * **Details**
 *
 * contextWindow and maxOutputTokens must be positive and the output limit cannot exceed the
 * context window. Optional capability fields control accepted request options.
 *
 * **Gotchas**
 *
 * Entries describe application policy; they do not discover current remote models or account
 * entitlements.
 *
 * @category models
 */
export const Entry = Schema.Struct({
  modelId: Schema.NonEmptyString,
  contextWindow: Limit,
  maxOutputTokens: Limit,
  thinking: Schema.optional(
    Schema.Union([
      Schema.TaggedStruct('adaptive', {}),
      Schema.TaggedStruct('budget', {
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
      entry.thinking?._tag !== 'budget' ||
      Record.values(entry.thinking.budgets).every((budget) => budget < entry.maxOutputTokens),
  ),
)
/**
 * Caller-declared model identity, token limits and supported request capabilities.
 *
 * @category models
 */
export type Entry = typeof Entry.Type
const decode = (value: unknown) =>
  Schema.decodeUnknownEffect(Options, { onExcessProperty: 'error' })(
    Predicate.isReadonlyObject(value)
      ? Record.filter(
          value,
          (entry, key) => entry !== undefined || !Object.hasOwn(Options.fields, key),
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
          ephemeral_1h_input_tokens: Schema.Natural,
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
  const cacheWrite1h = Result.getOrElse(
    Result.map(extended, (self) => self.anthropic.usage.cache_creation?.ephemeral_1h_input_tokens),
    constUndefined,
  )
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

/**
 * Native model with validated provider configuration, usage accounting and error
 * classification.
 *
 * **Details**
 *
 * This owned handle supports piping and bounded inspection. `toJSON` is a diagnostic
 * projection; use the original fields for protocol values and resource references.
 *
 * @category models
 */
export interface Descriptor extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [DescriptorTypeId]: typeof DescriptorTypeId
  readonly ref: Model.Descriptor['ref']
  readonly model: Model.Descriptor['model']
  readonly contextWindow: number
  readonly maxOutputTokens: number
  readonly configure: (
    options: Model.RequestOptions,
  ) => Effect.Effect<Context.Context<AnthropicLanguageModel.Config>, ModelError>
  readonly usage: NonNullable<Model.Descriptor['usage']>
  readonly classify: NonNullable<Model.Descriptor['classify']>
}

/**
 * Checks the established nominal `Descriptor` marker; it does not validate arbitrary payload fields.
 *
 * @category guards
 */
export const isDescriptor = (u: unknown): u is Descriptor =>
  Predicate.hasProperty(u, DescriptorTypeId) && u[DescriptorTypeId] === DescriptorTypeId

/**
 * Owns a `Descriptor` handle while preserving payload descriptors and exact resource references.
 *
 * **Details**
 *
 * Construction and diagnostics do not evaluate payload accessors. Inspection is a bounded
 * diagnostic projection; read the original fields for protocol values.
 *
 * @category constructors
 */
export const makeDescriptor = (
  input: Omit<
    Descriptor,
    typeof DescriptorTypeId | keyof Pipeable.Pipeable | keyof Inspectable.Inspectable
  >,
): Descriptor => {
  const handle: Descriptor = Object.create(DescriptorProto)
  const descriptors = Object.getOwnPropertyDescriptors(input)
  // The owned protocol cannot be replaced by extra runtime payload keys.
  for (const key of [DescriptorTypeId, 'pipe', 'toJSON', 'toString', Inspectable.NodeInspectSymbol])
    Reflect.deleteProperty(descriptors, key)
  Object.defineProperties(handle, descriptors)
  Object.defineProperty(handle, DescriptorTypeId, { value: DescriptorTypeId, enumerable: false })
  return handle
}

const DescriptorProto = {
  ...Pipeable.Prototype,
  ...Inspectable.BaseProto,
  toJSON(): unknown {
    return {
      _id: 'effect-harness/provider-anthropic/Catalog/Descriptor',
      model: '<LanguageModel>',
      configure: '<function>',
    }
  },
}

/** Captures the standard native client; public user metadata correlates UUID7 requests without private affinity headers. */
const descriptorImpl = Effect.fnUntraced(function* (
  self: Entry,
  provider: string = 'anthropic',
): Effect.fn.Return<Descriptor, ModelError, AnthropicClient.AnthropicClient> {
  yield* Schema.decodeEffect(Schema.toType(Entry))(self).pipe(
    Effect.mapError((cause) =>
      fail('Invalid Anthropic catalogue entry, defaults or thinking budget', cause),
    ),
  )
  const defaults = yield* decode(self.config ?? {})
  yield* Schema.decodeEffect(Schema.toType(Entry))({ ...self, config: defaults }).pipe(
    Effect.mapError((cause) =>
      fail('Invalid Anthropic catalogue entry, defaults or thinking budget', cause),
    ),
  )
  const model = yield* AnthropicLanguageModel.make({
    model: self.modelId,
    config: { ...defaults, max_tokens: defaults.max_tokens ?? self.maxOutputTokens },
  })
  const configure = Effect.fnUntraced(function* (request: Model.RequestOptions) {
    if (request.sessionId !== undefined)
      yield* Schema.decodeEffect(session)(request.sessionId).pipe(
        Effect.mapError((cause) => fail('Conversation sessionId must be UUID7', cause)),
      )
    const supplied = yield* decode(request.options)
    const merged = { ...defaults, ...supplied }
    const max = request.maxTokens ?? merged.max_tokens ?? self.maxOutputTokens
    if (!positive(max) || max > self.maxOutputTokens)
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
      if (self.thinking?._tag === 'adaptive') {
        if (!self.efforts?.some((value) => value === request.thinking))
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
      } else if (self.thinking?._tag === 'budget') {
        const budget = Object.hasOwn(self.thinking.budgets, request.thinking)
          ? self.thinking.budgets[request.thinking]
          : undefined
        if (budget === undefined || budget >= max)
          return yield* fail('No declared thinking budget fits this output limit')
        thinking = { type: 'enabled', budget_tokens: budget }
      } else return yield* fail('This model does not declare thinking support')
    }
    if (effort !== undefined && effort !== null && !self.efforts?.includes(effort))
      return yield* fail('Native effort option is not declared supported by this model')
    if (
      supplied.thinking !== undefined &&
      (supplied.thinking.type !== thinking.type ||
        (supplied.thinking.type === 'enabled' &&
          thinking.type === 'enabled' &&
          supplied.thinking.budget_tokens !== thinking.budget_tokens))
    )
      return yield* fail('Use the pinned thinking field and declared budgets')
    if (request.cache !== undefined && request.cache !== 'none' && self.cache !== true)
      return yield* fail('This model does not declare prompt caching')
    if (merged.cache_control !== undefined && merged.cache_control !== null && self.cache !== true)
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
    return Context.make(
      AnthropicLanguageModel.Config,
      AnthropicLanguageModel.Config.of({
        ...config,
        model: self.modelId,
        max_tokens: max,
      }),
    )
  })
  return makeDescriptor({
    ref: { provider, modelId: self.modelId },
    model,
    contextWindow: self.contextWindow,
    maxOutputTokens: self.maxOutputTokens,
    configure,
    usage: (value, metadata) => usage(value, metadata, self.prices),
    classify: (error) => Model.classify(error, 'anthropic'),
  }) satisfies Model.Descriptor
})
/**
 * Creates a model descriptor from a validated catalogue entry.
 *
 * **Details**
 *
 * Binds native model configuration, usage accounting and error classification to declared
 * capabilities. Unsupported request options fail with ModelError.
 *
 * @category constructors
 */
export const descriptor: {
  (provider?: string): (self: Entry) => ReturnType<typeof descriptorImpl>
  (self: Entry, provider?: string): ReturnType<typeof descriptorImpl>
} = dual(
  Predicate.mapInput(Predicate.isObjectOrArray, (args: IArguments) => args[0]),
  descriptorImpl,
)

/**
 * Provides a Model.Catalog from declared provider model entries.
 *
 * **Details**
 *
 * Validates entries and resolves only the registered provider/model pairs. Duplicate model
 * IDs are rejected; unknown references fail with ModelNoModelError.
 *
 * **Gotchas**
 *
 * Supply the required native client or CLI services. Catalogue construction does not
 * authorize a remote account.
 *
 * @category layers
 */
export const layer = (options: {
  readonly models: ReadonlyArray<Entry>
  readonly provider?: string | undefined
}): Layer.Layer<Model.Catalog, ModelError, AnthropicClient.AnthropicClient> =>
  Layer.effect(Model.Catalog)(
    Effect.gen(function* () {
      if (
        Array.dedupe(options.models.map((entry) => entry.modelId)).length !== options.models.length
      )
        return yield* fail('Duplicate Anthropic catalogue model IDs')
      const entries = yield* Effect.forEach(options.models, (entry) =>
        descriptor(entry, options.provider),
      )
      const byId = HashMap.fromIterable(entries.map((entry) => [entry.ref.modelId, entry]))
      return Model.Catalog.of({
        resolve: (ref) => {
          const found = HashMap.get(byId, ref.modelId)
          return Effect.fromOption(
            Option.filter(found, (self) => self.ref.provider === ref.provider),
            () =>
              new ModelError({
                reason: new ModelNoModelError({
                  message: 'Anthropic model is not available in this catalogue',
                }),
              }),
          )
        },
      })
    }),
  )
// effect-nit-allow P3-provide-vs-provideMerge: the public catalogue
// exposes the exact captured native client alongside its descriptors, so callers
// share one transport lifecycle and retain per-request native Config injection.
/**
 * Provides a declared model catalogue and native API-key client.
 *
 * **Details**
 *
 * Consumes HttpClient and a Redacted API key. Model entries still supply limits and
 * supported request capabilities.
 *
 * @category layers
 */
export const layerApiKey = (
  options: AnthropicClient.Options & {
    readonly apiKey: Redacted.Redacted<string>
    readonly models: ReadonlyArray<Entry>
    readonly provider?: string | undefined
  },
): Layer.Layer<
  Model.Catalog | AnthropicClient.AnthropicClient,
  ModelError,
  HttpClient.HttpClient
> => layer(options).pipe(Layer.provideMerge(AnthropicClient.layer(options)))

/**
 * Resolves all layer options through the caller's ConfigProvider.
 *
 * @category layers
 */
export const layerConfig = (
  config: Config.Wrap<NonNullable<Parameters<typeof layer>[0]>>,
): Layer.Layer<Model.Catalog, ModelError | Config.ConfigError, AnthropicClient.AnthropicClient> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layer(yield* Config.unwrap(config))
    }),
  )

/**
 * Resolves all layerApiKey options through the caller's ConfigProvider.
 *
 * @category layers
 */
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

/** Checks the decoded Prices contract without decoding or coercing input.
 * @category guards
 */
export const isPrices: (u: unknown) => u is Prices = Schema.is(Schema.toType(Prices))

/** Checks the decoded Entry contract without decoding or coercing input.
 * @category guards
 */
export const isEntry: (u: unknown) => u is Entry = Schema.is(Schema.toType(Entry))
