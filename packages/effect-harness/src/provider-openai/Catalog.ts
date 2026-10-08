/**
 * Validated model catalogues with pinned request configuration and usage accounting.
 */
import { dual } from 'effect/Function'
import * as Arr from 'effect/Array'
import * as HashMap from 'effect/HashMap'
import * as Option from 'effect/Option'
import type * as HttpClient from 'effect/http/HttpClient'
import type { ChatGpt } from './ChatGpt.ts'
import * as Config from 'effect/Config'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as OpenAiLanguageModel from '@effect/ai-openai/OpenAiLanguageModel'
import * as OpenAiSchema from '@effect/ai-openai/OpenAiSchema'
import * as Model from 'effect-harness/Model'
import { ModelError, ModelNoModel, ModelUnsupported } from 'effect-harness/ModelError'
import * as Usage from 'effect-harness/Usage'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import type * as Response from 'effect/ai/Response'
import type * as Redacted from 'effect/Redacted'
// effect-review-allow P9-namespace-alias-equals-module: @effect/ai-openai/OpenAiLanguageModel and ./OpenAiLanguageModel.ts both bind OpenAiLanguageModel; openAiLanguageModel distinguishes the owned model constructor.
import * as openAiLanguageModel from './OpenAiLanguageModel.ts'
import * as ChatGptClient from './ChatGptClient.ts'

const fail = (message: string, cause?: unknown) =>
  new ModelError({
    reason: new ModelUnsupported({ message, ...(cause === undefined ? {} : { cause }) }),
  })
const fields = OpenAiSchema.CreateResponse.fields
const Options = Schema.Struct({
  metadata: fields.metadata,
  top_logprobs: fields.top_logprobs,
  temperature: fields.temperature,
  top_p: fields.top_p,
  user: fields.user,
  prompt_cache_key: fields.prompt_cache_key,
  prompt_cache_options: fields.prompt_cache_options,
  service_tier: fields.service_tier,
  reasoning: fields.reasoning,
  max_output_tokens: fields.max_output_tokens,
  max_tool_calls: fields.max_tool_calls,
  text: Schema.optional(
    Schema.Struct({ verbosity: Schema.optional(Schema.Literals(['low', 'medium', 'high'])) }),
  ),
  truncation: fields.truncation,
  include: fields.include,
  store: fields.store,
  seed: fields.seed,
  strictJsonSchema: Schema.optional(Schema.Boolean),
  useItemReferences: Schema.optional(Schema.Boolean),
  fileIdPrefixes: Schema.optional(Schema.Array(Schema.String)),
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
  reasoningEfforts: Schema.optional(
    Schema.Array(Schema.Literals(['none', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max'])),
  ),
  cache: Schema.optional(Schema.Literal('prompt-cache-options')),
  config: Schema.optional(Options),
  prices: Schema.optional(Prices),
}).check(
  Schema.makeFilter((entry) => entry.maxOutputTokens <= entry.contextWindow),
  Schema.makeFilter(
    (entry) =>
      entry.config?.max_output_tokens == null ||
      (Number.isSafeInteger(entry.config.max_output_tokens) &&
        entry.config.max_output_tokens > 0 &&
        entry.config.max_output_tokens <= entry.maxOutputTokens),
  ),
)
/**
 * Caller-declared model identity, token limits and supported request capabilities.
 *
 * @category models
 */
export type Entry = typeof Entry.Type
const decode = (value: unknown) =>
  Schema.decodeUnknownEffect(Options, { onExcessProperty: 'error' })(value).pipe(
    Effect.mapError((cause) => fail('Unsupported or invalid OpenAI request options', cause)),
  )
const session = Schema.String.check(Schema.isUUID(7))
const positive = (value: number) => Number.isSafeInteger(value) && value > 0
const validate = Effect.fnUntraced(function* (entry: Entry) {
  yield* Schema.decodeEffect(Schema.toType(Entry))(entry).pipe(
    Effect.mapError((cause) => fail('Invalid OpenAI catalogue entry or defaults', cause)),
  )
  const defaults = yield* decode(entry.config ?? {})
  yield* Schema.decodeEffect(Schema.toType(Entry))({ ...entry, config: defaults }).pipe(
    Effect.mapError((cause) => fail('Invalid OpenAI catalogue entry or defaults', cause)),
  )
  return defaults
})
const priced = (value: Response.Usage, prices?: Prices): Usage.Usage => {
  const result = Usage.fromResponse(value)
  if (prices === undefined) return result
  const knownInput =
    value.inputTokens.uncached !== undefined ||
    (value.inputTokens.total !== undefined &&
      value.inputTokens.cacheRead !== undefined &&
      value.inputTokens.cacheWrite !== undefined)
  const input = knownInput ? (result.input * prices.input) / 1_000_000 : 0
  const output = (result.output * prices.output) / 1_000_000
  const cacheRead = (result.cacheRead * prices.cacheRead) / 1_000_000
  const cacheWrite = (result.cacheWrite * prices.cacheWrite) / 1_000_000
  const known =
    (knownInput || prices.input === 0) &&
    (value.outputTokens.total !== undefined || prices.output === 0) &&
    (value.inputTokens.cacheRead !== undefined || prices.cacheRead === 0) &&
    (value.inputTokens.cacheWrite !== undefined || prices.cacheWrite === 0)
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
 * @category models
 */
export interface Descriptor {
  readonly ref: { provider: string; modelId: string }
  readonly model: Model.Descriptor['model']
  readonly contextWindow: number
  readonly maxOutputTokens: number
  readonly configure: (
    options: Model.RequestOptions,
  ) => Effect.Effect<Context.Context<OpenAiLanguageModel.Config>, ModelError>
  readonly usage: NonNullable<Model.Descriptor['usage']>
  readonly classify: NonNullable<Model.Descriptor['classify']>
}

/** Captures the native client now; configuration pins each request to this exact model ID. */
const descriptorImpl = Effect.fnUntraced(function* (
  self: Entry,
  options?: { readonly provider?: string | undefined; readonly account?: boolean | undefined },
): Effect.fn.Return<Descriptor, ModelError, OpenAiClient.OpenAiClient> {
  const defaults = yield* validate(self)
  const account = options?.account === true
  if (account && defaults.store === true)
    return yield* fail('ChatGPT account Responses require store:false')
  const model = yield* openAiLanguageModel.make({
    model: self.modelId,
    config: {
      ...defaults,
      max_output_tokens: defaults.max_output_tokens ?? self.maxOutputTokens,
      ...(account ? { store: false, useItemReferences: false } : {}),
    },
  })
  const configure = Effect.fnUntraced(function* (request: Model.RequestOptions) {
    if (request.sessionId !== undefined)
      yield* Schema.decodeEffect(session)(request.sessionId).pipe(
        Effect.mapError((cause) => fail('Conversation sessionId must be UUID7', cause)),
      )
    const supplied = yield* decode(request.options)
    const merged = { ...defaults, ...supplied }
    const max = request.maxTokens ?? merged.max_output_tokens ?? self.maxOutputTokens
    if (!positive(max) || max > self.maxOutputTokens)
      return yield* fail(
        'maxTokens must be a positive integer within the declared model output limit',
      )
    if (
      request.maxTokens !== undefined &&
      supplied.max_output_tokens !== undefined &&
      request.maxTokens !== supplied.max_output_tokens
    )
      return yield* fail('Conflicting OpenAI output token limits')
    let effort: string | undefined = request.thinking
    if (request.thinking === 'off') {
      effort = undefined
      if (self.reasoningEfforts?.includes('none')) effort = 'none'
    }
    if (effort !== undefined && !self.reasoningEfforts?.some((value) => value === effort))
      return yield* fail('Requested reasoning effort is not declared supported by this model')
    if (supplied.reasoning?.effort !== undefined && supplied.reasoning.effort !== effort)
      return yield* fail('Use the pinned thinking field for reasoning effort')
    if (effort === undefined && supplied.reasoning !== undefined)
      return yield* fail('Reasoning options require a declared reasoning capability')
    if (request.cache !== undefined && self.cache !== 'prompt-cache-options')
      return yield* fail('This model does not declare native prompt-cache-options support')
    if (merged.prompt_cache_options !== undefined && self.cache !== 'prompt-cache-options')
      return yield* fail('Native cache options are not declared supported')
    let cacheOptions = merged.prompt_cache_options
    if (request.cache === 'none') cacheOptions = { mode: 'explicit' }
    else if (request.cache === 'long') cacheOptions = { mode: 'implicit', ttl: '30m' }
    else if (request.cache === 'short') cacheOptions = { mode: 'implicit' }
    if (request.cache !== undefined && supplied.prompt_cache_options !== undefined)
      return yield* fail('Specify either cache or native prompt_cache_options')
    if (
      request.sessionId !== undefined &&
      supplied.prompt_cache_key !== undefined &&
      supplied.prompt_cache_key !== request.sessionId
    )
      return yield* fail('prompt_cache_key must match the pinned conversation sessionId')
    if (account && merged.store === true)
      return yield* fail('ChatGPT account Responses require store:false')
    const config = yield* decode({
      ...merged,
      max_output_tokens: max,
      reasoning: effort === undefined ? undefined : { ...merged.reasoning, effort },
      prompt_cache_options: cacheOptions,
      prompt_cache_key:
        request.cache === 'none' ? undefined : (request.sessionId ?? merged.prompt_cache_key),
      ...(account ? { store: false, useItemReferences: false } : {}),
    })
    return Context.make(OpenAiLanguageModel.Config, { ...config, model: self.modelId })
  })
  return {
    ref: {
      provider: options?.provider ?? (account ? 'openai-chatgpt' : 'openai'),
      modelId: self.modelId,
    },
    model,
    contextWindow: self.contextWindow,
    maxOutputTokens: self.maxOutputTokens,
    configure,
    usage: (value, _metadata) => priced(value, self.prices),
    classify: (error) => Model.classify(error, 'openai'),
  } satisfies Model.Descriptor
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
  (): (self: Entry) => ReturnType<typeof descriptorImpl>
  (
    options: NonNullable<Parameters<typeof descriptorImpl>[1]> &
      (
        | { readonly provider: NonNullable<Parameters<typeof descriptorImpl>[1]>['provider'] }
        | { readonly account: NonNullable<Parameters<typeof descriptorImpl>[1]>['account'] }
      ),
  ): (self: Entry) => ReturnType<typeof descriptorImpl>
  (self: Entry, options?: Parameters<typeof descriptorImpl>[1]): ReturnType<typeof descriptorImpl>
  // Empty objects are malformed subjects, not meaningful curried options.
  // The data-last form requires a known supplied option key, or no argument for defaults.
} = dual(
  (args) =>
    args.length >= 2 ||
    (args.length === 1 &&
      args[0] !== undefined &&
      !(
        typeof args[0] === 'object' &&
        args[0] !== null &&
        !('modelId' in args[0]) &&
        ('provider' in args[0] || 'account' in args[0])
      )),
  descriptorImpl,
)

/**
 * Provides a Model.Catalog from declared provider model entries.
 *
 * **Details**
 *
 * Validates entries and resolves only the registered provider/model pairs. Duplicate model
 * IDs are rejected; unknown references fail with ModelNoModel.
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
  readonly account?: boolean | undefined
}): Layer.Layer<Model.Catalog, ModelError, OpenAiClient.OpenAiClient> =>
  Layer.effect(Model.Catalog)(
    Effect.gen(function* () {
      if (Arr.dedupe(options.models.map((entry) => entry.modelId)).length !== options.models.length)
        return yield* fail('Duplicate OpenAI catalogue model IDs')
      const entries = yield* Effect.forEach(options.models, (entry) => descriptor(entry, options))
      const byId = HashMap.fromIterable(entries.map((entry) => [entry.ref.modelId, entry]))
      return Model.Catalog.of({
        resolve: (ref) => {
          const found = HashMap.get(byId, ref.modelId)
          return Effect.fromOption(
            Option.filter(found, (self) => self.ref.provider === ref.provider),
            () =>
              new ModelError({
                reason: new ModelNoModel({
                  message: 'OpenAI model is not available in this catalogue',
                }),
              }),
          )
        },
      })
    }),
  )
// effect-review-allow P4-layer-provide-vs-provideMerge: the public catalogue
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
  options: OpenAiClient.Options & {
    readonly apiKey: Redacted.Redacted<string>
    readonly models: ReadonlyArray<Entry>
    readonly provider?: string | undefined
  },
): Layer.Layer<Model.Catalog | OpenAiClient.OpenAiClient, ModelError, HttpClient.HttpClient> =>
  layer(options).pipe(Layer.provideMerge(OpenAiClient.layer(options)))
// effect-review-allow P4-layer-provide-vs-provideMerge: catalogue descriptors and callers share the exact native account client and its lifecycle; per-call Config remains open.
/**
 * Provides a declared catalogue through an authorized ChatGPT account.
 *
 * **Details**
 *
 * Consumes ChatGpt and HttpClient and exposes the captured OpenAI client. Use
 * models(account) to discover visibility, then declare capabilities and limits for selected
 * entries.
 *
 * @category layers
 */
export const layerChatGpt = (options: {
  readonly account: string
  readonly models: ReadonlyArray<Entry>
  readonly provider?: string | undefined
}): Layer.Layer<
  Model.Catalog | OpenAiClient.OpenAiClient,
  ModelError,
  ChatGpt | HttpClient.HttpClient
> =>
  layer({ ...options, account: true }).pipe(
    Layer.provideMerge(ChatGptClient.layer({ account: options.account })),
  )

/**
 * Resolves all layer options through the caller's ConfigProvider.
 *
 * @category layers
 */
export const layerConfig = (
  config: Config.Wrap<NonNullable<Parameters<typeof layer>[0]>>,
): Layer.Layer<Model.Catalog, ModelError | Config.ConfigError, OpenAiClient.OpenAiClient> =>
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
  Model.Catalog | OpenAiClient.OpenAiClient,
  ModelError | Config.ConfigError,
  HttpClient.HttpClient
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layerApiKey(yield* Config.unwrap(config))
    }),
  )

/**
 * Resolves all layerChatGpt options through the caller's ConfigProvider.
 *
 * @category layers
 */
export const layerChatGptConfig = (
  config: Config.Wrap<NonNullable<Parameters<typeof layerChatGpt>[0]>>,
): Layer.Layer<
  Model.Catalog | OpenAiClient.OpenAiClient,
  ModelError | Config.ConfigError,
  ChatGpt | HttpClient.HttpClient
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layerChatGpt(yield* Config.unwrap(config))
    }),
  )
