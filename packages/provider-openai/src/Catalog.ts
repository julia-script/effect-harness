import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as OpenAiLanguageModel from '@effect/ai-openai/OpenAiLanguageModel'
import * as OpenAiSchema from '@effect/ai-openai/OpenAiSchema'
import * as Model from '@effect-harness/harness/Model'
import { ModelError, ModelNoModel, ModelUnsupported } from '@effect-harness/harness/Error'
import * as Usage from '@effect-harness/harness/Usage'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import type * as Response from 'effect/ai/Response'
import type * as Redacted from 'effect/Redacted'
import * as Provider from './LanguageModel.ts'

/** Caller-declared USD rates per million tokens; no prices or model limits are guessed. */
export interface Prices {
  readonly input: number
  readonly output: number
  readonly cacheRead: number
  readonly cacheWrite: number
}
export interface Entry {
  readonly modelId: string
  readonly contextWindow: number
  readonly maxOutputTokens: number
  readonly reasoningEfforts?:
    | ReadonlyArray<'none' | 'minimal' | 'low' | 'medium' | 'high' | 'xhigh' | 'max'>
    | undefined
  readonly cache?: 'prompt-cache-options' | undefined
  readonly config?: Omit<typeof OpenAiLanguageModel.Config.Service, 'model'> | undefined
  readonly prices?: Prices | undefined
}
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
const decode = (value: unknown) =>
  Schema.decodeUnknownEffect(Options, { onExcessProperty: 'error' })(value).pipe(
    Effect.mapError((cause) => fail('Unsupported or invalid OpenAI request options', cause)),
  )
const session = Schema.String.check(Schema.isUUID(7))
const positive = (value: number) => Number.isSafeInteger(value) && value > 0
const validate = (entry: Entry) =>
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
    const config = yield* decode(entry.config ?? {})
    if (
      config.max_output_tokens !== undefined &&
      config.max_output_tokens !== null &&
      (!positive(config.max_output_tokens) || config.max_output_tokens > entry.maxOutputTokens)
    )
      return yield* fail('Default output limit exceeds the declared model output cap')
    return config
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

/** Captures the native client now; configuration pins each request to this exact model ID. */
export const descriptor = (
  entry: Entry,
  options?: { readonly provider?: string | undefined; readonly account?: boolean | undefined },
) =>
  Effect.gen(function* () {
    const defaults = yield* validate(entry)
    const account = options?.account === true
    if (account && defaults.store === true)
      return yield* fail('ChatGPT account Responses require store:false')
    const model = yield* Provider.make({
      model: entry.modelId,
      config: {
        ...defaults,
        max_output_tokens: defaults.max_output_tokens ?? entry.maxOutputTokens,
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
      const max = request.maxTokens ?? merged.max_output_tokens ?? entry.maxOutputTokens
      if (!positive(max) || max > entry.maxOutputTokens)
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
        if (entry.reasoningEfforts?.includes('none')) effort = 'none'
      }
      if (effort !== undefined && !entry.reasoningEfforts?.some((value) => value === effort))
        return yield* fail('Requested reasoning effort is not declared supported by this model')
      if (supplied.reasoning?.effort !== undefined && supplied.reasoning.effort !== effort)
        return yield* fail('Use the pinned thinking field for reasoning effort')
      if (effort === undefined && supplied.reasoning !== undefined)
        return yield* fail('Reasoning options require a declared reasoning capability')
      if (request.cache !== undefined && entry.cache !== 'prompt-cache-options')
        return yield* fail('This model does not declare native prompt-cache-options support')
      if (merged.prompt_cache_options !== undefined && entry.cache !== 'prompt-cache-options')
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
      return Context.make(OpenAiLanguageModel.Config, { ...config, model: entry.modelId })
    })
    return {
      ref: {
        provider: options?.provider ?? (account ? 'openai-chatgpt' : 'openai'),
        modelId: entry.modelId,
      },
      model,
      contextWindow: entry.contextWindow,
      maxOutputTokens: entry.maxOutputTokens,
      configure,
      usage: (value, _metadata) => priced(value, entry.prices),
      classify: (error) => Model.classify(error, 'openai'),
    } satisfies Model.Descriptor
  })

/** Normal Catalog Layer requiring an already-selected standard OpenAiClient. */
export const layer = (options: {
  readonly models: ReadonlyArray<Entry>
  readonly provider?: string | undefined
  readonly account?: boolean | undefined
}) =>
  Layer.effect(Model.Catalog)(
    Effect.gen(function* () {
      if (new Set(options.models.map((entry) => entry.modelId)).size !== options.models.length)
        return yield* fail('Duplicate OpenAI catalogue model IDs')
      const entries = yield* Effect.forEach(options.models, (entry) => descriptor(entry, options))
      const byId = new Map(entries.map((entry) => [entry.ref.modelId, entry]))
      return Model.Catalog.of({
        resolve: (ref) => {
          const found = byId.get(ref.modelId)
          return found !== undefined && found.ref.provider === ref.provider
            ? Effect.succeed(found)
            : Effect.fail(
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
export const layerApiKey = (
  options: OpenAiClient.Options & {
    readonly apiKey: Redacted.Redacted<string>
    readonly models: ReadonlyArray<Entry>
    readonly provider?: string | undefined
  },
) => layer(options).pipe(Layer.provideMerge(OpenAiClient.layer(options)))
export const layerChatGpt = (options: {
  readonly account: string
  readonly models: ReadonlyArray<Entry>
  readonly provider?: string | undefined
}) =>
  layer({ ...options, account: true }).pipe(
    Layer.provideMerge(Provider.layerChatGptClient({ account: options.account })),
  )
