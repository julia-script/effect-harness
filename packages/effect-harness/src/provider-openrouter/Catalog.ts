/** Public OpenRouter metadata, independent of API-key authentication. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as ModelCatalog from '../ModelCatalog.js'
import * as ModelInfo from '../ModelInfo.js'
import { ModelCatalogError } from '../ModelCatalogError.js'

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))
const Entry = Schema.Struct({
  ...ModelInfo.SummarySchema.fields,
  architecture: Schema.Struct({
    input_modalities: Schema.Array(Schema.String),
    output_modalities: Schema.Array(Schema.String),
  }),
  supported_parameters: Schema.Array(Schema.String),
  context_length: Schema.optionalKey(Schema.NullOr(PositiveInt)),
  top_provider: Schema.optionalKey(
    Schema.NullOr(
      Schema.Struct({ max_completion_tokens: Schema.optionalKey(Schema.NullOr(PositiveInt)) }),
    ),
  ),
  pricing: Schema.optionalKey(
    Schema.Struct({ prompt: Schema.NonEmptyString, completion: Schema.NonEmptyString }),
  ),
  reasoning: Schema.optionalKey(
    Schema.NullOr(
      Schema.Struct({
        supported_efforts: Schema.optionalKey(Schema.NullOr(Schema.Array(Schema.NonEmptyString))),
        default_effort: Schema.optionalKey(Schema.NullOr(Schema.NonEmptyString)),
        default_enabled: Schema.optionalKey(Schema.Boolean),
        mandatory: Schema.optionalKey(Schema.Boolean),
      }),
    ),
  ),
})
const Page = Schema.Struct({ data: Schema.Array(ModelInfo.SummarySchema) })
const Detail = Schema.Struct({ data: Entry })

const invalid = (message: string, modelId?: string) =>
  new ModelCatalogError({
    reason: 'invalidMetadata',
    provider: 'openrouter',
    message,
    ...(modelId === undefined ? {} : { modelId }),
  })

const request = Effect.fnUntraced(function* (path: string, modelId?: string) {
  const client = yield* HttpClient.HttpClient
  const response = yield* client.execute(
    HttpClientRequest.get(`https://openrouter.ai/api/v1/${path}`),
  )
  if (response.status === 404 && modelId !== undefined)
    return yield* new ModelCatalogError({
      reason: 'modelNotFound',
      provider: 'openrouter',
      modelId,
      message: 'Unknown OpenRouter model',
    })
  return yield* HttpClientResponse.filterStatusOk(response)
})

const normalize = Effect.fnUntraced(function* (entry: typeof Entry.Type, modelId: string) {
  if (!entry.architecture.output_modalities.includes('text'))
    return yield* invalid('The compatible transport requires a text-output model', modelId)
  const reasoning = entry.reasoning
  // OpenRouter documents null as accepting all gateway efforts, and omission as
  // no effort selector. Do not infer effort support from the model's name.
  const declared =
    reasoning?.supported_efforts === null
      ? ['max', 'xhigh', 'high', 'medium', 'low', 'minimal', 'none']
      : (reasoning?.supported_efforts ?? [])
  const efforts = declared.filter((effort) => !(reasoning?.mandatory && effort === 'none'))
  const defaultEffort = reasoning?.default_effort
  // A default effort describes what to use when enabled. Sending it for a model
  // whose reasoning is disabled by default would silently turn reasoning on.
  const useDefault =
    defaultEffort != null && (reasoning?.default_enabled !== false || defaultEffort === 'none')
  let pricing: ModelInfo.ModelInfo['pricing']
  if (entry.pricing !== undefined) {
    const inputToken = Number(entry.pricing.prompt)
    const outputToken = Number(entry.pricing.completion)
    if (
      entry.pricing.prompt.trim() === '' ||
      entry.pricing.completion.trim() === '' ||
      !Number.isFinite(inputToken) ||
      !Number.isFinite(outputToken)
    )
      return yield* invalid('Invalid OpenRouter token pricing', modelId)
    // Dynamic routers report negative sentinel prices, not free inference.
    if (inputToken >= 0 && outputToken >= 0)
      pricing = { inputToken, outputToken, currency: 'USD', source: 'openrouter/models' }
  }
  const parameters = entry.supported_parameters
  return yield* ModelInfo.decode({
    // Aliases and routing suffixes can return the base entry. Preserve the
    // requested ID so inference retains the user's routing selection.
    id: modelId,
    name: entry.name,
    capabilities: {
      tools: parameters.includes('tools'),
      images: entry.architecture.input_modalities.includes('image'),
      reasoning:
        reasoning != null ||
        parameters.includes('reasoning') ||
        parameters.includes('reasoning_effort'),
      structuredOutput: parameters.includes('structured_outputs'),
      temperature: parameters.includes('temperature'),
    },
    ...(entry.context_length == null ? {} : { contextWindow: entry.context_length }),
    ...(entry.top_provider?.max_completion_tokens == null
      ? {}
      : { maxOutputTokens: entry.top_provider.max_completion_tokens }),
    ...(pricing === undefined ? {} : { pricing }),
    ...(efforts.length === 0
      ? {}
      : {
          reasoning: {
            effort: {
              values: efforts,
              ...(useDefault ? { default: defaultEffort } : {}),
            },
          },
        }),
  })
})

/** Fetch the public text-model list, then search and paginate it locally. No secrets are read. */
export const list = Effect.fnUntraced(function* (input: ModelCatalog.ListOptions = {}) {
  const response = yield* request('models')
  const page = yield* HttpClientResponse.schemaBodyJson(Page)(response).pipe(
    Effect.mapError(() => invalid('Invalid OpenRouter model list')),
  )
  return yield* ModelCatalog.fromModels(page.data).list(input)
})

/** Fetch one model's public details, preserving aliases and routing suffixes for inference. */
export const get = Effect.fnUntraced(function* (modelId: string) {
  const segments = modelId.split('/')
  if (
    segments.length !== 2 ||
    segments.some((segment) => segment === '' || segment === '.' || segment === '..')
  )
    return yield* new ModelCatalogError({
      reason: 'unsupportedOptions',
      provider: 'openrouter',
      modelId,
      message: 'OpenRouter model IDs must have an author and slug',
    })
  const response = yield* request(`model/${segments.map(encodeURIComponent).join('/')}`, modelId)
  const detail = yield* HttpClientResponse.schemaBodyJson(Detail)(response).pipe(
    Effect.mapError(() => invalid('Invalid OpenRouter model details', modelId)),
  )
  return yield* normalize(detail.data, modelId)
})
