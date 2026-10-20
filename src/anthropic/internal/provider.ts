/**
 * The Anthropic provider over `@effect/ai-anthropic`, which stays an implementation detail: each
 * request builds a client with the key and a language model configured from the request's options.
 * Thinking follows pi-ai: an effort for adaptive models, a token budget for the others.
 */
import { AnthropicClient, AnthropicLanguageModel } from '@effect/ai-anthropic'
import { Effect, Redacted, Schema } from 'effect'
import { HttpClientRequest } from 'effect/http'
import type { HttpClient } from 'effect/http'
import { stored } from '../../model/internal/apiKey.ts'
import { effort, find } from '../../model/internal/catalog.ts'
import type { Entry } from '../../model/internal/catalog.ts'
import { getJson, listed } from '../../model/internal/listing.ts'
import * as LanguageModelProvider from '../../model/LanguageModelProvider.ts'
import type { ThinkingLevel } from '../../model/Message.ts'
import type { Info, Request } from '../../model/Model.ts'
import type { ModelError } from '../../model/ModelError.ts'
import type { Options } from '../Anthropic.ts'
import { models as catalog } from './catalog.ts'

/** pi-ai's default budgets; `xhigh` and `max` take the `high` budget. */
const BUDGETS = { minimal: 1024, low: 2048, medium: 8192, high: 16_384 }

/** Tokens always left for the answer when the budget shares the response ceiling. */
const ANSWER_TOKENS = 1024

/** The effort the API types take: `minimal` rises to `low`, `xhigh` and `max` fall to `high`. */
const adaptiveEffort = (
  entry: Entry | undefined,
  level: ThinkingLevel,
): 'low' | 'medium' | 'high' => {
  const named = effort(entry, level) ?? level
  if (named === 'low' || named === 'medium') return named
  return named === 'minimal' ? 'low' : 'high'
}

const budget = (level: ThinkingLevel, ceiling: number) => {
  const wanted = level === 'xhigh' || level === 'max' ? BUDGETS.high : BUDGETS[level]
  return Math.min(wanted, Math.max(0, ceiling - ANSWER_TOKENS))
}

/** The request options Anthropic takes, for `model`. */
export const config = (model: Info, request: Request) => {
  const options = request.options ?? {}
  const entry = find(catalog, model.id)
  const maxTokens = options.maxTokens ?? model.maxTokens
  const level = model.reasoning ? options.reasoning : undefined
  const thinking = () => {
    if (level === undefined) return {}
    if (entry?.adaptive === true) {
      return {
        thinking: { type: 'adaptive' as const },
        output_config: { effort: adaptiveEffort(entry, level) },
      }
    }
    return { thinking: { type: 'enabled' as const, budget_tokens: budget(level, maxTokens) } }
  }
  return {
    max_tokens: maxTokens,
    ...(options.temperature === undefined || entry?.temperature === false || level !== undefined
      ? {}
      : { temperature: options.temperature }),
    ...thinking(),
  }
}

const DEFAULT_URL = 'https://api.anthropic.com'

const ModelPage = Schema.Struct({
  data: Schema.Array(Schema.Struct({ id: Schema.String, display_name: Schema.String })),
  has_more: Schema.Boolean,
  last_id: Schema.NullOr(Schema.String),
})
type Found = (typeof ModelPage.Type)['data'][number]

/** Every page of the model list, from `after` on. */
const pages = (
  apiUrl: string,
  key: Redacted.Redacted,
  after?: string,
): Effect.Effect<ReadonlyArray<Found>, ModelError, HttpClient.HttpClient> =>
  getJson(
    'The Anthropic model list',
    HttpClientRequest.get(`${apiUrl}/v1/models`).pipe(
      HttpClientRequest.setUrlParams({
        limit: '1000',
        ...(after === undefined ? {} : { after_id: after }),
      }),
      HttpClientRequest.setHeaders({
        'x-api-key': Redacted.value(key),
        'anthropic-version': '2023-06-01',
      }),
    ),
    ModelPage,
  ).pipe(
    Effect.flatMap((page) =>
      page.has_more && page.last_id !== null
        ? Effect.map(pages(apiUrl, key, page.last_id), (rest) => [...page.data, ...rest])
        : Effect.succeed(page.data),
    ),
  )

export const make = (options: Options) => {
  const name = options.name ?? 'anthropic'
  const apiKey = options.apiKey === undefined ? stored(name) : Effect.succeed(options.apiKey)
  const provider = LanguageModelProvider.make({
    name,
    api: 'anthropic-messages',
    models: options.models ?? catalog,
    languageModel: (model, request) =>
      Effect.flatMap(apiKey, (key) =>
        Effect.flatMap(
          AnthropicClient.make({
            apiKey: key,
            ...(options.apiUrl === undefined ? {} : { apiUrl: options.apiUrl }),
          }),
          (client) =>
            AnthropicLanguageModel.make({ model: model.id, config: config(model, request) }).pipe(
              Effect.provideService(AnthropicClient.AnthropicClient, client),
            ),
        ),
      ),
  })
  const listModels = Effect.flatMap(apiKey, (key) =>
    pages(options.apiUrl ?? DEFAULT_URL, key),
  ).pipe(
    Effect.map((found) =>
      found.map((model) =>
        listed(name, provider.models, catalog, { id: model.id, name: model.display_name }),
      ),
    ),
  )
  return { ...provider, listModels }
}
