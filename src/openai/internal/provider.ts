/**
 * The OpenAI provider over `@effect/ai-openai`, which stays an implementation detail: each request
 * builds a client with the key and a language model configured from the request's options.
 */
import { OpenAiClient, OpenAiLanguageModel } from '@effect/ai-openai'
import { Effect, Schema } from 'effect'
import { HttpClientRequest } from 'effect/http'
import { stored } from '../../model/internal/apiKey.ts'
import { effort, find } from '../../model/internal/catalog.ts'
import type { Entry } from '../../model/internal/catalog.ts'
import { getJson, listed } from '../../model/internal/listing.ts'
import * as LanguageModelProvider from '../../model/LanguageModelProvider.ts'
import type { Info, Request } from '../../model/Model.ts'
import type { Options } from '../OpenAi.ts'
import { models as catalog } from './catalog.ts'

const Effort = Schema.Literals(['none', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max'])
const isEffort = Schema.is(Effort)

/** The Responses API options for `model`, with its entry in `models` for efforts and temperature. */
export const config = (model: Info, request: Request, models: ReadonlyArray<Entry> = catalog) => {
  const options = request.options ?? {}
  const level = options.reasoning
  const entry = find(models, model.id)
  const named = level === undefined ? undefined : (effort(entry, level) ?? level)
  return {
    ...(options.maxTokens === undefined ? {} : { max_output_tokens: options.maxTokens }),
    ...(options.temperature === undefined || entry?.temperature === false
      ? {}
      : { temperature: options.temperature }),
    ...(model.reasoning && named !== undefined && isEffort(named)
      ? { reasoning: { effort: named } }
      : {}),
  }
}

const DEFAULT_URL = 'https://api.openai.com/v1'

const ModelList = Schema.Struct({ data: Schema.Array(Schema.Struct({ id: Schema.String })) })

export const make = (options: Options) => {
  const name = options.name ?? 'openai'
  const apiKey = options.apiKey === undefined ? stored(name) : Effect.succeed(options.apiKey)
  const provider = LanguageModelProvider.make({
    name,
    api: 'openai-responses',
    models: options.models ?? catalog,
    languageModel: (model, request) =>
      Effect.flatMap(apiKey, (key) =>
        Effect.flatMap(
          OpenAiClient.make({
            apiKey: key,
            ...(options.apiUrl === undefined ? {} : { apiUrl: options.apiUrl }),
          }),
          (client) =>
            OpenAiLanguageModel.make({ model: model.id, config: config(model, request) }).pipe(
              Effect.provideService(OpenAiClient.OpenAiClient, client),
            ),
        ),
      ),
  })
  const listModels = Effect.flatMap(apiKey, (key) =>
    getJson(
      'The OpenAI model list',
      HttpClientRequest.get(`${options.apiUrl ?? DEFAULT_URL}/models`).pipe(
        HttpClientRequest.bearerToken(key),
      ),
      ModelList,
    ),
  ).pipe(
    Effect.map(({ data }) => data.map((found) => listed(name, provider.models, catalog, found))),
  )
  return { ...provider, listModels }
}
