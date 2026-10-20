/**
 * The Codex model list, from `GET /codex/models` as the Codex CLI reads it: the models the account
 * reaches, each with its reasoning efforts and service tiers.
 */
import { Effect, Schema } from 'effect'
import { HttpClientRequest } from 'effect/http'
import { getJson } from '../../../model/internal/listing.ts'
import { ModelThinkingLevel } from '../../../model/Message.ts'
import { ServiceTier } from '../../../model/Model.ts'
import type { Info, Listed } from '../../../model/Model.ts'

/** The Codex CLI version the list is asked for; the backend lists what that version runs. */
const CLIENT_VERSION = '0.163.0'

/** The fields read from a model in the list. */
const CodexModel = Schema.Struct({
  slug: Schema.String,
  display_name: Schema.String,
  description: Schema.optionalKey(Schema.NullOr(Schema.String)),
  default_reasoning_level: Schema.optionalKey(Schema.NullOr(Schema.String)),
  supported_reasoning_levels: Schema.optionalKey(
    Schema.Array(Schema.Struct({ effort: Schema.String })),
  ),
  visibility: Schema.optionalKey(Schema.String),
  priority: Schema.optionalKey(Schema.Finite),
  service_tiers: Schema.optionalKey(Schema.Array(ServiceTier)),
  context_window: Schema.optionalKey(Schema.NullOr(Schema.Finite)),
})
type CodexModel = typeof CodexModel.Type

const ModelList = Schema.Struct({ models: Schema.Array(CodexModel) })

const isLevel = Schema.is(ModelThinkingLevel)

/** The thinking level Codex effort `effort` is; `none` is `off`, and efforts without one drop. */
const level = (effort: string | null | undefined): ModelThinkingLevel | undefined => {
  if (effort === 'none') return 'off'
  return isLevel(effort) ? effort : undefined
}

const levels = (model: CodexModel) =>
  model.supported_reasoning_levels?.flatMap(({ effort }) => {
    const found = level(effort)
    return found === undefined ? [] : [found]
  })

/** `model` as provider `name` lists it, served when one of `served` has its slug. */
const toListed = (name: string, served: ReadonlyArray<Info>, model: CodexModel): Listed => {
  const info = served.find((candidate) => candidate.id === model.slug)
  const thinkingLevels = levels(model)
  const defaultThinkingLevel = level(model.default_reasoning_level)
  const contextWindow = model.context_window ?? info?.contextWindow
  const description = model.description ?? undefined
  return {
    provider: name,
    id: model.slug,
    name: model.display_name,
    ...(description === undefined ? {} : { description }),
    served: info !== undefined,
    ...(thinkingLevels === undefined ? {} : { thinkingLevels }),
    ...(defaultThinkingLevel === undefined ? {} : { defaultThinkingLevel }),
    serviceTiers: model.service_tiers ?? [],
    ...(contextWindow === undefined ? {} : { contextWindow }),
  }
}

export interface ListOptions {
  readonly baseUrl: string
  readonly originator: string
  readonly clientVersion?: string
  /** The provider's name, and the models it serves. */
  readonly name: string
  readonly served: ReadonlyArray<Info>
  /** The account's access token and id. */
  readonly access: string
  readonly id: string
}

/** The models Codex lists for the account, in its picker's order, hidden ones left out. */
export const list = (options: ListOptions) =>
  getJson(
    'The Codex model list',
    HttpClientRequest.get(`${options.baseUrl}/codex/models`).pipe(
      HttpClientRequest.setUrlParam('client_version', options.clientVersion ?? CLIENT_VERSION),
      HttpClientRequest.bearerToken(options.access),
      HttpClientRequest.setHeaders({
        'chatgpt-account-id': options.id,
        originator: options.originator,
      }),
    ),
    ModelList,
  ).pipe(
    Effect.map(({ models }) =>
      models
        .filter((model) => (model.visibility ?? 'list') === 'list')
        .toSorted((a, b) => (a.priority ?? 0) - (b.priority ?? 0))
        .map((model) => toListed(options.name, options.served, model)),
    ),
  )
