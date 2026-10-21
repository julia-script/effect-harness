/**
 * Live model lists of the stock providers: a GET over the provider's own credentials, joined with
 * what the provider serves and its catalog's thinking levels.
 */
import { Effect } from 'effect'
import type { Schema } from 'effect'
import { HttpClient, HttpClientResponse } from 'effect/http'
import type { HttpClientRequest } from 'effect/http'
import type { Info, Listed } from '../../model/Model.ts'
import { ModelError, Rejected, Retryable } from '../../model/ModelError.ts'
import { find, thinkingLevels } from './catalog.ts'
import type { Entry } from './catalog.ts'

/**
 * The JSON body of `request`, decoded as `schema`. A rate limit, a server error or a dropped
 * connection fails `Retryable`; any other status or an unexpected body fails `Rejected`.
 */
export const getJson = <S extends Schema.ConstraintDecoder<unknown>>(
  what: string,
  request: HttpClientRequest.HttpClientRequest,
  schema: S,
): Effect.Effect<S['Type'], ModelError, HttpClient.HttpClient> =>
  Effect.flatMap(HttpClient.HttpClient, (client) => client.execute(request)).pipe(
    Effect.mapError(
      (cause) => new ModelError({ message: `${what} failed`, reason: new Retryable({}), cause }),
    ),
    Effect.flatMap((response) => {
      if (response.status >= 200 && response.status < 300) {
        return HttpClientResponse.schemaBodyJson(schema)(response).pipe(
          Effect.mapError(
            (cause) =>
              new ModelError({
                message: `${what} returned an unexpected body`,
                reason: new Rejected({}),
                cause,
              }),
          ),
        )
      }
      const retryable = response.status === 429 || response.status >= 500
      return Effect.fail(
        new ModelError({
          message: `${what} failed (${response.status})`,
          reason: retryable ? new Retryable({}) : new Rejected({}),
        }),
      )
    }),
  )

export interface Found {
  readonly id: string
  readonly name?: string
}

/**
 * `found` as `provider` lists it: served when one of `served` has its id, with the thinking levels
 * of its catalog entry, or of the served model when the catalog does not know it.
 */
export const listed = (
  provider: string,
  served: ReadonlyArray<Info>,
  catalog: ReadonlyArray<Entry>,
  found: Found,
): Listed => {
  const info = served.find((model) => model.id === found.id)
  const entry = find(catalog, found.id) ?? info
  const name = found.name ?? info?.name
  return {
    provider,
    id: found.id,
    name: name ?? found.id,
    served: info !== undefined,
    ...(entry === undefined ? {} : { thinkingLevels: thinkingLevels(entry) }),
    serviceTiers: [],
    ...(info === undefined ? {} : { contextWindow: info.contextWindow }),
  }
}
