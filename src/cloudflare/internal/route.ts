/** The Worker in front of the objects: check the token, then forward to the named object. */
import { Effect, Redacted } from 'effect'
import type { Scope } from 'effect'
import { HttpServerRequest } from 'effect/http/HttpServerRequest'
import type { HttpServerError } from 'effect/http/HttpServerError'
import * as HttpServerResponse from 'effect/http/HttpServerResponse'

/** A namespace of objects that takes a request for the object of a name. */
export interface Objects {
  readonly fetch: (
    name: string,
    request: HttpServerRequest,
  ) => Effect.Effect<HttpServerResponse.HttpServerResponse, HttpServerError>
}

export type Token = Redacted.Redacted

export type Handler = Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  HttpServerError,
  HttpServerRequest | Scope.Scope
>

const NAME = /^[A-Za-z0-9_-]{1,64}$/u

/** The token a request carries, from its `Authorization` header or its `token` parameter. */
const carried = (request: HttpServerRequest, url: URL) => {
  const header = request.headers['authorization']
  return header?.startsWith('Bearer ') === true
    ? header.slice('Bearer '.length)
    : url.searchParams.get('token')
}

/** Compare without stopping at the first difference, so timing does not reveal the token. */
const same = (given: string, expected: string) => {
  let difference = given.length ^ expected.length
  for (let i = 0; i < expected.length; i++)
    difference |= (given.codePointAt(i) ?? 0) ^ (expected.codePointAt(i) ?? 0)
  return difference === 0
}

export const make = (
  objects: Objects,
  options: { readonly prefix?: string; readonly token?: Token } = {},
): Handler =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest
    const url = new URL(request.url, 'http://worker')
    const prefix = `${options.prefix ?? '/agents'}/`
    const name = url.pathname.startsWith(prefix) ? url.pathname.slice(prefix.length) : ''
    if (!NAME.test(name)) return HttpServerResponse.empty({ status: 404 })
    const given = carried(request, url)
    if (
      options.token !== undefined &&
      (given === null || !same(given, Redacted.value(options.token)))
    )
      return HttpServerResponse.empty({ status: 401 })
    return yield* objects.fetch(name, request)
  })
