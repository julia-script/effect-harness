/**
 * The test's Worker: `/agents/<name>` reaches the object of that name, with the token. `POST /ask`
 * starts an `Ask` run with the request's JSON body, and `GET /ask/<id>` reports that run.
 */
import * as Cloudflare from 'alchemy/Cloudflare'
import { Config, Effect } from 'effect'
import { HttpServerRequest } from 'effect/http/HttpServerRequest'
import * as HttpServerResponse from 'effect/http/HttpServerResponse'
import { HarnessObject } from '../../index.ts'
import Agent from './agent.ts'
import Ask from './ask.ts'
import type { AskInput } from './ask.ts'

export default class Api extends Cloudflare.Worker<Api>()(
  'Api',
  { main: import.meta.url },
  Effect.gen(function* () {
    const agents = yield* Agent
    const ask = yield* Ask
    const token = yield* Config.Redacted('HARNESS_TOKEN')
    const route = HarnessObject.route(agents, { token })
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest
        const { pathname } = new URL(request.url, 'http://worker')
        if (pathname === '/ask' && request.method === 'POST') {
          const params = (yield* request.json) as unknown as AskInput
          const instance = yield* ask.create({ params })
          return HttpServerResponse.jsonUnsafe({ id: instance.id })
        }
        if (pathname.startsWith('/ask/')) {
          const instance = yield* ask.get(pathname.slice('/ask/'.length))
          return HttpServerResponse.jsonUnsafe(yield* instance.status())
        }
        return yield* route
      }),
    }
  }),
) {}
