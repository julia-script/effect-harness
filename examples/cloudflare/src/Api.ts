/** The Worker in front: `/agents/<name>` reaches the object of that name, with the token. */
import * as Cloudflare from 'alchemy/Cloudflare'
import { Config, Effect } from 'effect'
import { HarnessObject } from 'effect-harness/cloudflare'
import Agent from './Agent.ts'

export default class Api extends Cloudflare.Worker<Api>()(
  'Api',
  { main: import.meta.url },
  Effect.gen(function* () {
    const agents = yield* Agent
    const token = yield* Config.Redacted('HARNESS_TOKEN')
    return { fetch: HarnessObject.route(agents, { token }) }
  }),
) {}
