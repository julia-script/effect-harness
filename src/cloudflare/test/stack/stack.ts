/** The test's stack: one Worker in front of the harness objects. */
import * as Alchemy from 'alchemy'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Effect } from 'effect'
import Api from './worker.ts'

export default Alchemy.Stack(
  'EffectHarnessTest',
  { providers: Cloudflare.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const api = yield* Api
    return { url: api.url }
  }),
)
