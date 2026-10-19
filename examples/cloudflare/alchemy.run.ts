/**
 * The stack: one Worker in front of the harness objects.
 *
 *   alchemy dev examples/cloudflare/alchemy.run.ts      # a local workerd, no account needed
 *   alchemy deploy examples/cloudflare/alchemy.run.ts   # your Cloudflare account
 */
import * as Alchemy from 'alchemy'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Effect } from 'effect'
import Api from './src/Api.ts'

export default Alchemy.Stack(
  'EffectHarnessExample',
  { providers: Cloudflare.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const api = yield* Api
    return { url: api.url }
  }),
)
