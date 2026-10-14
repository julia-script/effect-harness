import * as Alchemy from 'alchemy'
import * as Cloudflare from 'alchemy/Cloudflare'
import * as Effect from 'effect/Effect'

export const Website = Cloudflare.Website.Foldkit('Docs')

export default Alchemy.Stack(
  'effect-harness-docs',
  { providers: Cloudflare.providers(), state: Cloudflare.state() },
  Effect.gen(function* () {
    const website = yield* Website
    return { url: website.url }
  }),
)
