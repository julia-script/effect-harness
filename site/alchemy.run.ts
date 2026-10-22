/**
 * The docs site: the Foldkit build, served as static assets by a Cloudflare Worker.
 *
 *   bun run site:dev      # vite with hot reload
 *   bun run site:deploy   # your Cloudflare account
 *
 * State lives in your Cloudflare account, so deploys from a laptop and from CI share it.
 */
import * as Alchemy from 'alchemy'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Effect } from 'effect'

export default Alchemy.Stack(
  'EffectHarnessDocs',
  { providers: Cloudflare.providers(), state: Cloudflare.state() },
  Effect.gen(function* () {
    const site = yield* Cloudflare.Website.Foldkit('Docs', {
      rootDir: 'site',
      // The guides and the demos read the docs and the library source from outside the site.
      memo: { include: ['**/*', '../src/**', '../docs/**'], lockfile: true },
    })
    return { url: site.url }
  }),
)
