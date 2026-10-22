/**
 * The guides under `docs/` are written for GitHub, which renders a little raw HTML. The site's
 * markdown pipeline takes none, so this rewrites what the guides use: formatter comments go, and
 * `<details>` becomes the `Details` island.
 */
import type { Plugin } from 'vite'

const COMMENT = /^<!--.*-->\n/gm
const DETAILS = /^<details>\n<summary>(?:<b>)?(.*?)(?:<\/b>)?<\/summary>\n/gm
const DETAILS_END = /^<\/details>$/gm

export const normalize = (source: string): string =>
  source
    .replace(COMMENT, '')
    .replace(DETAILS, (_, summary: string) => `:::Details{summary="${summary}"}\n`)
    .replace(DETAILS_END, ':::')

export const githubMarkdown = (): Plugin => ({
  name: 'site:github-markdown',
  enforce: 'pre',
  transform: (source, id) => (id.endsWith('.md') ? normalize(source) : undefined),
})
