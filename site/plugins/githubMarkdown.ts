/**
 * The guides under `docs/` are written for GitHub, which renders a little raw HTML. The site's
 * markdown pipeline takes none, so this rewrites what the guides use: formatter comments go, and
 * `<details>` becomes the `Details` island. The site also leaves out the guides' emoji, keeping
 * the meaning of the ones in tables.
 */
import type { Plugin } from 'vite'

const COMMENT = /^<!--.*-->\n/gm
const DETAILS = /^<details>\n<summary>(?:<b>)?(.*?)(?:<\/b>)?<\/summary>\n/gm
const DETAILS_END = /^<\/details>$/gm
const FENCE = /(^```[^\n]*\n[\s\S]*?^```$)/m
const EMOJI = /\p{Extended_Pictographic}️? ?/gu

const prose = (text: string): string =>
  text.replaceAll('✅', 'Yes').replaceAll('❌', 'No').replace(EMOJI, '')

export const normalize = (source: string): string =>
  source
    .split(FENCE)
    .map((part, index) => (index % 2 === 0 ? prose(part) : part))
    .join('')
    .replace(COMMENT, '')
    .replace(DETAILS, (_, summary: string) => `:::Details{summary="${summary}"}\n`)
    .replace(DETAILS_END, ':::')

export const githubMarkdown = (): Plugin => ({
  name: 'site:github-markdown',
  enforce: 'pre',
  transform: (source, id) => (id.endsWith('.md') ? normalize(source) : undefined),
})
