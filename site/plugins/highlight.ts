/**
 * Syntax highlighting at build time, so the browser gets markup and no highlighter.
 *
 * - `guide.md?code` is the guide's code blocks in document order, highlighted, with `null` for
 *   diagrams, which render in the browser.
 * - `file.ts?highlight` is a source file, raw and highlighted, for the demos' code panels.
 */
import { readFile } from 'node:fs/promises'
import type * as Markdown from '@foldkit/markdown'
import { parseMarkdown } from '@foldkit/markdown/vite'
import { Array, Option, pipe } from 'effect'
import { createHighlighter } from 'shiki'
import type { Plugin } from 'vite'
import { islandAttributes } from '../src/markdown/islands.ts'
import { normalize } from './githubMarkdown.ts'

export const THEME = 'github-dark-default'

const LANGUAGES = ['typescript', 'tsx', 'json', 'shellscript', 'yaml', 'text'] as const

const ALIASES: Readonly<Record<string, string>> = {
  ts: 'typescript',
  sh: 'shellscript',
  bash: 'shellscript',
  shell: 'shellscript',
  console: 'shellscript',
  text: 'text',
}

const highlighter = createHighlighter({ themes: [THEME], langs: [...LANGUAGES] })

const highlight = async (code: string, language: string): Promise<string> =>
  (await highlighter).codeToHtml(code, {
    lang: ALIASES[language] ?? (Array.contains(LANGUAGES, language) ? language : 'text'),
    theme: THEME,
  })

const codeBlocks = (blocks: ReadonlyArray<Markdown.Block>): ReadonlyArray<Markdown.CodeBlock> =>
  Array.flatMap(blocks, (block) => {
    if (block._tag === 'CodeBlock') {
      return [block]
    }
    if (block._tag === 'List') {
      return Array.flatMap(block.items, (item) => codeBlocks(item.blocks))
    }
    if (block._tag === 'Blockquote' || block._tag === 'Island') {
      return codeBlocks(block.blocks)
    }
    return []
  })

const highlightGuide = async (source: string): Promise<ReadonlyArray<string | null>> =>
  Promise.all(
    Array.map(codeBlocks(parseMarkdown(source, { islands: islandAttributes }).blocks), (block) =>
      pipe(
        block.maybeLanguage,
        Option.getOrElse(() => 'text'),
        (language) =>
          language === 'mermaid' ? Promise.resolve(null) : highlight(block.value, language),
      ),
    ),
  )

const CODE = '?code'
const HIGHLIGHT = '?highlight'

export const highlightCode = (): Plugin => ({
  name: 'site:highlight',
  enforce: 'pre',
  async load(id) {
    if (id.endsWith(CODE)) {
      const file = id.slice(0, -CODE.length)
      this.addWatchFile(file)
      const blocks = await highlightGuide(normalize(await readFile(file, 'utf8')))
      return `export default ${JSON.stringify(blocks)}`
    }
    if (id.endsWith(HIGHLIGHT)) {
      const file = id.slice(0, -HIGHLIGHT.length)
      this.addWatchFile(file)
      const raw = (await readFile(file, 'utf8')).trimEnd()
      const html = await highlight(raw, file.endsWith('.tsx') ? 'tsx' : 'typescript')
      return `export default ${JSON.stringify({ raw, html })}`
    }
    return undefined
  },
})
