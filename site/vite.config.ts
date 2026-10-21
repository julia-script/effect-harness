import { fileURLToPath } from 'node:url'
import { markdown } from '@foldkit/markdown/vite'
import { foldkit } from '@foldkit/vite-plugin'
import tailwindcss from '@tailwindcss/vite'
import { defineConfig } from 'vite'
import { githubMarkdown } from './plugins/githubMarkdown.ts'
import { highlightCode } from './plugins/highlight.ts'
import { islandAttributes } from './src/markdown/islands.ts'

const source = fileURLToPath(new URL('../src', import.meta.url))

export default defineConfig({
  plugins: [
    githubMarkdown(),
    highlightCode(),
    tailwindcss(),
    foldkit(),
    markdown({ islands: islandAttributes }),
  ],
  // The demos run effect-harness from this checkout, not from a published build.
  resolve: { alias: [{ find: /^effect-harness\/(.*)$/, replacement: `${source}/$1/index.ts` }] },
  server: { fs: { allow: ['..'] } },
  optimizeDeps: { entries: ['src/entry.ts'] },
  // Mermaid and its layout engines are large, but they load only when a page draws a diagram.
  build: { chunkSizeWarningLimit: 2048 },
})
