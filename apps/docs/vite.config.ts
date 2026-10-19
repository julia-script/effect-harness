import { readFileSync, readdirSync } from 'node:fs'
import { foldkit } from '@foldkit/vite-plugin'
import tailwindcss from '@tailwindcss/vite'
import { defineConfig, type Plugin } from 'vite'

// Serve the exact authored sources with the site, including before a git push.
const sources = new Map(
  [
    'src/Lab.ts',
    ...readdirSync(new URL('./content/', import.meta.url))
      .filter((name) => name.endsWith('.md'))
      .map((name) => `content/${name}`),
    ...readdirSync(new URL('./src/examples/', import.meta.url))
      .filter((name) => name.endsWith('.ts'))
      .map((name) => `src/examples/${name}`),
  ].map((path) => [`sources/${path}.txt`, new URL(path, import.meta.url)]),
)
const sourceFiles = (): Plugin => ({
  name: 'docs-source-files',
  buildStart() {
    for (const [fileName, url] of sources) {
      this.emitFile({ type: 'asset', fileName, source: readFileSync(url, 'utf8') })
    }
  },
  configureServer(server) {
    server.middlewares.use((request, response, next) => {
      const url = sources.get((request.url ?? '').replace(/^\//, ''))
      if (url === undefined) return next()
      response.setHeader('Content-Type', 'text/plain; charset=utf-8')
      response.end(readFileSync(url, 'utf8'))
    })
  },
})

export default defineConfig({
  plugins: [foldkit(), tailwindcss(), sourceFiles()],
  optimizeDeps: { entries: ['src/entry.ts'] },
  server: { port: 5173, strictPort: true },
  // Keep the offline demo usable in already-open tabs across deployments.
  build: { rolldownOptions: { output: { codeSplitting: false } } },
})
