import { defineConfig } from 'vitest/config'

export default defineConfig({
  resolve: {
    alias: [
      {
        find: /^@effect-harness\/([^/]+)\/(testing|tools|storage|workflow|env)$/,
        replacement: `${new URL('./packages/', import.meta.url).pathname}$1/src/$2/index.ts`,
      },
      {
        find: /^@effect-harness\/([^/]+)\/(.+)$/,
        replacement: `${new URL('./packages/', import.meta.url).pathname}$1/src/$2.ts`,
      },
      {
        find: /^@effect-harness\/([^/]+)$/,
        replacement: `${new URL('./packages/', import.meta.url).pathname}$1/src/index.ts`,
      },
    ],
  },
  test: {
    include: ['packages/*/test/**/*.test.ts', 'apps/*/test/**/*.test.ts'],
    testTimeout: 30000,
    hookTimeout: 30000,
  },
})
