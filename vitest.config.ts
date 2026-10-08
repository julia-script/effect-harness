import { defineConfig } from 'vitest/config'

// effect-nit-allow PX-no-default-export: Vitest loads its configuration through this default export.
export default defineConfig({
  resolve: {
    alias: [
      {
        find: /^effect-harness\/(durable\/(?:testing|storage|workflow)|testing|tools|env)$/,
        replacement: `${new URL('./packages/effect-harness/src/', import.meta.url).pathname}$1/index.ts`,
      },
      {
        find: /^effect-harness\/(durable|auth|provider-openai|provider-anthropic|provider-claude-code)$/,
        replacement: `${new URL('./packages/effect-harness/src/', import.meta.url).pathname}$1/index.ts`,
      },
      {
        find: /^effect-harness\/(.+)$/,
        replacement: `${new URL('./packages/effect-harness/src/', import.meta.url).pathname}$1.ts`,
      },
      {
        find: /^effect-harness$/,
        replacement: new URL('./packages/effect-harness/src/index.ts', import.meta.url).pathname,
      },
    ],
  },
  test: {
    include: ['packages/*/test/**/*.test.ts', 'apps/*/test/**/*.test.ts'],
    // The seeded storage workload and native restart fixtures share host CPU/process resources.
    maxWorkers: 1,
    testTimeout: 30000,
    hookTimeout: 30000,
  },
})
