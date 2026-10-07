import { defineConfig } from 'vitest/config'

// Vitest requires a default export for its configuration.
export default defineConfig({
  // No source aliases: the example exercises built packages and app output.
  test: {
    include: ['apps/example/test/**/*.test.ts'],
    maxWorkers: 1,
    testTimeout: 30000,
    hookTimeout: 30000,
  },
})
