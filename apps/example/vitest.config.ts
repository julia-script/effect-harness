import { defineConfig } from 'vitest/config'

// effect-nit-allow PX-no-default-export: Vitest/Vite loads this configuration from the module default export.
export default defineConfig({
  // No source aliases: the example exercises built packages and app output.
  test: {
    include: ['apps/example/test/**/*.test.ts'],
    maxWorkers: 1,
    testTimeout: 30000,
    hookTimeout: 30000,
  },
})
