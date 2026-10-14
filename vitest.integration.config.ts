import { defineConfig } from 'vitest/config'
import config from './vitest.config.js'

// effect-nit-allow PX-no-default-export: Vitest loads its configuration through this default export.
export default defineConfig({
  ...config,
  test: { ...config.test, include: ['packages/*/integration/**/*.test.ts'] },
})
