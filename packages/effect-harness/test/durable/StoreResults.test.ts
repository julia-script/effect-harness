import { describe } from '@effect/vitest'

import * as Store from 'effect-harness/durable/Store'

import { cases } from './StoreResultCases.ts'

describe('StoreResults', () => {
  describe('memory', () => {
    cases(Store.layerMemory)
  })
})
