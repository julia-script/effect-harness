import { describe } from '@effect/vitest'

import * as Layer from 'effect/Layer'

import * as TestStore from './storage/TestStore.ts'

import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'

import { cases } from './StoreResultCases.ts'

describe('StoreResults', () => {
  describe('sqlite', () => {
    cases(TestStore.layer.pipe(Layer.provide(SqliteClient.layer({ filename: ':memory:' }))))
  })
})
