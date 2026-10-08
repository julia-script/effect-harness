import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'

import { describe } from '@effect/vitest'

import * as Layer from 'effect/Layer'

import { persistence } from './TestStore.ts'

import { cases } from './SnapshotStoreReadCases.ts'

const SQLite = persistence.pipe(Layer.provide(SqliteClient.layer({ filename: ':memory:' })))
describe('SnapshotStoreRead', () => {
  cases('SQLite', SQLite)
})
