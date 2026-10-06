import * as SqliteClient from '@effect/sql-sqlite-bun/SqliteClient'
import * as Layer from 'effect/Layer'
import * as Sqlite from './Sqlite.ts'

export const layer = (options: SqliteClient.SqliteClientConfig) =>
  Sqlite.layer.pipe(Layer.provide(SqliteClient.layer(options)))
