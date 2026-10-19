import { expect, test } from 'bun:test'
import { SqliteClient } from '@effect/sql-sqlite-bun'
import { Effect, Exit, Layer, String } from 'effect'
import { SqlClient } from 'effect/sql'
import { createRoot, entryWrite } from '../support.ts'
import { sqlite } from './support.ts'
import { Storage } from '../../index.ts'
import { Ids } from '../../schema/index.ts'

test('fails every operation after its scope closes', () =>
  sqlite.run(() =>
    Effect.gen(function* () {
      const storage = yield* Effect.scoped(Storage.makeSql())
      const exit = yield* Effect.exit(storage.conversation(Ids.ROOT_CONVERSATION_ID))
      expect(Exit.isFailure(exit)).toBe(true)
      expect((yield* Effect.flip(storage.commit([]))).message).toBe('SqlStorage is closed')
      expect((yield* Effect.flip(storage.mintId(Ids.EntryId))).message).toBe('SqlStorage is closed')
    }),
  ))

test('keeps stores with different prefixes apart in one database', () =>
  sqlite.run(() =>
    Effect.gen(function* () {
      const first = yield* Storage.makeSql({ prefix: 'first_' })
      const second = yield* Storage.makeSql({ prefix: 'second_' })
      yield* createRoot(first)
      expect(yield* second.conversation(Ids.ROOT_CONVERSATION_ID)).toBeUndefined()
      yield* createRoot(second)
      const sql = yield* SqlClient.SqlClient
      const tables = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`
      expect(tables.map(({ name }) => name)).toContain('first_conversations')
      expect(tables.map(({ name }) => name)).toContain('second_conversations')
    }),
  ))

test('rejects a prefix that is not a plain identifier', () =>
  sqlite.run(() =>
    Effect.gen(function* () {
      const invalid = yield* Effect.flip(Storage.makeSql({ prefix: 'x"; DROP TABLE y; --' }))
      expect(invalid.message).toBe('Invalid SQL table prefix "x\\"; DROP TABLE y; --"')
      const reserved = yield* Effect.flip(Storage.makeSql({ prefix: 'sqlite_x' }))
      expect(reserved.message).toBe('SQL table prefix "sqlite_x" is reserved')
    }),
  ))

test('ignores the name transforms of the client it is given', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const storage = yield* Storage.makeSql()
        const rootId = yield* createRoot(storage)
        const entryId = yield* storage.mintId(Ids.EntryId)
        const seq = yield* storage.commit([entryWrite(entryId, rootId)])
        expect((yield* storage.entry(entryId))?.commitSeq).toBe(seq)
      }),
    ).pipe(
      // This test is an entry point that provides its own database.
      // oxlint-disable-next-line effecttsgo/strict-effect-provide
      Effect.provide(
        Layer.orDie(
          SqliteClient.layer({
            filename: ':memory:',
            transformQueryNames: String.camelToSnake,
            transformResultNames: String.snakeToCamel,
          }),
        ),
      ),
    ),
  ))

test('reports corrupt rows and driver failures as storage errors', () =>
  sqlite.run(() =>
    Effect.gen(function* () {
      const storage = yield* Storage.makeSql()
      yield* createRoot(storage)
      const sql = yield* SqlClient.SqlClient
      yield* sql`UPDATE conversations SET record = '{"id":"one"}'`
      const corrupt = yield* Effect.flip(storage.conversation(Ids.ROOT_CONVERSATION_ID))
      expect(corrupt.message).toBe('Corrupt SQL storage row')
      yield* sql`DROP TABLE entries`
      const failed = yield* Effect.flip(storage.entry(Ids.EntryId.make(2)))
      expect(failed.message).toBe('SQL storage query failed')
    }),
  ))
