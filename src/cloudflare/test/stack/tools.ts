/**
 * Tools a Worker agent can run: notes and a counter in the object's own SQLite, a page fetch,
 * and a wait. Notes are idempotent writes, so they replay; the counter and the wait do not.
 */
import { DurableObjectState } from 'alchemy/Cloudflare/Workers/DurableObjectState'
import { Duration, Effect, Schema } from 'effect'
import { HttpClient } from 'effect/http'
import { Extension } from '../../../harness/index.ts'
import { Tool, Toolkit } from '../../../tool/index.ts'

const NoteSave = Tool.make('note_save', {
  description: 'Save a note under a key, replacing any note of that key.',
  parameters: Schema.Struct({ key: Schema.String, value: Schema.String }),
  success: Schema.String,
  replay: 'safe',
})

const NoteList = Tool.make('note_list', {
  description: 'List every saved note.',
  parameters: Schema.Struct({}),
  success: Schema.String,
  replay: 'safe',
})

const CounterBump = Tool.make('counter_bump', {
  description: 'Add one to the counter and return its new value.',
  parameters: Schema.Struct({}),
  success: Schema.Finite,
})

const HttpGet = Tool.make('http_get', {
  description: 'Fetch a URL and return its body.',
  parameters: Schema.Struct({ url: Schema.String }),
  success: Schema.String,
  failure: Schema.String,
  replay: 'safe',
})

const Wait = Tool.make('wait', {
  description: 'Wait some seconds.',
  parameters: Schema.Struct({ seconds: Schema.Finite }),
  success: Schema.String,
})

export const toolkit = Toolkit.make(NoteSave, NoteList, CounterBump, HttpGet, Wait)

export const extension = Extension.make('worker-tools', { toolkit })

const sql = (state: DurableObjectState['Service']) => state.raw.storage.sql

/**
 * What the object's SQLite holds, for tests to read: rows per table, the largest value stored in
 * any column (Cloudflare refuses values over 2 MB), and the counter.
 */
export const stats = Effect.gen(function* () {
  const state = yield* DurableObjectState
  const db = sql(state)
  db.exec('CREATE TABLE IF NOT EXISTS test_notes (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
  const tables = db
    .exec<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table'")
    .toArray()
    .map(({ name }) => name)
    .filter((name) => !name.startsWith('_cf') && !name.startsWith('sqlite'))
  const rows: Record<string, number> = {}
  let largestValue = 0
  for (const table of tables) {
    rows[table] = Number(db.exec<{ n: number }>(`SELECT count(*) AS n FROM "${table}"`).one().n)
    for (const { name } of db.exec<{ name: string }>(`PRAGMA table_info("${table}")`).toArray()) {
      const [largest] = db
        .exec<{ n: number | null }>(`SELECT max(length("${name}")) AS n FROM "${table}"`)
        .toArray()
      largestValue = Math.max(largestValue, Number(largest?.n ?? 0))
    }
  }
  const counter = db
    .exec<{ value: string }>("SELECT value FROM test_notes WHERE key = '$counter'")
    .toArray()
  return {
    databaseSize: db.databaseSize,
    largestValue,
    rows,
    counter: Number(counter[0]?.value ?? 0),
  }
})

export const layer = toolkit.toLayer(
  Effect.gen(function* () {
    const state = yield* DurableObjectState
    const client = yield* HttpClient.HttpClient
    const db = sql(state)
    db.exec('CREATE TABLE IF NOT EXISTS test_notes (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
    const put = (key: string, value: string) =>
      db.exec('INSERT OR REPLACE INTO test_notes (key, value) VALUES (?, ?)', key, value)
    return {
      note_save: ({ key, value }) =>
        Effect.sync(() => {
          put(key, value)
          return `Saved ${key}`
        }),
      note_list: () =>
        Effect.sync(() =>
          db
            .exec<{ key: string; value: string }>(
              "SELECT key, value FROM test_notes WHERE key NOT LIKE '$%' ORDER BY key",
            )
            .toArray()
            .map(({ key, value }) => `${key}: ${value}`)
            .join('\n'),
        ),
      counter_bump: () =>
        Effect.sync(() => {
          const rows = db
            .exec<{ value: string }>("SELECT value FROM test_notes WHERE key = '$counter'")
            .toArray()
          const next = Number(rows[0]?.value ?? 0) + 1
          put('$counter', String(next))
          return next
        }),
      http_get: ({ url }) =>
        client.get(url).pipe(
          Effect.flatMap((response) => response.text),
          Effect.mapError((error) => error.message),
        ),
      wait: ({ seconds }) =>
        Effect.as(Effect.sleep(Duration.seconds(seconds)), `Waited ${seconds} seconds`),
    }
  }),
)
