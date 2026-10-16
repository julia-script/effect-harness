/** SQLite persistence of opaque commit frames and named payload streams. */
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SqlClient from 'effect/sql/SqlClient'
import type { Persistence } from './Persistence.js'
import * as Errors from '../StorageError.js'

// Transaction COMMIT / ROLLBACK failures are defects in Effect's SQL client.
// Translate them at this driver boundary as well as ordinary query failures.
const io = <A, E, R>(operation: string, effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.failCause(
            Cause.map(cause, (error) => Errors.make('io', operation, 'SQL interrupted', error)),
          )
        : Effect.fail(
            Errors.make(
              operation === 'append' ? 'uncertain' : 'io',
              operation,
              `SQL ${operation} failed`,
              Cause.squash(cause),
            ),
          ),
    ),
  )

export const make = Effect.fnUntraced(function* (options?: { readonly storeId?: string }) {
  const sql = (yield* SqlClient.SqlClient).withoutTransforms()
  const storeId = yield* Schema.decodeEffect(Schema.NonEmptyString)(
    options?.storeId ?? 'default',
  ).pipe(Effect.mapError(Errors.invalid('initialize')))
  if (!sql.onDialectOrElse({ sqlite: () => true, orElse: () => false }))
    return yield* Errors.make(
      'invalid',
      'initialize',
      'SQL storage currently supports SQLite clients',
    )
  const checkWrite = Effect.gen(function* () {
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* Errors.make(
        'invalid',
        'transaction',
        'Storage must run outside application SQL transactions',
      )
  })
  yield* checkWrite
  yield* io(
    'initialize',
    sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`CREATE TABLE IF NOT EXISTS effect_harness_commits_v1 (
      store_id TEXT NOT NULL,
      position INTEGER NOT NULL CHECK (position > 0),
      frame TEXT NOT NULL,
      PRIMARY KEY (store_id, position)
    ) STRICT`
        yield* sql`CREATE TABLE IF NOT EXISTS effect_harness_payloads_v1 (
      store_id TEXT NOT NULL,
      stream TEXT NOT NULL,
      position INTEGER NOT NULL CHECK (position > 0),
      line TEXT NOT NULL,
      PRIMARY KEY (store_id, stream, position)
    ) STRICT`
      }),
    ),
  )
  let head = 0
  const currentHead = sql<{ position: number }>`
    SELECT COALESCE(MAX(position), 0) AS position
    FROM effect_harness_commits_v1 WHERE store_id = ${storeId}
  `.pipe(Effect.map((rows) => rows[0]!.position))
  const checkHead = Effect.gen(function* () {
    if ((yield* currentHead) !== head)
      return yield* Errors.make(
        'conflict',
        'write',
        'SQL store changed; close and reopen this writer',
      )
  })
  const insertLines = Effect.fnUntraced(function* (
    key: string,
    lines: ReadonlyArray<string>,
    after: number,
  ) {
    for (const [offset, line] of lines.entries())
      yield* sql`INSERT INTO effect_harness_payloads_v1 (store_id, stream, position, line)
        VALUES (${storeId}, ${key}, ${after + offset + 1}, ${line})`
  })
  return {
    checkWrite,
    accept: Effect.void,
    journal: io(
      'read',
      sql<{ position: number; frame: string }>`
      SELECT position, frame FROM effect_harness_commits_v1
      WHERE store_id = ${storeId} ORDER BY position
    `,
    ).pipe(
      Effect.flatMap((rows) =>
        Effect.gen(function* () {
          if (rows.some((row, index) => row.position !== index + 1))
            return yield* Errors.make(
              'corrupt',
              'recover',
              'SQL commit positions are not contiguous',
            )
          head = rows.length
          return rows.map((row) => row.frame)
        }),
      ),
    ),
    keys: io(
      'read',
      sql<{ stream: string }>`
      SELECT DISTINCT stream FROM effect_harness_payloads_v1 WHERE store_id = ${storeId}
    `,
    ).pipe(Effect.map((rows) => rows.map((row) => row.stream))),
    read: (key) =>
      io(
        'read',
        sql<{ line: string }>`
      SELECT line FROM effect_harness_payloads_v1
      WHERE store_id = ${storeId} AND stream = ${key} ORDER BY position
    `,
      ).pipe(Effect.map((rows) => rows.map((row) => row.line))),
    append: (batch) =>
      checkWrite.pipe(
        Effect.andThen(
          Effect.gen(function* () {
            yield* io(
              'append',
              sql.withTransaction(
                Effect.gen(function* () {
                  yield* checkHead
                  for (const stream of batch.append) {
                    const rows = yield* sql<{ position: number }>`
            SELECT COALESCE(MAX(position), 0) AS position FROM effect_harness_payloads_v1
            WHERE store_id = ${storeId} AND stream = ${stream.key}
          `
                    yield* insertLines(stream.key, stream.lines, rows[0]!.position)
                  }
                  yield* sql`INSERT INTO effect_harness_commits_v1 (store_id, position, frame)
          VALUES (${storeId}, ${head + 1}, ${batch.frame})`
                }),
              ),
            )
            head++
          }),
        ),
      ),
    replace: ({ key, lines }) =>
      checkWrite.pipe(
        Effect.andThen(
          io(
            'reclaim',
            sql.withTransaction(
              Effect.gen(function* () {
                yield* checkHead
                yield* sql`DELETE FROM effect_harness_payloads_v1 WHERE store_id = ${storeId} AND stream = ${key}`
                yield* insertLines(key, lines, 0)
              }),
            ),
          ),
        ),
      ),
  } satisfies Persistence
})
