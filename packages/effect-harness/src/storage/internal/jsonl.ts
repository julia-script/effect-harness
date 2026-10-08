/** Framed record batches with a kernel-held SQLite sidecar ownership lock. */
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Schema from 'effect/Schema'
import * as SqlClient from 'effect/sql/SqlClient'
import type * as Scope from 'effect/Scope'
import * as Persistence from '../../Persistence.ts'
import {
  CorruptError,
  IoError,
  rejected,
  uncertain,
  type StorageError,
} from '../../StorageError.ts'
import { detachedEffect } from '../../internal/records.ts'
import * as records from './records.ts'

export const Options = Schema.Struct({
  directory: Schema.NonEmptyString,
  fsync: Schema.optionalKey(Schema.Boolean),
})
export type Options = typeof Options.Type
const Batch = Schema.Struct({
  format: Schema.Literal(2),
  metadata: Persistence.Metadata,
  rows: Schema.Array(records.Row),
})
const BatchFromString = Schema.fromJsonString(Batch)

/** The sidecar stores no harness records. Its exclusive connection lock dies with the process. */
export const make = Effect.fn('Jsonl.make')(function* (
  options: Options,
): Effect.fn.Return<
  Persistence.Service,
  StorageError,
  FileSystem.FileSystem | SqlClient.SqlClient | Scope.Scope
> {
  const fs = yield* FileSystem.FileSystem
  const sql = yield* SqlClient.SqlClient
  yield* sql`PRAGMA locking_mode = EXCLUSIVE`.pipe(
    Effect.mapError((cause) => rejected('Cannot acquire JSONL ownership', IoError, cause)),
  )
  yield* sql
    .withTransaction(
      sql`CREATE TABLE IF NOT EXISTS owner (singleton INTEGER PRIMARY KEY CHECK(singleton = 1))`,
    )
    .pipe(
      Effect.mapError((cause) => rejected('JSONL directory already has an owner', IoError, cause)),
    )
  const filename = `${options.directory}/commits.jsonl`
  const io = <A, R>(effect: Effect.Effect<A, import('effect/PlatformError').PlatformError, R>) =>
    effect.pipe(
      Effect.mapError((cause) => rejected('JSONL filesystem operation failed', IoError, cause)),
    )
  const handle = yield* io(fs.open(filename, { flag: 'a+' }))
  const bytes = yield* io(fs.readFile(filename))
  const complete =
    bytes.length === 0 || bytes.at(-1) === 10 ? bytes.length : bytes.lastIndexOf(10) + 1
  if (complete !== bytes.length) yield* io(fs.truncate(filename, complete))
  let metadata: Persistence.Metadata = { revision: 0, nextId: 2 }
  const rows = new Map<number, records.Row>()
  let start = 0
  for (let end = 0; end < complete; end++) {
    if (bytes[end] !== 10) continue
    const text = yield* Effect.try({
      try: () => new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(start, end)),
      catch: (cause) => rejected('Malformed complete JSONL frame', CorruptError, cause),
    })
    const batch = yield* Schema.decodeEffect(BatchFromString)(text).pipe(
      Effect.mapError((cause) => rejected('Invalid complete JSONL frame', CorruptError, cause)),
    )
    if (
      batch.metadata.revision !== metadata.revision + 1 ||
      batch.metadata.nextId < metadata.nextId
    )
      return yield* rejected('JSONL revision or allocation sequence is corrupt', CorruptError)
    for (const row of batch.rows) {
      if (records.idOf(row) >= batch.metadata.nextId)
        return yield* rejected('JSONL allocated identity is corrupt', CorruptError)
      const previous = rows.get(records.idOf(row))
      if (previous !== undefined && previous._tag !== row._tag)
        return yield* rejected('JSONL record category changed', CorruptError)
      rows.set(records.idOf(row), row)
    }
    metadata = batch.metadata
    start = end + 1
  }
  const flush = Effect.gen(function* () {
    yield* handle.sync
    const directory = yield* fs.open(options.directory, { flag: 'r' })
    yield* directory.sync
  }).pipe(Effect.scoped)
  // Flush creation and any repaired tail before accepting new work. By default successful
  // commits synchronize both the data file and its directory; fsync:false opts out.
  if (options.fsync !== false) yield* io(flush)
  return yield* records.make({
    metadata: Effect.sync(() => ({ ...metadata })),
    get: (id) =>
      Effect.suspend(() => {
        const row = rows.get(id)
        return row === undefined ? Effect.succeedNone : detachedEffect(row).pipe(Effect.asSome)
      }),
    page: (kind, after, limit, filter) =>
      Effect.suspend(() =>
        detachedEffect(
          [...rows.values()]
            .filter(
              (row) =>
                row._tag === kind && records.idOf(row) > after && records.matches(row, filter),
            )
            .sort((a, b) => records.idOf(a) - records.idOf(b))
            .slice(0, limit),
        ),
      ),
    save: Effect.fn('Jsonl.save')(function* (batch, next) {
      const detached = yield* detachedEffect(batch)
      const text = yield* Schema.encodeEffect(BatchFromString)({
        format: 2,
        metadata: next,
        rows: detached,
      }).pipe(
        Effect.mapError((cause) => rejected('JSONL batch cannot be encoded', undefined, cause)),
      )
      yield* fs
        .writeFileString(filename, `${text}\n`, { flag: 'a' })
        .pipe(Effect.mapError((cause) => uncertain('JSONL append outcome is uncertain', cause)))
      if (options.fsync !== false)
        yield* flush.pipe(
          Effect.mapError((cause) =>
            uncertain('JSONL synchronization outcome is uncertain', cause),
          ),
        )
      for (const row of detached) rows.set(records.idOf(row), row)
      metadata = { ...next }
    }),
  })
})
