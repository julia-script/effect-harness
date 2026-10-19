import { Effect, Schema, SchemaTransformation } from 'effect'
import type { SqlClient } from 'effect/sql'
import { ConversationRecord } from '../../schema/Conversation.ts'
import { DocumentContent, DocumentRecord } from '../../schema/Document.ts'
import { EntryRecord } from '../../schema/Entry.ts'
import { Seq } from '../../schema/Ids.ts'
import type { Page, ScanOrder } from '../../schema/Scan.ts'
import { SubmissionRecord } from '../../schema/Submission.ts'
import { TaskRecord } from '../../schema/Task.ts'
import { StorageError } from '../../StorageError.ts'
import { nextCursor } from '../scan.ts'
import type { Schema as TableSchema, Tables } from './tables.ts'

/**
 * Row codecs for the SQL backend. Records are stored whole as JSON text and decoded with their
 * schemas on the way out, so a row a newer or foreign writer left behind fails as a `StorageError`
 * instead of reaching the Session.
 */

/** The client and table names every query of one store uses. */
export interface Db {
  readonly sql: SqlClient.SqlClient
  readonly tables: Tables
}

export const makeDb = (sql: SqlClient.SqlClient, schema: TableSchema): Db => ({
  sql,
  tables: schema.tables,
})

/** A numeric column. Drivers return `BIGINT` as a number, a decimal string, or a bigint. */
const numeric = <S extends Schema.Codec<number, number>>(to: S) =>
  Schema.Union([
    to,
    Schema.String.pipe(Schema.decodeTo(to, SchemaTransformation.numberFromString)),
    Schema.BigInt.pipe(
      Schema.decodeTo(to, SchemaTransformation.transform({ decode: Number, encode: BigInt })),
    ),
  ])

const Int = numeric(Schema.Int)

/** Once the ID space is exhausted, the next ID is one past the largest safe integer. */
const NextId = numeric(Schema.Finite)

const IntAs = <S extends Schema.Codec<number, number>>(schema: S) =>
  Int.pipe(Schema.decodeTo(schema))

const json = <S extends Schema.Top>(schema: S) => Schema.fromJsonString(schema)

export const ConversationRow = Schema.Struct({ record: json(ConversationRecord) })
export const EntryRow = Schema.Struct({ record: json(EntryRecord), commit_seq: IntAs(Seq) })
export const TaskRow = Schema.Struct({ record: json(TaskRecord) })
export const SubmissionRow = Schema.Struct({ record: json(SubmissionRecord) })
export const DocumentRow = Schema.Struct({ record: json(DocumentRecord) })
export const IdRow = Schema.Struct({ id: Int })
export const RecordTypeRow = Schema.Struct({
  id: Int,
  record_type: Schema.Literals(['conversation', 'entry', 'task', 'submission', 'document']),
})
export const VersionRow = Schema.Struct({ version: Int })
export const MetadataRow = Schema.Struct({ next_id: NextId, next_seq: IntAs(Seq) })

const corrupt = (cause: unknown) => new StorageError({ message: 'Corrupt SQL storage row', cause })

/** Decode every row a query returned. */
export const decodeRows = <S extends Schema.Decoder<unknown>>(schema: S) => {
  const decode = Schema.decodeUnknownEffect(Schema.Array(schema))
  return (rows: unknown): Effect.Effect<ReadonlyArray<S['Type']>, StorageError> =>
    Effect.mapError(decode(rows), corrupt)
}

/** Decode the first row a query returned, if any. */
export const decodeFirst = <S extends Schema.Decoder<unknown>>(schema: S) => {
  const decode = decodeRows(schema)
  return (rows: unknown): Effect.Effect<S['Type'] | undefined, StorageError> =>
    Effect.map(decode(rows), (decoded) => decoded[0])
}

/** Encode a record as the JSON text of its `record` column. */
export const encodeJson = <S extends Schema.Encoder<unknown>>(schema: S) => {
  const encode = Schema.encodeEffect(Schema.fromJsonString(schema))
  return (value: S['Type']): Effect.Effect<string, StorageError> =>
    Effect.mapError(
      encode(value),
      (cause) => new StorageError({ message: 'Record cannot be stored as JSON', cause }),
    )
}

/**
 * Some drivers replace lone UTF-16 surrogates in bound strings, and PostgreSQL rejects NUL, so
 * indexed strings are stored JSON-encoded to keep their identities lossless.
 */
export const indexedString = (value: string): string => JSON.stringify(value)

export const flag = (value: boolean): number => (value ? 1 : 0)

/** One page of `values`, which were read one past `limit` to detect a next page. */
export const page = <T extends { readonly id: number }>(
  values: ReadonlyArray<T>,
  limit: number,
  order: ScanOrder,
): Page<T> => {
  const items = values.slice(0, limit)
  const last = items.at(-1)
  return values.length <= limit || last === undefined
    ? { items }
    : { items, next: nextCursor(last.id, order) }
}

/** A document joined to the revisions from its latest base; revision columns are null without one. */
export const MaterializeRow = Schema.Struct({
  record: json(DocumentRecord),
  content: Schema.NullOr(json(DocumentContent)),
})
