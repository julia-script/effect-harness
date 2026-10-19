import { Effect, Record } from 'effect'
import { Migrator } from 'effect/sql'
import type { SqlClient, SqlError, Statement } from 'effect/sql'
import { StorageError } from '../../StorageError.ts'

/**
 * The SQL schema, after pi's `storage/sqlite/schema.ts` and `migrations.ts`, written in the SQL that
 * SQLite and PostgreSQL share. Every name carries the store's prefix, and each record is kept whole
 * as JSON text beside the columns its scans filter and order by.
 */

/** Table names; a rename adds a new migration, so existing values never change. */
const TABLES = {
  migrations: 'durable_migrations',
  metadata: 'durable_metadata',
  recordIds: 'record_ids',
  conversations: 'conversations',
  entries: 'entries',
  tasks: 'tasks',
  submissions: 'submissions',
  documents: 'documents',
  revisions: 'document_revisions',
} as const

export type Tables = { readonly [K in keyof typeof TABLES]: string }

/** One store's tables, with the prefix that also names its indexes. */
export interface Schema {
  readonly prefix: string
  readonly tables: Tables
}

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/u

/** Resolve the names under a prefix, which must be a plain SQL identifier. */
export const makeSchema = (prefix: string): Effect.Effect<Schema, StorageError> => {
  if (prefix !== '' && !IDENTIFIER.test(prefix)) {
    return Effect.fail(
      new StorageError({ message: `Invalid SQL table prefix ${JSON.stringify(prefix)}` }),
    )
  }
  if (prefix.toLowerCase().startsWith('sqlite_') || prefix.toLowerCase().startsWith('pg_')) {
    return Effect.fail(
      new StorageError({ message: `SQL table prefix ${JSON.stringify(prefix)} is reserved` }),
    )
  }
  const tables: Tables = Record.map(TABLES, (name) => `${prefix}${name}`)
  return Effect.succeed({ prefix, tables })
}

const unsupported = (dialect: string) =>
  Effect.fail(
    new StorageError({
      message: `SQL storage supports the sqlite and pg dialects, not ${dialect}`,
    }),
  )

/** Fails unless the client speaks a dialect this schema is written for. */
export const checkDialect = (sql: SqlClient.SqlClient): Effect.Effect<void, StorageError> =>
  sql.onDialect({
    sqlite: () => Effect.void,
    pg: () => Effect.void,
    mysql: () => unsupported('mysql'),
    mssql: () => unsupported('mssql'),
    clickhouse: () => unsupported('clickhouse'),
  })

/** What the DDL needs: the client, table and index names, and the ID column type. */
interface Ddl {
  readonly sql: SqlClient.SqlClient
  readonly t: (key: keyof Tables) => Statement.Identifier
  readonly index: (name: string) => Statement.Identifier
  readonly id: Statement.Fragment
}

/** Store metadata, the global ID registry, and conversations. */
const metadataTables = ({ sql, t, index, id }: Ddl) => [
  sql`CREATE TABLE ${t('metadata')} (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      next_id BIGINT NOT NULL,
      next_seq BIGINT NOT NULL
    )`,
  sql`INSERT INTO ${t('metadata')} (singleton, next_id, next_seq) VALUES (1, 2, 1)`,
  sql`CREATE TABLE ${t('recordIds')} (
      id ${id} PRIMARY KEY,
      record_type TEXT NOT NULL
        CHECK (record_type IN ('conversation', 'entry', 'task', 'submission', 'document'))
    )`,
  sql`CREATE TABLE ${t('conversations')} (
      id ${id} PRIMARY KEY,
      owner_conversation_id BIGINT,
      owner_task_id BIGINT,
      record TEXT NOT NULL
    )`,
  sql`CREATE INDEX ${index('conversations_by_owner_conversation')}
      ON ${t('conversations')} (owner_conversation_id, id)`,
  sql`CREATE INDEX ${index('conversations_by_owner_task')}
      ON ${t('conversations')} (owner_task_id, id)`,
]

/** Entries, with the partial index head-marker lookups use. */
const entryTables = ({ sql, t, index, id }: Ddl) => [
  sql`CREATE TABLE ${t('entries')} (
      id ${id} PRIMARY KEY,
      conversation_id BIGINT NOT NULL,
      head BIGINT,
      commit_seq BIGINT NOT NULL,
      record TEXT NOT NULL
    )`,
  sql`CREATE INDEX ${index('entries_by_conversation')} ON ${t('entries')} (conversation_id, id)`,
  sql`CREATE INDEX ${index('entry_heads_by_conversation')}
      ON ${t('entries')} (conversation_id, id) WHERE head IS NOT NULL`,
]

/** Tasks and submissions, indexed by every filter their scans take. */
const taskTables = ({ sql, t, index, id }: Ddl) => [
  sql`CREATE TABLE ${t('tasks')} (
      id ${id} PRIMARY KEY,
      conversation_id BIGINT NOT NULL,
      kind TEXT NOT NULL,
      status TEXT NOT NULL,
      abort_requested INTEGER NOT NULL CHECK (abort_requested IN (0, 1)),
      background INTEGER NOT NULL CHECK (background IN (0, 1)),
      record TEXT NOT NULL
    )`,
  sql`CREATE INDEX ${index('tasks_by_status')} ON ${t('tasks')} (status, id)`,
  sql`CREATE INDEX ${index('tasks_by_conversation')} ON ${t('tasks')} (conversation_id, id)`,
  sql`CREATE INDEX ${index('tasks_by_kind')} ON ${t('tasks')} (kind, id)`,
  sql`CREATE INDEX ${index('tasks_by_abort_requested')} ON ${t('tasks')} (abort_requested, id)`,
  sql`CREATE INDEX ${index('tasks_by_background')} ON ${t('tasks')} (background, id)`,
  sql`CREATE TABLE ${t('submissions')} (
      id ${id} PRIMARY KEY,
      conversation_id BIGINT NOT NULL,
      request_id TEXT,
      status TEXT NOT NULL,
      record TEXT NOT NULL
    )`,
  sql`CREATE INDEX ${index('submissions_by_request')}
      ON ${t('submissions')} (conversation_id, request_id)`,
  sql`CREATE INDEX ${index('submissions_by_conversation')}
      ON ${t('submissions')} (conversation_id, id)`,
  sql`CREATE INDEX ${index('submissions_by_status')} ON ${t('submissions')} (status, id)`,
]

/** Document incarnations and their revisions. */
const documentTables = ({ sql, t, index, id }: Ddl) => [
  sql`CREATE TABLE ${t('documents')} (
      id ${id} PRIMARY KEY,
      kind TEXT NOT NULL,
      family INTEGER NOT NULL CHECK (family IN (0, 1)),
      key_value TEXT NOT NULL,
      scope_kind TEXT NOT NULL CHECK (scope_kind IN ('session', 'conversation', 'task')),
      owner_id BIGINT NOT NULL,
      created_at BIGINT NOT NULL,
      retired_at BIGINT,
      record TEXT NOT NULL
    )`,
  sql`CREATE INDEX ${index('documents_by_address')}
      ON ${t('documents')} (kind, scope_kind, owner_id, family, key_value, created_at)`,
  sql`CREATE INDEX ${index('documents_by_scope')} ON ${t('documents')} (scope_kind, owner_id, id)`,
  sql`CREATE TABLE ${t('revisions')} (
      document_id BIGINT NOT NULL,
      seq BIGINT NOT NULL,
      kind TEXT NOT NULL CHECK (kind IN ('base', 'delta')),
      version BIGINT NOT NULL,
      content TEXT NOT NULL,
      PRIMARY KEY (document_id, seq)
    )`,
  sql`CREATE INDEX ${index('document_revisions_by_kind')}
      ON ${t('revisions')} (document_id, kind, seq)`,
]

/** The initial schema. SQLite's `INTEGER PRIMARY KEY` is its rowid; PostgreSQL needs `BIGINT`. */
const initial = (sql: SqlClient.SqlClient, { prefix, tables }: Schema) => {
  const ddl: Ddl = {
    sql,
    t: (key) => sql(tables[key]),
    index: (name) => sql(`${prefix}${name}`),
    id: sql.literal(sql.onDialectOrElse({ sqlite: () => 'INTEGER', orElse: () => 'BIGINT' })),
  }
  const statements = [
    ...metadataTables(ddl),
    ...entryTables(ddl),
    ...taskTables(ddl),
    ...documentTables(ddl),
  ]
  return Effect.forEach(statements, (statement) => statement, { discard: true })
}

const migrationFailed = (cause: Migrator.MigrationError) =>
  new StorageError({ message: `SQL storage migration failed: ${cause.message}`, cause })

/**
 * Apply pending migrations with Effect's `Migrator`, which records them in its own table and runs
 * them in one transaction. The `Migrator` turns a failed migration into a defect, so that defect
 * becomes a `StorageError` here.
 */
export const migrate = (
  sql: SqlClient.SqlClient,
  schema: Schema,
): Effect.Effect<void, StorageError | SqlError.SqlError, SqlClient.SqlClient> =>
  Migrator.make({})({
    loader: Migrator.fromRecord({ '1_initial': initial(sql, schema) }),
    table: schema.tables.migrations,
  }).pipe(
    Effect.catchTag('MigrationError', (cause) => Effect.fail(migrationFailed(cause))),
    Effect.catchDefect((defect) =>
      defect instanceof Migrator.MigrationError
        ? Effect.fail(migrationFailed(defect))
        : Effect.die(defect),
    ),
    Effect.asVoid,
  )
