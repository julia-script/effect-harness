import { PgliteClient } from '@effect/sql-pglite'
import { SqliteClient } from '@effect/sql-sqlite-bun'
import { Effect, Exit, Layer, ManagedRuntime, Scope } from 'effect'
import type { SqlClient } from 'effect/sql'
import type { RunCase } from '../support.ts'
import { Storage } from '../../index.ts'

/** Runs a body against a database; each run gets tables of its own. */
export interface Database {
  readonly run: <A, E>(
    body: (options: Storage.SqlOptions) => Effect.Effect<A, E, SqlClient.SqlClient | Scope.Scope>,
  ) => Promise<A>
}

/** A fresh in-memory SQLite database through Bun's driver for every run. */
export const sqlite: Database = {
  run: (body) =>
    Effect.runPromise(
      Effect.scoped(body({})).pipe(
        // Each test is an entry point that provides its own database.
        // oxlint-disable-next-line effecttsgo/strict-effect-provide
        Effect.provide(Layer.orDie(SqliteClient.layer({ filename: ':memory:' }))),
      ),
    ),
}

/**
 * One in-memory PostgreSQL database through PGlite, shared by every run of the file, which starts
 * it once. Each run uses its own table prefix, so runs also check that prefixes isolate stores.
 */
export const pglite = (): Database & { readonly dispose: () => Promise<void> } => {
  const runtime = ManagedRuntime.make(Layer.orDie(PgliteClient.layer({})))
  let runs = 0
  return {
    run: (body) => {
      runs++
      return runtime.runPromise(Effect.scoped(body({ prefix: `run${runs}_` })))
    },
    dispose: () => runtime.dispose(),
  }
}

export const runSql =
  (database: Database): RunCase =>
  (body) =>
    database.run((options) => Effect.flatMap(Storage.makeSql(options), body))

/** A store reopened on the same database after every commit, so reads exercise reopening. */
const makeReopening = Effect.fnUntraced(function* (options: Storage.SqlOptions) {
  const client = yield* Effect.context<SqlClient.SqlClient>()
  let scope = yield* Scope.make()
  let current = yield* Storage.makeSql(options).pipe(Scope.provide(scope))
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
  const reopen = Effect.gen(function* () {
    yield* Scope.close(scope, Exit.void)
    scope = yield* Scope.make()
    current = yield* Storage.makeSql(options).pipe(Scope.provide(scope), Effect.orDie)
  }).pipe(Effect.provideContext(client))
  const service: Storage.Service = {
    commit: (writes) =>
      Effect.ensuring(
        Effect.suspend(() => current.commit(writes)),
        reopen,
      ),
    mintId: (id) => Effect.suspend(() => current.mintId(id)),
    conversation: (id) => Effect.suspend(() => current.conversation(id)),
    scanConversations: (...args) => Effect.suspend(() => current.scanConversations(...args)),
    entry: (id) => Effect.suspend(() => current.entry(id)),
    visibleEntry: (...args) => Effect.suspend(() => current.visibleEntry(...args)),
    findLatestHeadMarker: (...args) => Effect.suspend(() => current.findLatestHeadMarker(...args)),
    scanEntries: (...args) => Effect.suspend(() => current.scanEntries(...args)),
    task: (id) => Effect.suspend(() => current.task(id)),
    scanTasks: (...args) => Effect.suspend(() => current.scanTasks(...args)),
    submission: (id) => Effect.suspend(() => current.submission(id)),
    scanSubmissions: (...args) => Effect.suspend(() => current.scanSubmissions(...args)),
    submissionByRequest: (...args) => Effect.suspend(() => current.submissionByRequest(...args)),
    findDocument: (...args) => Effect.suspend(() => current.findDocument(...args)),
    document: (...args) => Effect.suspend(() => current.document(...args)),
    scanDocuments: (...args) => Effect.suspend(() => current.scanDocuments(...args)),
  }
  return service
})

export const runReopening =
  (database: Database): RunCase =>
  (body) =>
    database.run((options) => Effect.flatMap(makeReopening(options), body))
