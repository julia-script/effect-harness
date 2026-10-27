import { Effect, Exit, Scope } from 'effect'
import { IDBFactory } from 'fake-indexeddb'
import type { RunCase } from '../support.ts'
import { Storage } from '../../index.ts'

export const DATABASE = 'harness'

/** A factory with no databases yet, so every test starts from an empty browser profile. */
export const freshFactory = (): Storage.IndexedDbFactory => new IDBFactory()

/** Run a test body against a fresh factory. */
export const runInFactory = <A, E>(
  body: (factory: Storage.IndexedDbFactory) => Effect.Effect<A, E, Scope.Scope>,
): Promise<A> => Effect.runPromise(Effect.scoped(body(freshFactory())))

export const open = (factory: Storage.IndexedDbFactory, options?: Storage.IndexedDbOptions) =>
  Storage.makeIndexedDb(DATABASE, { ...options, factory })

export const runIndexedDb: RunCase = (body) =>
  runInFactory((factory) => Effect.flatMap(open(factory), body))

/** A store whose every commit closes and reopens the database, so reads exercise loading. */
const makeReopening = Effect.fnUntraced(function* (factory: Storage.IndexedDbFactory) {
  let scope = yield* Scope.make()
  let current = yield* open(factory).pipe(Scope.provide(scope))
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
  const reopen = Effect.gen(function* () {
    yield* Scope.close(scope, Exit.void)
    scope = yield* Scope.make()
    current = yield* open(factory).pipe(Scope.provide(scope), Effect.orDie)
  })
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

export const runReopening: RunCase = (body) =>
  runInFactory((factory) => Effect.flatMap(makeReopening(factory), body))
