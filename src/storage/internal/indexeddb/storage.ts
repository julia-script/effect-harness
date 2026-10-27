import { Effect, Semaphore } from 'effect'
import type { Scope } from 'effect'
import { prepareCommit } from '../memory/commit.ts'
import type { PreparedCommit } from '../memory/commit.ts'
import type { State } from '../memory/state.ts'
import { service as memoryService } from '../memory/storage.ts'
import { globalFactory, open, transact } from './idb.ts'
import type { Durability, IdbDatabase, IdbFactory } from './idb.ts'
import { STORES, VERSION, planCommit, restore, upgrade } from './stores.ts'
import type { IndexedDbOptions, Service } from '../../Storage.ts'
import type { StorageWrite } from '../../schema/Write.ts'
import { StorageError } from '../../StorageError.ts'

/**
 * IndexedDB implementation of `Storage` for browsers and other runtimes with an `IDBFactory`.
 *
 * As in the JSONL backend, the in-memory store serves every read and the database is its durable
 * copy: it is loaded whole on open, and a commit is validated in memory, written in one readwrite
 * transaction, and applied to memory only once that transaction completes. IndexedDB aborts a
 * failed transaction as a whole, so a failed commit leaves both the database and memory as they
 * were, and the store stays usable.
 */

interface IndexedDbState {
  readonly db: IdbDatabase
  readonly durability: Durability
  readonly memory: State
  closed: boolean
  /** Set when the connection is lost or another context upgrades the database. */
  lost: StorageError | undefined
}

const usable = (state: IndexedDbState): Effect.Effect<void, StorageError> =>
  Effect.suspend(() => {
    if (state.closed)
      return Effect.fail(new StorageError({ message: 'IndexedDbStorage is closed' }))
    return state.lost === undefined ? Effect.void : Effect.fail(state.lost)
  })

const persist = (state: IndexedDbState, prepared: PreparedCommit) => {
  const operations = planCommit(state.memory, prepared)
  // A commit cut short between its transaction and `apply` would desynchronize disk and memory.
  return Effect.uninterruptible(
    transact(state.db, STORES, 'readwrite', state.durability, (transaction) => {
      for (const operation of operations) {
        const store = transaction.objectStore(operation.store)
        if (operation.type === 'put') store.put(operation.value, operation.key)
        else store.delete(operation.key)
      }
    }).pipe(Effect.map(() => prepared.apply())),
  )
}

const makeCommit = (state: IndexedDbState) => {
  const lock = Semaphore.makeUnsafe(1)
  return (writes: ReadonlyArray<StorageWrite>) =>
    lock.withPermit(
      usable(state).pipe(
        Effect.andThen(prepareCommit(state.memory, writes)),
        Effect.flatMap((prepared) => persist(state, prepared)),
      ),
    )
}

const guard = <A, E>(state: IndexedDbState, effect: Effect.Effect<A, E>) =>
  Effect.andThen(usable(state), effect)

const service = (state: IndexedDbState): Service => {
  const memory = memoryService(state.memory)
  return {
    commit: makeCommit(state),
    mintId: (id) => guard(state, memory.mintId(id)),
    conversation: (id) => guard(state, memory.conversation(id)),
    scanConversations: (query, limit, cursor) =>
      guard(state, memory.scanConversations(query, limit, cursor)),
    entry: (id) => guard(state, memory.entry(id)),
    visibleEntry: (conversationId, id) => guard(state, memory.visibleEntry(conversationId, id)),
    findLatestHeadMarker: (conversationId, atOrBefore) =>
      guard(state, memory.findLatestHeadMarker(conversationId, atOrBefore)),
    scanEntries: (query, limit, cursor) => guard(state, memory.scanEntries(query, limit, cursor)),
    task: (id) => guard(state, memory.task(id)),
    scanTasks: (query, limit, cursor) => guard(state, memory.scanTasks(query, limit, cursor)),
    submission: (id) => guard(state, memory.submission(id)),
    scanSubmissions: (query, limit, cursor) =>
      guard(state, memory.scanSubmissions(query, limit, cursor)),
    submissionByRequest: (conversationId, requestId) =>
      guard(state, memory.submissionByRequest(conversationId, requestId)),
    findDocument: (address, at) => guard(state, memory.findDocument(address, at)),
    document: (id, at) => guard(state, memory.document(id, at)),
    scanDocuments: (query, limit, cursor) =>
      guard(state, memory.scanDocuments(query, limit, cursor)),
  }
}

/** Read every store in one transaction and rebuild memory from it. */
const load = (db: IdbDatabase) =>
  transact(db, STORES, 'readonly', 'default', (transaction) =>
    STORES.map((store) => transaction.objectStore(store).getAll()),
  ).pipe(Effect.flatMap((requests) => restore(requests.map((request) => request.result))))

const factoryOf = (options: IndexedDbOptions): Effect.Effect<IdbFactory, StorageError> => {
  const factory = options.factory ?? globalFactory()
  return factory === undefined
    ? Effect.fail(new StorageError({ message: 'IndexedDB is not available in this runtime' }))
    : Effect.succeed(factory)
}

/** Open or create the IndexedDB database `name`. Closing the scope closes the store. */
export const makeIndexedDb = Effect.fnUntraced(function* (
  name: string,
  options: IndexedDbOptions = {},
): Effect.fn.Return<Service, StorageError, Scope.Scope> {
  const factory = yield* factoryOf(options)
  const db = yield* Effect.acquireRelease(open(factory, name, VERSION, upgrade), (db) =>
    Effect.sync(() => db.close()),
  )
  const memory = yield* load(db)
  const state: IndexedDbState = {
    db,
    durability: options.durability ?? 'default',
    memory,
    closed: false,
    lost: undefined,
  }
  const lose = (message: string) => () => {
    state.lost ??= new StorageError({ message })
  }
  // Another context needs a newer schema: let it proceed, and stop writing to this one.
  db.onversionchange = () => {
    lose('IndexedDB database was upgraded by another connection and must be reopened')()
    db.close()
  }
  db.onclose = lose('IndexedDB connection was closed unexpectedly and must be reopened')
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      state.closed = true
    }),
  )
  return service(state)
})
