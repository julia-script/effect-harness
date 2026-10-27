import { expect, test } from 'bun:test'
import { Effect, Exit, Scope } from 'effect'
import { DATABASE, open, runInFactory } from './support.ts'
import { createRoot, entry, entryWrite, ids } from '../support.ts'
import { Storage } from '../../index.ts'
import { open as openDatabase, transact } from '../../internal/indexeddb/idb.ts'
import type { IdbDatabase, IdbFactory } from '../../internal/indexeddb/idb.ts'
import { REVISIONS, VERSION, upgrade } from '../../internal/indexeddb/stores.ts'
import type { Document } from '../../schema/index.ts'
import { Ids } from '../../schema/index.ts'

const rootId = Ids.ROOT_CONVERSATION_ID

/** Run `body` on a raw connection to the test database, closing it afterwards. */
const withDatabase = <A, E>(factory: IdbFactory, body: (db: IdbDatabase) => Effect.Effect<A, E>) =>
  Effect.acquireUseRelease(openDatabase(factory, DATABASE, VERSION, upgrade), body, (db) =>
    Effect.sync(() => db.close()),
  )

/** Every key of one object store. */
const keys = (factory: IdbFactory, store: string) =>
  withDatabase(factory, (db) =>
    transact(db, [store], 'readonly', 'default', (transaction) =>
      (
        transaction.objectStore(store) as unknown as {
          getAllKeys: () => { readonly result: ReadonlyArray<unknown> }
        }
      ).getAllKeys(),
    ).pipe(Effect.map((request) => request.result)),
  )

const putRaw = (factory: IdbFactory, store: string, key: number, value: unknown) =>
  withDatabase(factory, (db) =>
    transact(db, [store], 'readwrite', 'default', (transaction) => {
      transaction.objectStore(store).put(value, key)
    }),
  )

const sessionDocument = (id: Ids.DocumentId): Document.DocumentCreate => ({
  id,
  kind: 'test.document',
  scope: { kind: 'session' },
})

const base = (value: Document.DocumentBase['value']): Document.DocumentBase => ({
  kind: 'base',
  version: 1,
  value,
})

test('keeps commits and minted IDs across reopen', () =>
  runInFactory((factory) =>
    Effect.gen(function* () {
      const minted = yield* Effect.scoped(
        Effect.gen(function* () {
          const storage = yield* open(factory)
          yield* createRoot(storage)
          const entryId = yield* storage.mintId(Ids.EntryId)
          yield* storage.commit([entryWrite(entryId, rootId)])
          const unused = yield* storage.mintId(Ids.TaskId)
          // An empty commit still records every ID minted before it.
          yield* storage.commit([])
          return { entryId, unused }
        }),
      )
      const storage = yield* open(factory)
      expect(yield* storage.entry(minted.entryId)).toEqual({
        entry: entry(minted.entryId, rootId),
        commitSeq: Ids.Seq.make(2),
      })
      const page = yield* storage.scanEntries({ conversationId: rootId }, 10)
      expect(ids(page)).toEqual([minted.entryId])
      expect(yield* storage.mintId(Ids.EntryId)).toBeGreaterThan(minted.unused)
      expect(yield* storage.commit([])).toBe(Ids.Seq.make(4))
    }),
  ))

test('deletes the revisions a current-only document no longer keeps', () =>
  runInFactory((factory) =>
    Effect.gen(function* () {
      const storage = yield* open(factory)
      yield* createRoot(storage)
      const id = yield* storage.mintId(Ids.DocumentId)
      yield* storage.commit([
        { type: 'document.create', record: sessionDocument(id), content: base({ n: 1 }) },
      ])
      yield* storage.commit([
        {
          type: 'document.change',
          id,
          content: { kind: 'delta', version: 1, ops: [['s', ['n'], 2]] },
        },
      ])
      expect(yield* keys(factory, REVISIONS)).toEqual([
        [id, 2],
        [id, 3],
      ])
      yield* storage.commit([{ type: 'document.change', id, content: base({ n: 3 }) }])
      expect(yield* keys(factory, REVISIONS)).toEqual([[id, 4]])
      yield* storage.commit([{ type: 'document.retire', id }])
      expect(yield* keys(factory, REVISIONS)).toEqual([])
    }),
  ))

test('fails to open a database holding a row that does not match its schema', () =>
  runInFactory((factory) =>
    Effect.gen(function* () {
      yield* putRaw(factory, 'conversations', 1, { id: 'one' })
      const error = yield* Effect.flip(open(factory))
      expect(error.message).toBe('Corrupt IndexedDB storage: a row does not match its schema')
    }),
  ))

test('fails to open a database with two records under one ID', () =>
  runInFactory((factory) =>
    Effect.gen(function* () {
      yield* putRaw(factory, 'conversations', 1, { id: 1 })
      yield* putRaw(factory, 'tasks', 1, { id: 1 })
      const error = yield* Effect.flip(open(factory))
      expect(error.message).toStartWith('Corrupt IndexedDB storage')
    }),
  ))

test('fails when IndexedDB is unavailable', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const error = yield* Effect.flip(Effect.scoped(Storage.makeIndexedDb(DATABASE)))
      expect(error.message).toBe('IndexedDB is not available in this runtime')
    }),
  ))

test('fails every operation after its scope closes', () =>
  runInFactory((factory) =>
    Effect.gen(function* () {
      const scope = yield* Scope.make()
      const storage = yield* open(factory).pipe(Scope.provide(scope))
      yield* Scope.close(scope, Exit.void)
      expect((yield* Effect.flip(storage.commit([]))).message).toBe('IndexedDbStorage is closed')
      expect((yield* Effect.flip(storage.mintId(Ids.EntryId))).message).toBe(
        'IndexedDbStorage is closed',
      )
    }),
  ))

test('stops once another connection upgrades the database', () =>
  runInFactory((factory) =>
    Effect.gen(function* () {
      const storage = yield* open(factory)
      yield* createRoot(storage)
      const newer = yield* openDatabase(factory, DATABASE, VERSION + 1, () => {})
      newer.close()
      const error = yield* Effect.flip(storage.conversation(rootId))
      expect(error.message).toBe(
        'IndexedDB database was upgraded by another connection and must be reopened',
      )
    }),
  ))

test('leaves the store unchanged and usable when a commit transaction aborts', () =>
  runInFactory((factory) =>
    Effect.gen(function* () {
      let abortNext = false
      const faulty: IdbFactory = {
        open: (name, version) => {
          const request = factory.open(name, version)
          return new Proxy(request, {
            get: (target, property) =>
              property === 'result' ? withAbort(target.result) : Reflect.get(target, property),
          })
        },
      }
      const withAbort = (db: IdbDatabase): IdbDatabase =>
        new Proxy(db, {
          get: (target, property) => {
            if (property !== 'transaction') {
              const value = Reflect.get(target, property)
              return typeof value === 'function' ? value.bind(target) : value
            }
            const begin: IdbDatabase['transaction'] = (stores, mode, options) => {
              const transaction = target.transaction(stores, mode, options)
              if (abortNext && mode === 'readwrite') {
                abortNext = false
                queueMicrotask(() => transaction.abort())
              }
              return transaction
            }
            return begin
          },
          set: (target, property, value) => Reflect.set(target, property, value),
        })
      const storage = yield* open(faulty)
      yield* createRoot(storage)
      const lost = yield* storage.mintId(Ids.EntryId)
      abortNext = true
      const error = yield* Effect.flip(storage.commit([entryWrite(lost, rootId)]))
      expect(error.message).toBe('IndexedDB transaction was aborted')
      expect(yield* storage.entry(lost)).toBeUndefined()
      const kept = yield* storage.mintId(Ids.EntryId)
      expect(yield* storage.commit([entryWrite(kept, rootId)])).toBe(Ids.Seq.make(2))
      const reopened = yield* Effect.scoped(
        Effect.flatMap(open(factory), (fresh) => fresh.scanEntries({ conversationId: rootId }, 10)),
      )
      expect(ids(reopened)).toEqual([kept])
    }),
  ))
