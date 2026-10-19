import { expect } from 'bun:test'
import { Effect } from 'effect'
import { storageCase, createRoot, ids } from '../support.ts'
import type { StorageCase } from '../support.ts'
import type { Storage } from '../../index.ts'
import { StorageError } from '../../index.ts'
import type { Document, Json } from '../../schema/index.ts'
import { Ids } from '../../schema/index.ts'

const rewindable = (id: Ids.DocumentId, conversationId: Ids.ConversationId, kind = 'notes') =>
  ({
    id,
    kind,
    scope: { kind: 'conversation', conversationId },
    history: 'rewindable',
    fork: 'asOf',
  }) satisfies Document.DocumentCreate

const create = (
  storage: Storage.Service,
  record: Document.DocumentCreate,
  value: Json.JsonObject,
  version = 1,
) =>
  storage.commit([{ type: 'document.create', record, content: { kind: 'base', version, value } }])

const change = (
  storage: Storage.Service,
  id: Ids.DocumentId,
  ops: ReadonlyArray<Document.Op>,
  version = 1,
) => storage.commit([{ type: 'document.change', id, content: { kind: 'delta', version, ops } }])

export const documentCases: ReadonlyArray<StorageCase> = [
  storageCase(
    'reconstructs rewindable documents at every commit, detached from callers',
    (storage) =>
      Effect.gen(function* () {
        const record = rewindable(yield* storage.mintId(Ids.DocumentId), yield* createRoot(storage))
        const initial = { items: ['a'], nested: { count: 1 } }
        const createdAt = yield* create(storage, record, initial)
        const appended = ['b']
        const changedAt = yield* change(storage, record.id, [
          ['p', ['items'], 1, 0, appended],
          ['s', ['nested', 'count'], 2],
        ])
        initial.items.push('caller mutation')
        appended.push('caller mutation')

        expect(yield* storage.document(record.id, createdAt)).toMatchObject({
          version: 1,
          value: { items: ['a'], nested: { count: 1 } },
          deltasSinceBase: 0,
        })
        const changed = yield* storage.document(record.id, changedAt)
        expect(changed?.value).toEqual({ items: ['a', 'b'], nested: { count: 2 } })
        expect(changed?.deltasSinceBase).toBe(1)
        const items = changed?.value['items']
        if (Array.isArray(items)) items.push('read mutation')
        expect((yield* storage.document(record.id, 'current'))?.value).toEqual({
          items: ['a', 'b'],
          nested: { count: 2 },
        })

        const checkpointAt = yield* storage.commit([
          {
            type: 'document.change',
            id: record.id,
            content: { kind: 'base', version: 2, value: { items: ['checkpoint'] } },
          },
        ])
        const replacedAt = yield* change(storage, record.id, [['r', { items: ['replaced'] }]], 2)
        expect(yield* storage.document(record.id, checkpointAt)).toMatchObject({
          version: 2,
          deltasSinceBase: 0,
        })
        expect(yield* storage.document(record.id, replacedAt)).toMatchObject({
          value: { items: ['replaced'] },
          deltasSinceBase: 1,
        })
        expect((yield* storage.document(record.id, changedAt))?.version).toBe(1)
      }),
  ),

  storageCase(
    'preserves half-open incarnations when one replaces another at an address',
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage)
        const first = rewindable(yield* storage.mintId(Ids.DocumentId), rootId)
        const second = { ...first, id: yield* storage.mintId(Ids.DocumentId) }
        const createdAt = yield* create(storage, first, { owner: 'first' })
        const replacedAt = yield* storage.commit([
          {
            type: 'document.create',
            record: second,
            content: { kind: 'base', version: 1, value: {} },
          },
          { type: 'document.retire', id: first.id },
          {
            type: 'document.change',
            id: first.id,
            content: { kind: 'delta', version: 1, ops: [] },
          },
        ])
        const address = { kind: first.kind, scope: first.scope }
        expect((yield* storage.findDocument(address, createdAt))?.id).toBe(first.id)
        expect(yield* storage.findDocument(address, replacedAt)).toMatchObject({
          id: second.id,
          createdAt: replacedAt,
        })
        expect((yield* storage.findDocument(address, 'current'))?.id).toBe(second.id)
        const scan = (at: typeof createdAt) => storage.scanDocuments({ scope: first.scope, at }, 10)
        expect(ids(yield* scan(createdAt))).toEqual([first.id])
        expect(ids(yield* scan(replacedAt))).toEqual([second.id])
        expect(yield* storage.document(first.id, replacedAt)).toBeUndefined()
        expect((yield* storage.document(first.id, createdAt))?.value).toEqual({ owner: 'first' })
      }),
  ),

  storageCase(
    'copies stored document bases and rejects sources changed in the same batch',
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage)
        const childId = yield* storage.mintId(Ids.ConversationId)
        yield* storage.commit([{ type: 'conversation', value: { id: childId } }])
        const source = rewindable(yield* storage.mintId(Ids.DocumentId), rootId)
        const createdAt = yield* create(storage, source, { count: 1 }, 2)
        yield* change(storage, source.id, [['s', ['count'], 2]], 2)
        const historical = rewindable(yield* storage.mintId(Ids.DocumentId), childId)
        yield* storage.commit([
          { type: 'document.copy', record: historical, source: { id: source.id, at: createdAt } },
        ])
        expect(yield* storage.document(historical.id, 'current')).toMatchObject({
          version: 2,
          value: { count: 1 },
          deltasSinceBase: 0,
        })

        const conflict = rewindable(yield* storage.mintId(Ids.DocumentId), childId, 'other')
        const error = yield* Effect.flip(
          storage.commit([
            { type: 'document.copy', record: conflict, source: { id: source.id, at: 'current' } },
            { type: 'document.retire', id: source.id },
          ]),
        )
        expect(error.message).toBe(`Document copy ${conflict.id} was rejected`)
        expect(yield* storage.document(conflict.id, 'current')).toBeUndefined()
        expect((yield* storage.document(source.id, 'current'))?.value).toEqual({ count: 2 })

        const mismatch = yield* Effect.flip(
          storage.commit([
            { type: 'document.copy', record: conflict, source: { id: source.id, at: 'current' } },
          ]),
        )
        expect(mismatch.message).toBe(`Document copy ${conflict.id} was rejected`)
      }),
  ),

  storageCase(
    'uses bases for version transitions and keeps current-only documents current',
    (storage) =>
      Effect.gen(function* () {
        yield* createRoot(storage)
        const id = yield* storage.mintId(Ids.DocumentId)
        yield* create(storage, { id, kind: 'settings', scope: { kind: 'session' } }, { count: 1 })
        yield* change(storage, id, [['s', ['count'], 2]])
        const migratedAt = yield* storage.commit([
          {
            type: 'document.change',
            id,
            content: { kind: 'base', version: 2, value: { count: 3 } },
          },
        ])
        expect(yield* storage.document(id, 'current')).toMatchObject({
          version: 2,
          value: { count: 3 },
        })
        const historical = yield* Effect.flip(storage.document(id, migratedAt))
        expect(historical).toBeInstanceOf(StorageError.StorageRequestError)
        expect(historical.message).toBe(`Document ${id} does not retain historical content`)
        const stale = yield* Effect.flip(change(storage, id, [['s', ['count'], 4]]))
        expect(stale.message).toBe(`Document ${id} version transition requires a base`)
        yield* storage.commit([{ type: 'document.retire', id }])
        expect(yield* storage.document(id, 'current')).toBeUndefined()
        const retired = yield* Effect.flip(change(storage, id, [], 2))
        expect(retired.message).toBe(`Document ${id} is retired`)
      }),
  ),
]
