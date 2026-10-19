import { expect } from 'bun:test'
import { Effect, Schema } from 'effect'
import { storageCase, createRoot, entryWrite, ids, pendingTask } from '../support.ts'
import type { StorageCase } from '../support.ts'
import type { Storage } from '../../index.ts'
import { StorageError } from '../../index.ts'
import { Ids, Scan } from '../../schema/index.ts'

/** Root → child forked at `rootFork` → grandchild forked at `childFork`, with excluded entries. */
const forkChain = (storage: Storage.Service) =>
  Effect.gen(function* () {
    const rootId = yield* createRoot(storage)
    const [rootFirst, rootFork, rootExcluded] = [
      yield* storage.mintId(Ids.EntryId),
      yield* storage.mintId(Ids.EntryId),
      yield* storage.mintId(Ids.EntryId),
    ]
    const rootSeq = yield* storage.commit([
      entryWrite(rootFirst, rootId),
      entryWrite(rootFork, rootId, 'marker', { head: rootFirst }),
      entryWrite(rootExcluded, rootId),
    ])
    const childId = yield* storage.mintId(Ids.ConversationId)
    const childFork = yield* storage.mintId(Ids.EntryId)
    const childExcluded = yield* storage.mintId(Ids.EntryId)
    yield* storage.commit([
      {
        type: 'conversation',
        value: { id: childId, parent: { conversationId: rootId, at: rootFork } },
      },
      entryWrite(childFork, childId, 'note'),
      entryWrite(childExcluded, childId),
    ])
    const grandchildId = yield* storage.mintId(Ids.ConversationId)
    const head = yield* storage.mintId(Ids.EntryId)
    const tail = yield* storage.mintId(Ids.EntryId)
    yield* storage.commit([
      {
        type: 'conversation',
        value: { id: grandchildId, parent: { conversationId: childId, at: childFork } },
      },
      entryWrite(head, grandchildId, 'marker', { head }),
      entryWrite(tail, grandchildId),
    ])
    const entries = { rootFirst, rootFork, rootExcluded, childFork, childExcluded, head, tail }
    return { rootId, childId, grandchildId, rootSeq, ...entries }
  })

export const entryCases: ReadonlyArray<StorageCase> = [
  storageCase('scans deep fork history in either order through every ancestor cap', (storage) =>
    Effect.gen(function* () {
      const chain = yield* forkChain(storage)
      const query = { conversationId: chain.grandchildId }
      const first = yield* storage.scanEntries(query, 2)
      expect(ids(first)).toEqual([chain.tail, chain.head])
      const second = yield* storage.scanEntries(query, 2, first.next)
      expect(ids(second)).toEqual([chain.childFork, chain.rootFork])
      const third = yield* storage.scanEntries(query, 2, second.next)
      expect(ids(third)).toEqual([chain.rootFirst])
      expect(third.next).toBeUndefined()

      const ascending = { ...query, order: 'ascending' } as const
      const up = yield* storage.scanEntries(ascending, 2)
      expect(ids(up)).toEqual([chain.rootFirst, chain.rootFork])
      // The cursor carries its order, so the query may omit it.
      expect(ids(yield* storage.scanEntries(query, 2, up.next))).toEqual([
        chain.childFork,
        chain.head,
      ])
      const bounded = { ...ascending, minEntryId: chain.rootFork, maxEntryId: chain.childFork }
      expect(ids(yield* storage.scanEntries(bounded, 10))).toEqual([
        chain.rootFork,
        chain.childFork,
      ])
      const mismatch = yield* Effect.flip(
        storage.scanEntries({ ...query, order: 'descending' }, 2, up.next),
      )
      expect(mismatch).toBeInstanceOf(StorageError.StorageRequestError)
      expect(mismatch.message).toContain('cursor')
    }),
  ),

  storageCase('finds head markers and visible entries through the fork chain', (storage) =>
    Effect.gen(function* () {
      const chain = yield* forkChain(storage)
      const current = yield* storage.findLatestHeadMarker(chain.grandchildId)
      expect([current?.id, current?.head]).toEqual([chain.head, chain.head])
      const historical = yield* storage.findLatestHeadMarker(chain.grandchildId, chain.childFork)
      expect([historical?.id, historical?.head]).toEqual([chain.rootFork, chain.rootFirst])
      expect(
        yield* storage.findLatestHeadMarker(chain.grandchildId, chain.rootFirst),
      ).toBeUndefined()

      expect(yield* storage.visibleEntry(chain.grandchildId, chain.rootFirst)).toEqual({
        entry: { id: chain.rootFirst, conversationId: chain.rootId, kind: 'message' },
        commitSeq: chain.rootSeq,
      })
      expect(
        (yield* storage.visibleEntry(chain.grandchildId, chain.childFork))?.entry.conversationId,
      ).toBe(chain.childId)
      for (const hidden of [chain.rootExcluded, chain.childExcluded]) {
        expect(yield* storage.visibleEntry(chain.grandchildId, hidden)).toBeUndefined()
        expect((yield* storage.entry(hidden))?.entry.id).toBe(hidden)
      }
      expect(yield* storage.visibleEntry(chain.rootId, chain.head)).toBeUndefined()
      const unknown = Ids.ConversationId.make(999_999)
      const error = yield* Effect.flip(storage.visibleEntry(unknown, chain.rootFirst))
      expect(error.message).toBe('Unknown conversation: 999999')
    }),
  ),

  storageCase(
    'indexes entries committed out of ID order and continues a cursor after newer commits',
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage)
        yield* storage.commit([
          entryWrite(Ids.EntryId.make(30), rootId),
          entryWrite(Ids.EntryId.make(10), rootId),
          entryWrite(Ids.EntryId.make(20), rootId, 'marker', { head: Ids.EntryId.make(10) }),
        ])
        const first = yield* storage.scanEntries({ conversationId: rootId }, 2)
        expect(ids(first)).toEqual([30, 20])
        expect((yield* storage.findLatestHeadMarker(rootId))?.id).toBe(Ids.EntryId.make(20))
        yield* storage.commit([entryWrite(yield* storage.mintId(Ids.EntryId), rootId)])
        const second = yield* storage.scanEntries({ conversationId: rootId }, 2, first.next)
        expect(ids(second)).toEqual([10])
        expect(second.next).toBeUndefined()
      }),
  ),

  storageCase('filters and pages conversations by owner edges', (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage)
      const other = yield* storage.mintId(Ids.ConversationId)
      const [taskA, taskB] = [yield* storage.mintId(Ids.TaskId), yield* storage.mintId(Ids.TaskId)]
      const [a, b, c] = [
        yield* storage.mintId(Ids.ConversationId),
        yield* storage.mintId(Ids.ConversationId),
        yield* storage.mintId(Ids.ConversationId),
      ]
      yield* storage.commit([
        { type: 'conversation', value: { id: other } },
        {
          type: 'conversation',
          value: { id: a, owner: { conversationId: rootId, taskId: taskA } },
        },
        {
          type: 'conversation',
          value: { id: b, owner: { conversationId: rootId, taskId: taskB } },
        },
        { type: 'conversation', value: { id: c, owner: { conversationId: other, taskId: taskA } } },
      ])
      const first = yield* storage.scanConversations({ ownerConversationId: rootId }, 1)
      expect(ids(first)).toEqual([a])
      const roundTripped = yield* Schema.decodeUnknownEffect(Scan.Cursor)(
        JSON.parse(JSON.stringify(first.next)),
      )
      expect(
        ids(yield* storage.scanConversations({ ownerConversationId: rootId }, 1, roundTripped)),
      ).toEqual([b])
      expect(ids(yield* storage.scanConversations({ ownerTaskId: taskA }, 10))).toEqual([a, c])
      const both = { ownerConversationId: rootId, ownerTaskId: taskA }
      expect(ids(yield* storage.scanConversations(both, 10))).toEqual([a])
    }),
  ),

  storageCase('scans tables in either ID order and continues a cursor in its order', (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage)
      const taskIds: number[] = []
      for (let index = 0; index < 3; index++) {
        const id = yield* storage.mintId(Ids.TaskId)
        yield* storage.commit([{ type: 'task', value: pendingTask(id, rootId) }])
        taskIds.push(id)
      }
      const all = (order?: Scan.ScanOrder) =>
        Effect.gen(function* () {
          let page = yield* storage.scanTasks(order === undefined ? {} : { order }, 2)
          const found = ids(page)
          while (page.next !== undefined) {
            page = yield* storage.scanTasks({}, 2, page.next)
            found.push(...ids(page))
          }
          return found
        })
      expect(yield* all()).toEqual(taskIds)
      expect(yield* all('descending')).toEqual(taskIds.toReversed())
      const descending = yield* storage.scanTasks({ order: 'descending' }, 2)
      const error = yield* Effect.flip(
        storage.scanTasks({ order: 'ascending' }, 2, descending.next),
      )
      expect(error.message).toBe(
        'The cursor continues a descending scan; the query asks for ascending',
      )
      const malformed = yield* Effect.flip(storage.scanTasks({}, 2, { after: 'x' }))
      expect(malformed.message).toBe('Invalid storage cursor')
    }),
  ),
]
