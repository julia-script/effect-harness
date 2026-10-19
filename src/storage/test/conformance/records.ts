import { expect } from 'bun:test'
import { Effect, Schema } from 'effect'
import { storageCase, createRoot, entry, entryWrite, ids, pendingTask } from '../support.ts'
import type { StorageCase } from '../support.ts'
import { StorageError } from '../../index.ts'
import type { Submission, Task } from '../../schema/index.ts'
import { Ids } from '../../schema/index.ts'

export const recordCases: ReadonlyArray<StorageCase> = [
  storageCase('reserves ID 1 for the immutable root conversation', (storage) =>
    Effect.gen(function* () {
      expect(yield* storage.mintId(Ids.ConversationId)).toBe(Ids.ConversationId.make(2))
      expect(yield* createRoot(storage)).toBe(Ids.ROOT_CONVERSATION_ID)
      expect(yield* storage.conversation(Ids.ROOT_CONVERSATION_ID)).toEqual({
        id: Ids.ROOT_CONVERSATION_ID,
      })
      const error = yield* Effect.flip(
        storage.commit([{ type: 'conversation', value: { id: Ids.ROOT_CONVERSATION_ID } }]),
      )
      expect(error).toBeInstanceOf(StorageError.StorageError)
      expect(error.message).toBe('ID 1 already belongs to conversation')
    }),
  ),

  storageCase('commits mixed writes atomically and rolls all of them back on failure', (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage)
      const entryId = yield* storage.mintId(Ids.EntryId)
      const task = pendingTask(yield* storage.mintId(Ids.TaskId), rootId)
      const input: Submission.SubmissionRecord = {
        id: yield* storage.mintId(Ids.SubmissionId),
        conversationId: rootId,
        requestId: 'request-1',
        type: 'input',
        status: 'placed',
        entry: entryId,
      }
      const seq = yield* storage.commit([
        entryWrite(entryId, rootId, 'user', { data: { text: 'hello' } }),
        { type: 'task', value: task },
        { type: 'submission', value: input },
      ])
      expect(yield* storage.entry(entryId)).toEqual({
        entry: entry(entryId, rootId, 'user', { data: { text: 'hello' } }),
        commitSeq: seq,
      })

      const transientId = yield* storage.mintId(Ids.EntryId)
      const error = yield* Effect.flip(
        storage.commit([
          { type: 'task', value: { ...task, state: { status: 'running', checkpoint: {} } } },
          { type: 'submission', value: { ...input, status: 'done', answer: transientId } },
          entryWrite(transientId, rootId, 'assistant'),
          { type: 'conversation', value: { id: rootId } },
        ]),
      )
      expect(error.message).toBe('ID 1 already belongs to conversation')
      expect(yield* storage.task(task.id)).toEqual(task)
      expect(yield* storage.submission(input.id)).toEqual(input)
      expect(yield* storage.entry(transientId)).toBeUndefined()
      const after = yield* storage.commit([
        entryWrite(yield* storage.mintId(Ids.EntryId), rootId, 'after-rollback'),
      ])
      expect(after).toBeGreaterThan(seq)
    }),
  ),

  storageCase('detaches retained writes and every returned record', (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage)
      const entryId = yield* storage.mintId(Ids.EntryId)
      const data = { nested: [1, 2] }
      const checkpoint = { nested: { count: 1 } }
      const task: Task.TaskRecord = {
        ...pendingTask(yield* storage.mintId(Ids.TaskId), rootId),
        state: { status: 'pending', checkpoint },
      }
      yield* storage.commit([
        entryWrite(entryId, rootId, 'note', { data }),
        { type: 'task', value: task },
      ])

      data.nested.push(3)
      checkpoint.nested.count = 2
      expect((yield* storage.entry(entryId))?.entry.data).toEqual({ nested: [1, 2] })
      const read = yield* storage.task(task.id)
      expect(read?.state).toEqual({ status: 'pending', checkpoint: { nested: { count: 1 } } })

      const readData = (yield* storage.entry(entryId))?.entry.data
      if (Array.isArray(readData)) readData.push(9)
      expect((yield* storage.entry(entryId))?.entry.data).toEqual({ nested: [1, 2] })
    }),
  ),

  storageCase('detaches prototype-like JSON keys without changing object prototypes', (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage)
      const entryId = yield* storage.mintId(Ids.EntryId)
      const data = yield* Schema.decodeUnknownEffect(Schema.Json)(
        JSON.parse('{"__proto__":{"polluted":false},"constructor":"stored"}'),
      )
      yield* storage.commit([entryWrite(entryId, rootId, 'note', { data })])
      const read = (yield* storage.entry(entryId))?.entry.data ?? null
      if (typeof read !== 'object' || read === null) throw new Error('Expected an object')
      expect(Object.getPrototypeOf(read)).toBe(Object.prototype)
      expect(Object.hasOwn(read, '__proto__')).toBe(true)
      expect(Reflect.get(read, '__proto__')).toEqual({ polluted: false })
      expect(Reflect.get(read, 'constructor')).toBe('stored')
      expect(Reflect.get({}, 'polluted')).toBeUndefined()
    }),
  ),

  storageCase('keeps one global ID namespace and rejects exhausted ID minting', (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage)
      yield* storage.commit([entryWrite(Ids.EntryId.make(100), rootId)])
      expect(yield* storage.mintId(Ids.EntryId)).toBe(Ids.EntryId.make(101))
      const owned = yield* Effect.flip(
        storage.commit([{ type: 'task', value: pendingTask(Ids.TaskId.make(100), rootId) }]),
      )
      expect(owned.message).toBe('ID 100 already belongs to entry')
      yield* storage.commit([entryWrite(Ids.EntryId.make(Number.MAX_SAFE_INTEGER), rootId)])
      expect((yield* Effect.flip(storage.mintId(Ids.EntryId))).message).toBe(
        'ID space is exhausted',
      )
    }),
  ),

  storageCase(
    'replaces task and submission records and moves them between status indexes',
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage)
        const task = pendingTask(yield* storage.mintId(Ids.TaskId), rootId)
        const queued: Submission.SubmissionRecord = {
          id: yield* storage.mintId(Ids.SubmissionId),
          conversationId: rootId,
          requestId: 'request',
          type: 'write',
          status: 'queued',
        }
        yield* storage.commit([
          { type: 'task', value: task },
          { type: 'submission', value: queued },
        ])
        const running: Task.TaskRecord = { ...task, state: { status: 'running', checkpoint: {} } }
        const done: Submission.SubmissionRecord = {
          ...queued,
          status: 'done',
          entry: Ids.EntryId.make(99),
        }
        yield* storage.commit([
          { type: 'task', value: running },
          { type: 'submission', value: done },
        ])
        expect(ids(yield* storage.scanTasks({ status: 'pending' }, 10))).toEqual([])
        expect(ids(yield* storage.scanTasks({ status: 'running' }, 10))).toEqual([task.id])
        expect(ids(yield* storage.scanTasks({ kind: 'other' }, 10))).toEqual([])
        expect(ids(yield* storage.scanSubmissions({ status: 'done' }, 10))).toEqual([done.id])
        expect(yield* storage.submissionByRequest(rootId, 'request')).toEqual(done)
        expect(yield* storage.submissionByRequest(rootId, 'other')).toBeUndefined()
      }),
  ),

  storageCase('builds lazy effects that read the state they run against', (storage) =>
    Effect.gen(function* () {
      const read = storage.conversation(Ids.ROOT_CONVERSATION_ID)
      const scan = storage.scanConversations({}, 10)
      expect(yield* read).toBeUndefined()
      yield* createRoot(storage)
      expect(yield* read).toEqual({ id: Ids.ROOT_CONVERSATION_ID })
      expect(ids(yield* scan)).toEqual([Ids.ROOT_CONVERSATION_ID])
    }),
  ),
]
