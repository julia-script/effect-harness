import { expect, test } from 'bun:test'
import { Effect } from 'effect'
import {
  createDocument,
  fileLines,
  join,
  open,
  runInDirectory,
  sessionDocument,
  terminalTask,
} from './support.ts'
import type { Fault, Faults } from './support.ts'
import { createRoot, entryWrite, pendingTask } from '../support.ts'
import { Ids } from '../../schema/index.ts'

const rootId = Ids.ROOT_CONVERSATION_ID

test('publishes every commit before reclaiming current-only document and terminal-task sidecars', () =>
  runInDirectory((directory) =>
    Effect.gen(function* () {
      const storage = yield* open(directory)
      yield* createRoot(storage)
      const taskId = yield* storage.mintId(Ids.TaskId)
      const documentId = yield* storage.mintId(Ids.DocumentId)
      const taskFile = yield* join(directory, `task-${taskId}.jsonl`)
      const documentFile = yield* join(directory, `doc-${documentId}.jsonl`)
      yield* storage.commit([{ type: 'task', value: pendingTask(taskId, rootId) }])
      yield* storage.commit([createDocument(sessionDocument(documentId), { count: 0 })])
      yield* storage.commit([
        {
          type: 'document.change',
          id: documentId,
          content: { kind: 'delta', version: 1, ops: [] },
        },
      ])
      yield* storage.commit([
        {
          type: 'document.change',
          id: documentId,
          content: { kind: 'base', version: 1, value: { count: 1 } },
        },
      ])
      expect(yield* fileLines(taskFile)).toHaveLength(1)
      expect(yield* fileLines(documentFile)).toHaveLength(1)

      yield* storage.commit([{ type: 'document.retire', id: documentId }])
      yield* storage.commit([{ type: 'task', value: terminalTask(taskId) }])
      const main = yield* fileLines(yield* join(directory, 'main.jsonl'))
      expect(main).toHaveLength(7)
      for (const line of main) expect(line).toStartWith('{"format":1,"type":"commit"')
      expect(yield* fileLines(taskFile)).toEqual([])
      expect(yield* fileLines(documentFile)).toEqual([])

      const reopened = yield* open(directory)
      expect(yield* reopened.task(taskId)).toEqual(terminalTask(taskId))
      expect(yield* reopened.document(documentId, 'current')).toBeUndefined()
    }),
  ))

test('orders multiple live-task replacements in one sidecar by commit ordinal', () =>
  runInDirectory((directory) =>
    Effect.gen(function* () {
      const storage = yield* open(directory)
      yield* createRoot(storage)
      const taskId = yield* storage.mintId(Ids.TaskId)
      yield* storage.commit([
        { type: 'task', value: pendingTask(taskId, rootId, 'first') },
        { type: 'task', value: pendingTask(taskId, rootId, 'second') },
      ])
      expect(yield* fileLines(yield* join(directory, `task-${taskId}.jsonl`))).toHaveLength(2)
      const reopened = yield* open(directory)
      expect(yield* reopened.task(taskId)).toEqual(pendingTask(taskId, rootId, 'second'))
    }),
  ))

test('serializes the complete commit before I/O and leaves serialization failures usable', () =>
  runInDirectory((directory) =>
    Effect.gen(function* () {
      const faults: Faults = { appends: 0, fault: { call: 99, mode: 'before' } }
      const storage = yield* open(directory, faults)
      yield* createRoot(storage)
      faults.appends = 0
      const id = yield* storage.mintId(Ids.EntryId)
      const error = yield* Effect.flip(
        storage.commit([entryWrite(id, rootId, 'bad', { data: { value: Number.NaN } })]),
      )
      expect(error.message).toStartWith('Commit cannot be serialized')
      expect(faults.appends).toBe(0)
      expect(yield* storage.entry(id)).toBeUndefined()
      expect(yield* storage.commit([entryWrite(id, rootId, 'good')])).toBe(Ids.Seq.make(2))
    }),
  ))

const faults: ReadonlyArray<Fault> = [
  { call: 1, mode: 'before' },
  { call: 1, mode: 'after' },
  { call: 1, mode: 'short' },
  { call: 2, mode: 'before' },
  { call: 2, mode: 'after' },
  { call: 3, mode: 'before' },
  { call: 3, mode: 'short' },
  { call: 3, mode: 'after' },
]

for (const fault of faults) {
  test(`poisons after a ${fault.mode} failure at append ${fault.call} and recovers only published state`, () =>
    runInDirectory((directory) =>
      Effect.gen(function* () {
        const armed: Faults = { appends: 0 }
        const storage = yield* open(directory, armed)
        yield* createRoot(storage)
        const firstId = yield* storage.mintId(Ids.DocumentId)
        const secondId = yield* storage.mintId(Ids.DocumentId)
        armed.fault = fault
        const error = yield* Effect.flip(
          storage.commit([
            createDocument(sessionDocument(firstId, 'first'), { text: 'α' }),
            createDocument(sessionDocument(secondId, 'second'), { text: 'β' }),
          ]),
        )
        expect(error.message).toBe('JSONL storage is poisoned and must be reopened')
        const read = yield* Effect.flip(storage.document(firstId, 'current'))
        expect(read.message).toBe('JSONL storage is poisoned and must be reopened')

        const reopened = yield* open(directory)
        // Only the marker, the third append, publishes the commit.
        const published = fault.call === 3 && fault.mode === 'after'
        const first = yield* reopened.document(firstId, 'current')
        const second = yield* reopened.document(secondId, 'current')
        expect(first?.value).toEqual(published ? { text: 'α' } : undefined)
        expect(second?.value).toEqual(published ? { text: 'β' } : undefined)
        expect(yield* reopened.commit([])).toBe(Ids.Seq.make(published ? 3 : 2))
      }),
    ))
}

test('never reclaims rewindable document history, including after a base and retirement', () =>
  runInDirectory((directory) =>
    Effect.gen(function* () {
      const storage = yield* open(directory)
      yield* createRoot(storage)
      const id = yield* storage.mintId(Ids.DocumentId)
      const file = yield* join(directory, `doc-${id}.jsonl`)
      const createdAt = yield* storage.commit([
        createDocument(
          {
            id,
            kind: 'rewindable',
            scope: { kind: 'conversation', conversationId: rootId },
            history: 'rewindable',
            fork: 'asOf',
          },
          { count: 0 },
        ),
      ])
      const changedAt = yield* storage.commit([
        {
          type: 'document.change',
          id,
          content: { kind: 'delta', version: 1, ops: [['s', ['count'], 1]] },
        },
      ])
      yield* storage.commit([
        { type: 'document.change', id, content: { kind: 'base', version: 1, value: { count: 2 } } },
      ])
      yield* storage.commit([{ type: 'document.retire', id }])
      expect(yield* fileLines(file)).toHaveLength(3)

      const reopened = yield* open(directory)
      expect((yield* reopened.document(id, createdAt))?.value).toEqual({ count: 0 })
      expect((yield* reopened.document(id, changedAt))?.value).toEqual({ count: 1 })
      expect(yield* reopened.document(id, 'current')).toBeUndefined()
      expect(yield* fileLines(file)).toHaveLength(3)
    }),
  ))

test('fails every operation after its scope closes', () =>
  runInDirectory((directory) =>
    Effect.gen(function* () {
      const storage = yield* Effect.scoped(open(directory))
      expect((yield* Effect.flip(storage.commit([]))).message).toBe('JsonlStorage is closed')
      expect((yield* Effect.flip(storage.mintId(Ids.EntryId))).message).toBe(
        'JsonlStorage is closed',
      )
    }),
  ))
