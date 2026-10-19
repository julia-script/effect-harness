import { expect, test } from 'bun:test'
import { Effect, FileSystem } from 'effect'
import {
  appendBytes,
  createDocument,
  fileLines,
  fileSize,
  join,
  open,
  openFailure,
  runInDirectory,
  sessionDocument,
  terminalTask,
} from './support.ts'
import { createRoot, entryWrite, pendingTask } from '../support.ts'
import type { Document } from '../../schema/index.ts'
import { Ids } from '../../schema/index.ts'

const rootId = Ids.ROOT_CONVERSATION_ID

/** A store with the root conversation and one session document holding `value`. */
const withDocument = Effect.fnUntraced(function* (
  directory: string,
  value: Document.DocumentBase['value'] = {},
) {
  const storage = yield* open(directory)
  yield* createRoot(storage)
  const id = yield* storage.mintId(Ids.DocumentId)
  yield* storage.commit([createDocument(sessionDocument(id), value)])
  return { storage, id, file: yield* join(directory, `doc-${id}.jsonl`) }
})

const writeText = (path: string, text: string, flag: 'a' | 'w' = 'w') =>
  Effect.flatMap(Effect.service(FileSystem.FileSystem), (fs) =>
    fs.writeFileString(path, text, { flag }),
  )

test('truncates torn UTF-8 tails at exact byte offsets and reuses the unpublished sequence', () =>
  runInDirectory((directory) =>
    Effect.gen(function* () {
      const { id, file } = yield* withDocument(directory, { text: 'kept' })
      const main = yield* join(directory, 'main.jsonl')
      const sizes = [yield* fileSize(file), yield* fileSize(main)]
      const torn = new TextEncoder().encode('{"text":"€').subarray(0, -1)
      yield* appendBytes(file, torn)
      yield* appendBytes(main, torn)

      const reopened = yield* open(directory)
      expect([yield* fileSize(file), yield* fileSize(main)]).toEqual(sizes)
      const seq = yield* reopened.commit([
        { type: 'document.change', id, content: { kind: 'delta', version: 1, ops: [] } },
      ])
      expect(seq).toBe(Ids.Seq.make(3))
    }),
  ))

test('removes complete unpublished sidecar tails without resurrecting terminal tasks', () =>
  runInDirectory((directory) =>
    Effect.gen(function* () {
      const storage = yield* open(directory)
      yield* createRoot(storage)
      const taskId = yield* storage.mintId(Ids.TaskId)
      yield* storage.commit([{ type: 'task', value: pendingTask(taskId, rootId) }])
      yield* storage.commit([{ type: 'task', value: terminalTask(taskId) }])
      const file = yield* join(directory, `task-${taskId}.jsonl`)
      const stale = {
        format: 1,
        type: 'record',
        seq: 4,
        ordinal: 0,
        payload: { type: 'task', value: pendingTask(taskId, rootId, 'stale') },
      }
      yield* writeText(file, `${JSON.stringify(stale)}\n`, 'a')

      const reopened = yield* open(directory)
      expect(yield* fileLines(file)).toEqual([])
      expect(yield* reopened.task(taskId)).toEqual(terminalTask(taskId))
      expect(yield* reopened.commit([])).toBe(Ids.Seq.make(4))
    }),
  ))

test('mints IDs above every recovered record', () =>
  runInDirectory((directory) =>
    Effect.gen(function* () {
      const storage = yield* open(directory)
      yield* createRoot(storage)
      yield* storage.commit([entryWrite(Ids.EntryId.make(40), rootId)])
      const reopened = yield* open(directory)
      expect(yield* reopened.mintId(Ids.EntryId)).toBe(Ids.EntryId.make(41))
    }),
  ))

test('fails to open when published sidecar data is missing', () =>
  runInDirectory((directory) =>
    Effect.gen(function* () {
      const { id, file } = yield* withDocument(directory)
      yield* writeText(file, '')
      expect(yield* openFailure(directory)).toBe(
        `Missing confirmed sidecar record doc-${id}.jsonl at sequence 2`,
      )
    }),
  ))

test('rejects a published record after an unpublished sidecar record', () =>
  runInDirectory((directory) =>
    Effect.gen(function* () {
      const { storage, id, file } = yield* withDocument(directory, { count: 0 })
      yield* storage.commit([
        {
          type: 'document.change',
          id,
          content: { kind: 'delta', version: 1, ops: [['s', ['count'], 1]] },
        },
      ])
      const [first, second] = yield* fileLines(file)
      const unpublished = JSON.stringify({
        format: 1,
        type: 'record',
        seq: 2,
        ordinal: 999,
        payload: { type: 'document', id, content: { kind: 'delta', version: 1, ops: [] } },
      })
      yield* writeText(file, `${first}\n${unpublished}\n${second}\n`)
      expect(yield* openFailure(directory)).toBe(
        `Confirmed record follows an unconfirmed tail in doc-${id}.jsonl`,
      )
    }),
  ))

test('rejects non-increasing commit sequences', () =>
  runInDirectory((directory) =>
    Effect.gen(function* () {
      const storage = yield* open(directory)
      yield* createRoot(storage)
      const main = yield* join(directory, 'main.jsonl')
      const [marker] = yield* fileLines(main)
      yield* writeText(main, `${marker}\n`, 'a')
      expect(yield* openFailure(directory)).toBe(
        'Commit sequence does not strictly increase in main.jsonl',
      )
    }),
  ))

test('rejects structurally invalid published document content', () =>
  runInDirectory((directory) =>
    Effect.gen(function* () {
      const { id, file } = yield* withDocument(directory)
      const [line = ''] = yield* fileLines(file)
      yield* writeText(file, `${line.replace('"value":{}', '"value":7')}\n`)
      expect(yield* openFailure(directory)).toBe(`Invalid sidecar record in doc-${id}.jsonl line 1`)
    }),
  ))

test('rejects malformed complete main and sidecar lines', () =>
  runInDirectory((directory) =>
    Effect.gen(function* () {
      const storage = yield* open(directory)
      yield* createRoot(storage)
      const main = yield* join(directory, 'main.jsonl')
      yield* writeText(main, '{bad}\n', 'a')
      expect(yield* openFailure(directory)).toBe('Malformed complete main.jsonl line 2')
      yield* writeText(main, '')
      yield* writeText(yield* join(directory, 'doc-99.jsonl'), '{bad}\n')
      expect(yield* openFailure(directory)).toBe('Malformed complete doc-99.jsonl line 1')
    }),
  ))

test('rejects invalid UTF-8 in a complete line', () =>
  runInDirectory((directory) =>
    Effect.gen(function* () {
      yield* appendBytes(yield* join(directory, 'main.jsonl'), Uint8Array.of(0xff, 0x0a))
      expect(yield* openFailure(directory)).toBe('Invalid UTF-8 in complete main.jsonl line 1')
    }),
  ))
