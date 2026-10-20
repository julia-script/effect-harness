import { expect } from 'bun:test'
import { Effect } from 'effect'
import { storageCase, createRoot, entryWrite, ids, pendingTask } from '../support.ts'
import type { StorageCase } from '../support.ts'
import type { Storage } from '../../index.ts'
import type { Document, Submission } from '../../schema/index.ts'
import { Ids } from '../../schema/index.ts'

interface TaskScope {
  readonly kind: 'task'
  readonly taskId: Ids.TaskId
}

const base = { kind: 'base', version: 1, value: {} } as const

/** Documents at distinct addresses whose keys look like prototype members. */
const addressRecords = (
  storage: Storage.Service,
  rootId: Ids.ConversationId,
  taskScope: TaskScope,
) =>
  Effect.gen(function* () {
    const session = { kind: 'session' } as const
    const first: Document.DocumentCreate = {
      id: yield* storage.mintId(Ids.DocumentId),
      kind: 'cache',
      scope: session,
      key: '__proto__',
    }
    const second: Document.DocumentCreate = {
      ...first,
      id: yield* storage.mintId(Ids.DocumentId),
      key: 'constructor',
    }
    const conversation: Document.DocumentCreate = {
      id: yield* storage.mintId(Ids.DocumentId),
      kind: 'cache',
      scope: { kind: 'conversation', conversationId: rootId },
      history: 'latest',
      fork: 'current',
      key: '__proto__',
    }
    const singleton: Document.DocumentCreate = {
      id: yield* storage.mintId(Ids.DocumentId),
      kind: 'task.cache',
      scope: taskScope,
    }
    const member: Document.DocumentCreate = {
      ...singleton,
      id: yield* storage.mintId(Ids.DocumentId),
      key: 'member',
    }
    const other: Document.DocumentCreate = {
      id: yield* storage.mintId(Ids.DocumentId),
      kind: 'task.other',
      scope: taskScope,
    }
    return { first, second, conversation, singleton, member, other }
  })

export const addressCases: ReadonlyArray<StorageCase> = [
  storageCase('indexes logical addresses and exact-scope scans independently', (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage)
      const taskId = yield* storage.mintId(Ids.TaskId)
      const taskScope = { kind: 'task', taskId } as const
      const records = yield* addressRecords(storage, rootId, taskScope)
      const { first, second, conversation, singleton, member } = records
      yield* storage.commit([
        { type: 'task', value: pendingTask(taskId, rootId) },
        ...Object.values(records).map(
          (record) => ({ type: 'document.create', record, content: base }) as const,
        ),
      ])

      const session = { kind: 'session' } as const
      const proto = { kind: 'cache', scope: session, key: '__proto__' }
      expect((yield* storage.findDocument(proto, 'current'))?.id).toBe(first.id)
      const page = yield* storage.scanDocuments({ scope: session, at: 'current' }, 1)
      const next = yield* storage.scanDocuments({ scope: session, at: 'current' }, 1, page.next)
      expect([...ids(page), ...ids(next)]).toEqual([first.id, second.id])
      const conversationScope = { kind: 'conversation', conversationId: rootId } as const
      const inConversation = { scope: conversationScope, at: 'current' } as const
      expect(ids(yield* storage.scanDocuments(inConversation, 10))).toEqual([conversation.id])
      const taskCache = { kind: 'task.cache', scope: taskScope }
      expect((yield* storage.findDocument(taskCache, 'current'))?.id).toBe(singleton.id)
      expect((yield* storage.findDocument({ ...taskCache, key: 'member' }, 'current'))?.id).toBe(
        member.id,
      )
      const byKind = { scope: taskScope, at: 'current', kind: 'task.cache' } as const
      expect(ids(yield* storage.scanDocuments(byKind, 10))).toEqual([singleton.id, member.id])
    }),
  ),

  storageCase(
    'gives create-plus-retire an empty lifetime and rejects a second current incarnation',
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage)
        const record = {
          id: yield* storage.mintId(Ids.DocumentId),
          kind: 'one',
          scope: { kind: 'session' },
        } as const
        yield* storage.commit([{ type: 'document.create', record, content: base }])
        const duplicate = yield* Effect.flip(
          storage.commit([
            {
              type: 'document.create',
              record: { ...record, id: yield* storage.mintId(Ids.DocumentId) },
              content: base,
            },
          ]),
        )
        expect(duplicate.message).toBe('Document address already has a current incarnation')

        const empty: Document.DocumentCreate = {
          id: yield* storage.mintId(Ids.DocumentId),
          kind: 'one',
          key: 'empty',
          scope: { kind: 'conversation', conversationId: rootId },
          history: 'rewindable',
          fork: 'initial',
        }
        const emptyAt = yield* storage.commit([
          { type: 'document.create', record: empty, content: base },
          { type: 'document.retire', id: empty.id },
        ])
        expect(yield* storage.document(empty.id, 'current')).toBeUndefined()
        expect(yield* storage.document(empty.id, emptyAt)).toBeUndefined()
        const address = { kind: 'one', scope: empty.scope, key: 'empty' }
        expect(yield* storage.findDocument(address, emptyAt)).toBeUndefined()
      }),
  ),

  storageCase(
    'rolls back record tables and secondary indexes when a document command fails',
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage)
        const task = pendingTask(yield* storage.mintId(Ids.TaskId), rootId)
        const submission: Submission.SubmissionRecord = {
          id: yield* storage.mintId(Ids.SubmissionId),
          conversationId: rootId,
          requestId: 'atomic',
          type: 'input',
          status: 'queued',
        }
        const record = {
          id: yield* storage.mintId(Ids.DocumentId),
          kind: 'atomic',
          scope: { kind: 'session' },
        } as const
        yield* storage.commit([
          { type: 'task', value: task },
          { type: 'submission', value: submission },
          { type: 'document.create', record, content: base },
        ])
        const entryId = yield* storage.mintId(Ids.EntryId)
        yield* Effect.flip(
          storage.commit([
            { type: 'task', value: { ...task, state: { status: 'running', checkpoint: {} } } },
            {
              type: 'submission',
              value: { ...submission, status: 'unanswered', reason: 'failed' },
            },
            entryWrite(entryId, rootId, 'transient'),
            {
              type: 'document.create',
              record: { ...record, id: yield* storage.mintId(Ids.DocumentId) },
              content: base,
            },
          ]),
        )
        expect(ids(yield* storage.scanTasks({ status: 'pending' }, 10))).toEqual([task.id])
        expect(ids(yield* storage.scanTasks({ status: 'running' }, 10))).toEqual([])
        expect(yield* storage.submissionByRequest(rootId, 'atomic')).toEqual(submission)
        expect(yield* storage.entry(entryId)).toBeUndefined()
        expect(ids(yield* storage.scanEntries({ conversationId: rootId }, 10))).toEqual([])
      }),
  ),
]
