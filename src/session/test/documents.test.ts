import { expect, test } from 'bun:test'
import { Effect } from 'effect'
import type { Storage } from '../../storage/index.ts'
import type { Document } from '../../storage/schema/index.ts'
import { Ids } from '../../storage/schema/index.ts'
import { SessionError } from '../index.ts'
import { createConversation, createTask, published, runSession, terminal } from './support.ts'

type Semantics =
  | { readonly history: 'latest'; readonly fork: 'current' | 'initial' }
  | { readonly history: 'rewindable'; readonly fork: 'asOf' | 'current' | 'initial' }

/** Commit a conversation document straight to Storage, as the document layer will. */
const createDocument = Effect.fnUntraced(function* (
  storage: Storage.Service,
  conversationId: Ids.ConversationId,
  kind: string,
  semantics: Semantics,
) {
  const id = yield* storage.mintId(Ids.DocumentId)
  const scope = { kind: 'conversation' as const, conversationId }
  const record: Document.DocumentCreate = { id, kind, scope, ...semantics }
  const content = { version: 1, kind: 'base' as const, value: { kind } }
  yield* storage.commit([{ type: 'document.create', record, content }])
  return id
})

const current = { kind: 'session' as const }

/** A Storage read that must find its record. */
const required = <A, E>(read: Effect.Effect<A, E>) =>
  Effect.orDie(Effect.flatMap(read, (value) => Effect.fromNullishOr(value)))

test('forks a conversation and copies its documents by fork policy', () =>
  runSession(({ session, storage }) =>
    Effect.gen(function* () {
      const parentId = yield* createConversation(session)
      const asOf = yield* createDocument(storage, parentId, 'notes', {
        history: 'rewindable',
        fork: 'asOf',
      })
      yield* createDocument(storage, parentId, 'state', { history: 'latest', fork: 'current' })
      yield* createDocument(storage, parentId, 'scratch', { history: 'latest', fork: 'initial' })
      const entry = yield* session.commit((tx) => tx.appendEntry(parentId, { kind: 'note' }))
      const at = (yield* required(storage.entry(entry.id))).commitSeq
      const { result: child, publication } = yield* published(session, (tx) =>
        tx.forkConversation(parentId, entry.id, { ownership: { kind: 'ownerless' } }),
      )
      expect(child.parent).toEqual({ conversationId: parentId, at: entry.id })
      const copies = publication.changes.filter((change) => change.type === 'document.copy')
      expect(copies.map(({ record }) => record.kind).toSorted()).toEqual(['notes', 'state'])
      for (const copy of copies) {
        expect(copy.conversationId).toBe(child.id)
        expect(copy.record.createdAt).toBe(publication.seq)
      }
      expect(copies.find(({ record }) => record.kind === 'notes')?.source).toEqual({ id: asOf, at })
      const scope = { kind: 'conversation' as const, conversationId: child.id }
      const copied = yield* storage.findDocument({ kind: 'state', scope }, 'current')
      const stored =
        copied === undefined ? undefined : yield* storage.document(copied.id, 'current')
      expect(stored?.value).toEqual({ kind: 'state' })
      expect(yield* storage.findDocument({ kind: 'scratch', scope }, 'current')).toBeUndefined()
      expect(
        yield* storage.findDocument({ kind: 'notes', scope: current }, 'current'),
      ).toBeUndefined()
    }),
  ))

test('rejects an invisible fork point and stays usable', () =>
  runSession(({ session }) =>
    Effect.gen(function* () {
      const parentId = yield* createConversation(session)
      const ownership = { kind: 'ownerless' as const }
      const error = yield* Effect.flip(
        session.commit((tx) =>
          tx.forkConversation(parentId, Ids.EntryId.make(4040), { ownership }),
        ),
      )
      expect(error).toBeInstanceOf(SessionError.TransactionError)
      expect(error.message).toBe(`Entry 4040 is not visible from conversation ${parentId}`)
      expect(yield* createConversation(session)).toBeNumber()
    }),
  ))

test('retires every task document when its task becomes terminal', () =>
  runSession(({ session, storage }) =>
    Effect.gen(function* () {
      const conversationId = yield* createConversation(session)
      const taskId = yield* createTask(session, conversationId)
      const scope = { kind: 'task' as const, taskId }
      const id = yield* storage.mintId(Ids.DocumentId)
      const content = { version: 1, kind: 'base' as const, value: { lines: [] } }
      yield* storage.commit([
        { type: 'document.create', record: { id, kind: 'progress', scope }, content },
      ])
      const created = yield* required(storage.findDocument({ kind: 'progress', scope }, 'current'))
      const { publication } = yield* published(session, (tx) =>
        Effect.flatMap(tx.task(taskId), (task) =>
          task === undefined ? Effect.void : tx.setTask(terminal(task)),
        ),
      )
      expect(yield* storage.findDocument({ kind: 'progress', scope }, 'current')).toBeUndefined()
      const retired = publication.changes.find((change) => change.type === 'document')
      expect(retired).toEqual({
        type: 'document',
        record: { ...created, retiredAt: publication.seq },
        conversationId,
        version: undefined,
        value: null,
        ops: [],
      })
    }),
  ))
