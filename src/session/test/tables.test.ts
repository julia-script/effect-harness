import { expect, test } from 'bun:test'
import { Effect } from 'effect'
import { Ids } from '../../storage/schema/index.ts'
import { SessionError } from '../index.ts'
import type { Session } from '../index.ts'
import { createConversation, createTask, published, runSession, terminal, Work } from './support.ts'

test('allows table reads only before the first table write', () =>
  runSession(({ session }) =>
    Effect.gen(function* () {
      const conversationId = yield* createConversation(session)
      yield* session.commit((tx) =>
        Effect.gen(function* () {
          expect(yield* tx.conversation(conversationId)).toEqual({ id: conversationId })
          expect(yield* tx.scanTasks({ conversationId }, 10)).toEqual({ items: [] })
          yield* tx.appendEntry(conversationId, { kind: 'note' })
          const error = yield* Effect.flip(tx.conversation(conversationId))
          expect(error).toBeInstanceOf(SessionError.ReadAfterWrite)
          expect(error.message).toBe(
            'Tx.conversation() cannot read tables after the first table write',
          )
          const late = yield* Effect.flip(tx.scanEntries({ conversationId }, 10))
          expect(late).toBeInstanceOf(SessionError.ReadAfterWrite)
        }),
      )
    }),
  ))

test('passes caller-selected limits and cursors through table scans', () =>
  runSession(({ session }) =>
    Effect.gen(function* () {
      const ids = [
        yield* createConversation(session),
        yield* createConversation(session),
        yield* createConversation(session),
      ]
      yield* session.commit((tx) =>
        Effect.gen(function* () {
          const first = yield* tx.scanConversations({}, 2)
          expect(first.items.map(({ id }) => id)).toEqual(ids.slice(0, 2))
          const second = yield* tx.scanConversations({}, 2, first.next)
          expect(second.items.map(({ id }) => id)).toEqual(ids.slice(2))
          expect(second.next).toBeUndefined()
        }),
      )
    }),
  ))

const createAll = (tx: Session.Tx) =>
  Effect.gen(function* () {
    const conversation = yield* tx.createConversation({ ownership: { kind: 'ownerless' } })
    const first = yield* tx.appendEntry(conversation.id, { kind: 'note', data: 'one' })
    const headed = yield* tx.appendEntry(conversation.id, { kind: 'summary', head: 'self' })
    const options = {
      ownership: { kind: 'conversation' as const },
      conversationId: conversation.id,
      background: true,
    }
    const task = yield* tx.createTask(Work, { path: 'x' }, options)
    return { conversation, first, headed, task }
  })

test('creates conversations, entries, and tasks with minted IDs and publishes them', () =>
  runSession(({ session, storage, recorder }) =>
    Effect.gen(function* () {
      const { result, publication } = yield* published(session, createAll)
      const { conversation, first, headed, task } = result
      expect(new Set([conversation.id, first.id, headed.id, task]).size).toBe(4)
      expect(headed.head).toBe(headed.id)
      expect(first).toEqual({
        id: first.id,
        conversationId: conversation.id,
        kind: 'note',
        data: 'one',
      })
      expect((yield* storage.entry(headed.id))?.entry).toEqual(headed)
      expect(yield* storage.task(task)).toEqual({
        id: task,
        conversationId: conversation.id,
        kind: 'test.work',
        version: 1,
        input: { path: 'x' },
        background: true,
        abortRequested: false,
        state: { status: 'pending', checkpoint: { phase: 'start', path: 'x' } },
      })
      const stored = yield* Effect.orDie(
        Effect.flatMap(storage.entry(first.id), Effect.fromNullishOr),
      )
      expect(publication.seq).toBe(stored.commitSeq)
      expect(publication.changes.map((change) => change.type).toSorted()).toEqual([
        'conversation',
        'entry',
        'entry',
        'task',
      ])
      // Publications share the committed records.
      const committed: ReadonlyArray<unknown> = recorder.commits.at(-1) ?? []
      expect(publication.changes.every((change) => committed.includes(change))).toBe(true)
    }),
  ))

test('rejects operations on missing records without committing anything', () =>
  runSession(({ session, recorder }) =>
    Effect.gen(function* () {
      const commits = recorder.commits.length
      const missingConversation = yield* Effect.flip(
        session.commit((tx) => tx.appendEntry(Ids.ConversationId.make(12_345), { kind: 'note' })),
      )
      expect(missingConversation).toBeInstanceOf(SessionError.TransactionError)
      expect(missingConversation.message).toBe('Conversation 12345 does not exist')
      const unbound = yield* Effect.flip(
        session.commit((tx) =>
          tx.createTask(Work, { path: 'x' }, { ownership: { kind: 'conversation' } }),
        ),
      )
      expect(unbound.message).toBe('Tx.createTask() requires options.conversationId')
      expect(recorder.commits.length).toBe(commits)
    }),
  ))

test('detaches table JSON from the caller and binds commits to their scope', () =>
  runSession(({ session, storage }) =>
    Effect.gen(function* () {
      const conversationId = yield* createConversation(session)
      const taskId = yield* createTask(session, conversationId)
      const payload = { nested: { value: 1 } }
      const scope = { conversationId, taskId }
      const { entry, child } = yield* session.commit(
        (tx) =>
          Effect.gen(function* () {
            const created = yield* tx.appendEntry(conversationId, { kind: 'data', data: payload })
            payload.nested.value = 2
            // The scope's conversation is the default for new tasks.
            const created2 = yield* tx.createTask(
              Work,
              { path: 'b' },
              { ownership: { kind: 'conversation' } },
            )
            return { entry: created, child: created2 }
          }),
        scope,
      )
      expect((yield* storage.entry(entry.id))?.entry).toEqual({
        id: entry.id,
        conversationId,
        kind: 'data',
        data: { nested: { value: 1 } },
        byTaskId: taskId,
      })
      expect((yield* storage.task(child))?.conversationId).toBe(conversationId)
    }),
  ))

test('creates a conversation owned by a task staged in the same transaction', () =>
  runSession(({ session, storage }) =>
    Effect.gen(function* () {
      const parentId = yield* createConversation(session)
      const { supervisor, child } = yield* session.commit((tx) =>
        Effect.gen(function* () {
          const ownership = { kind: 'conversation' as const }
          const id = yield* tx.createTask(
            Work,
            { path: 's' },
            { ownership, conversationId: parentId },
          )
          const owned = yield* tx.createConversation({ ownership: { kind: 'task', taskId: id } })
          return { supervisor: id, child: owned }
        }),
      )
      expect(child.owner).toEqual({ conversationId: parentId, taskId: supervisor })
      expect(yield* storage.conversation(child.id)).toEqual(child)
      const moved = yield* Effect.flip(
        session.commit((tx) =>
          Effect.gen(function* () {
            const task = yield* tx.task(supervisor)
            if (task !== undefined) yield* tx.setTask({ ...task, conversationId: child.id })
          }),
        ),
      )
      expect(moved.message).toBe(`Task ${supervisor} cannot change conversations`)
    }),
  ))

test('rejects missing and abort-marked owners atomically', () =>
  runSession(({ session, storage }) =>
    Effect.gen(function* () {
      const parentId = yield* createConversation(session)
      const missing = yield* Effect.flip(
        session.commit((tx) =>
          tx.createConversation({ ownership: { kind: 'task', taskId: Ids.TaskId.make(999) } }),
        ),
      )
      expect(missing.message).toBe('Conversation owner task 999 does not exist')

      let childId: Ids.ConversationId | undefined
      const marked = yield* Effect.flip(
        session.commit((tx) =>
          Effect.gen(function* () {
            const ownership = { kind: 'conversation' as const }
            const id = yield* tx.createTask(
              Work,
              { path: 'm' },
              { ownership, conversationId: parentId },
            )
            childId = (yield* tx.createConversation({ ownership: { kind: 'task', taskId: id } })).id
            const task = yield* tx.setTask({
              id,
              conversationId: parentId,
              kind: Work.name,
              version: Work.version,
              input: { path: 'm' },
              background: false,
              abortRequested: true,
              state: { status: 'pending', checkpoint: {} },
            })
            return task
          }),
        ),
      )
      expect(marked.message).toMatch(/^Conversation owner task \d+ is abort-marked$/u)
      expect(
        childId === undefined ? undefined : yield* storage.conversation(childId),
      ).toBeUndefined()
    }),
  ))

test('rejects owned work of a terminal owner', () =>
  runSession(({ session }) =>
    Effect.gen(function* () {
      const parentId = yield* createConversation(session)
      const finished = yield* createTask(session, parentId)
      yield* session.commit((tx) =>
        Effect.flatMap(tx.task(finished), (task) =>
          task === undefined ? Effect.void : tx.setTask(terminal(task)),
        ),
      )
      const late = yield* Effect.flip(
        session.commit((tx) =>
          Effect.gen(function* () {
            const ownership = { kind: 'task' as const, taskId: finished }
            yield* tx.createTask(Work, { path: 'late' }, { ownership })
          }),
        ),
      )
      expect(late.message).toBe(`Task owner ${finished} is terminal`)
    }),
  ))
