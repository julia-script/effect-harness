import { expect, test } from 'bun:test'
import { Effect } from 'effect'
import type { Task } from '../../storage/schema/index.ts'
import { Ids } from '../../storage/schema/index.ts'
import type { Session } from '../index.ts'
import { createConversation, createTask, readTask, runSession, terminal } from './support.ts'

const withState = (task: Task.TaskRecord, state: Task.LiveTaskState): Task.TaskRecord => ({
  ...terminal(task),
  state,
})

/** Commit a replacement of the task built from its committed record. */
const replaceTask = (
  session: Session.Service,
  id: Ids.TaskId,
  next: (task: Task.TaskRecord) => Task.TaskRecord,
) =>
  session.commit((tx) =>
    Effect.flatMap(tx.task(id), (task) =>
      task === undefined ? Effect.die('missing task') : tx.setTask(next(task)),
    ),
  )

test('replaces task records completely and stamps lifecycle times', () =>
  runSession(({ session, storage }) =>
    Effect.gen(function* () {
      const conversationId = yield* createConversation(session)
      const taskId = yield* createTask(session, conversationId)
      const running = yield* replaceTask(session, taskId, (task) => ({
        ...withState(task, { status: 'running', checkpoint: { phase: 'next', step: 2 } }),
        memos: { choice: 'b' },
      }))
      expect(running.startedAt).toBeNumber()
      expect(yield* storage.task(taskId)).toEqual(running)
      const ended = yield* replaceTask(session, taskId, terminal)
      expect(ended.endedAt).toBeNumber()
      expect(yield* storage.task(taskId)).toEqual(ended)

      // Within a transaction, times carry over from the candidate a record replaces.
      const otherId = yield* createTask(session, conversationId)
      const both = yield* session.commit((tx) =>
        Effect.gen(function* () {
          const task = yield* tx.task(otherId)
          if (task === undefined) return yield* Effect.die('missing task')
          const started = yield* tx.setTask(withState(task, { status: 'running', checkpoint: {} }))
          return { started, finished: yield* tx.setTask(terminal(task)) }
        }),
      )
      expect(both.finished.startedAt).toBe(both.started.startedAt)
      expect(both.finished.endedAt).toBeNumber()
    }),
  ))

test('rejects replacing a missing, terminal, or terminal-candidate task', () =>
  runSession(({ session }) =>
    Effect.gen(function* () {
      const conversationId = yield* createConversation(session)
      const taskId = yield* createTask(session, conversationId)
      const committed = yield* readTask(session, taskId)
      const twice = yield* Effect.flip(
        session.commit((tx) =>
          Effect.andThen(tx.setTask(terminal(committed)), tx.setTask(committed)),
        ),
      )
      expect(twice.message).toBe(`Task ${taskId} already has a terminal candidate`)
      yield* replaceTask(session, taskId, terminal)
      const after = yield* Effect.flip(session.commit((tx) => tx.setTask(committed)))
      expect(after.message).toBe(`Task ${taskId} is already terminal`)
      const ghost = { ...committed, id: Ids.TaskId.make(4242) }
      const missing = yield* Effect.flip(session.commit((tx) => tx.setTask(ghost)))
      expect(missing.message).toBe('Task 4242 does not exist')
    }),
  ))

test('places and settles submissions against their latest record', () =>
  runSession(({ session, storage }) =>
    Effect.gen(function* () {
      const conversationId = yield* createConversation(session)
      const { input, write, entry } = yield* session.commit((tx) =>
        Effect.gen(function* () {
          const queued = { conversationId, status: 'queued' as const }
          const created = yield* tx.createSubmission({ ...queued, type: 'input', requestId: 'r1' })
          const passive = yield* tx.createSubmission({ ...queued, type: 'write' })
          const user = yield* tx.appendEntry(conversationId, { kind: 'user' })
          // Changes resolve against the records this transaction just created.
          yield* tx.placeSubmission(created.id, user.id)
          yield* tx.placeSubmission(passive.id, user.id)
          return { input: created, write: passive, entry: user }
        }),
      )
      expect(yield* storage.submission(input.id)).toEqual({
        id: input.id,
        conversationId,
        requestId: 'r1',
        type: 'input',
        status: 'placed',
        entry: entry.id,
      })
      expect((yield* storage.submission(write.id))?.status).toBe('done')
      yield* session.commit((tx) =>
        tx.settleSubmission(input.id, { status: 'unanswered', reason: 'aborted' }),
      )
      expect(yield* storage.submission(input.id)).toEqual({
        id: input.id,
        conversationId,
        requestId: 'r1',
        type: 'input',
        status: 'unanswered',
        reason: 'aborted',
        entry: entry.id,
      })
      // A settled submission stays as it is.
      yield* session.commit((tx) =>
        tx.settleSubmission(write.id, { status: 'done', answer: entry.id }),
      )
      expect((yield* storage.submission(write.id))?.status).toBe('done')
    }),
  ))

test('rejects answering an input that is not placed, or a missing submission', () =>
  runSession(({ session }) =>
    Effect.gen(function* () {
      const conversationId = yield* createConversation(session)
      const { input, entry } = yield* session.commit((tx) =>
        Effect.gen(function* () {
          const create = { conversationId, type: 'input' as const, status: 'queued' as const }
          const created = yield* tx.createSubmission(create)
          return { input: created, entry: yield* tx.appendEntry(conversationId, { kind: 'a' }) }
        }),
      )
      const answer = { status: 'done' as const, answer: entry.id }
      const queued = yield* Effect.flip(
        session.commit((tx) => tx.settleSubmission(input.id, answer)),
      )
      expect(queued.message).toBe(`Submission ${input.id} is not a placed input`)
      const missingId = Ids.SubmissionId.make(777)
      const missing = yield* Effect.flip(
        session.commit((tx) => tx.placeSubmission(missingId, entry.id)),
      )
      expect(missing.message).toBe('Submission 777 does not exist')
    }),
  ))
