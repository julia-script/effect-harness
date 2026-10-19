import { expect, test } from 'bun:test'
import { Effect, Exit, Fiber, Scope } from 'effect'
import { Ids } from '../../storage/schema/index.ts'
import { StorageError, StorageRequestError } from '../../storage/StorageError.ts'
import { SessionError } from '../index.ts'
import type { Session } from '../index.ts'
import { createConversation, openSession, runSession } from './support.ts'

test('serializes commits on the mutation line', () =>
  runSession(({ session }) =>
    Effect.gen(function* () {
      const conversationId = yield* createConversation(session)
      const events: Array<string> = []
      const commit = (name: string) =>
        session.commit((tx) =>
          Effect.gen(function* () {
            events.push(`${name} start`)
            yield* Effect.sleep('5 millis')
            yield* tx.appendEntry(conversationId, { kind: name })
            events.push(`${name} end`)
          }),
        )
      yield* Effect.all([commit('a'), commit('b')], { concurrency: 'unbounded' })
      expect(events).toEqual(['a start', 'a end', 'b start', 'b end'])
    }),
  ))

test('commits nothing when the callback fails, and seals its transaction', () =>
  runSession(({ session, recorder }) =>
    Effect.gen(function* () {
      const conversationId = yield* createConversation(session)
      const commits = recorder.commits.length
      let leaked: Session.Tx | undefined
      const error = yield* Effect.flip(
        session.commit((tx) =>
          Effect.gen(function* () {
            leaked = tx
            yield* tx.appendEntry(conversationId, { kind: 'lost' })
            return yield* Effect.fail('callback failed')
          }),
        ),
      )
      expect(error).toBe('callback failed')
      expect(recorder.commits.length).toBe(commits)
      if (leaked === undefined) return
      const late = yield* Effect.flip(leaked.appendEntry(conversationId, { kind: 'late' }))
      expect(late).toBeInstanceOf(SessionError.TransactionError)
      expect(late.message).toBe('Transaction has settled')
    }),
  ))

test('writes nothing for a read-only commit', () =>
  runSession(({ session, recorder }) =>
    Effect.gen(function* () {
      const conversationId = yield* createConversation(session)
      const commits = recorder.commits.length
      const found = yield* session.commit((tx) => tx.conversation(conversationId))
      expect(found).toEqual({ id: conversationId })
      expect(recorder.commits.length).toBe(commits)
    }),
  ))

test('fails the Session on the first storage error', () =>
  runSession(({ session, recorder }) =>
    Effect.gen(function* () {
      const conversationId = yield* createConversation(session)
      recorder.failCommits()
      const append = session.commit((tx) => tx.appendEntry(conversationId, { kind: 'note' }))
      const first = yield* Effect.flip(append)
      expect(first).toBeInstanceOf(SessionError.SessionFailed)
      expect(first.cause).toBeInstanceOf(StorageError)
      const commits = recorder.commits.length
      const later = yield* Effect.flip(session.commit((tx) => tx.conversation(conversationId)))
      expect(later).toBeInstanceOf(SessionError.SessionFailed)
      expect(recorder.commits.length).toBe(commits)
      const end = yield* session.ended
      expect(end.reason).toBe('failed')
      expect(end.reason === 'failed' ? end.error.message : undefined).toBe('disk full')
    }),
  ))

test('fails only the read for a storage request error', () =>
  runSession(({ session }) =>
    Effect.gen(function* () {
      const unknown = Ids.ConversationId.make(999)
      const error = yield* Effect.flip(
        session.commit((tx) => tx.scanEntries({ conversationId: unknown }, 10)),
      )
      expect(error).toBeInstanceOf(StorageRequestError)
      expect(yield* createConversation(session)).toBeNumber()
    }),
  ))

test('lets admitted commits finish when its scope closes, then refuses new ones', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const scope = yield* Scope.make()
      const { session } = yield* Scope.provide(openSession, scope)
      const conversationId = yield* createConversation(session)
      const events: Array<string> = []
      const slow = session.commit((tx) =>
        Effect.gen(function* () {
          yield* Effect.sleep('5 millis')
          yield* tx.appendEntry(conversationId, { kind: 'slow' })
          events.push('committed')
        }),
      )
      const fiber = yield* Effect.forkChild(slow)
      yield* Effect.yieldNow
      yield* Scope.close(scope, Exit.void)
      events.push('closed')
      yield* Fiber.join(fiber)
      expect(events).toEqual(['committed', 'closed'])
      expect(yield* session.ended).toEqual({ reason: 'closed' })
      const refused = yield* Effect.flip(createConversation(session))
      expect(refused).toBeInstanceOf(SessionError.SessionClosed)
    }).pipe(Effect.orDie),
  ))
