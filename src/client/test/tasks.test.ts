/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * Reads and task controls across the transport: a conversation's model context, one task's
 * record, wait and abort, and the summed usage.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, Exit, Fiber, Schema } from 'effect'
import { Initializer } from '../../harness/index.ts'
import * as Usage from '../../harness/Usage.ts'
import { EntryId, TaskId } from '../../storage/schema/Ids.ts'
import { settle } from '../../task/test/support.ts'
import { HarnessTransport, Init } from '../index.ts'
import { open, runTest } from './support.ts'

const spend = (tokens: number) => ({
  input: tokens,
  output: tokens,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 2 * tokens,
  cost: { input: tokens, output: tokens, cacheRead: 0, cacheWrite: 0, total: 2 * tokens },
})

const Spent = Init.make('spent', Schema.Struct({ tokens: Schema.Finite }))
const spent = Initializer.make(Spent, (tx, conversationId, { tokens }) =>
  Usage.record(tx, conversationId, 'models', 'faux/faux-1', spend(tokens)),
)

const transports = [
  ['memory', HarnessTransport.makeMemory],
  ['encoded memory', HarnessTransport.makeMemoryEncoded],
] as const

for (const [name, transport] of transports) {
  describe(`client over ${name}`, () => {
    test('reads what the model sees, now or as of an entry', () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const { client, answer } = yield* open(storage, {}, transport)
          const root = yield* client.root()
          expect(yield* root.context()).toEqual({ entries: [], contributions: [], messages: [] })
          const submission = yield* root.submit({ type: 'input', content: 'hi' })
          yield* answer('hello')
          yield* submission.wait
          const now = yield* root.context()
          expect(now.head).toBeUndefined()
          expect(now.entries.map(({ kind }) => kind)).toEqual(['pi.user', 'pi.assistant'])
          // The stand-in's assistant message is not a whole model message: it contributes nothing.
          expect(now.contributions.map((messages) => messages.length)).toEqual([1, 0])
          expect(now.messages).toEqual(now.contributions[0]!)
          const first = now.entries[0]!
          const before = yield* root.context({ at: first.id })
          expect(before).toEqual({
            entries: [first],
            contributions: [now.messages],
            messages: now.messages,
          })
          expect(yield* Effect.flip(root.context({ at: EntryId.make(999_999) }))).toMatchObject({
            _tag: 'TransactionError',
          })
        }),
      ))

    test('reads, aborts and waits for one task', () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const { client } = yield* open(storage, {}, transport)
          const root = yield* client.root()
          const submission = yield* root.submit({ type: 'input', content: 'a' })
          yield* settle
          const [live] = (yield* client.inspect).tasks
          const id = live!.record.id
          expect((yield* client.task(id))?.state.status).not.toBe('terminal')
          const waiting = yield* Effect.forkChild(client.waitForTask(id))
          expect(yield* client.abortTask(id)).toBe('marked')
          const terminal = yield* Fiber.await(waiting)
          expect(Exit.isSuccess(terminal)).toBe(true)
          expect((yield* client.task(id))?.state).toMatchObject({ status: 'terminal' })
          expect(yield* client.abortTask(id)).toBe('terminal')
          expect(yield* submission.wait).toMatchObject({ status: 'unanswered' })
          yield* client.waitForIdle

          const unknown = TaskId.make(999_999)
          expect(yield* client.task(unknown)).toBeUndefined()
          expect(yield* Effect.flip(client.waitForTask(unknown))).toMatchObject({
            _tag: 'TaskNotFound',
          })
        }),
      ))

    test('sums every conversation’s usage', () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const { client } = yield* open(storage, { initializers: [spent] }, transport)
          expect(yield* client.usage).toEqual({ models: {}, tools: {} })
          yield* client.root({ init: Init.call(Spent, { tokens: 1 }) })
          yield* client.createConversation({
            ownership: { kind: 'ownerless' },
            init: Init.call(Spent, { tokens: 2 }),
          })
          expect(yield* client.usage).toEqual({ models: { 'faux/faux-1': spend(3) }, tools: {} })
        }),
      ))
  })
}
