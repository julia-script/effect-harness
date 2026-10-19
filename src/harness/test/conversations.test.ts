/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * Conversation handles: root, creation and fork with `init`, the creation hook, conversation
 * abort, and inspection.
 */
import { describe, expect, test } from 'bun:test'
import { Data, Effect, Exit } from 'effect'
import * as Inbox from '../Inbox.ts'
import * as Live from '../Live.ts'
import { eventually, inboxOf, liveOf, open, queuedNote, runTest, transcript } from './support.ts'

class InitFailed extends Data.TaggedError('InitFailed')<{ readonly message: string }> {}

const note = (text: string) => ({ kind: 'note', data: { text } }) as const

describe('conversations', () => {
  test('creates the root once, running init only when it creates it', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness } = yield* open(storage)
        let inits = 0
        const init = () =>
          Effect.sync(() => {
            inits++
          })
        const first = yield* harness.root({ init })
        const second = yield* harness.root({ init })
        expect(second.id).toBe(first.id)
        expect(inits).toBe(1)
        expect(yield* liveOf(harness, first.id)).toEqual({})
        expect(yield* inboxOf(harness, first.id)).toEqual([])
      }),
    ))

  test('rolls the conversation back when init fails', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness } = yield* open(storage)
        const exit = yield* Effect.exit(
          harness.createConversation({
            ownership: { kind: 'ownerless' },
            init: () => Effect.fail(new InitFailed({ message: 'no' })),
          }),
        )
        expect(Exit.isFailure(exit)).toBe(true)
        const page = yield* Effect.orDie(
          harness.session.commit((tx) => tx.scanConversations({}, 10)),
        )
        expect(page.items).toEqual([])
      }),
    ))

  test('forks at an entry with the built-in documents and init', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness } = yield* open(storage)
        const root = yield* harness.root()
        const at = yield* root.commit((tx) => tx.appendEntry(root.id, note('kept')))
        yield* root.commit((tx) => tx.appendEntry(root.id, note('after')))
        const fork = yield* root.fork(at.id, {
          ownership: { kind: 'ownerless' },
          init: (tx, id) => Effect.asVoid(tx.appendEntry(id, note('init'))),
        })
        expect(fork.id).not.toBe(root.id)
        expect(yield* liveOf(harness, fork.id)).toEqual({})
        expect(yield* inboxOf(harness, fork.id)).toEqual([])
        const entries = yield* Effect.orDie(fork.entries({ order: 'ascending' }, 10))
        expect(entries.items.map((entry) => entry.data)).toEqual([
          { text: 'kept' },
          { text: 'init' },
        ])
      }),
    ))

  test('runs the creation hook for conversations created through the Session', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const created: Array<number> = []
        const { harness } = yield* open(storage, {
          conversationCreated: (_tx, record) =>
            Effect.sync(() => {
              created.push(record.id)
            }),
        })
        const record = yield* Effect.orDie(
          harness.session.commit((tx) =>
            tx.createConversation({ ownership: { kind: 'ownerless' } }),
          ),
        )
        expect(created).toEqual([record.id])
        const documents = yield* Effect.orDie(
          harness.session.commit((tx) =>
            Effect.map(
              Effect.all([tx.doc(Live.document, record.id), tx.doc(Inbox.document, record.id)]),
              // Drafts are proxies: compare their JSON.
              (drafts) => JSON.stringify(drafts),
            ),
          ),
        )
        expect(documents).toBe(JSON.stringify([{}, { items: [] }]))
      }),
    ))

  test('abort withdraws queued inputs and keeps queued writes', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness } = yield* open(storage)
        const root = yield* harness.root()
        const input = yield* root.submit({ type: 'input', content: 'a' })
        const write = yield* root.submit({ type: 'write', entry: queuedNote('w') })
        const queued = yield* root.submit({ type: 'input', content: 'f' })
        yield* root.abort()
        expect(yield* input.wait).toMatchObject({ status: 'unanswered', reason: 'aborted' })
        expect(yield* queued.wait).toMatchObject({ status: 'unanswered' })
        yield* eventually(Effect.map(liveOf(harness, root.id), (live) => live.run === undefined))
        expect((yield* inboxOf(harness, root.id)).map((item) => item.id)).toEqual([write.id])
        expect(yield* transcript(root)).toEqual(['pi.user: a'])
      }),
    ))

  test('inspects tasks and unsettled submissions', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer } = yield* open(storage)
        const root = yield* harness.root()
        const input = yield* root.submit({ type: 'input', content: 'a' })
        const queued = yield* root.submit({ type: 'input', content: 'f' })
        const inspection = yield* Effect.orDie(harness.inspect)
        expect(inspection.submissions.map(({ id, status }) => [id, status])).toEqual([
          [input.id, 'placed'],
          [queued.id, 'queued'],
        ])
        yield* answer('one')
        yield* answer('two')
        yield* queued.wait
        expect((yield* Effect.orDie(harness.inspect)).submissions).toEqual([])
      }),
    ))
})
