/* oxlint-disable max-lines-per-function -- ported describe blocks */
/**
 * Ported from pi's `packages/durable/test/harness-submissions.test.ts` and
 * `harness-input-entries.test.ts`. The stand-in for the generation task answers instead of a
 * faux model. Cases about resuming scheduling and typed entry tokens are left out: an Effect
 * Scheduler schedules from the start, and entry tokens are not ported yet.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, Exit, Fiber } from 'effect'
import { SessionError } from '../../session/index.ts'
import { SubmissionId } from '../../storage/schema/Ids.ts'
import { HarnessError } from '../index.ts'
import {
  counting,
  eventually,
  liveOf,
  open,
  openClosable,
  ownerless,
  runTest,
  settle,
  transcript,
  anyEntryId,
  anyNumber,
} from './support.ts'

describe('submissions', () => {
  test('appends an idle write and settles it done without a run', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness } = yield* open(storage)
        const conversation = yield* ownerless(harness)
        const submission = yield* conversation.submit({
          type: 'write',
          entry: { kind: 'note', data: { text: 'x' } },
        })
        const settled = yield* submission.wait
        expect(settled).toEqual({
          id: submission.id,
          conversationId: conversation.id,
          type: 'write',
          status: 'done',
          entry: anyEntryId,
        })
        const page = yield* conversation.entries({ order: 'ascending' }, 10)
        expect(page.items).toEqual([
          {
            id: anyEntryId,
            conversationId: conversation.id,
            kind: 'note',
            data: { text: 'x' },
          },
        ])
        expect(yield* liveOf(harness, conversation.id)).toEqual({})
        const tasks = yield* harness.session.commit((tx) =>
          tx.scanTasks({ conversationId: conversation.id }, 10),
        )
        expect(tasks.items).toEqual([])
      }),
    ))

  test('places idle input, and rejects busy input with whenBusy reject without writing', () =>
    runTest((backend) =>
      Effect.gen(function* () {
        const { storage, commits } = counting(backend)
        const { harness } = yield* open(storage)
        const conversation = yield* ownerless(harness)
        const submission = yield* conversation.submit({ type: 'input', content: 'hi' })
        const record = yield* submission.status
        if (record.status !== 'placed') throw new Error(`Unexpected ${record.status}`)
        const entry = yield* conversation.commit((tx) => tx.entry(record.entry))
        expect(entry?.kind).toBe('pi.user')
        expect(entry?.model).toEqual([{ role: 'user', content: 'hi', timestamp: anyNumber }])
        const live = yield* liveOf(harness, conversation.id)
        expect(live.run?.inputs).toEqual([submission.id])
        const runTask = yield* harness.session.commit((tx) => tx.task(live.run!.taskId))
        expect(runTask?.kind).toBe('pi.generation')

        yield* settle
        const before = commits()
        const rejected = yield* Effect.flip(
          conversation.submit({ type: 'input', content: 'again', whenBusy: 'reject' }),
        )
        expect(rejected).toBeInstanceOf(HarnessError.ConversationBusy)
        expect(rejected).toMatchObject({ conversationId: conversation.id })
        expect(commits()).toBe(before)
      }),
    ))

  test('deduplicates request IDs per conversation before any write', () =>
    runTest((backend) =>
      Effect.gen(function* () {
        const { storage, commits } = counting(backend)
        const { harness } = yield* open(storage)
        const conversation = yield* ownerless(harness)
        const first = yield* conversation.submit({ type: 'input', content: 'hi', requestId: 'r1' })
        yield* settle
        const before = commits()
        // Deduplication runs before the busy check.
        const again = yield* conversation.submit({
          type: 'input',
          content: 'different',
          requestId: 'r1',
          whenBusy: 'reject',
        })
        expect(again.id).toBe(first.id)
        expect(commits()).toBe(before)
        const mismatch = yield* Effect.flip(
          conversation.submit({ type: 'write', entry: { kind: 'note' }, requestId: 'r1' }),
        )
        expect(mismatch).toBeInstanceOf(HarnessError.InvalidSubmission)
        expect(mismatch.message).toBe('Request r1 already identifies a submission of type input')
        expect(yield* first.status).toMatchObject({ requestId: 'r1', status: 'placed' })

        const other = yield* ownerless(harness)
        const write = { type: 'write', entry: { kind: 'note' }, requestId: 'r1' } as const
        const written = yield* other.submit(write)
        expect(written.id).not.toBe(first.id)
        expect((yield* other.submit(write)).id).toBe(written.id)
      }),
    ))

  test('reports abort results and looks submissions up by conversation', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer } = yield* open(storage)
        const conversation = yield* ownerless(harness)
        const submission = yield* conversation.submit({ type: 'input', content: 'hi' })
        expect(yield* submission.abort).toBe('already_placed')
        expect(yield* harness.abortSubmission(submission.id, conversation.id)).toBe(
          'already_placed',
        )
        const other = yield* ownerless(harness)
        expect(yield* harness.abortSubmission(submission.id, other.id)).toBe('not_found')
        const unknown = SubmissionId.make(999_999)
        expect(yield* harness.abortSubmission(unknown)).toBe('not_found')
        expect(yield* harness.submission(unknown)).toBeUndefined()

        yield* answer('answer')
        yield* submission.wait
        expect(yield* submission.abort).toBe('settled')
        expect(yield* harness.abortSubmission(submission.id)).toBe('settled')
      }),
    ))

  test('interrupting a wait leaves the submission, and close fails pending waits', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, close } = yield* openClosable(storage)
        const conversation = yield* ownerless(harness)
        const submission = yield* conversation.submit({ type: 'input', content: 'hi' })
        const cancelled = yield* Effect.forkChild(submission.wait)
        const pending = yield* Effect.forkChild(submission.wait)
        yield* settle
        yield* Fiber.interrupt(cancelled)
        expect((yield* submission.status).status).toBe('placed')
        yield* close
        const exit = yield* Fiber.await(pending)
        expect(Exit.isFailure(exit)).toBe(true)
        const error = yield* Effect.flip(Fiber.join(pending))
        expect(error).toBeInstanceOf(SessionError.SessionClosed)
        expect(error.message).toBe('Harness is closed')
      }),
    ))

  test('reacquires a submission after reopen and settles it durably', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        // The first Harness never answers; the reopened one does.
        const first = yield* openClosable(storage)
        const conversation = yield* ownerless(first.harness)
        const draft = { type: 'input', content: 'hi', requestId: 'print' } as const
        const { id } = yield* conversation.submit(draft)
        yield* settle
        yield* first.close

        const second = yield* openClosable(storage)
        const submission = (yield* second.harness.submission(id))!
        expect((yield* submission.status).status).toBe('placed')
        yield* second.answer('after reopen')
        const settled = yield* submission.wait
        expect(settled).toMatchObject({ status: 'done', type: 'input' })
        yield* second.close

        const third = yield* openClosable(storage)
        expect(yield* (yield* third.harness.submission(id))!.wait).toEqual(settled)
        const reopened = (yield* third.harness.conversation(conversation.id))!
        expect((yield* reopened.submit(draft)).id).toBe(id)
        expect(yield* transcript(reopened)).toEqual(['pi.user: hi', 'pi.assistant: after reopen'])
        yield* third.close
      }),
    ))

  test('settles submissions by their current record in the transaction', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness } = yield* open(storage)
        const conversation = yield* ownerless(harness)
        const { session } = harness
        const entry = yield* conversation.commit((tx) =>
          Effect.map(tx.appendEntry(conversation.id, { kind: 'note' }), (record) => record.id),
        )
        const placed = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const { id } = yield* tx.createSubmission({
              conversationId: conversation.id,
              type: 'input',
              status: 'placed',
              entry,
            })
            yield* tx.settleSubmission(id, { status: 'done', answer: entry })
            yield* tx.settleSubmission(id, { status: 'unanswered', reason: 'late' })
            return id
          }),
        )
        expect(yield* (yield* harness.submission(placed))!.status).toMatchObject({
          status: 'done',
          entry,
          answer: entry,
        })
      }),
    ))
})

describe('input entries', () => {
  test('places an idle input as its entry: the given kind and data, with the content', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer } = yield* open(storage)
        const conversation = yield* ownerless(harness)
        const submission = yield* conversation.submit({
          type: 'input',
          content: 'hi',
          entry: { kind: 'app.chat', data: { author: 'julia' } },
        })
        const record = yield* submission.status
        if (record.status !== 'placed') throw new Error(`Unexpected ${record.status}`)
        const entry = yield* conversation.commit((tx) => tx.entry(record.entry))
        expect(entry).toMatchObject({ kind: 'app.chat', data: { author: 'julia' } })
        expect(entry?.model).toEqual([{ role: 'user', content: 'hi', timestamp: anyNumber }])
        yield* answer('hello')
        expect((yield* submission.wait).status).toBe('done')
      }),
    ))

  test('rejects an input entry whose kind is empty or starts with pi., writing nothing', () =>
    runTest((backend) =>
      Effect.gen(function* () {
        const { storage, commits } = counting(backend)
        const { harness } = yield* open(storage)
        const conversation = yield* ownerless(harness)
        const before = commits()
        for (const kind of ['', 'pi.user']) {
          const error = yield* Effect.flip(
            conversation.submit({ type: 'input', content: 'hi', entry: { kind } }),
          )
          expect(error).toBeInstanceOf(HarnessError.InvalidSubmission)
        }
        expect(commits()).toBe(before)
      }),
    ))

  test('queues busy inputs with their entries and places them as those entries', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer } = yield* open(storage)
        const conversation = yield* ownerless(harness)
        yield* conversation.submit({ type: 'input', content: 'first' })
        const queued = yield* conversation.submit({
          type: 'input',
          content: 'second',
          entry: { kind: 'app.chat', data: { author: 'sam' } },
        })
        expect((yield* queued.status).status).toBe('queued')
        yield* answer('one')
        yield* eventually(Effect.map(queued.status, (record) => record.status === 'placed'))
        yield* answer('two')
        expect((yield* queued.wait).status).toBe('done')
        expect(yield* transcript(conversation)).toEqual([
          'pi.user: first',
          'pi.assistant: one',
          'app.chat: second',
          'pi.assistant: two',
        ])
      }),
    ))
})
