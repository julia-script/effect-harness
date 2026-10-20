/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
/**
 * Ported from pi's `packages/durable/test/harness-inbox.test.ts`, "inbox". The stand-in for the
 * generation task plays the model: `answer` ends a run at the final boundary, `tools` passes the
 * `postTools` boundary of a tool round. Cases about tool controls, `onYield` hooks, model context,
 * and usage come with the built-in tasks.
 */
import { describe, expect, test } from 'bun:test'
import { Effect } from 'effect'
import type { Conversation } from '../Conversation.ts'
import type * as Harness from '../Harness.ts'
import type { Submission } from '../Submission.ts'
import {
  eventually,
  inboxOf,
  inboxOps,
  liveOf,
  open,
  openClosable,
  queuedNote,
  runTest,
  transcript,
  placedAs,
  anyNumber,
  answerOf,
  containing,
} from './support.ts'

const modes = (harness: Harness.Service, conversation: Conversation) =>
  Effect.map(inboxOf(harness, conversation.id), (items) =>
    items.map((item) => [item.id, item.mode]),
  )

const answers = (conversation: Conversation) =>
  Effect.map(conversation.entries({ order: 'ascending' }, 100), (page) =>
    page.items.filter((entry) => entry.kind === '$assistant').map((entry) => entry.id),
  )

const idle = (harness: Harness.Service, conversation: Conversation) =>
  eventually(Effect.map(liveOf(harness, conversation.id), (live) => live.run === undefined))

const statusOf = (submission: Submission) => Effect.orDie(submission.status)

describe('inbox', () => {
  test('queues busy submissions and places writes before user items, one follow-up per run', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer } = yield* open(storage)
        const root = yield* harness.root()
        const input = yield* root.submit({ type: 'input', content: 'a' })
        const f1 = yield* root.submit({ type: 'input', content: 'f1' })
        const write = yield* root.submit({ type: 'write', entry: queuedNote('w') })
        const f2 = yield* root.submit({ type: 'input', content: 'f2', whenBusy: 'followUp' })
        for (const each of [f1, write, f2]) expect((yield* statusOf(each)).status).toBe('queued')
        expect(yield* modes(harness, root)).toEqual([
          [f1.id, 'followUp'],
          [write.id, 'write'],
          [f2.id, 'followUp'],
        ])
        expect(yield* transcript(root)).toEqual(['$user: a'])

        yield* answer('first')
        yield* answer('second')
        yield* answer('third')
        yield* f2.wait
        expect(yield* transcript(root)).toEqual([
          '$user: a',
          '$assistant: first',
          'note: w',
          '$user: f1',
          '$assistant: second',
          '$user: f2',
          '$assistant: third',
        ])
        const [first, second, third] = yield* answers(root)
        expect(yield* statusOf(input)).toMatchObject({ status: 'done', answer: first })
        expect(yield* statusOf(write)).toMatchObject({ status: 'done' })
        expect(yield* statusOf(f1)).toMatchObject({ status: 'done', answer: second })
        expect(yield* statusOf(f2)).toMatchObject({ status: 'done', answer: third })
        expect(yield* modes(harness, root)).toEqual([])
        yield* idle(harness, root)
        expect(yield* liveOf(harness, root.id)).toEqual({})
      }),
    ))

  test('places every follow-up in one successor run with followUpMode all', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer } = yield* open(storage, { settings: { followUpMode: 'all' } })
        const root = yield* harness.root()
        yield* root.submit({ type: 'input', content: 'a' })
        const f1 = yield* root.submit({ type: 'input', content: 'f1' })
        const f2 = yield* root.submit({ type: 'input', content: 'f2' })
        yield* answer('first')
        yield* answer('both')
        const settled = yield* f2.wait
        expect(yield* statusOf(f1)).toEqual(placedAs({ ...settled, id: f1.id }))
        expect(yield* transcript(root)).toEqual([
          '$user: a',
          '$assistant: first',
          '$user: f1',
          '$user: f2',
          '$assistant: both',
        ])
      }),
    ))

  test('reads the settings at each boundary', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        let followUpMode: 'all' | 'one-at-a-time' = 'one-at-a-time'
        const settings = Effect.sync(() => ({ followUpMode }))
        const { harness, answer } = yield* open(storage, { settings })
        const root = yield* harness.root()
        yield* root.submit({ type: 'input', content: 'a' })
        const f1 = yield* root.submit({ type: 'input', content: 'f1' })
        const f2 = yield* root.submit({ type: 'input', content: 'f2' })
        followUpMode = 'all'
        yield* answer('first')
        yield* answer('both')
        const settled = yield* f2.wait
        expect(yield* statusOf(f1)).toEqual(placedAs({ ...settled, id: f1.id }))
      }),
    ))

  test('adds steers to the run at the postTools boundary and holds follow-ups for the final one', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer, tools } = yield* open(storage)
        const root = yield* harness.root()
        const input = yield* root.submit({ type: 'input', content: 'a' })
        const steer = yield* root.submit({ type: 'input', content: 's', whenBusy: 'steer' })
        const followUp = yield* root.submit({ type: 'input', content: 'f' })
        yield* tools('calling')
        yield* answer('after tools')
        yield* answer('follow-up')
        yield* followUp.wait
        expect(yield* transcript(root)).toEqual([
          '$user: a',
          '$assistant: calling',
          '$user: s',
          '$assistant: after tools',
          '$user: f',
          '$assistant: follow-up',
        ])
        const [, afterTools, last] = yield* answers(root)
        expect(yield* statusOf(input)).toMatchObject({ status: 'done', answer: afterTools })
        expect(yield* statusOf(steer)).toMatchObject({ status: 'done', answer: afterTools })
        expect(yield* statusOf(followUp)).toMatchObject({ status: 'done', answer: last })
      }),
    ))

  test('adds every steer to the run at the postTools boundary with steeringMode all', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer, tools } = yield* open(storage, {
          settings: { steeringMode: 'all' },
        })
        const root = yield* harness.root()
        const input = yield* root.submit({ type: 'input', content: 'a' })
        const s1 = yield* root.submit({ type: 'input', content: 's1', whenBusy: 'steer' })
        const s2 = yield* root.submit({ type: 'input', content: 's2', whenBusy: 'steer' })
        yield* tools('calling')
        yield* answer('done')
        const settled = yield* input.wait
        expect(yield* statusOf(s1)).toMatchObject({ status: 'done', answer: answerOf(settled) })
        expect(yield* statusOf(s2)).toMatchObject({ status: 'done', answer: answerOf(settled) })
        expect(yield* transcript(root)).toEqual([
          '$user: a',
          '$assistant: calling',
          '$user: s1',
          '$user: s2',
          '$assistant: done',
        ])
      }),
    ))

  test('ends the run at a queued reset after tools and runs earlier follow-ups after it', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer, tools } = yield* open(storage)
        const root = yield* harness.root()
        const input = yield* root.submit({ type: 'input', content: 'a' })
        const followUp = yield* root.submit({ type: 'input', content: 'f' })
        yield* root.reset()
        yield* tools('calling')
        yield* answer('fresh')
        yield* followUp.wait
        expect(yield* statusOf(input)).toMatchObject({ status: 'unanswered', reason: 'reset' })
        expect(yield* transcript(root)).toEqual([
          '$user: a',
          '$assistant: calling',
          '$reset: ',
          '$user: f',
          '$assistant: fresh',
        ])
        const page = yield* root.entries({}, 100)
        const reset = page.items.find((entry) => entry.kind === '$reset')!
        expect(reset.head).toBe(reset.id)
      }),
    ))

  test('places a queued reset after the answer at the final boundary', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer } = yield* open(storage)
        const root = yield* harness.root()
        const input = yield* root.submit({ type: 'input', content: 'a' })
        yield* root.reset('handoff')
        yield* answer('first')
        expect((yield* input.wait).status).toBe('done')
        yield* root.waitForIdle
        expect(yield* transcript(root)).toEqual([
          '$user: a',
          '$assistant: first',
          '$reset: handoff',
        ])
      }),
    ))

  test('resets an idle conversation at once, with or without handoff text', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness } = yield* open(storage)
        const root = yield* harness.root()
        yield* root.reset()
        yield* root.reset('carry on')
        const page = yield* root.entries({ order: 'ascending' }, 10)
        expect(page.items.map((entry) => entry.kind)).toEqual(['$reset', '$reset'])
        const [plain, handoff] = page.items
        expect(plain?.model).toBeUndefined()
        expect(handoff?.head).toBe(handoff?.id)
        expect(handoff?.model).toEqual([
          { role: 'user', content: 'carry on', timestamp: anyNumber },
        ])
      }),
    ))

  test('makes a queued head write stale when it targets an entry before the active range', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer } = yield* open(storage)
        const root = yield* harness.root()
        const old = yield* root.commit((tx) => tx.appendEntry(root.id, { kind: 'note' }))
        const resetSubmission = yield* root.reset()
        const resetRecord = yield* resetSubmission.wait
        if (resetRecord.status !== 'done') throw new Error('reset not placed')
        const input = yield* root.submit({ type: 'input', content: 'a' })
        const stale = yield* root.submit({
          type: 'write',
          entry: { kind: 'summary', head: old.id },
        })
        const fresh = yield* root.submit({
          type: 'write',
          entry: { kind: 'summary', head: resetRecord.entry },
        })
        yield* answer('first')
        yield* input.wait
        expect(yield* statusOf(stale)).toMatchObject({ status: 'unanswered', reason: 'stale' })
        expect(yield* statusOf(fresh)).toMatchObject({ status: 'done' })
        expect(yield* modes(harness, root)).toEqual([])

        // The fresh summary starts the range at the reset: a target inside the range is not stale,
        // even when it is older than the marker itself.
        const page = yield* root.entries({ order: 'ascending' }, 100)
        const inside = page.items.find((entry) => entry.kind === '$user')!
        const second = yield* root.submit({ type: 'input', content: 'b' })
        const kept = yield* root.submit({
          type: 'write',
          entry: { kind: 'summary', head: inside.id },
        })
        yield* answer('second')
        yield* second.wait
        expect(yield* statusOf(kept)).toMatchObject({ status: 'done' })
      }),
    ))

  test('makes a head write stale behind a reset placed earlier in the same boundary', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer } = yield* open(storage)
        const root = yield* harness.root()
        const input = yield* root.submit({ type: 'input', content: 'a' })
        const [target] = (yield* root.entries({ order: 'ascending' }, 10)).items
        yield* root.reset()
        const summary = yield* root.submit({
          type: 'write',
          entry: { kind: 'summary', head: target!.id },
        })
        yield* answer('first')
        yield* input.wait
        expect(yield* summary.wait).toMatchObject({ status: 'unanswered', reason: 'stale' })
      }),
    ))

  test('settles a stale write at once while idle', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness } = yield* open(storage)
        const root = yield* harness.root()
        const old = yield* root.commit((tx) => tx.appendEntry(root.id, { kind: 'note' }))
        yield* root.reset()
        const before = (yield* root.entries({}, 100)).items.length
        const stale = yield* root.submit({
          type: 'write',
          entry: { kind: 'summary', head: old.id },
        })
        expect(yield* stale.wait).toMatchObject({ status: 'unanswered', reason: 'stale' })
        expect((yield* root.entries({}, 100)).items).toHaveLength(before)
      }),
    ))

  test('withdraws a queued submission and removes its item', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer } = yield* open(storage)
        const root = yield* harness.root()
        const input = yield* root.submit({ type: 'input', content: 'a' })
        const kept = yield* root.submit({ type: 'write', entry: queuedNote('kept') })
        const withdrawn = yield* root.submit({ type: 'input', content: 'f' })
        expect(yield* withdrawn.abort).toBe('aborted')
        expect(yield* withdrawn.wait).toMatchObject({ status: 'unanswered', reason: 'aborted' })
        expect(yield* modes(harness, root)).toEqual([[kept.id, 'write']])
        yield* answer('first')
        yield* input.wait
        expect(yield* statusOf(kept)).toMatchObject({ status: 'done' })
        yield* idle(harness, root)
      }),
    ))

  test('leaves queued items after a faulted run until the next submission places them in order', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer, fail } = yield* open(storage)
        const root = yield* harness.root()
        const input = yield* root.submit({ type: 'input', content: 'a' })
        const f = yield* root.submit({ type: 'input', content: 'f' })
        yield* fail
        expect(yield* input.wait).toMatchObject({
          status: 'unanswered',
          reason: 'faulted',
          detail: containing('scripted failure'),
        })
        yield* root.waitForIdle
        expect((yield* statusOf(f)).status).toBe('queued')
        expect(yield* modes(harness, root)).toEqual([[f.id, 'followUp']])

        // Idle with a queued item: new input queues behind it, and a final boundary places the
        // older one first.
        const g = yield* root.submit({ type: 'input', content: 'g', whenBusy: 'reject' })
        yield* answer('for f')
        yield* answer('for g')
        yield* g.wait
        expect(yield* statusOf(f)).toMatchObject({ status: 'done' })
        expect((yield* transcript(root)).slice(-4)).toEqual([
          '$user: f',
          '$assistant: for f',
          '$user: g',
          '$assistant: for g',
        ])
      }),
    ))

  test('queues an idle steer behind waiting items and places it with the first follow-up', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer, fail } = yield* open(storage)
        const root = yield* harness.root()
        yield* root.submit({ type: 'input', content: 'a' })
        const f = yield* root.submit({ type: 'input', content: 'f' })
        yield* fail
        yield* root.waitForIdle
        const steer = yield* root.submit({ type: 'input', content: 's', whenBusy: 'steer' })
        yield* answer('both')
        const settled = yield* steer.wait
        expect(yield* statusOf(f)).toEqual(placedAs({ ...settled, id: f.id }))
        expect((yield* transcript(root)).slice(-3)).toEqual([
          '$user: f',
          '$user: s',
          '$assistant: both',
        ])
      }),
    ))

  test('leaves the inbox alone when the run task is aborted', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness } = yield* open(storage)
        const root = yield* harness.root()
        const input = yield* root.submit({ type: 'input', content: 'a' })
        const f = yield* root.submit({ type: 'input', content: 'f' })
        const taskId = (yield* liveOf(harness, root.id)).run!.taskId
        yield* harness.scheduler.abort(taskId)
        expect(yield* input.wait).toMatchObject({ status: 'unanswered', reason: 'aborted' })
        yield* root.waitForIdle
        expect((yield* statusOf(f)).status).toBe('queued')
        expect(yield* modes(harness, root)).toEqual([[f.id, 'followUp']])
      }),
    ))

  test('returns a queued submission for its repeated request ID without a second item', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness } = yield* open(storage)
        const root = yield* harness.root()
        yield* root.submit({ type: 'input', content: 'a' })
        const queued = yield* root.submit({ type: 'input', content: 'f', requestId: 'r' })
        const again = yield* root.submit({ type: 'input', content: 'f', requestId: 'r' })
        expect(again.id).toBe(queued.id)
        expect(yield* modes(harness, root)).toEqual([[queued.id, 'followUp']])
      }),
    ))

  test('keeps queued submissions across reopen and settles them afterwards', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const first = yield* openClosable(storage)
        const root = yield* first.harness.root()
        yield* root.submit({ type: 'input', content: 'a' })
        const { id } = yield* root.submit({ type: 'input', content: 'f' })
        yield* first.close

        const second = yield* openClosable(storage)
        yield* second.answer('first again')
        yield* second.answer('f')
        const settled = yield* (yield* second.harness.submission(id))!.wait
        expect(settled).toMatchObject({ status: 'done' })
        const reopened = yield* second.harness.root()
        expect((yield* transcript(reopened)).slice(-2)).toEqual(['$user: f', '$assistant: f'])
        yield* second.close
      }),
    ))

  test('commits inbox changes as positional operations', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer } = yield* open(storage)
        const root = yield* harness.root()
        const ops = yield* inboxOps(harness)
        const input = yield* root.submit({ type: 'input', content: 'a' })
        const w1 = yield* root.submit({ type: 'write', entry: queuedNote('w') })
        const f1 = yield* root.submit({ type: 'input', content: 'f1' })
        const f2 = yield* root.submit({ type: 'input', content: 'f2' })
        expect(ops).toEqual([
          [['p', ['items'], 0, 0, [{ id: w1.id, mode: 'write', entry: queuedNote('w') }]]],
          [['p', ['items'], 1, 0, [{ id: f1.id, mode: 'followUp', content: 'f1' }]]],
          [['p', ['items'], 2, 0, [{ id: f2.id, mode: 'followUp', content: 'f2' }]]],
        ])
        yield* answer('first')
        yield* input.wait
        yield* eventually(Effect.sync(() => ops.length > 3))
        // The write and the first follow-up leave; no retained value is carried.
        expect(JSON.stringify(ops[3])).not.toContain('f2')
        yield* answer('second')
        yield* answer('third')
        yield* f2.wait
      }),
    ))

  test('withdraws a middle item with one positional removal', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness } = yield* open(storage)
        const root = yield* harness.root()
        yield* root.submit({ type: 'input', content: 'a' })
        const items: Array<Submission> = []
        for (const text of ['x', 'y', 'z']) {
          items.push(yield* root.submit({ type: 'input', content: text }))
        }
        const ops = yield* inboxOps(harness)
        yield* items[1]!.abort
        expect(ops).toEqual([[['p', ['items'], 1, 1, []]]])
        expect(yield* modes(harness, root)).toEqual([
          [items[0]!.id, 'followUp'],
          [items[2]!.id, 'followUp'],
        ])
      }),
    ))
})

describe('data-only writes', () => {
  test('appends a data-only write mid-run at once', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer } = yield* open(storage)
        const root = yield* harness.root()
        const input = yield* root.submit({ type: 'input', content: 'a' })
        const note = yield* root.submit({ type: 'write', entry: { kind: 'note', data: { n: 1 } } })
        expect(yield* note.wait).toMatchObject({ status: 'done' })
        yield* answer('first')
        yield* input.wait
        expect(yield* transcript(root)).toEqual(['$user: a', 'note: ', '$assistant: first'])
      }),
    ))

  test('queues a data-only write behind a queued reset, so it lands after the reset', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, answer } = yield* open(storage)
        const root = yield* harness.root()
        const input = yield* root.submit({ type: 'input', content: 'a' })
        yield* root.reset()
        const note = yield* root.submit({ type: 'write', entry: { kind: 'note', data: { n: 1 } } })
        expect((yield* statusOf(note)).status).toBe('queued')
        yield* answer('first')
        yield* input.wait
        yield* note.wait
        expect(yield* transcript(root)).toEqual([
          '$user: a',
          '$assistant: first',
          '$reset: ',
          'note: ',
        ])
      }),
    ))
})
