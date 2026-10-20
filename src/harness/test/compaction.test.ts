/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * The built-in compaction task and the usage ledger over the `Faux` model, through whole runs.
 * Ported in spirit from pi's `harness-compaction.test.ts`; `beforeCompact` hooks come with
 * extensions.
 */
import { describe as group, expect, test } from 'bun:test'
import { Effect } from 'effect'
import { Faux } from '../../model/index.ts'
import { ContextOverflow, ModelError, Rejected } from '../../model/ModelError.ts'
import type { Conversation } from '../Conversation.ts'
import * as Usage from '../Usage.ts'
import type { Options } from './generationSupport.ts'
import { anyNumber } from './support.ts'
import {
  describe,
  entriesOf,
  eventually,
  liveOf,
  open,
  ownerless,
  reasonOf,
  runTest,
} from './generationSupport.ts'

const ask = (conversation: Conversation, content: string) =>
  Effect.orDie(Effect.flatMap(conversation.submit({ type: 'input', content }), (s) => s.wait))

const overflow = new ModelError({ message: 'prompt is too long', reason: new ContextOverflow({}) })

/** A small model, so a long input crosses the blocking threshold. */
const small: Options = {
  agent: (faux) => ({ model: faux.ref(), tools: [] }),
  faux: { models: [{ id: 'faux-1', contextWindow: 400 }] },
  settings: {
    compaction: { reserveTokens: 200, keepRecentTokens: 1, backgroundTokens: 0 },
  },
}

/** The summary and an answer can race, so each request is answered by what it asks. */
const respond: Faux.Respond = (request) =>
  Effect.succeed(Faux.message(request.messages[0]?.role === 'system' ? 'the summary' : 'an answer'))

const roles = (messages: ReadonlyArray<{ readonly role: string }>) =>
  messages.map((message) => message.role)

group('usage', () => {
  test('adds every response to the ledger by model', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage)
        yield* faux.setResponses([Faux.message('a'), Faux.message('b')])
        const conversation = yield* ownerless(harness)
        yield* ask(conversation, 'one')
        const first = (yield* Effect.orDie(
          harness.session.snapshot(Usage.document, conversation.id),
        ))?.models['faux/faux-1']
        yield* ask(conversation, 'two')
        const both = (yield* Effect.orDie(
          harness.session.snapshot(Usage.document, conversation.id),
        ))?.models['faux/faux-1']
        expect(first?.input).toBeGreaterThan(0)
        expect(both?.input).toBeGreaterThan(first?.input ?? 0)
        expect(both?.output).toBeGreaterThan(first?.output ?? 0)
      }),
    ))
})

group('compaction', () => {
  test('compacts manually and starts the next context at the summary', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage, {
          settings: { compaction: { keepRecentTokens: 1 } },
        })
        yield* faux.setResponses([
          Faux.message('first answer'),
          Faux.message('second answer'),
          Faux.message('the summary'),
          Faux.message('third answer'),
        ])
        const conversation = yield* ownerless(harness)
        yield* ask(conversation, 'one')
        yield* ask(conversation, 'two')
        const taskId = yield* Effect.orDie(conversation.compact('keep names'))
        const { state } = yield* Effect.orDie(harness.scheduler.waitForTask(taskId))
        expect(state.outcome).toMatchObject({
          status: 'completed',
          result: { submissionId: anyNumber },
        })
        const summarizer = (yield* faux.requests)[2]
        expect(roles(summarizer?.messages ?? [])).toEqual(['system', 'user'])
        expect(JSON.stringify(summarizer?.messages[1])).toContain('Additional focus: keep names')
        yield* ask(conversation, 'three')
        const entries = yield* entriesOf(harness, conversation.id)
        const summary = entries.find((entry) => entry.kind === '$compaction')
        expect(summary?.head).toBe(entries[4]?.id)
        expect(describe(entries[4]!)).toBe('$assistant: second answer')
        const last = (yield* faux.requests)[3]
        // The new baseline, the summary, the kept answer, and the new input.
        expect(roles(last?.messages ?? [])).toEqual(['system', 'user', 'assistant', 'user'])
        expect(JSON.stringify(last?.messages[1])).toContain('the summary')
        expect(yield* liveOf(harness, conversation.id)).toEqual({})
      }),
    ))

  test('compacts first above the blocking threshold', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage, small)
        yield* faux.setResponses([
          Faux.message('first answer'),
          Faux.message('the summary'),
          Faux.message('second answer'),
        ])
        const conversation = yield* ownerless(harness)
        yield* ask(conversation, 'hi')
        const settled = yield* ask(conversation, 'x'.repeat(2000))
        expect(settled.status).toBe('done')
        const kinds = (yield* entriesOf(harness, conversation.id)).map((entry) => entry.kind)
        // Without sections or tools nothing needs a system entry, until the summary's baseline.
        expect(kinds).toEqual([
          '$user',
          '$assistant',
          '$user',
          '$compaction',
          '$system',
          '$assistant',
        ])
        expect(yield* liveOf(harness, conversation.id)).toEqual({})
      }),
    ))

  test('starts a background compaction above the background threshold, and keeps going', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage, {
          ...small,
          settings: {
            compaction: { reserveTokens: 200, keepRecentTokens: 1, backgroundTokens: 150 },
          },
        })
        yield* faux.setResponses([respond, respond, respond, respond])
        const conversation = yield* ownerless(harness)
        yield* ask(conversation, 'hi')
        expect((yield* ask(conversation, 'x'.repeat(400))).status).toBe('done')
        yield* eventually(
          Effect.map(entriesOf(harness, conversation.id), (entries) =>
            entries.some((entry) => entry.kind === '$compaction'),
          ),
        )
        yield* eventually(
          Effect.map(liveOf(harness, conversation.id), (live) => live.compactions === undefined),
        )
        const summarizer = (yield* faux.requests).find(
          (request) => request.messages[0]?.role === 'system',
        )
        expect(summarizer?.options?.cacheRetention).toBe('none')
      }),
    ))

  test('compacts after a context overflow and asks again', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage, {
          settings: { compaction: { keepRecentTokens: 1 } },
        })
        yield* faux.setResponses([
          Faux.message('first answer'),
          overflow,
          Faux.message('the summary'),
          Faux.message('second answer'),
        ])
        const conversation = yield* ownerless(harness)
        yield* ask(conversation, 'one')
        const settled = yield* ask(conversation, 'two')
        expect(settled.status).toBe('done')
        const entries = yield* entriesOf(harness, conversation.id)
        expect(entries.map((entry) => describe(entry)).slice(3)).toEqual([
          '$user: two',
          '$compaction: The conversation history before this point was compacted into the following summary:\n\n<summary>\nthe summary\n</summary>',
          '$system',
          '$assistant: second answer',
        ])
      }),
    ))

  test('ends the run model_error when an overflow leaves nothing to compact', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage)
        yield* faux.setResponses([overflow])
        const conversation = yield* ownerless(harness)
        expect(yield* ask(conversation, 'hi')).toMatchObject({
          status: 'unanswered',
          reason: 'model_error',
          detail: 'prompt is too long',
        })
      }),
    ))

  test('keeps a failed summarization in $live, and ends the overflowed run', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage, {
          settings: { compaction: { keepRecentTokens: 1 } },
        })
        const rejected = new ModelError({ message: 'no', reason: new Rejected({}) })
        yield* faux.setResponses([Faux.message('first answer'), overflow, rejected])
        const conversation = yield* ownerless(harness)
        yield* ask(conversation, 'one')
        const settled = yield* ask(conversation, 'two')
        expect(reasonOf(settled)).toBe('model_error')
        yield* eventually(
          Effect.map(liveOf(harness, conversation.id), (live) => live.run === undefined),
        )
        expect(yield* liveOf(harness, conversation.id)).toMatchObject({
          compactionFailure: { reason: 'overflow', error: 'Summarization failed: no' },
        })
      }),
    ))
})
