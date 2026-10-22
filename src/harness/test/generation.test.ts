/* oxlint-disable max-lines, max-lines-per-function -- describe blocks */
/**
 * The built-in generation task over the `Faux` model, through whole runs. Ported in spirit from
 * pi's `harness-generation.test.ts` and `harness-tools.test.ts`; compaction, hooks, usage, and
 * tool controls come later.
 */
import { describe as group, expect, test } from 'bun:test'
import { Deferred, Effect, Exit, Ref } from 'effect'
import { Faux } from '../../model/index.ts'
import { ModelError, Rejected, Retryable, Unavailable } from '../../model/ModelError.ts'
import { Read } from '../../test/tool.ts'
import type * as Extension from '../Extension.ts'
import {
  describe,
  entriesOf,
  eventually,
  liveOf,
  messageOf,
  open,
  ownerless,
  reasonOf,
  runTest,
} from '../../test/generation.ts'

const calls = (...each: ReadonlyArray<readonly [string, Record<string, string>, string]>) =>
  Faux.message(
    each.map(([name, args, id]) => Faux.toolCall(name, args, id)),
    { stopReason: 'toolUse' },
  )

const retryable = new ModelError({ message: 'overloaded', reason: new Retryable({}) })

group('generation', () => {
  test('answers a run, with the system prompt and tools leading the request', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const section: Extension.Section = {
          key: 'rules',
          render: () => Effect.succeed('Be brief.'),
        }
        const { harness, faux } = yield* open(storage, {
          agent: (model) => ({ model: model.ref(), sections: [section], tools: [Read] }),
        })
        yield* faux.setResponses([Faux.message('hello')])
        const conversation = yield* ownerless(harness)
        const settled = yield* Effect.orDie(
          Effect.flatMap(conversation.submit({ type: 'input', content: 'hi' }), (s) => s.wait),
        )
        const entries = yield* entriesOf(harness, conversation.id)
        expect(entries.map((entry) => describe(entry))).toEqual([
          '$user: hi',
          '$system',
          '$assistant: hello',
        ])
        expect(settled).toMatchObject({ status: 'done', answer: entries[2]?.id })
        expect(messageOf(entries[1] ?? entries[0]!)).toMatchObject({
          role: 'system',
          sections: { rules: '<rules>\nBe brief.\n</rules>' },
          toolsAdded: [{ name: 'read' }],
        })
        const [request] = yield* faux.requests
        expect(request?.messages.map((message) => message.role)).toEqual(['system', 'user'])
        expect(request?.options?.sessionId).toBe(String(conversation.id))
        expect(yield* liveOf(harness, conversation.id)).toEqual({})
      }),
    ))

  test('writes system entries only for what changed', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const text = yield* Ref.make('one')
        const section: Extension.Section = { key: 's', tag: false, render: () => Ref.get(text) }
        const { harness, faux } = yield* open(storage, {
          agent: (model) => ({ model: model.ref(), sections: [section], tools: [] }),
        })
        yield* faux.setResponses([Faux.message('a'), Faux.message('b'), Faux.message('c')])
        const conversation = yield* ownerless(harness)
        const ask = (content: string) =>
          Effect.orDie(
            Effect.flatMap(conversation.submit({ type: 'input', content }), (s) => s.wait),
          )
        yield* ask('1')
        yield* ask('2')
        yield* Ref.set(text, 'two')
        yield* ask('3')
        const entries = yield* entriesOf(harness, conversation.id)
        expect(entries.map((entry) => describe(entry))).toEqual([
          '$user: 1',
          '$system',
          '$assistant: a',
          '$user: 2',
          '$assistant: b',
          '$user: 3',
          '$system',
          '$assistant: c',
        ])
        expect(messageOf(entries[6]!)).toMatchObject({ sections: { s: 'two' } })
      }),
    ))

  test('runs a tool round and hands the run to the next generation', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage)
        yield* faux.setResponses([
          calls(['read', { path: 'a.txt' }, 'c1'], ['read', { path: 'b.txt' }, 'c2']),
          Faux.message('done'),
        ])
        const conversation = yield* ownerless(harness)
        const settled = yield* Effect.orDie(
          Effect.flatMap(conversation.submit({ type: 'input', content: 'go' }), (s) => s.wait),
        )
        const entries = yield* entriesOf(harness, conversation.id)
        expect(entries.map((entry) => describe(entry))).toEqual([
          '$user: go',
          '$system',
          '$assistant: call read | call read (toolUse)',
          '$tool-result: contents of a.txt',
          '$tool-result: contents of b.txt',
          '$assistant: done',
        ])
        expect(settled).toMatchObject({ status: 'done', answer: entries[5]?.id })
        const second = (yield* faux.requests)[1]
        expect(second?.messages.map((message) => message.role)).toEqual([
          'system',
          'user',
          'assistant',
          'toolResult',
          'toolResult',
        ])
        expect(yield* liveOf(harness, conversation.id)).toEqual({})
      }),
    ))

  test('answers calls to tools the request did not offer without a task', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage, {
          agent: (model) => ({ model: model.ref(), tools: [Read] }),
        })
        yield* faux.setResponses([
          calls(['lines', { lines: '3' }, 'c1'], ['write', {}, 'c2']),
          Faux.message('ok'),
        ])
        const conversation = yield* ownerless(harness)
        yield* Effect.orDie(
          Effect.flatMap(conversation.submit({ type: 'input', content: 'go' }), (s) => s.wait),
        )
        const entries = yield* entriesOf(harness, conversation.id)
        const results = entries.filter((entry) => entry.kind === '$tool-result')
        // The generation answers them itself, in the commit that appends the calls.
        const generation = entries.find((entry) => entry.kind === '$assistant')?.byTaskId
        expect(results.map((entry) => entry.byTaskId)).toEqual([generation, generation])
        expect(results.map((entry) => entry.data)).toMatchObject([
          { diagnostics: [{ code: 'tool_unavailable', message: 'Tool lines is not available' }] },
          { diagnostics: [{ code: 'tool_unavailable', message: 'Tool write is not available' }] },
        ])
      }),
    ))

  test('runs a sequential round one call after another', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux, script } = yield* open(storage, {
          settings: { toolExecution: 'sequential' },
        })
        yield* faux.setResponses([
          calls(['slow', {}, 'c1'], ['read', { path: 'a.txt' }, 'c2']),
          Faux.message('done'),
        ])
        const conversation = yield* ownerless(harness)
        const submission = yield* Effect.orDie(
          conversation.submit({ type: 'input', content: 'go' }),
        )
        yield* Deferred.await(script.started)
        const live = yield* liveOf(harness, conversation.id)
        expect(
          live.tools?.map((slot) => [slot.callId, slot.status, slot.taskId !== undefined]),
        ).toEqual([
          ['c1', 'running', true],
          ['c2', 'pending', false],
        ])
        yield* Deferred.done(script.release, Exit.void)
        yield* Effect.orDie(submission.wait)
        const entries = yield* entriesOf(harness, conversation.id)
        expect(entries.map((entry) => describe(entry)).slice(3)).toEqual([
          '$tool-result: slow done',
          '$tool-result: contents of a.txt',
          '$assistant: done',
        ])
      }),
    ))

  test('retries a retryable failure, then answers', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage)
        yield* faux.setResponses([retryable, retryable, Faux.message('third time')])
        const conversation = yield* ownerless(harness)
        const settled = yield* Effect.orDie(
          Effect.flatMap(conversation.submit({ type: 'input', content: 'hi' }), (s) => s.wait),
        )
        expect(settled.status).toBe('done')
        expect((yield* faux.state).callCount).toBe(3)
        const entries = yield* entriesOf(harness, conversation.id)
        expect(entries.map((entry) => describe(entry))).toEqual([
          '$user: hi',
          '$system',
          '$assistant: third time',
        ])
      }),
    ))

  test('ends the run model_error once retries run out, or on a rejection', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage, { settings: { retry: { maxRetries: 1 } } })
        const rejected = new ModelError({
          message: 'bad request',
          reason: new Rejected({}),
          response: Faux.message([], { stopReason: 'error', errorMessage: 'bad request' }),
        })
        yield* faux.setResponses([retryable, retryable, rejected])
        const conversation = yield* ownerless(harness)
        const ask = Effect.orDie(
          Effect.flatMap(conversation.submit({ type: 'input', content: 'hi' }), (s) => s.wait),
        )
        const first = yield* ask
        expect(first).toMatchObject({
          status: 'unanswered',
          reason: 'model_error',
          detail: 'overloaded',
        })
        const second = yield* ask
        expect(second).toMatchObject({ status: 'unanswered', reason: 'model_error' })
        const entries = yield* entriesOf(harness, conversation.id)
        expect(entries.map((entry) => describe(entry)).at(-1)).toBe('$assistant (error)')
        // The failed response is kept, but the next request does not see it.
        yield* faux.setResponses([Faux.message('ok')])
        yield* ask
        const last = (yield* faux.requests).at(-1)
        expect(last?.messages.map((message) => message.role)).toEqual([
          'system',
          'user',
          'user',
          'user',
        ])
      }),
    ))

  group('fallback', () => {
    const models = [{ id: 'main' }, { id: 'backup' }]
    const withBackup = (fallbacks: ReadonlyArray<string> = ['backup']) =>
      ({
        faux: { models },
        agent: (faux: Faux.Faux) => ({
          model: faux.ref('main'),
          fallbacks: fallbacks.map((id) => faux.ref(id)),
        }),
      }) as const
    const unavailable = new ModelError({
      message: 'quota exhausted',
      reason: new Unavailable({}),
      response: Faux.message([], { stopReason: 'error', errorMessage: 'quota exhausted' }),
    })
    const modelsAsked = (faux: Faux.Faux) =>
      Effect.map(faux.requests, (requests) => requests.map((request) => request.model.modelId))

    test('an unavailable model hands the run to the fallback, and the next run starts on it again', () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const { harness, faux } = yield* open(storage, withBackup())
          yield* faux.setResponses([unavailable, Faux.message('from backup'), Faux.message('main')])
          const conversation = yield* ownerless(harness)
          const ask = Effect.orDie(
            Effect.flatMap(conversation.submit({ type: 'input', content: 'hi' }), (s) => s.wait),
          )
          expect((yield* ask).status).toBe('done')
          const entries = yield* entriesOf(harness, conversation.id)
          expect(entries.map((entry) => describe(entry))).toEqual([
            '$user: hi',
            '$system',
            '$assistant (error)',
            '$fallback',
            '$assistant: from backup',
          ])
          expect(entries[3]?.data).toEqual({
            from: faux.ref('main'),
            to: faux.ref('backup'),
            error: 'quota exhausted',
          })
          expect(yield* liveOf(harness, conversation.id)).toEqual({})
          expect((yield* ask).status).toBe('done')
          expect(yield* modelsAsked(faux)).toEqual(['main', 'backup', 'main'])
        }),
      ))

    test('moves on once retries run out, or when the provider asks to wait too long', () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const { harness, faux } = yield* open(storage, {
            ...withBackup(),
            settings: { retry: { maxRetries: 1, baseDelayMs: 1, maxDelayMs: 1 } },
          })
          const later = new ModelError({
            message: 'usage limit',
            reason: new Retryable({ retryAfterMs: 60_000 }),
          })
          yield* faux.setResponses([
            retryable,
            retryable,
            Faux.message('one'),
            later,
            Faux.message('two'),
          ])
          const conversation = yield* ownerless(harness)
          const ask = Effect.orDie(
            Effect.flatMap(conversation.submit({ type: 'input', content: 'hi' }), (s) => s.wait),
          )
          expect((yield* ask).status).toBe('done')
          expect((yield* ask).status).toBe('done')
          expect(yield* modelsAsked(faux)).toEqual(['main', 'main', 'backup', 'main', 'backup'])
        }),
      ))

    test('skips a fallback no provider serves, and ends model_error once none is left', () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const { harness, faux } = yield* open(storage, withBackup(['gone', 'backup']))
          yield* faux.setResponses([unavailable, unavailable])
          const conversation = yield* ownerless(harness)
          const settled = yield* Effect.orDie(
            Effect.flatMap(conversation.submit({ type: 'input', content: 'hi' }), (s) => s.wait),
          )
          expect(settled).toMatchObject({
            status: 'unanswered',
            reason: 'model_error',
            detail: 'quota exhausted',
          })
          expect(yield* modelsAsked(faux)).toEqual(['main', 'backup'])
        }),
      ))
  })

  test('ends the run no_model without a model', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness } = yield* open(storage, { agent: () => ({ model: null }) })
        const conversation = yield* ownerless(harness)
        const settled = yield* Effect.orDie(
          Effect.flatMap(conversation.submit({ type: 'input', content: 'hi' }), (s) => s.wait),
        )
        expect(reasonOf(settled)).toBe('no_model')
      }),
    ))

  test('polls a deferred response', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage, {
          settings: { stream: { deferred: true } },
          faux: { deferred: { pendingFetches: 1, pollAfterMs: 1 } },
        })
        yield* faux.setResponses([Faux.message('later')])
        const conversation = yield* ownerless(harness)
        const settled = yield* Effect.orDie(
          Effect.flatMap(conversation.submit({ type: 'input', content: 'hi' }), (s) => s.wait),
        )
        expect(settled.status).toBe('done')
        expect((yield* faux.state).deferredFetchCount).toBe(2)
        const entries = yield* entriesOf(harness, conversation.id)
        expect(entries.map((entry) => describe(entry)).at(-1)).toBe('$assistant: later')
      }),
    ))
})

group('generation abort', () => {
  test('keeps a streamed partial as an aborted entry', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage, {
          faux: { tokensPerSecond: 20, tokenSize: 1 },
          settings: { partialIntervalMs: 10 },
        })
        yield* faux.setResponses([Faux.message('a long answer that streams slowly '.repeat(4))])
        const conversation = yield* ownerless(harness)
        const submission = yield* Effect.orDie(
          conversation.submit({ type: 'input', content: 'hi' }),
        )
        yield* eventually(
          Effect.map(
            liveOf(harness, conversation.id),
            (live) => live.generation?.message !== undefined,
          ),
        )
        yield* Effect.orDie(conversation.abort())
        expect(reasonOf(yield* Effect.orDie(submission.wait))).toBe('aborted')
        const entries = yield* entriesOf(harness, conversation.id)
        const last = entries.at(-1)
        expect(last === undefined ? '' : describe(last)).toStartWith('$assistant: a')
        expect(last === undefined ? '' : describe(last)).toEndWith('(aborted)')
        expect(yield* liveOf(harness, conversation.id)).toEqual({})
      }),
    ))

  test('answers the running call and the calls not started yet', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux, script } = yield* open(storage, {
          settings: { toolExecution: 'sequential' },
        })
        yield* faux.setResponses([calls(['slow', {}, 'c1'], ['read', { path: 'a.txt' }, 'c2'])])
        const conversation = yield* ownerless(harness)
        const submission = yield* Effect.orDie(
          conversation.submit({ type: 'input', content: 'go' }),
        )
        yield* Deferred.await(script.started)
        yield* Effect.orDie(conversation.abort())
        expect(reasonOf(yield* Effect.orDie(submission.wait))).toBe('aborted')
        const results = (yield* entriesOf(harness, conversation.id)).filter(
          (entry) => entry.kind === '$tool-result',
        )
        expect(results.map((entry) => entry.data)).toMatchObject([
          { diagnostics: [{ code: 'aborted', message: 'Tool slow was aborted' }] },
          { diagnostics: [{ code: 'aborted', message: 'Tool read was aborted' }] },
        ])
        expect(yield* liveOf(harness, conversation.id)).toEqual({})
      }),
    ))
})
