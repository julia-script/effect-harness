/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * The `Model` service over the `Faux` provider: streaming, routing, failures, and deferred
 * responses.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, Stream, SubscriptionRef } from 'effect'
import { Faux, Model, ModelError, Transcript } from '../index.ts'
import type { Message } from '../index.ts'

const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect)

const user = (content: string): Message.Message => ({ role: 'user', content, timestamp: 1 })

/** A Model over one faux provider, and a request to its first model. */
const setup = (options: Faux.Options = {}) =>
  Effect.gen(function* () {
    const faux = yield* Faux.make(options)
    const model = yield* Model.make([faux.provider])
    const request = (
      messages: ReadonlyArray<Message.Message>,
      requestOptions: Model.Options = {},
    ): Model.Request => ({ model: faux.ref(), messages, options: requestOptions })
    return { faux, model, request, info: faux.models[0] }
  })

describe('Model over Faux', () => {
  test('streams a text answer as growing partials, then the response', () =>
    run(
      Effect.gen(function* () {
        const { faux, model, request, info } = yield* setup({ tokenSize: 1 })
        yield* faux.setResponses([Faux.message('hello there')])
        const all = yield* Stream.runCollect(
          model.stream(request([user('hi')])).pipe(Model.responses(info, 5)),
        )
        const texts = all.map((message) =>
          Transcript.contentText(
            message.content.flatMap((block) => (block.type === 'text' ? [block] : [])),
          ),
        )
        expect(texts).toEqual(['hell', 'hello th', 'hello there', 'hello there'])
        expect(all.slice(0, -1).every((message) => message.stopReason === 'pending')).toBe(true)
        const final = all.at(-1)!
        expect(final).toMatchObject({ provider: 'faux', model: 'faux-1', stopReason: 'stop' })
        expect(final.usage).toMatchObject({ input: 2, output: 3, totalTokens: 5 })
      }),
    ))

  test('ends a tool round with toolUse and assigns call IDs', () =>
    run(
      Effect.gen(function* () {
        const { faux, model, request } = yield* setup()
        const calls = [Faux.toolCall('read', { path: 'a' }), Faux.toolCall('read', { path: 'b' })]
        yield* faux.setResponses([
          Faux.message([Faux.thinking('two files'), ...calls], { stopReason: 'toolUse' }),
        ])
        const events = yield* Stream.runCollect(model.stream(request([user('read')])))
        expect(events.map((event) => event.type)).toEqual([
          'thinking_delta',
          'toolcall_start',
          'toolcall_end',
          'toolcall_start',
          'toolcall_end',
          'done',
        ])
        const done = events.at(-1)
        const message = done?.type === 'done' ? done.message : Faux.message('')
        expect(message.stopReason).toBe('toolUse')
        expect(Transcript.toolCalls(message).map(({ id, arguments: args }) => [id, args])).toEqual([
          ['faux:1:1', { path: 'a' }],
          ['faux:1:2', { path: 'b' }],
        ])
      }),
    ))

  test('routes by provider and model, and fails an unknown one', () =>
    run(
      Effect.gen(function* () {
        const first = yield* Faux.make({ provider: 'a', models: [{ id: 'm1' }] })
        const second = yield* Faux.make({ provider: 'b', models: [{ id: 'm2' }, { id: 'm3' }] })
        const replaced = yield* Faux.make({ provider: 'a', models: [{ id: 'm4' }] })
        const model = yield* Model.make([first.provider, second.provider, replaced.provider])
        expect((yield* model.models).map(({ provider, id }) => `${provider}/${id}`)).toEqual([
          'a/m4',
          'b/m2',
          'b/m3',
        ])
        expect((yield* model.info({ provider: 'b', modelId: 'm3' }))?.contextWindow).toBe(128_000)
        expect(yield* model.info({ provider: 'a', modelId: 'm1' })).toBeUndefined()

        yield* second.setResponses([Faux.message('from b')])
        const response = yield* Model.collect(
          model.stream({ model: second.ref('m3'), messages: [user('hi')] }),
          second.models[1]!,
        )
        expect(response.model).toBe('m3')
        const error = yield* Effect.flip(
          Model.collect(
            model.stream({ model: { provider: 'c', modelId: 'x' }, messages: [] }),
            second.models[0],
          ),
        )
        expect(error.reason).toMatchObject({ _tag: 'UnknownModel', provider: 'c', modelId: 'x' })
      }),
    ))

  test('routes to the providers a dynamic set holds at each call', () =>
    run(
      Effect.gen(function* () {
        const work = yield* Faux.make({ provider: 'work', models: [{ id: 'm1' }] })
        const home = yield* Faux.make({ provider: 'home', models: [{ id: 'm1' }] })
        const providers = yield* SubscriptionRef.make<ReadonlyArray<Model.Provider>>([
          work.provider,
        ])
        const model = yield* Model.makeDynamic(SubscriptionRef.get(providers))
        expect(yield* model.info(home.ref('m1'))).toBeUndefined()

        yield* SubscriptionRef.set(providers, [work.provider, home.provider])
        yield* home.setResponses([Faux.message('from home')])
        const info = yield* model.info(home.ref('m1'))
        const response = yield* Model.collect(
          model.stream({ model: home.ref('m1'), messages: [user('hi')] }),
          home.models[0],
        )
        expect([info?.provider, response.content]).toEqual([
          'home',
          [{ type: 'text', text: 'from home' }],
        ])

        // A removed provider's requests fail as an unknown model.
        yield* SubscriptionRef.set(providers, [home.provider])
        const error = yield* Effect.flip(
          Model.collect(
            model.stream({ model: work.ref('m1'), messages: [user('hi')] }),
            work.models[0],
          ),
        )
        expect(error.reason).toMatchObject({ _tag: 'UnknownModel', provider: 'work' })
        expect((yield* model.models).map(({ provider }) => provider)).toEqual(['home'])
      }),
    ))

  test('fails with a scripted error, an error response, or an empty script', () =>
    run(
      Effect.gen(function* () {
        const { faux, model, request, info } = yield* setup()
        yield* faux.setResponses([
          new ModelError.ModelError({
            message: 'slow down',
            reason: new ModelError.Retryable({ retryAfterMs: 50 }),
          }),
          Faux.message([], { stopReason: 'error', errorMessage: 'bad request' }),
        ])
        const collect = Model.collect(model.stream(request([user('hi')])), info)
        const retryable = yield* Effect.flip(collect)
        expect(retryable.isRetryable).toBe(true)
        expect(retryable.reason).toMatchObject({ _tag: 'Retryable', retryAfterMs: 50 })
        const rejected = yield* Effect.flip(collect)
        expect(rejected).toMatchObject({ message: 'bad request', reason: { _tag: 'Rejected' } })
        expect(rejected.response?.stopReason).toBe('error')
        const empty = yield* Effect.flip(collect)
        expect(empty.message).toBe('No more faux responses queued')
        expect(empty.response?.usage.input).toBeGreaterThan(0)
        expect((yield* faux.state).callCount).toBe(3)
      }),
    ))

  test('estimates cache reads for a repeated session prefix', () =>
    run(
      Effect.gen(function* () {
        const { faux, model, request, info } = yield* setup()
        yield* faux.setResponses([Faux.message('one'), Faux.message('two')])
        const options = { sessionId: 's' }
        const first = yield* Model.collect(
          model.stream(request([user('a long first prompt')], options)),
          info,
        )
        expect(first.usage).toMatchObject({ input: 6, cacheRead: 0, cacheWrite: 6 })
        const messages = [user('a long first prompt'), first, user('next')]
        const second = yield* Model.collect(model.stream(request(messages, options)), info)
        // The repeated first message is read from the cache; the rest is new input.
        expect(second.usage).toMatchObject({ cacheRead: 6, input: 7 })
      }),
    ))

  test('answers a deferred request by polling, and cancels one', () =>
    run(
      Effect.gen(function* () {
        const { faux, model, request, info } = yield* setup({
          deferred: { pendingFetches: 1, pollAfterMs: 10 },
        })
        yield* faux.setResponses([Faux.message('later'), Faux.message('never')])
        const deferred = yield* Model.collect(
          model.stream(request([user('batch')], { deferred: true })),
          info,
        )
        expect(deferred).toMatchObject({ stopReason: 'deferred', content: [] })
        const handle = deferred.deferred!
        expect(handle).toMatchObject({ provider: 'faux', modelId: 'faux-1', pollAfterMs: 10 })
        expect((yield* model.poll(faux.ref(), handle)).stopReason).toBe('deferred')
        const ready = yield* model.poll(faux.ref(), handle)
        expect(ready).toMatchObject({
          stopReason: 'stop',
          content: [{ type: 'text', text: 'later' }],
        })
        expect(yield* model.poll(faux.ref(), handle)).toEqual(ready)

        const other = yield* Model.collect(
          model.stream(request([user('batch')], { deferred: true })),
          info,
        )
        yield* model.cancel(faux.ref(), other.deferred!)
        const cancelled = yield* Effect.flip(model.poll(faux.ref(), other.deferred!))
        expect(cancelled.message).toContain('cancelled')
        expect(yield* faux.state).toMatchObject({
          deferredFetchCount: 4,
          cancelledDeferred: [other.deferred],
        })
      }),
    ))

  test('a provider without deferred responses rejects a poll and ignores a cancel', () =>
    run(
      Effect.gen(function* () {
        const { faux } = yield* setup()
        const { poll: _poll, cancel: _cancel, ...plain } = faux.provider
        const model = yield* Model.make([plain])
        const handle = { provider: 'faux', modelId: 'faux-1', api: 'faux', id: 'x' }
        yield* model.cancel(faux.ref(), handle)
        const error = yield* Effect.flip(model.poll(faux.ref(), handle))
        expect(error.reason).toBeInstanceOf(ModelError.Rejected)
      }),
    ))
})

describe('Model.responses', () => {
  test('ignores events after done, and fails a stream that never finishes', () =>
    run(
      Effect.gen(function* () {
        const { info } = yield* setup()
        const done = Faux.message('x')
        const after = yield* Stream.runCollect(
          Stream.fromIterable<Model.Event>([
            { type: 'done', message: done },
            { type: 'text_delta', contentIndex: 0, delta: 'late' },
          ]).pipe(Model.responses(info, 0)),
        )
        expect(after).toEqual([done])
        const unfinished = yield* Effect.flip(
          Stream.runCollect(
            Stream.fromIterable<Model.Event>([
              { type: 'text_delta', contentIndex: 0, delta: 'a' },
            ]).pipe(Model.responses(info, 0)),
          ),
        )
        expect(unfinished.reason).toBeInstanceOf(ModelError.Retryable)
      }),
    ))
})
