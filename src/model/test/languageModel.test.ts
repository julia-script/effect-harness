/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * `LanguageModelProvider` over a scripted effect/ai `LanguageModel`: the prompt and tools it
 * sends, and the events, response and failures it maps back.
 */
import { describe, expect, test } from 'bun:test'
import { Duration, Effect, Ref, Stream } from 'effect'
import { AiError, LanguageModel } from 'effect/ai'
import type { Response } from 'effect/ai'
import { Faux, LanguageModelProvider, Model, ModelError, Transcript } from '../index.ts'
import type { Message } from '../index.ts'

type Script = ReadonlyArray<Response.StreamPartEncoded> | AiError.AiError

/** A Model over one provider whose LanguageModel answers each request with the next script. */
const setup = (scripts: ReadonlyArray<Script>) =>
  Effect.gen(function* () {
    const queue = yield* Ref.make(scripts)
    const seen = yield* Ref.make<ReadonlyArray<LanguageModel.ProviderOptions>>([])
    const languageModel = LanguageModel.make({
      generateText: () => Effect.succeed([]),
      streamText: (options) =>
        Stream.unwrap(
          Effect.gen(function* () {
            yield* Ref.update(seen, (all) => [...all, options])
            const script = yield* Ref.modify(
              queue,
              ([next, ...rest]) => [next ?? [], rest] as const,
            )
            return AiError.isAiError(script) ? Stream.fail(script) : Stream.fromIterable(script)
          }),
        ),
    })
    const provider = LanguageModelProvider.make({
      name: 'acme',
      api: 'acme-messages',
      models: [{ id: 'm', cost: { input: 3, output: 15, cacheRead: 0.3, cacheWrite: 3.75 } }],
      languageModel: () => languageModel,
    })
    const model = yield* Model.make([provider])
    const info = provider.models[0]!
    const ask = (messages: ReadonlyArray<Message.Message>) =>
      Model.collect(model.stream({ model: { provider: 'acme', modelId: 'm' }, messages }), info)
    return { model, info, ask, seen: Ref.get(seen) }
  })

const usage = (input: number, output: number) => ({
  inputTokens: { uncached: input, total: input + 5, cacheRead: 5 },
  outputTokens: { total: output },
})

const user = (content: string): Message.Message => ({ role: 'user', content, timestamp: 1 })

const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect)

const failure = (reason: AiError.AiErrorReason) =>
  AiError.make({ module: 'Test', method: 'streamText', reason })

describe('LanguageModelProvider', () => {
  test('streams text, then a response with usage, cost and response IDs', () =>
    run(
      Effect.gen(function* () {
        const { model, info } = yield* setup([
          [
            { type: 'response-metadata', id: 'r1', modelId: 'm-2026' },
            { type: 'text-start', id: 't' },
            { type: 'text-delta', id: 't', delta: 'Hel' },
            { type: 'text-delta', id: 't', delta: 'lo' },
            { type: 'text-end', id: 't' },
            { type: 'finish', reason: 'stop', usage: usage(10, 20) },
          ],
        ])
        const request = { model: { provider: 'acme', modelId: 'm' }, messages: [user('hi')] }
        const all = yield* Stream.runCollect(model.stream(request).pipe(Model.responses(info, 0)))
        expect(
          all.map((message) =>
            Transcript.contentText(
              message.content.flatMap((block) => (block.type === 'text' ? [block] : [])),
            ),
          ),
        ).toEqual(['', 'Hel', 'Hello', 'Hello'])
        const final = all.at(-1)!
        expect(final).toMatchObject({
          api: 'acme-messages',
          provider: 'acme',
          model: 'm',
          responseId: 'r1',
          responseModel: 'm-2026',
          stopReason: 'stop',
          rawStopReason: 'stop',
        })
        expect(final.usage).toMatchObject({ input: 10, output: 20, cacheRead: 5, totalTokens: 35 })
        expect(final.usage.cost.total).toBeCloseTo((10 * 3 + 20 * 15 + 5 * 0.3) / 1_000_000)
      }),
    ))

  test('sends the collapsed prompt and the current tools, and ends a tool round with toolUse', () =>
    run(
      Effect.gen(function* () {
        const { ask, seen } = yield* setup([
          [
            { type: 'tool-params-start', id: 'c2', name: 'read' },
            { type: 'tool-params-delta', id: 'c2', delta: '{"path"' },
            { type: 'tool-call', id: 'c2', name: 'read', params: { path: 'b' } },
            { type: 'finish', reason: 'tool-calls', usage: usage(1, 1) },
          ],
        ])
        const read: Message.ToolDefinition = {
          name: 'read',
          description: 'Read a file',
          parameters: { type: 'object', properties: { path: { type: 'string' } } },
        }
        const response = yield* ask([
          { role: 'system', content: 'Be brief.', toolsAdded: [read], timestamp: 0 },
          user('read a'),
          {
            ...Faux.message([Faux.toolCall('read', { path: 'a' }, 'c1')], {
              stopReason: 'toolUse',
            }),
          },
          {
            role: 'toolResult',
            toolCallId: 'c1',
            toolName: 'read',
            content: [{ type: 'text', text: 'A' }],
            isError: false,
            timestamp: 2,
          },
          Faux.message('cut off', { stopReason: 'aborted' }),
          { role: 'system', content: 'Then read b.', timestamp: 3 },
        ])
        expect(response.stopReason).toBe('toolUse')
        expect(Transcript.toolCalls(response)).toEqual([
          { type: 'toolCall', id: 'c2', name: 'read', arguments: { path: 'b' } },
        ])
        const [options] = yield* seen
        expect(options!.prompt.content.map((message) => message.role)).toEqual([
          'system',
          'user',
          'assistant',
          'tool',
        ])
        expect(options!.prompt.content[0]).toMatchObject({ content: 'Be brief.\n\nThen read b.' })
        expect(options!.tools).toHaveLength(1)
        expect(options!.tools[0]).toMatchObject({ name: 'read', description: 'Read a file' })
      }),
    ))

  test('keeps reasoning metadata as the signature, and replays it as part options', () =>
    run(
      Effect.gen(function* () {
        const { ask, seen } = yield* setup([
          [
            { type: 'reasoning-start', id: 'r' },
            { type: 'reasoning-delta', id: 'r', delta: 'think' },
            { type: 'reasoning-end', id: 'r', metadata: { acme: { signature: 's' } } },
            { type: 'text-delta', id: 't', delta: 'ok' },
            { type: 'finish', reason: 'stop', usage: usage(1, 1) },
          ],
          [{ type: 'finish', reason: 'stop', usage: usage(1, 1) }],
        ])
        const first = yield* ask([user('hi')])
        expect(first.content).toEqual([
          { type: 'thinking', thinking: 'think', thinkingSignature: '{"acme":{"signature":"s"}}' },
          { type: 'text', text: 'ok' },
        ])
        yield* ask([user('hi'), first, user('more')])
        const replayed = (yield* seen)[1]!.prompt.content[1]!
        expect(replayed).toMatchObject({
          role: 'assistant',
          content: [
            { type: 'reasoning', text: 'think', options: { acme: { signature: 's' } } },
            { type: 'text', text: 'ok' },
          ],
        })
      }),
    ))

  test('maps failures to their reasons', () =>
    run(
      Effect.gen(function* () {
        const { ask } = yield* setup([
          failure(new AiError.RateLimitError({ retryAfter: Duration.seconds(2) })),
          failure(
            new AiError.InvalidRequestError({ description: 'prompt is too long: 300000 tokens' }),
          ),
          failure(new AiError.AuthenticationError({ kind: 'InvalidKey' })),
          failure(new AiError.QuotaExhaustedError({})),
          failure(new AiError.InternalProviderError({ description: 'overloaded' })),
          [{ type: 'finish', reason: 'content-filter', usage: usage(1, 0) }],
        ])
        const reasons = yield* Effect.forEach([1, 2, 3, 4, 5, 6], () =>
          Effect.map(Effect.flip(ask([user('hi')])), (error) => error),
        )
        expect(reasons.map((error) => error.reason)).toEqual([
          new ModelError.Retryable({ retryAfterMs: 2000 }),
          new ModelError.ContextOverflow({}),
          new ModelError.Unavailable({}),
          new ModelError.Unavailable({}),
          new ModelError.Retryable({}),
          new ModelError.Rejected({}),
        ])
        expect(reasons[5]!.response).toMatchObject({
          stopReason: 'error',
          rawStopReason: 'content-filter',
        })
      }),
    ))

  test('reads the credential for every request', () =>
    run(
      Effect.gen(function* () {
        // Stands in for a credential store.
        const store = yield* Ref.make('first')
        const keys = yield* Ref.make<ReadonlyArray<string>>([])
        const provider = LanguageModelProvider.make({
          name: 'acme',
          api: 'acme-messages',
          models: [{ id: 'm' }],
          languageModel: () =>
            Effect.gen(function* () {
              const key = yield* Ref.get(store)
              yield* Ref.update(keys, (all) => [...all, key])
              return yield* LanguageModel.make({
                generateText: () => Effect.succeed([]),
                streamText: () =>
                  Stream.make({ type: 'finish', reason: 'stop', usage: usage(1, 1) } as const),
              })
            }),
        })
        const model = yield* Model.make([provider])
        const request = { model: { provider: 'acme', modelId: 'm' }, messages: [user('hi')] }
        const ask = Model.collect(model.stream(request), provider.models[0]!)
        yield* ask
        yield* Ref.set(store, 'second')
        yield* ask
        expect(yield* Ref.get(keys)).toEqual(['first', 'second'])
      }),
    ))
})
