import { assert, describe, it } from '@effect/vitest'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as NativeConfig from '@effect/ai-openai/OpenAiConfig'
import * as NativeModel from '@effect/ai-openai/OpenAiLanguageModel'
import * as Cause from 'effect/Cause'
import * as Clock from 'effect/Clock'
import * as Context from 'effect/Context'
import * as Duration from 'effect/Duration'
import * as Deferred from 'effect/Deferred'
import * as Fiber from 'effect/Fiber'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Redacted from 'effect/Redacted'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as TestClock from 'effect/testing/TestClock'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as Response from 'effect/ai/Response'
import * as Tool from 'effect/ai/Tool'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as ToolResult from 'effect-harness/ToolResult'
import * as Toolkit from 'effect/ai/Toolkit'
import * as HttpClient from 'effect/http/HttpClient'
import type * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Model from 'effect-harness/Model'
import * as Identity from 'effect-harness/Identity'
import * as Usage from 'effect-harness/Usage'
import * as DeferredResponse from 'effect-harness/provider-openai/DeferredResponse'
import * as OwnedModel from 'effect-harness/provider-openai/OpenAiLanguageModel'
import { ProviderAffinity } from 'effect-harness/ProviderAffinity'

const CustomHandle = Schema.Struct({ id: Schema.NonEmptyString })
const request: Model.DeferredRequest = {
  requestId: Identity.RequestId.make('task:17/request:2/config:1'),
  attempt: Usage.AttemptId.make(0),
  prompt: Prompt.make('question'),
  toolkit: Toolkit.empty,
}
const response = (status: string, extra: object = {}) => ({
  id: 'resp/a b',
  model: 'gpt-4.1',
  created_at: 0,
  status,
  output: [],
  ...extra,
})
const complete: Model.Complete = {
  _tag: 'Complete',
  response: new LanguageModel.GenerateTextResponse([Response.makePart('text', { text: 'done' })]),
}
const failure = <A, E>(exit: Exit.Exit<A, E>): E => {
  assert.isTrue(Exit.isFailure(exit))
  if (Exit.isSuccess(exit)) assert.fail('Expected failure')
  const result = Cause.findError(exit.cause)
  assert.isTrue(Result.isSuccess(result))
  if (Result.isFailure(result)) assert.fail('Expected typed failure')
  return result.success
}
const fixture = (
  replies: ReadonlyArray<{
    readonly body: unknown
    readonly status?: number
    readonly headers?: Record<string, string>
  }>,
) => {
  const calls: Array<HttpClientRequest.HttpClientRequest> = []
  const transport = HttpClient.make((req) => {
    calls.push(req)
    const next = replies[calls.length - 1]
    return next === undefined
      ? Effect.die('Unexpected HTTP call')
      : Effect.succeed(
          HttpClientResponse.fromWeb(
            req,
            new globalThis.Response(JSON.stringify(next.body), {
              status: next.status ?? 200,
              headers: { 'content-type': 'application/json', ...next.headers },
            }),
          ),
        )
  })
  const client = OpenAiClient.make({
    apiUrl: 'https://proxy.invalid/v2',
    apiKey: Redacted.make('fixture-secret'),
    organizationId: Redacted.make('fixture-org'),
    projectId: Redacted.make('fixture-project'),
    transformClient: (client) =>
      HttpClient.mapRequest(client, (req) => ({
        ...req,
        headers: { ...req.headers, 'x-owned': 'yes' },
      })),
  }).pipe(Effect.provideService(HttpClient.HttpClient, transport))
  return { calls, client }
}
const body = (req: HttpClientRequest.HttpClientRequest | undefined) => {
  assert.isDefined(req)
  if (req === undefined || req.body._tag !== 'Uint8Array')
    return assert.fail('Expected JSON request')
  return Schema.decodeSync(Schema.fromJsonString(Schema.JsonObject))(
    new TextDecoder().decode(req.body.body),
  )
}

describe('deferred model contract', () => {
  it.effect(
    'supports custom-provider pending, completion, expiry and cancellation classification',
    () =>
      Effect.gen(function* () {
        const handleSchema = Schema.Struct({ id: Schema.NonEmptyString, expiresAt: Schema.Int })
        let cancelled = false
        const adapter = yield* Model.makeDeferred<typeof handleSchema, never>({
          handle: handleSchema,
          start: () =>
            Effect.gen(function* () {
              cancelled = false
              const now = yield* Clock.currentTimeMillis
              return {
                _tag: 'Deferred' as const,
                handle: { id: 'custom', expiresAt: now + 10 },
                nextPollAt: now + 1,
                expiresAt: now + 10,
              }
            }),
          poll: (handle, previous) =>
            Effect.gen(function* () {
              if (cancelled)
                return yield* new Model.DeferredTerminalError({
                  reason: 'cancelled',
                  responseId: handle.id,
                  message: 'custom work cancelled',
                })
              if ((yield* Clock.currentTimeMillis) >= handle.expiresAt)
                return yield* new Model.DeferredTerminalError({
                  reason: 'expired',
                  responseId: handle.id,
                  message: 'custom work expired',
                })
              return previous === 1 ? { _tag: 'Pending' as const, nextPollAt: 2 } : complete
            }),
          cancel: () =>
            Effect.sync(() => {
              cancelled = true
            }),
        })
        const first = yield* adapter.start(request)
        if (first._tag !== 'Deferred') return assert.fail('Expected deferred')
        assert.deepStrictEqual(yield* adapter.poll(first.handle, 1, request), {
          _tag: 'Pending',
          nextPollAt: 2,
        })
        assert.strictEqual((yield* adapter.poll(first.handle, 2, request))._tag, 'Complete')
        const second = yield* adapter.start(request)
        if (second._tag !== 'Deferred') return assert.fail('Expected deferred')
        yield* adapter.cancel(second.handle)
        assert.strictEqual(
          failure(yield* Effect.exit(adapter.poll(second.handle, 1, request)))._tag,
          'DeferredTerminalError',
        )
        const third = yield* adapter.start(request)
        if (third._tag !== 'Deferred') return assert.fail('Expected deferred')
        yield* TestClock.adjust(Duration.millis(10))
        const error = failure(yield* Effect.exit(adapter.poll(third.handle, 1, request)))
        if (error._tag !== 'DeferredTerminalError')
          return assert.fail('Expected custom expiry error')
        assert.strictEqual(error.reason, 'expired')
      }),
  )
  it.effect(
    'validates against the admission clock when callback work crosses a short deadline',
    () =>
      Effect.gen(function* () {
        const adapter = yield* Model.makeDeferred<typeof CustomHandle, never>({
          handle: CustomHandle,
          start: () =>
            TestClock.adjust(Duration.millis(2)).pipe(
              Effect.as({ _tag: 'Deferred' as const, handle: { id: 'a' }, nextPollAt: 1 }),
            ),
          poll: () => Effect.succeed({ _tag: 'Pending', nextPollAt: 3 }),
          cancel: () => Effect.void,
        })
        const result = yield* adapter.start(request)
        assert.strictEqual(result._tag, 'Deferred')
        if (result._tag !== 'Deferred') return assert.fail('Expected deferred')
        assert.deepStrictEqual(yield* adapter.poll(result.handle, 1, request), {
          _tag: 'Pending',
          nextPollAt: 3,
        })
      }),
  )
  it.effect('captures both directions of a serviceful handle codec and reports codec failure', () =>
    Effect.gen(function* () {
      class Decoder extends Context.Service<Decoder, number>()('DeferredModel/Decoder') {}
      class Encoder extends Context.Service<Encoder, number>()('DeferredModel/Encoder') {}
      const codec = Schema.String.pipe(
        Schema.decodeTo(Schema.Finite, {
          decode: SchemaGetter.transformEffect((value) =>
            Decoder.pipe(Effect.map((offset) => Number(value) - offset)),
          ),
          encode: SchemaGetter.transformEffect((value) =>
            Encoder.pipe(Effect.map((offset) => String(value + offset))),
          ),
        }),
      )
      let observed = -1
      const adapter = yield* Model.makeDeferred<typeof codec, never>({
        handle: codec,
        start: () => Effect.succeed({ _tag: 'Deferred', handle: 8, nextPollAt: 1 }),
        poll: (handle) =>
          Effect.sync(() => {
            observed = handle
            return complete
          }),
        cancel: () => Effect.void,
      }).pipe(Effect.provideService(Decoder, 3), Effect.provideService(Encoder, 3))
      const start = yield* adapter.start(request).pipe(Effect.provideService(Encoder, 100))
      if (start._tag !== 'Deferred') return assert.fail('Expected deferred')
      assert.strictEqual(start.handle, '11')
      yield* adapter.poll(start.handle, 1, request).pipe(Effect.provideService(Decoder, 100))
      assert.strictEqual(observed, 8)
      assert.strictEqual(
        failure(yield* Effect.exit(adapter.poll('invalid', 1, request)))._tag,
        'ModelError',
      )
      assert.strictEqual(observed, 8)
    }),
  )
  it.effect(
    'captures custom services, encodes handles, polls and cancels without ambient rebinding',
    () =>
      Effect.gen(function* () {
        class Owner extends Context.Service<Owner, { readonly name: string }>()(
          'DeferredModel/Owner',
        ) {}
        const events: Array<string> = []
        let released = false
        const layer = Layer.effect(
          Owner,
          Effect.acquireRelease(Effect.succeed({ name: 'owner' }), () =>
            Effect.sync(() => {
              released = true
            }),
          ),
        )
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const services = yield* Layer.build(layer)
            const adapter = yield* Model.makeDeferred<typeof CustomHandle, Owner>({
              handle: CustomHandle,
              start: () =>
                Effect.map(Owner, (owner) => {
                  events.push(owner.name)
                  return { _tag: 'Deferred' as const, handle: { id: 'custom' }, nextPollAt: 1 }
                }),
              poll: (handle, previous) =>
                Effect.map(Owner, (owner) => {
                  events.push(`${owner.name}/${handle.id}`)
                  return previous === 1 ? { _tag: 'Pending' as const, nextPollAt: 2 } : complete
                }),
              cancel: (handle) =>
                Effect.map(Owner, (owner) => {
                  events.push(`cancel/${owner.name}/${handle.id}`)
                }),
            }).pipe(Effect.provideContext(services))
            const start = yield* adapter
              .start(request)
              .pipe(Effect.provideService(Owner, { name: 'ambient' }))
            assert.strictEqual(start._tag, 'Deferred')
            if (start._tag !== 'Deferred') return assert.fail('Expected handle')
            const restored = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(
              JSON.stringify(start.handle),
            )
            assert.deepStrictEqual(yield* adapter.poll(restored, 1, request), {
              _tag: 'Pending',
              nextPollAt: 2,
            })
            assert.strictEqual((yield* adapter.poll(restored, 2, request))._tag, 'Complete')
            yield* adapter.cancel(restored)
            assert.isFalse(released)
            return events
          }),
        )
        assert.isTrue(released)
        assert.deepStrictEqual(result, [
          'owner',
          'owner/custom',
          'owner/custom',
          'cancel/owner/custom',
        ])
      }),
  )

  it.effect(
    'rejects bad handles and non-increasing deadlines and preserves typed provider failures',
    () =>
      Effect.gen(function* () {
        let polls = 0
        const adapter = yield* Model.makeDeferred<typeof CustomHandle, never>({
          handle: CustomHandle,
          start: () => Effect.succeed({ _tag: 'Deferred', handle: { id: 'a' }, nextPollAt: 0 }),
          poll: () =>
            Effect.sync(() => {
              polls++
              return { _tag: 'Pending', nextPollAt: 3 }
            }),
          cancel: () =>
            Effect.fail(
              new Model.DeferredTerminalError({
                reason: 'failed',
                responseId: 'a',
                message: 'cancel failed',
              }),
            ),
        })
        assert.strictEqual(failure(yield* Effect.exit(adapter.start(request)))._tag, 'ModelError')
        assert.strictEqual(
          failure(yield* Effect.exit(adapter.poll({ id: 2 }, 1, request)))._tag,
          'ModelError',
        )
        assert.strictEqual(polls, 0)
        assert.strictEqual(
          failure(yield* Effect.exit(adapter.poll({ id: 'a' }, 3, request)))._tag,
          'ModelError',
        )
        assert.strictEqual(
          failure(yield* Effect.exit(adapter.cancel({ id: 'a' })))._tag,
          'DeferredTerminalError',
        )
      }),
  )

  it.effect('keeps ordinary native generation and optional unsupported behavior unchanged', () =>
    Effect.gen(function* () {
      const f = fixture([
        {
          body: response('completed', {
            output: [
              {
                type: 'message',
                id: 'm',
                role: 'assistant',
                status: 'completed',
                content: [{ type: 'output_text', text: 'ordinary', annotations: [] }],
              },
            ],
          }),
        },
      ])
      const native = yield* f.client
      const languageModel = yield* OwnedModel.make({ model: 'gpt-4.1' }).pipe(
        Effect.provideService(OpenAiClient.OpenAiClient, native),
      )
      const model = Model.make({
        definition: {
          ref: { provider: 'openai', modelId: 'gpt-4.1' },
          capabilities: { tools: true, reasoning: true, images: true, structuredOutput: true },
        },
        options: Schema.JsonObject,
        languageModel,
        configure: () => Effect.succeed(Context.empty()),
      })
      assert.strictEqual(
        failure(yield* Effect.exit(Model.deferred(model))).reason._tag,
        'ModelUnsupportedError',
      )
      assert.strictEqual(
        (yield* model.languageModel.generateText({ prompt: 'question' })).text,
        'ordinary',
      )
      assert.notStrictEqual(body(f.calls[0]).background, true)
    }),
  )
})

describe('native OpenAI deferred wire', () => {
  it.effect('interruption releases local transport resources without remote cancellation', () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>()
      let releases = 0
      const client = yield* OpenAiClient.make({}).pipe(
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make(() =>
            Effect.acquireUseRelease(
              Deferred.succeed(started, undefined),
              () => Effect.never,
              () =>
                Effect.sync(() => {
                  releases++
                }),
            ),
          ),
        ),
      )
      const adapter = yield* DeferredResponse.make({ model: 'gpt-4.1' }).pipe(
        Effect.provideService(OpenAiClient.OpenAiClient, client),
      )
      const fiber = yield* Effect.forkChild(adapter.start(request))
      yield* Deferred.await(started)
      yield* Fiber.interrupt(fiber)
      const exit = yield* Fiber.await(fiber)
      assert.isTrue(Exit.isFailure(exit))
      assert.strictEqual(releases, 1)
    }),
  )

  it.effect('poll and cancel failures remain explicit and retrieval identity is checked', () =>
    Effect.gen(function* () {
      const f = fixture([
        { body: response('queued') },
        { body: response('failed') },
        { body: response('completed', { id: 'different' }) },
        { body: response('in_progress') },
        { status: 401, body: { error: { message: 'unauthorized', code: 'invalid_api_key' } } },
      ])
      const adapter = yield* DeferredResponse.make({ model: 'gpt-4.1' }).pipe(
        Effect.provideService(OpenAiClient.OpenAiClient, yield* f.client),
      )
      const start = yield* adapter.start(request)
      if (start._tag !== 'Deferred') return assert.fail('Expected deferred')
      assert.strictEqual(
        failure(yield* Effect.exit(adapter.poll(start.handle, 1000, request)))._tag,
        'DeferredTerminalError',
      )
      assert.strictEqual(
        failure(yield* Effect.exit(adapter.poll(start.handle, 1000, request)))._tag,
        'ModelError',
      )
      assert.strictEqual(
        failure(yield* Effect.exit(adapter.cancel(start.handle)))._tag,
        'ModelError',
      )
      const error = failure(yield* Effect.exit(adapter.cancel(start.handle)))
      assert.strictEqual(error._tag, 'AiError')
      if (error._tag === 'AiError') assert.strictEqual(error.reason._tag, 'AuthenticationError')
      assert.strictEqual(
        f.calls.filter((c) => c.method === 'POST' && c.url.endsWith('/responses')).length,
        1,
      )
    }),
  )
  it.effect(
    'uses configured transport, native media/tools/options, GET polls and real POST cancel',
    () =>
      Effect.gen(function* () {
        const f = fixture([
          { body: response('queued', { expires_at: 10 }) },
          { body: response('in_progress') },
          {
            body: response('completed', {
              output: [
                {
                  type: 'function_call',
                  id: 'fc',
                  call_id: 'call1',
                  name: 'convert',
                  arguments: '{"value":"3"}',
                  status: 'completed',
                },
              ],
              usage: { input_tokens: 7, output_tokens: 2, total_tokens: 9 },
            }),
          },
          { body: response('cancelled') },
        ])
        const client = yield* f.client
        const adapter = yield* DeferredResponse.make({
          model: 'gpt-4.1',
          config: { temperature: 0.1 },
        }).pipe(Effect.provideService(OpenAiClient.OpenAiClient, client))
        const convert = Tool.make('convert', {
          parameters: Schema.Struct({ value: Schema.FiniteFromString }),
        })
        const configured = Context.make(NativeModel.Config, {
          model: 'gpt-4.1',
          temperature: 0.7,
          max_output_tokens: 81,
        }).pipe(
          Context.add(NativeConfig.OpenAiConfig, {
            transformClient: (client) =>
              HttpClient.mapRequest(client, (req) => ({
                ...req,
                headers: { ...req.headers, 'x-request': 'scoped' },
              })),
          }),
        )
        const prompt = Prompt.make([
          Prompt.makeMessage('user', {
            content: [
              Prompt.makePart('text', { text: 'look' }),
              Prompt.makePart('file', {
                mediaType: 'image/png',
                data: new Uint8Array([1, 2]),
                options: { openai: { imageDetail: 'low' } },
              }),
            ],
          }),
          Prompt.makeMessage('tool', {
            content: [
              Prompt.makePart('tool-result', {
                id: 'prior',
                name: 'convert',
                isFailure: false,
                providerExecuted: false,
                result: yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.Envelope))({
                  _tag: '@effect-harness/ToolContent',
                  content: [
                    Prompt.makePart('text', { text: 'previous' }),
                    Prompt.makePart('file', { mediaType: 'image/png', data: new Uint8Array([3]) }),
                  ],
                }),
              }),
            ],
          }),
        ])
        const prepared = {
          ...request,
          prompt,
          toolkit: Toolkit.make(convert),
          configuration: configured,
        }
        const start = yield* adapter
          .start(prepared)
          .pipe(Effect.provideService(ProviderAffinity, { id: 'affinity' }))
        assert.strictEqual(start._tag, 'Deferred')
        if (start._tag !== 'Deferred') return assert.fail('Expected deferred')
        assert.strictEqual(start.nextPollAt, 1000)
        assert.strictEqual(start.expiresAt, 10000)
        const wire = body(f.calls[0])
        assert.strictEqual(wire.temperature, 0.7)
        assert.strictEqual(wire.max_output_tokens, 81)
        assert.strictEqual(wire.background, true)
        assert.strictEqual(wire.store, true)
        assert.strictEqual(wire.stream, false)
        assert.strictEqual(wire.prompt_cache_key, 'affinity')
        assert.include(JSON.stringify(wire.input), 'data:image/png;base64,AQI=')
        assert.include(JSON.stringify(wire.input), 'previous')
        assert.include(JSON.stringify(wire.input), 'data:image/png;base64,Aw==')
        assert.notInclude(JSON.stringify(wire.input), '@effect-harness/ToolContent')
        assert.include(JSON.stringify(wire.tools), 'convert')
        const pending = yield* adapter.poll(start.handle, start.nextPollAt, prepared)
        assert.deepStrictEqual(pending, { _tag: 'Pending', nextPollAt: 1001 })
        const final = yield* adapter.poll(start.handle, 1001, prepared)
        assert.strictEqual(final._tag, 'Complete')
        if (final._tag !== 'Complete') return assert.fail('Expected completed')
        assert.strictEqual(final.response.usage.inputTokens.total, 7)
        const toolCall = final.response.content.find((part) => part.type === 'tool-call')
        assert.deepStrictEqual(toolCall?.params, { value: '3' })
        yield* adapter.cancel(start.handle).pipe(
          NativeConfig.withClientTransform((client) =>
            HttpClient.mapRequest(client, (req) => ({
              ...req,
              headers: { ...req.headers, 'x-cancel': 'scoped' },
            })),
          ),
        )
        assert.deepStrictEqual(
          f.calls.map((c) => [c.method, c.url]),
          [
            ['POST', 'https://proxy.invalid/v2/responses'],
            ['GET', 'https://proxy.invalid/v2/responses/resp%2Fa%20b'],
            ['GET', 'https://proxy.invalid/v2/responses/resp%2Fa%20b'],
            ['POST', 'https://proxy.invalid/v2/responses/resp%2Fa%20b/cancel'],
          ],
        )
        for (const call of f.calls) {
          assert.strictEqual(call.headers.authorization, 'Bearer fixture-secret')
          assert.strictEqual(call.headers['openai-organization'], 'fixture-org')
          assert.strictEqual(call.headers['openai-project'], 'fixture-project')
          assert.strictEqual(call.headers['x-owned'], 'yes')
        }
        assert.strictEqual(f.calls[3]?.headers['x-cancel'], 'scoped')
        for (const call of f.calls.slice(0, 3))
          assert.strictEqual(call.headers['x-request'], 'scoped')
      }),
  )

  for (const status of ['completed', 'incomplete'])
    it.effect(`returns immediate native ${status} without a handle`, () =>
      Effect.gen(function* () {
        const f = fixture([
          {
            body: response(status, {
              output: [
                {
                  type: 'message',
                  id: 'm',
                  role: 'assistant',
                  status: 'completed',
                  content: [{ type: 'output_text', text: 'answer', annotations: [] }],
                },
              ],
              ...(status === 'incomplete'
                ? { incomplete_details: { reason: 'max_output_tokens' } }
                : {}),
            }),
          },
        ])
        const adapter = yield* DeferredResponse.make({ model: 'gpt-4.1' }).pipe(
          Effect.provideService(OpenAiClient.OpenAiClient, yield* f.client),
        )
        const result = yield* adapter.start(request)
        assert.strictEqual(result._tag, 'Complete')
        if (result._tag !== 'Complete') return assert.fail('Expected completion')
        assert.strictEqual(result.response.text, 'answer')
        assert.strictEqual(
          result.response.finishReason,
          status === 'incomplete' ? 'unknown' : 'stop',
        )
        assert.strictEqual(f.calls.length, 1)
      }),
    )

  for (const status of ['failed', 'cancelled'])
    it.effect(`preserves terminal ${status} as a semantic failure`, () =>
      Effect.gen(function* () {
        const f = fixture([
          { body: response(status, { error: { code: 'failed', message: 'provider said no' } }) },
        ])
        const adapter = yield* DeferredResponse.make({ model: 'gpt-4.1' }).pipe(
          Effect.provideService(OpenAiClient.OpenAiClient, yield* f.client),
        )
        const error = failure(yield* Effect.exit(adapter.start(request)))
        assert.strictEqual(error._tag, 'DeferredTerminalError')
        if (error._tag === 'DeferredTerminalError') assert.strictEqual(error.reason, status)
      }),
    )

  it.effect('expiry, wrong identity and conflicting caller options stop before HTTP', () =>
    Effect.gen(function* () {
      const f = fixture([{ body: response('queued', { expires_at: 1 }) }])
      const adapter = yield* DeferredResponse.make({ model: 'gpt-4.1' }).pipe(
        Effect.provideService(OpenAiClient.OpenAiClient, yield* f.client),
      )
      const conflicting = {
        ...request,
        configuration: Context.make(NativeModel.Config, { model: 'gpt-4.1', store: false }),
      }
      assert.strictEqual(failure(yield* Effect.exit(adapter.start(conflicting)))._tag, 'ModelError')
      const start = yield* adapter.start(request)
      if (start._tag !== 'Deferred') return assert.fail('Expected deferred')
      assert.strictEqual(
        failure(
          yield* Effect.exit(
            adapter.poll(start.handle, 1000, { ...request, attempt: Usage.AttemptId.make(1) }),
          ),
        )._tag,
        'ModelError',
      )
      yield* TestClock.adjust('1 second')
      const error = failure(yield* Effect.exit(adapter.poll(start.handle, 1000, request)))
      assert.strictEqual(error._tag, 'DeferredTerminalError')
      if (error._tag === 'DeferredTerminalError') assert.strictEqual(error.reason, 'expired')
      assert.strictEqual(f.calls.length, 1)
    }),
  )

  for (const code of ['rate_limit_exceeded', 'insufficient_quota'])
    it.effect(`retains native ${code} retry classification`, () =>
      Effect.gen(function* () {
        const f = fixture([
          {
            status: 429,
            headers: { 'retry-after': '2' },
            body: { error: { message: 'limited', code } },
          },
        ])
        const adapter = yield* DeferredResponse.make({ model: 'gpt-4.1' }).pipe(
          Effect.provideService(OpenAiClient.OpenAiClient, yield* f.client),
        )
        const error = failure(yield* Effect.exit(adapter.start(request)))
        assert.strictEqual(error._tag, 'AiError')
        if (error._tag !== 'AiError') return assert.fail('Expected AiError')
        assert.strictEqual(error.isRetryable, code === 'rate_limit_exceeded')
        if (code === 'rate_limit_exceeded')
          assert.strictEqual(
            Option.getOrThrow(Option.fromNullishOr(error.retryAfter)).pipe(Duration.toMillis),
            2000,
          )
      }),
    )

  it.effect('rejects malformed background status and keeps unrelated transport defects', () =>
    Effect.gen(function* () {
      const f = fixture([{ body: response('invented') }])
      const adapter = yield* DeferredResponse.make({ model: 'gpt-4.1' }).pipe(
        Effect.provideService(OpenAiClient.OpenAiClient, yield* f.client),
      )
      assert.strictEqual(failure(yield* Effect.exit(adapter.start(request)))._tag, 'ModelError')
      const defect = { sentinel: 'unrelated' }
      const client = yield* OpenAiClient.make({}).pipe(
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make(() => Effect.die(defect)),
        ),
      )
      const broken = yield* DeferredResponse.make({ model: 'gpt-4.1' }).pipe(
        Effect.provideService(OpenAiClient.OpenAiClient, client),
      )
      const exit = yield* Effect.exit(broken.start(request))
      if (Exit.isSuccess(exit)) return assert.fail('Expected defect')
      assert.strictEqual(Result.getOrThrow(Cause.findDefect(exit.cause)), defect)
    }),
  )
})
