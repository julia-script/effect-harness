import { vi } from 'vitest'
vi.mock('effect/ai/LanguageModel', { spy: true })
import * as Config from 'effect/Config'
import * as ConfigProvider from 'effect/ConfigProvider'
import * as Context from 'effect/Context'
import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as NativeLanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Cli from '../src/Cli.ts'
import * as IntentServer from '../src/IntentServer.ts'
import * as Provider from '../src/LanguageModel.ts'
import * as Protocol from '../src/Protocol.ts'

const init = (tools: ReadonlyArray<string> = []) => ({
  type: 'system',
  subtype: 'init',
  tools,
  model: 'actual-model',
})
const frame = (event: unknown) => ({ type: 'stream_event', event, parent_tool_use_id: null })
const start = frame({
  type: 'message_start',
  message: {
    id: 'msg-1',
    model: 'actual-model',
    content: [],
    usage: { input_tokens: 5, cache_read_input_tokens: 3, cache_creation_input_tokens: 2 },
  },
})
const textFrames = [
  init(),
  start,
  frame({ type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } }),
  frame({ type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'Hello' } }),
  frame({ type: 'content_block_stop', index: 0 }),
  frame({ type: 'message_delta', delta: { stop_reason: 'end_turn' }, usage: { output_tokens: 2 } }),
  frame({ type: 'message_stop' }),
  {
    type: 'result',
    subtype: 'success',
    is_error: false,
    num_turns: 1,
    usage: {
      input_tokens: 5,
      cache_read_input_tokens: 3,
      cache_creation_input_tokens: 2,
      output_tokens: 2,
    },
    total_cost_usd: 0.02,
  },
]
const decode = (events: ReadonlyArray<unknown>) =>
  Stream.fromIterable(events).pipe(
    Stream.mapEffect((event) => Protocol.decode(JSON.stringify(event))),
  )
const nativeTool = Tool.make('write.document', {
  description: 'Write a document',
  parameters: Schema.Struct({ value: Schema.String }),
  success: Schema.String,
})
const toolkit = Toolkit.make(nativeTool)
const fixture = (events: ReadonlyArray<unknown> = textFrames, options?: Provider.Options) => {
  const requests: Array<Cli.Request> = []
  let closed = 0
  let opened = 0
  const cli = Layer.succeed(Cli.Cli, {
    status: Effect.succeed({ loggedIn: true, account: true }),
    run: (request) => {
      requests.push(request)
      return decode(events)
    },
  })
  const server = Layer.succeed(IntentServer.IntentServer, {
    open: () =>
      Effect.gen(function* () {
        opened++
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            closed++
          }),
        )
        return {
          url: 'http://127.0.0.1:1/mcp/test',
          aliases: new Map([['mcp__harness__tool_0', nativeTool.name]]),
        }
      }),
  })
  return {
    requests,
    get closed() {
      return closed
    },
    get opened() {
      return opened
    },
    layer: Provider.layer(options ?? { model: 'requested-model' }).pipe(
      Layer.provide(Layer.merge(cli, server)),
    ),
  }
}
const toolFrames = (count = 2): Array<unknown> => {
  const events: Array<unknown> = [init(['mcp__harness__tool_0']), start]
  for (let index = 0; index < count; index++)
    events.push(
      frame({
        type: 'content_block_start',
        index,
        content_block: {
          type: 'tool_use',
          id: `call-${index}`,
          name: 'mcp__harness__tool_0',
          input: {},
        },
      }),
      frame({
        type: 'content_block_delta',
        index,
        delta: { type: 'input_json_delta', partial_json: `{"value":"${index}"}` },
      }),
      frame({ type: 'content_block_stop', index }),
    )
  events.push(
    frame({
      type: 'message_delta',
      delta: { stop_reason: 'tool_use' },
      usage: { output_tokens: 7 },
    }),
    frame({ type: 'message_stop' }),
  )
  return events
}

describe('native Claude Code LanguageModel', () => {
  it.effect('complete translates text, response metadata, usage and cost', () =>
    Effect.gen(function* () {
      const f = fixture()
      const response = yield* NativeLanguageModel.generateText({ prompt: 'Hi' }).pipe(
        Effect.provide(f.layer),
      )
      assert.strictEqual(response.text, 'Hello')
      const finish = response.content.find((part) => part.type === 'finish')
      assert.isDefined(finish)
      assert.strictEqual(finish?.usage.inputTokens.total, 10)
      assert.strictEqual(finish?.usage.outputTokens.total, 2)
      assert.deepEqual(finish?.metadata, { claudeCode: { totalCostUsd: 0.02 } })
      assert.strictEqual(f.requests[0]?.model, 'requested-model')
    }),
  )
  it.effect('stream emits native deltas and exactly one finish', () =>
    Effect.gen(function* () {
      const f = fixture()
      const parts = yield* NativeLanguageModel.streamText({ prompt: 'Hi' }).pipe(
        Stream.runCollect,
        Effect.provide(f.layer),
      )
      assert.deepEqual(
        parts.map((part) => part.type),
        ['response-metadata', 'text-start', 'text-delta', 'text-end', 'finish'],
      )
    }),
  )
  it.effect('captures all parallel tool intents before stopping the CLI; no handler executes', () =>
    Effect.gen(function* () {
      const f = fixture([
        ...toolFrames(),
        { type: 'result', subtype: 'error_during_execution', is_error: true, usage: {} },
      ])
      let executions = 0
      const handlers = toolkit.toLayer({
        'write.document': () =>
          Effect.sync(() => {
            executions++
            return 'executed'
          }),
      })
      const response = yield* NativeLanguageModel.generateText({
        prompt: 'Write',
        toolkit,
        disableToolCallResolution: true,
      }).pipe(Effect.provide(Layer.merge(f.layer, handlers)))
      assert.strictEqual(executions, 0)
      assert.strictEqual(response.toolCalls.length, 2)
      assert.deepEqual(
        response.toolCalls.map((call) => [call.id, call.name, call.params, call.providerExecuted]),
        [
          ['call-0', 'write.document', { value: '0' }, false],
          ['call-1', 'write.document', { value: '1' }, false],
        ],
      )
      assert.strictEqual(f.opened, 1)
      assert.strictEqual(f.closed, 1)
      assert.strictEqual(
        response.content.find((part) => part.type === 'finish')?.reason,
        'tool-calls',
      )
    }),
  )
  it.effect(
    'assistant block fallback works and partial/full duplicates do not duplicate text',
    () =>
      Effect.gen(function* () {
        const assistant = {
          type: 'assistant',
          message: {
            id: 'msg-1',
            model: 'actual-model',
            content: [{ type: 'text', text: 'Hello' }],
            usage: { output_tokens: 2 },
            stop_reason: 'end_turn',
          },
        }
        const f = fixture([init(), assistant, textFrames[textFrames.length - 1]])
        assert.strictEqual(
          (yield* NativeLanguageModel.generateText({ prompt: 'Hi' }).pipe(Effect.provide(f.layer)))
            .text,
          'Hello',
        )
        const duplicate = fixture([
          ...textFrames.slice(0, -1),
          assistant,
          textFrames[textFrames.length - 1],
        ])
        assert.strictEqual(
          (yield* NativeLanguageModel.generateText({ prompt: 'Hi' }).pipe(
            Effect.provide(duplicate.layer),
          )).text,
          'Hello',
        )
      }),
  )
  it.effect(
    'default rejects arbitrary history; opt-in transcript preserves role, tool records, and real attachments',
    () =>
      Effect.gen(function* () {
        const prompt = Prompt.fromMessages([
          Prompt.userMessage({ content: [Prompt.makePart('text', { text: 'First' })] }),
          Prompt.assistantMessage({
            content: [
              Prompt.makePart('tool-call', {
                id: 'call',
                name: 'write.document',
                params: { value: 'prior' },
                providerExecuted: false,
              }),
            ],
          }),
          Prompt.toolMessage({
            content: [
              Prompt.makePart('tool-result', {
                id: 'call',
                name: 'write.document',
                result: 'done',
                isFailure: false,
                providerExecuted: false,
              }),
            ],
          }),
          Prompt.userMessage({
            content: [
              Prompt.makePart('file', { mediaType: 'image/png', data: new Uint8Array([1, 2, 3]) }),
              Prompt.makePart('text', { text: 'Continue' }),
            ],
          }),
        ])
        const rejected = fixture()
        const error = yield* NativeLanguageModel.generateText({ prompt }).pipe(
          Effect.provide(rejected.layer),
          Effect.flip,
        )
        assert.strictEqual(error.reason._tag, 'InvalidRequestError')
        assert.strictEqual(rejected.requests.length, 0)
        const f = fixture(textFrames, { model: 'model', historyMode: 'transcript' })
        yield* NativeLanguageModel.generateText({ prompt }).pipe(Effect.provide(f.layer))
        const content = f.requests[0]?.content
        assert.include(JSON.stringify(content), 'effect-harness-transcript/1')
        assert.include(JSON.stringify(content), 'tool-result')
        assert.include(JSON.stringify(content), 'prior')
        assert.include(JSON.stringify(content), 'AQID')
        assert.include(JSON.stringify(content), 'media_type')
      }),
  )
  it.effect('native unsupported options fail before the CLI is started', () =>
    Effect.gen(function* () {
      const f = fixture()
      const required = yield* NativeLanguageModel.generateText({
        prompt: 'Hi',
        toolkit,
        toolChoice: 'required',
        disableToolCallResolution: true,
      }).pipe(Effect.provide(f.layer), Effect.flip)
      assert.strictEqual(required.reason._tag, 'InvalidRequestError')
      const structured = yield* NativeLanguageModel.generateObject({
        prompt: 'Hi',
        schema: Schema.Struct({ value: Schema.String }),
      }).pipe(Effect.provide(f.layer), Effect.flip)
      assert.strictEqual(structured.reason._tag, 'InvalidRequestError')
      const url = Prompt.fromMessages([
        Prompt.userMessage({
          content: [
            Prompt.makePart('file', {
              mediaType: 'image/png',
              data: new URL('https://example.com/image'),
            }),
          ],
        }),
      ])
      assert.strictEqual(
        (yield* NativeLanguageModel.generateText({ prompt: url }).pipe(
          Effect.provide(f.layer),
          Effect.flip,
        )).reason._tag,
        'InvalidRequestError',
      )
      assert.strictEqual(f.requests.length, 0)
    }),
  )
  it.effect(
    'rejects unsafe manifests, malformed tools, failures, missing terminal, nested output and hooks',
    () =>
      Effect.gen(function* () {
        const negatives: ReadonlyArray<ReadonlyArray<unknown>> = [
          [init(['Bash']), ...textFrames.slice(1)],
          [init(), start],
          [init(), { type: 'system', subtype: 'hook_started' }],
          [init(), { type: 'result', subtype: 'error_max_turns', is_error: true, usage: {} }],
          [
            init(),
            {
              type: 'assistant',
              parent_tool_use_id: 'nested',
              message: { id: 'm', model: 'm', content: [], usage: {} },
            },
          ],
          [
            init(),
            start,
            frame({
              type: 'content_block_start',
              index: 0,
              content_block: { type: 'tool_use', id: 'bad', name: 'Bash', input: {} },
            }),
          ],
        ]
        for (const events of negatives)
          assert.strictEqual(
            (yield* NativeLanguageModel.generateText({ prompt: 'Hi' }).pipe(
              Effect.provide(fixture(events).layer),
              Effect.flip,
            ))._tag,
            'AiError',
          )
        const malformed = toolFrames(1)
        malformed[3] = frame({
          type: 'content_block_delta',
          index: 0,
          delta: { type: 'input_json_delta', partial_json: '{bad' },
        })
        assert.strictEqual(
          (yield* NativeLanguageModel.generateText({
            prompt: 'Hi',
            toolkit,
            disableToolCallResolution: true,
          }).pipe(Effect.provide(fixture(malformed).layer), Effect.flip)).reason._tag,
          'InvalidOutputError',
        )
      }),
  )
  it.effect('cancellation closes the scoped MCP session and CLI stream', () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>()
      let stopped = false
      let closed = false
      const cli = Layer.succeed(Cli.Cli, {
        status: Effect.succeed({ loggedIn: true, account: true }),
        run: () =>
          Stream.unwrap(
            Effect.gen(function* () {
              yield* Effect.addFinalizer(() =>
                Effect.sync(() => {
                  stopped = true
                }),
              )
              yield* Deferred.succeed(started, undefined)
              return Stream.never
            }),
          ),
      })
      const server = Layer.succeed(IntentServer.IntentServer, {
        open: () =>
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                closed = true
              }),
            )
            return {
              url: 'http://127.0.0.1:1/mcp/test',
              aliases: new Map([['mcp__harness__tool_0', nativeTool.name]]),
            }
          }),
      })
      const layer = Provider.layer({ model: 'model' }).pipe(Layer.provide(Layer.merge(cli, server)))
      const fiber = yield* Effect.forkChild(
        NativeLanguageModel.generateText({
          prompt: 'Hi',
          toolkit,
          disableToolCallResolution: true,
        }).pipe(Effect.provide(layer)),
      )
      yield* Deferred.await(started)
      yield* Fiber.interrupt(fiber)
      assert.isTrue(stopped)
      assert.isTrue(closed)
    }),
  )
  it.effect('preserves reasoning signatures and cumulative per-model token accounting', () =>
    Effect.gen(function* () {
      const stats = {
        inputTokens: 9,
        outputTokens: 8,
        thinkingTokens: 3,
        cacheReadInputTokens: 4,
        cacheCreationInputTokens: 2,
        webSearchRequests: 0,
        costUSD: 0.04,
        contextWindow: 200000,
        maxOutputTokens: 64000,
      }
      const events = [
        init(),
        start,
        frame({
          type: 'content_block_start',
          index: 0,
          content_block: { type: 'thinking', thinking: '' },
        }),
        frame({
          type: 'content_block_delta',
          index: 0,
          delta: { type: 'thinking_delta', thinking: 'Reasoning' },
        }),
        frame({
          type: 'content_block_delta',
          index: 0,
          delta: { type: 'signature_delta', signature: 'signed' },
        }),
        frame({ type: 'content_block_stop', index: 0 }),
        frame({
          type: 'message_delta',
          delta: { stop_reason: 'end_turn' },
          usage: { output_tokens: 8 },
        }),
        frame({ type: 'message_stop' }),
        {
          type: 'result',
          subtype: 'success',
          is_error: false,
          usage: { input_tokens: 1, output_tokens: 1 },
          modelUsage: { 'actual-model': stats },
          total_cost_usd: 0.04,
        },
      ]
      const response = yield* NativeLanguageModel.generateText({ prompt: 'Think' }).pipe(
        Effect.provide(fixture(events).layer),
      )
      assert.strictEqual(response.reasoningText, 'Reasoning')
      assert.deepEqual(response.reasoning[0]?.metadata, { claudeCode: { signature: 'signed' } })
      assert.deepEqual(response.usage.inputTokens, {
        uncached: 9,
        total: 15,
        cacheRead: 4,
        cacheWrite: 2,
      })
      assert.deepEqual(response.usage.outputTokens, { total: 8, text: 5, reasoning: 3 })
      const bad = [
        ...events.slice(0, -1),
        {
          type: 'result',
          subtype: 'success',
          is_error: false,
          usage: {},
          modelUsage: { 'actual-model': { ...stats, thinkingTokens: 99 } },
        },
      ]
      assert.strictEqual(
        (yield* NativeLanguageModel.generateText({ prompt: 'Think' }).pipe(
          Effect.provide(fixture(bad).layer),
          Effect.flip,
        )).reason._tag,
        'InvalidOutputError',
      )
    }),
  )
  it.effect('preserves opaque redacted reasoning through complete generation', () =>
    Effect.gen(function* () {
      const events = [
        init(),
        {
          type: 'assistant',
          message: {
            id: 'msg-1',
            model: 'actual-model',
            content: [{ type: 'redacted_thinking', data: 'opaque' }],
            usage: {},
            stop_reason: 'end_turn',
          },
        },
        { type: 'result', subtype: 'success', is_error: false, usage: {} },
      ]
      const response = yield* NativeLanguageModel.generateText({ prompt: 'Think' }).pipe(
        Effect.provide(fixture(events).layer),
      )
      assert.deepEqual(response.reasoning[0]?.metadata, {
        claudeCode: { redactedThinking: 'opaque' },
      })
    }),
  )
  it.effect(
    'rejects contradictory assistant snapshots, autonomous extra turns, and unsafe accounting',
    () =>
      Effect.gen(function* () {
        const snapshot = {
          type: 'assistant',
          message: {
            id: 'msg-1',
            model: 'actual-model',
            content: [{ type: 'text', text: 'Different' }],
            usage: {},
            stop_reason: 'end_turn',
          },
        }
        const bad = [
          [...textFrames.slice(0, -1), snapshot, textFrames[textFrames.length - 1]],
          [
            ...textFrames.slice(0, -1),
            { type: 'result', subtype: 'success', is_error: false, num_turns: 2, usage: {} },
          ],
          [
            init(),
            {
              type: 'assistant',
              message: {
                id: 'msg',
                model: 'model',
                content: [{ type: 'text', text: 'x' }],
                usage: { input_tokens: Number.MAX_SAFE_INTEGER, cache_read_input_tokens: 1 },
                stop_reason: 'end_turn',
              },
            },
          ],
        ]
        for (const events of bad)
          assert.strictEqual(
            (yield* NativeLanguageModel.generateText({ prompt: 'Hi' }).pipe(
              Effect.provide(fixture(events).layer),
              Effect.flip,
            ))._tag,
            'AiError',
          )
      }),
  )
})

describe('CLI configured provider capability', () => {
  it.effect(
    'reexports exact substituted Cli and constructs one model with configured options',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const requests: Array<Cli.Request> = []
          const cli = Cli.Cli.of({
            status: Effect.succeed({ loggedIn: true, account: true }),
            run: (request) => {
              requests.push(request)
              return decode(textFrames)
            },
          })
          const spy = vi.mocked(NativeLanguageModel.make)
          spy.mockClear()
          const layer = Provider.layerConfig({
            model: Config.String('MODEL'),
            cwd: Config.String('CWD'),
            effort: Config.succeed('high'),
          }).pipe(
            Layer.provide(Layer.merge(Layer.succeed(Cli.Cli, cli), IntentServer.layerDisabled)),
          )
          yield* Effect.gen(function* () {
            const context = yield* Layer.build(layer).pipe(
              Effect.provideService(
                ConfigProvider.ConfigProvider,
                ConfigProvider.fromUnknown({ MODEL: 'configured-cli', CWD: '/caller/workspace' }),
              ),
            )
            assert.strictEqual(Context.get(context, Cli.Cli), cli)
            yield* NativeLanguageModel.generateText({ prompt: 'Hello' }).pipe(
              Effect.provideContext(context),
            )
            assert.strictEqual(requests[0]?.model, 'configured-cli')
            assert.strictEqual(requests[0]?.cwd, '/caller/workspace')
            assert.strictEqual(requests[0]?.effort, 'high')
            assert.strictEqual(spy.mock.calls.length, 1)
          })
        }),
      ),
  )
})
