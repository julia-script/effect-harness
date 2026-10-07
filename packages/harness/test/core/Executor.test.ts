import * as Duration from 'effect/Duration'
import * as DateTime from 'effect/DateTime'
import * as Time from '../../src/Time.ts'
import * as Deferred from 'effect/Deferred'
import * as Fiber from 'effect/Fiber'
import * as Scope from 'effect/Scope'
import { TestClock } from 'effect/testing'
import * as Identity from '../../src/Identity.ts'
import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Layer from 'effect/Layer'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as AiError from 'effect/ai/AiError'
import * as Prompt from 'effect/ai/Prompt'
import * as Response from 'effect/ai/Response'
import * as AiTool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Agent from '../../src/Agent.ts'
import * as ConversationContext from '../../src/Context.ts'
import {
  Executor,
  layer as executorLayer,
  Request,
  SummaryRequest,
  Disposition,
} from '../../src/Executor.ts'
import type * as Extension from '../../src/Extension.ts'
import { HookError, ToolError, HookFailure, ToolExecution } from '../../src/Error.ts'
import { Invocation, ToolCall, Result } from '../../src/Invocation.ts'
import * as Model from '../../src/Model.ts'
import * as Registry from '../../src/Registry.ts'
import * as Tool from '../../src/Tool.ts'

const entryId = Schema.decodeSync(Identity.EntryId)
const ref = { provider: 'test', modelId: 'model' }
const quiet = { cwd: '.', report: () => Effect.void, progress: () => Effect.void }
const nativeUsage = {
  inputTokens: { uncached: 10, total: 10, cacheRead: undefined, cacheWrite: undefined },
  outputTokens: { total: 5, text: 5, reasoning: undefined },
}
const finish = (reason: Response.FinishReason = 'stop'): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: nativeUsage,
  response: undefined,
})
const echoTool = AiTool.make('echo', {
  parameters: Schema.Struct({ n: Schema.FiniteFromString }),
  success: Result,
  failure: ToolError,
})
  .addDependency(Invocation)
  .addDependency(ToolCall)
const Tools = Toolkit.make(echoTool)
const project = (value: unknown) => Tool.decodeResult('echo', value)
const binding = (
  handler: Toolkit.HandlersFrom<Toolkit.Tools<typeof Tools>>['echo'],
  metadata: Tool.Metadata = {},
) =>
  Tool.bind(Tools, { echo: Object.assign({ project }, metadata) }).pipe(
    Effect.provide(Tools.toLayer({ echo: handler })),
  )
const model = (
  content: ReadonlyArray<Response.PartEncoded> = [{ type: 'text', text: 'answer' }, finish()],
) =>
  LanguageModel.make({
    generateText: () => Effect.succeed([...content]),
    streamText: () =>
      Stream.fromIterable([
        { type: 'text-start' as const, id: 't' },
        { type: 'text-delta' as const, id: 't', delta: 'answer' },
        { type: 'text-end' as const, id: 't' },
        finish(),
      ]),
  })
const runtime = Effect.fnUntraced(function* (
  extensions: ReadonlyArray<Extension.Extension> = [],
  native?: LanguageModel.LanguageModel,
  deferred?: Model.DeferredCapability,
) {
  const actual = native ?? (yield* model())
  return yield* Executor.pipe(
    Effect.provide(
      executorLayer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Registry.layer(extensions),
            Model.layer([
              {
                ref,
                model: actual,
                contextWindow: 1000,
                maxOutputTokens: 500,
                configure: () => Effect.succeed(Context.empty()),
                ...(deferred === undefined ? {} : { deferred }),
              },
            ]),
          ),
        ),
      ),
    ),
  )
})
const state: Agent.State = { model: ref }
const settings = Effect.runSync(Agent.settings({ progress: { outputIntervalMs: 0 } }))
const user = (text: string) => Prompt.userMessage({ content: [Prompt.textPart({ text })] })

describe('native AI executor intent/request boundaries', () => {
  it.effect(
    'terminal invalid projection settles a paced progress waiter with the exact typed failure',
    () =>
      Effect.gen(function* () {
        const owner = yield* Scope.Scope
        const pending = yield* Deferred.make<Fiber.Fiber<void, ToolError>>()
        const returned = yield* Deferred.make<void>()
        const progress = yield* Ref.make<ReadonlyArray<unknown>>([])
        const tools = yield* binding(
          (_args) =>
            Effect.gen(function* () {
              const call = yield* ToolCall
              yield* call.details({ phase: 'first' })
              const waiter = yield* call.details({ phase: 'pending' }).pipe(Effect.forkIn(owner))
              yield* Deferred.succeed(pending, waiter)
              yield* Deferred.await(returned)
              return {}
            }),
          { project: () => Tool.decodeResult('echo', { content: [{ type: 'text', text: 42 }] }) },
        )
        const executor = yield* runtime([{ name: 'tools', tools }])
        const agent = yield* executor.resolve(
          state,
          yield* Agent.settings({ progress: { outputIntervalMs: 10000 } }),
        )
        const running = yield* executor
          .tool({ id: 'projection', name: 'echo', args: { n: 1 }, replay: 'unsafe' }, agent)
          .pipe(
            Effect.provideService(
              Invocation,
              Invocation.of({
                ...quiet,
                progress: (value) => Ref.update(progress, (old) => [...old, value]),
              }),
            ),
            Effect.forkChild,
          )
        const waiter = yield* Deferred.await(pending)
        yield* TestClock.adjust(0)
        assert.strictEqual(waiter.pollUnsafe(), undefined)
        assert.strictEqual((yield* Ref.get(progress)).length, 1)
        yield* Deferred.succeed(returned, undefined)
        const failure = yield* Effect.flip(Fiber.join(running))
        assert.strictEqual(failure.reason._tag, 'ToolInvalidResult')
        assert.ok(failure.cause instanceof Schema.SchemaError)
        const receipt = yield* Effect.flip(Fiber.join(waiter))
        assert.strictEqual(receipt, failure)
        assert.strictEqual((yield* Ref.get(progress)).length, 1)
      }).pipe(Effect.provide(Layer.succeed(Invocation, Invocation.of(quiet)))),
  )
  it.effect(
    'schema-equivalent reordered progress details acknowledge without a second emission',
    () =>
      Effect.gen(function* () {
        const queued = yield* Deferred.make<void>()
        const progress = yield* Ref.make<ReadonlyArray<unknown>>([])
        const tools = yield* binding(() =>
          Effect.gen(function* () {
            const call = yield* ToolCall
            yield* call.details({ a: 1, b: 2 })
            const second = yield* call.details({ b: 2, a: 1 }).pipe(Effect.forkChild)
            yield* Deferred.succeed(queued, undefined)
            yield* Fiber.join(second)
            return {}
          }),
        )
        const executor = yield* runtime([{ name: 'tools', tools }])
        const running = yield* executor
          .tool(
            { id: 'equivalent', name: 'echo', args: { n: 1 }, replay: 'safe' },
            yield* executor.resolve(state, settings),
          )
          .pipe(
            Effect.provideService(
              Invocation,
              Invocation.of({
                ...quiet,
                progress: (value) => Ref.update(progress, (old) => [...old, value]),
              }),
            ),
            Effect.forkChild,
          )
        yield* Deferred.await(queued)
        assert.strictEqual((yield* Ref.get(progress)).length, 1)
        yield* TestClock.adjust(100)
        assert.strictEqual((yield* Fiber.join(running)).outcome, 'completed')
        assert.strictEqual((yield* Ref.get(progress)).length, 1)
      }).pipe(Effect.provideService(Invocation, Invocation.of(quiet))),
  )
  it.effect(
    'preliminary projection decoder failure is reported as invalid_result and publishes no preview',
    () =>
      Effect.gen(function* () {
        const reports = yield* Ref.make<ReadonlyArray<unknown>>([])
        const tools = yield* binding(
          (_args, context) => context.preliminary({}).pipe(Effect.as({})),
          {
            project: () => Tool.decodeResult('echo', { isError: 'invalid' }),
          },
        )
        const registration = tools[0]
        assert.ok(registration)
        if (registration === undefined) return yield* Effect.die('Missing bound tool')
        yield* registration.execute({ n: 1 }, 'preview').pipe(
          Effect.provideService(
            ToolCall,
            ToolCall.of({
              id: 'preview',
              output: () => Effect.void,
              details: () => Effect.void,
              diagnostic: () => Effect.void,
              preliminary: () => Effect.die('invalid preview must not publish'),
            }),
          ),
          Effect.provideService(
            Invocation,
            Invocation.of({
              ...quiet,
              progress: () => Effect.die('invalid preview must not publish'),
              report: (cause) => Ref.update(reports, (old) => [...old, cause]),
            }),
          ),
        )
        const failure = (yield* Ref.get(reports))[0]
        assert.ok(failure instanceof ToolError)
        if (failure instanceof ToolError) {
          assert.strictEqual(failure.reason._tag, 'ToolInvalidResult')
          assert.ok(failure.cause instanceof Schema.SchemaError)
        }
        assert.strictEqual((yield* Ref.get(reports)).length, 1)
      }),
  )
  it.effect('intent codecs receive the selected call id and silent ToolCall capabilities', () =>
    Effect.gen(function* () {
      const seen = yield* Ref.make<ReadonlyArray<string>>([])
      const originals = yield* binding(() => Effect.die('intent preparation must not execute'))
      const inspect = Effect.gen(function* () {
        const call = yield* ToolCall
        yield* Ref.update(seen, (values) => [...values, call.id])
        yield* call.output('codec output')
        yield* call.details({ phase: 'codec' })
        yield* call.diagnostic({ kind: 'codec' })
      })
      const tools = originals.map((original): Tool.Registration => ({
        ...original,
        decode: (args) => inspect.pipe(Effect.andThen(original.decode(args))),
        encodeArgs: (args) => inspect.pipe(Effect.andThen(original.encodeArgs(args))),
      }))
      const executor = yield* runtime([{ name: 'tools', tools }])
      const agent = yield* executor.resolve(state, settings)
      const intent = yield* executor.prepareTool(agent, {
        id: 'selected-call',
        name: 'echo',
        args: { n: '7' },
      })
      assert.strictEqual(intent.id, 'selected-call')
      assert.deepStrictEqual(intent.args, { n: 7 })
      assert.deepStrictEqual(yield* Ref.get(seen), ['selected-call', 'selected-call'])
    }).pipe(
      Effect.provide(
        Layer.succeed(
          Invocation,
          Invocation.of({
            ...quiet,
            progress: () => Effect.die('intent codecs must not publish execution progress'),
          }),
        ),
      ),
    ),
  )
  it.effect(
    'prepares native Prompt, pins declaration/options/cutoff and does not execute offered tools',
    () =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0)
        const tools = yield* binding(() => Ref.update(calls, (n) => n + 1).pipe(Effect.as({})))
        const executor = yield* runtime([
          {
            name: 'tools',
            tools,
            sections: [{ key: 'rules', render: () => Effect.succeed('obey') }],
          },
        ])
        const view = ConversationContext.derive([{ id: entryId(5), messages: [user('hello')] }])
        const prepared = yield* executor.prepare({ state, settings, view, sessionId: 'affinity' })
        assert.strictEqual(prepared.request.tail, 5)
        assert.strictEqual(prepared.request.options.sessionId, 'affinity')
        assert.strictEqual(prepared.request.tools[0]?.name, 'echo')
        assert.strictEqual(prepared.request.prompt.content[0]?.role, 'system')
        assert.strictEqual(prepared.plan.patches.length, 1)
        const encoded = yield* Schema.encodeEffect(Request)(prepared.request)
        const decoded = yield* Schema.decodeEffect(Request)(encoded)
        assert.strictEqual(decoded.options.sessionId, 'affinity')
        const parts = yield* executor.generate(decoded, prepared.agent).pipe(Stream.runCollect)
        assert.strictEqual(parts.at(-1)?.type, 'finish')
        assert.strictEqual(yield* Ref.get(calls), 0)
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it.effect(
    'provider sees pinned full tool declaration despite later schema replacement and beforeRequest reruns',
    () =>
      Effect.gen(function* () {
        const hooks = yield* Ref.make(0)
        const seen = yield* Ref.make<ReadonlyArray<unknown>>([])
        const native = yield* LanguageModel.make({
          generateText: () => Effect.succeed([finish()]),
          streamText: (options) =>
            Stream.fromEffect(Ref.update(seen, (old) => [...old, options.tools])).pipe(
              Stream.flatMap(() => Stream.fromIterable([finish()])),
            ),
        })
        const original = yield* binding(({ n }) =>
          Effect.succeed({ content: [Prompt.textPart({ text: String(n) })] }),
        )
        const executor = yield* runtime(
          [
            {
              name: 'tools',
              tools: original,
              hooks: [
                {
                  operation: 'generation',
                  handlers: {
                    beforeRequest: () => Ref.update(hooks, (n) => n + 1).pipe(Effect.as(undefined)),
                  },
                },
              ],
            },
          ],
          native,
        )
        const prepared = yield* executor.prepare({
          state,
          settings,
          view: ConversationContext.empty(),
        })
        yield* executor.generate(prepared.request, prepared.agent).pipe(Stream.runDrain)
        yield* executor
          .generate(prepared.request, { ...prepared.agent, tools: [] })
          .pipe(Stream.runDrain)
        assert.strictEqual(yield* Ref.get(hooks), 2)
        assert.deepStrictEqual((yield* Ref.get(seen))[0], (yield* Ref.get(seen))[1])
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it.effect(
    'repair/coercion/hooks run before durable intent; pinned decoded JSON schema round trips',
    () =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0)
        const repairs = yield* Ref.make(0)
        const tools = yield* binding(
          ({ n }) =>
            Ref.update(calls, (x) => x + 1).pipe(
              Effect.as({ content: [Prompt.textPart({ text: String(n) })] }),
            ),
          {
            replay: 'safe',
            repair: () => Ref.update(repairs, (n) => n + 1).pipe(Effect.as({ n: '12' })),
          },
        )
        const executor = yield* runtime([{ name: 'tools', tools }])
        const agent = yield* executor.resolve(state, settings)
        const intent = yield* executor.prepareTool(agent, {
          id: 'call',
          name: 'echo',
          args: { broken: true },
        })
        assert.deepStrictEqual(intent.args, { n: 12 })
        assert.strictEqual(yield* Ref.get(calls), 0)
        const encoded = yield* Schema.encodeEffect(Tool.Intent)(intent)
        const execution = yield* executor.tool(
          yield* Schema.decodeEffect(Tool.Intent)(encoded),
          agent,
        )
        assert.strictEqual(execution.outcome, 'completed')
        assert.strictEqual(yield* Ref.get(calls), 1)
        assert.strictEqual(yield* Ref.get(repairs), 1)
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it.effect(
    'beforeTool thrown/first block stops later hooks and execution; invalid schema rejects intent',
    () =>
      Effect.gen(function* () {
        const tools = yield* binding(() => Effect.die('must not execute'))
        const executor = yield* runtime([
          {
            name: 'tools',
            tools,
            hooks: [
              {
                operation: 'tool',
                handlers: {
                  beforeTool: () =>
                    Effect.fail(new HookError({ reason: new HookFailure({ message: 'blocked' }) })),
                },
              },
              { operation: 'tool', handlers: { beforeTool: () => Effect.die('must not run') } },
            ],
          },
        ])
        const agent = yield* executor.resolve(state, settings)
        const blocked = yield* Effect.flip(
          executor.prepareTool(agent, { id: 'c', name: 'echo', args: { n: '1' } }),
        )
        assert.strictEqual(blocked.reason._tag, 'ToolBlocked')
        const invalid = yield* Effect.flip(
          executor.prepareTool(agent, { id: 'c', name: 'echo', args: { n: 'NaN' } }),
        )
        assert.strictEqual(invalid.reason._tag, 'ToolInvalidParameters')
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it.effect(
    'both-safe recovery uses new implementation/current cwd, skips repair/validation/before hooks, clears old progress',
    () =>
      Effect.gen(function* () {
        const tools = yield* binding(
          ({ n }) =>
            Effect.gen(function* () {
              const invocation = yield* Invocation
              return { details: { n, cwd: invocation.cwd } }
            }),
          {
            replay: 'safe',
            repair: () =>
              Effect.fail(
                new ToolError({
                  reason: new ToolExecution({ name: 'echo', message: 'must not repair' }),
                }),
              ),
          },
        )
        const executor = yield* runtime([
          {
            name: 'tools',
            tools,
            hooks: [
              { operation: 'tool', handlers: { beforeTool: () => Effect.die('must not run') } },
            ],
          },
        ])
        const agent = yield* executor.resolve(state, settings)
        const progress = yield* Ref.make<ReadonlyArray<unknown>>([])
        const result = yield* executor
          .tool({ id: 'c', name: 'echo', args: { n: 9 }, replay: 'safe' }, agent, {
            recovering: true,
          })
          .pipe(
            Effect.provideService(Invocation, {
              ...quiet,
              cwd: '/fresh',
              progress: (value) => Ref.update(progress, (old) => [...old, value]),
            }),
          )
        assert.strictEqual(result.outcome, 'completed')
        assert.deepStrictEqual(result.result.details, { n: 9, cwd: '/fresh' })
        assert.deepStrictEqual((yield* Ref.get(progress))[0], {
          clear: true,
          output: '',
          details: null,
          diagnostics: [],
        })
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it.effect(
    'unsafe stored/current gate and missing code settle interrupted with partial output and no hooks',
    () =>
      Effect.gen(function* () {
        for (const [stored, current] of [
          ['unsafe', 'safe'],
          ['safe', 'unsafe'],
        ] as const) {
          const tools = yield* binding(() => Effect.die('must not execute'), { replay: current })
          const executor = yield* runtime([{ name: 'tools', tools }])
          const agent = yield* executor.resolve(state, settings)
          const result = yield* executor.tool(
            { id: 'c', name: 'echo', args: { n: 1 }, replay: stored },
            agent,
            { recovering: true, previous: { content: [Prompt.textPart({ text: 'partial' })] } },
          )
          assert.strictEqual(result.outcome, 'interrupted')
          assert.strictEqual(
            result.result.content?.[0]?.type === 'text' ? result.result.content[0].text : '',
            'partial',
          )
        }
        const executor = yield* runtime()
        assert.strictEqual(
          (yield* executor.tool(
            { id: 'c', name: 'gone', args: {}, replay: 'safe' },
            yield* executor.resolve(state, settings),
            { recovering: true },
          )).outcome,
          'interrupted',
        )
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it.effect(
    'thrown tool fails outcome, afterTool runs with partial fallback; explicit isError remains completed with controls',
    () =>
      Effect.gen(function* () {
        const after = yield* Ref.make(0)
        const tools = yield* binding(() =>
          Effect.gen(function* () {
            yield* (yield* ToolCall).output('partial')
            return yield* new ToolError({
              reason: new ToolExecution({ name: 'echo', message: 'boom' }),
            })
          }),
        )
        const executor = yield* runtime([
          {
            name: 'tools',
            tools,
            hooks: [
              {
                operation: 'tool',
                handlers: {
                  afterTool: (_input, result) =>
                    Ref.update(after, (n) => n + 1).pipe(Effect.as({ ...result, details: null })),
                },
              },
            ],
          },
        ])
        const failed = yield* executor.tool(
          { id: 'c', name: 'echo', args: { n: 1 }, replay: 'unsafe' },
          yield* executor.resolve(state, settings),
        )
        assert.strictEqual(failed.outcome, 'failed')
        assert.strictEqual(failed.result.details, null)
        assert.strictEqual(
          failed.result.content?.[0]?.type === 'text' ? failed.result.content[0].text : '',
          'partial',
        )
        assert.strictEqual(yield* Ref.get(after), 1)
        const explicit = yield* binding(() =>
          Effect.succeed({ isError: true, control: { terminate: true } }),
        )
        const normal = yield* runtime([{ name: 'tools', tools: explicit }])
        const result = yield* normal.tool(
          { id: 'c', name: 'echo', args: { n: 1 }, replay: 'unsafe' },
          yield* normal.resolve(state, settings),
        )
        assert.strictEqual(result.outcome, 'completed')
        assert.strictEqual(result.result.control?.terminate, true)
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it.effect(
    'reported fallback truncation diagnostic suppressed by afterTool replacement; explicit content bounded after hooks',
    () =>
      Effect.gen(function* () {
        const tools = yield* binding(
          () =>
            Effect.gen(function* () {
              yield* (yield* ToolCall).output('123456')
              return {}
            }),
          { output: { maxBytes: 3 } },
        )
        const executor = yield* runtime([{ name: 'tools', tools }])
        const result = yield* executor.tool(
          { id: 'c', name: 'echo', args: { n: 1 }, replay: 'unsafe' },
          yield* executor.resolve(state, settings),
        )
        assert.strictEqual(result.result.diagnostics?.at(-1)?.kind, 'truncated')
        const replaced = yield* runtime([
          {
            name: 'tools',
            tools,
            hooks: [
              {
                operation: 'tool',
                handlers: {
                  afterTool: () => Effect.succeed({ content: [Prompt.textPart({ text: 'ok' })] }),
                },
              },
            ],
          },
        ])
        const output = yield* replaced.tool(
          { id: 'c', name: 'echo', args: { n: 1 }, replay: 'unsafe' },
          yield* replaced.resolve(state, settings),
        )
        assert.deepStrictEqual(output.result.diagnostics, [])
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it.effect(
    'clean summary trims text and records usage; invalid summary error preserves billed usage',
    () =>
      Effect.gen(function* () {
        const executor = yield* runtime(
          [],
          yield* model([{ type: 'text', text: ' summary ' }, finish()]),
        )
        const selected = yield* executor.prepareCompaction({
          state,
          settings: yield* Agent.settings({
            compaction: { keepRecentTokens: 1, reserveTokens: 20 },
          }),
          view: ConversationContext.derive([
            { id: entryId(1), messages: [user('old long text')] },
            { id: entryId(2), messages: [user('new')] },
          ]),
          reason: 'manual',
          sessionId: 'identity',
        })
        assert.strictEqual(selected.type, 'request')
        if (selected.type !== 'request') return
        assert.strictEqual(selected.request.request.options.maxTokens, 16)
        assert.strictEqual(selected.request.request.options.cache, 'none')
        const encoded = yield* Schema.encodeEffect(SummaryRequest)(selected.request)
        assert.strictEqual(
          (yield* executor.compact(yield* Schema.decodeEffect(SummaryRequest)(encoded))).summary,
          'summary',
        )
        const invalid = yield* runtime(
          [],
          yield* model([{ type: 'text', text: 'partial' }, finish('length')]),
        )
        const failure = yield* Effect.flip(invalid.compact(selected.request))
        assert.strictEqual(failure._tag, 'ModelError')
        if (failure._tag === 'ModelError') assert.strictEqual(failure.usage?.totalTokens, 15)
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it.effect(
    'compaction no model fails before no-cut; first hook summary bypasses provider and pins head',
    () =>
      Effect.gen(function* () {
        const executor = yield* runtime([
          {
            name: 'hook',
            hooks: [
              {
                operation: 'compaction',
                handlers: { beforeCompact: () => Effect.succeed({ summary: '' }) },
              },
            ],
          },
        ])
        assert.strictEqual(
          Exit.isFailure(
            yield* Effect.exit(
              executor.prepareCompaction({
                state: {},
                settings,
                view: ConversationContext.empty(),
                reason: 'manual',
              }),
            ),
          ),
          true,
        )
        const selected = yield* executor.prepareCompaction({
          state,
          settings: yield* Agent.settings({ compaction: { keepRecentTokens: 1 } }),
          view: ConversationContext.derive([
            { id: entryId(1), messages: [user('old')] },
            { id: entryId(2), messages: [user('new')] },
          ]),
          reason: 'manual',
        })
        assert.deepStrictEqual(selected, { type: 'summary', summary: '', firstKept: entryId(2) })
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it.effect('unsupported options and missing catalog model produce typed capability errors', () =>
    Effect.gen(function* () {
      const failure = yield* Effect.flip(Model.noOptions({ thinking: 'high', options: {} }))
      assert.strictEqual(failure.reason._tag, 'ModelUnsupported')
      const catalog = yield* Model.Catalog.pipe(Effect.provide(Model.layer([])))
      assert.strictEqual((yield* Effect.flip(catalog.resolve(ref))).reason._tag, 'ModelNoModel')
    }),
  )
  it.effect(
    'bound native handler services survive their Layer build while Invocation/ToolCall stay request local',
    () =>
      Effect.gen(function* () {
        class Client extends Context.Service<Client, string>()('test/harness/Client') {}
        const nativeTool = AiTool.make('capture', {
          parameters: Schema.Struct({}),
          success: Schema.String,
        })
          .addDependency(Client)
          .addDependency(Invocation)
        const toolkit = Toolkit.make(nativeTool)
        const tools = yield* Tool.bind(toolkit).pipe(
          Effect.provide(
            Layer.mergeAll(
              toolkit.toLayer({
                capture: () =>
                  Effect.gen(function* () {
                    return `${yield* Client}:${(yield* Invocation).cwd}`
                  }),
              }),
              Layer.succeed(Client, 'client'),
            ),
          ),
        )
        const executor = yield* runtime([{ name: 'capture', tools }])
        const agent = yield* executor.resolve(state, settings)
        const intent = yield* executor.prepareTool(agent, {
          id: 'capture',
          name: 'capture',
          args: {},
        })
        const outcome = yield* executor
          .tool(intent, agent)
          .pipe(Effect.provideService(Invocation, { ...quiet, cwd: '/request' }))
        assert.strictEqual(
          outcome.result.content?.[0]?.type === 'text' ? outcome.result.content[0].text : '',
          'client:/request',
        )
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it.effect(
    'beforeTool replacement is validated as decoded native type and native codec encoded before intent persistence',
    () =>
      Effect.gen(function* () {
        const tools = yield* binding(({ n }) => Effect.succeed({ details: n }))
        const executor = yield* runtime([
          {
            name: 'tools',
            tools,
            hooks: [
              {
                operation: 'tool',
                handlers: { beforeTool: () => Effect.succeed({ args: { n: 13 } }) },
              },
            ],
          },
        ])
        const intent = yield* executor.prepareTool(yield* executor.resolve(state, settings), {
          id: 'c',
          name: 'echo',
          args: { n: '1' },
        })
        assert.deepStrictEqual(intent.args, { n: 13 })
        assert.deepStrictEqual(intent.encodedArgs, { n: '13' })
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it.effect(
    'tool capabilities reject use after settlement and advertise bounded tail output windows',
    () =>
      Effect.gen(function* () {
        const saved = yield* Ref.make<ToolCall['Service'] | undefined>(undefined)
        const tools = yield* binding(
          () =>
            Effect.gen(function* () {
              const api = yield* ToolCall
              yield* Ref.set(saved, api)
              assert.strictEqual(api.outputWindow?.maxBytes, 3)
              yield* api.output('new', { bytes: 100, newlines: 2, endsWithNewline: true })
              return {}
            }),
          { output: { maxBytes: 3, retain: 'tail' } },
        )
        const executor = yield* runtime([{ name: 'tools', tools }])
        const result = yield* executor.tool(
          { id: 'c', name: 'echo', args: { n: 1 }, replay: 'unsafe' },
          yield* executor.resolve(state, settings),
        )
        assert.strictEqual(
          result.result.content?.[0]?.type === 'text' ? result.result.content[0].text : '',
          'new',
        )
        const api = yield* Ref.get(saved)
        if (api === undefined) return yield* Effect.die('Tool capabilities were not supplied')
        assert.strictEqual(Exit.isFailure(yield* Effect.exit(api.output('late'))), true)
        assert.strictEqual(Exit.isFailure(yield* Effect.exit(api.details(null))), true)
        assert.strictEqual(
          Exit.isFailure(yield* Effect.exit(api.diagnostic({ kind: 'late' }))),
          true,
        )
      }).pipe(Effect.provideService(Invocation, quiet)),
  )

  it.effect(
    'classifies terminal native responses, preserves usage and codec, provider calls stay remote',
    () =>
      Effect.gen(function* () {
        const hooks = yield* Ref.make(0)
        const executor = yield* runtime([
          {
            name: 'hooks',
            hooks: [
              {
                operation: 'generation',
                handlers: { afterResponse: () => Ref.update(hooks, (n) => n + 1) },
              },
            ],
          },
        ])
        const prepared = yield* executor.prepare({
          state,
          settings,
          view: ConversationContext.empty(),
        })
        const call = Response.toolCallPart({
          id: 'c',
          name: 'echo',
          params: {},
          providerExecuted: false,
        })
        const remote = Response.toolCallPart({
          id: 'r',
          name: 'remote',
          params: {},
          providerExecuted: true,
        })
        const terminal = (reason: Response.FinishReason) =>
          Response.makePart('finish', { reason, usage: nativeUsage })
        const tools = yield* executor.classifyResponse(prepared.request, prepared.agent, [
          call,
          remote,
          terminal('tool-calls'),
        ])
        assert.strictEqual(tools.type, 'tools')
        if (tools.type !== 'tools') return yield* Effect.die('Expected tools')
        assert.strictEqual(tools.calls.length, 1)
        assert.strictEqual(tools.usage.totalTokens, 15)
        assert.strictEqual(
          (yield* Schema.decodeEffect(Disposition)(yield* Schema.encodeEffect(Disposition)(tools)))
            .type,
          'tools',
        )
        assert.strictEqual(
          (yield* executor.classifyResponse(prepared.request, prepared.agent, [
            remote,
            terminal('tool-calls'),
          ])).type,
          'answer',
        )
        assert.strictEqual(
          (yield* executor.classifyResponse(prepared.request, prepared.agent, [terminal('length')]))
            .type,
          'answer',
        )
        const failure = yield* executor.classifyResponse(prepared.request, prepared.agent, [
          Response.makePart('error', { error: 'prompt too long' }),
          terminal('error'),
        ])
        assert.strictEqual(failure.type, 'failure')
        if (failure.type === 'failure') {
          assert.strictEqual(failure.overflow, true)
          assert.strictEqual(failure.retryable, false)
        }
        assert.strictEqual(yield* Ref.get(hooks), 4)
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it.effect(
    'response classification preserves typed reasons instead of retrying diagnostic text',
    () =>
      Effect.gen(function* () {
        const executor = yield* runtime()
        const prepared = yield* executor.prepare({
          state,
          settings,
          view: ConversationContext.empty(),
        })
        const errors = [
          new AiError.AiError({
            module: 'provider',
            method: 'response',
            reason: new AiError.AuthenticationError({
              kind: 'InvalidKey',
              description: '503 overloaded rate limit; please retry your request',
            }),
          }),
          new AiError.AiError({
            module: 'provider',
            method: 'response',
            reason: new AiError.RateLimitError({
              metadata: { provider: { diagnostic: 'billing context_length_exceeded' } },
            }),
          }),
        ]
        for (const error of errors) {
          const disposition = yield* executor.classifyResponse(prepared.request, prepared.agent, [
            Response.makePart('error', { error }),
            Response.makePart('finish', { reason: 'error', usage: nativeUsage }),
          ])
          assert.strictEqual(disposition.type, 'failure')
          if (disposition.type !== 'failure') return yield* Effect.die('Expected failure')
          assert.strictEqual(disposition.retryable, error.isRetryable)
          assert.strictEqual(disposition.overflow, false)
          assert.deepStrictEqual(yield* executor.classifyFailure(prepared.request, error), {
            retryable: error.isRetryable,
            overflow: false,
          })
        }
        const mixed = yield* executor.classifyResponse(
          prepared.request,
          prepared.agent,
          errors.map((error) => Response.makePart('error', { error })),
        )
        if (mixed.type !== 'failure') return yield* Effect.die('Expected mixed failure')
        assert.strictEqual(mixed.retryable, false)
      }).pipe(Effect.provideService(Invocation, quiet)),
  )

  it.effect(
    'deferred adapters pin options, skip response hooks until terminal, fetch and cancel without a scheduler',
    () =>
      Effect.gen(function* () {
        const hooks = yield* Ref.make(0)
        const seen = yield* Ref.make<ReadonlyArray<unknown>>([])
        const capability: Model.DeferredCapability = {
          inspect: (parts) =>
            parts.some((part) => part.type === 'finish')
              ? undefined
              : { handle: { request: 'provider-request' }, pollAfterMs: Duration.millis(-10) },
          fetch: (handle, options) =>
            Stream.fromEffect(Ref.update(seen, (values) => [...values, { handle, options }])).pipe(
              Stream.flatMap(() => Stream.empty),
            ),
          cancel: (handle, options) =>
            Ref.update(seen, (values) => [...values, { cancelled: handle, options }]),
        }
        const executor = yield* runtime(
          [
            {
              name: 'hooks',
              hooks: [
                {
                  operation: 'generation',
                  handlers: { afterResponse: () => Ref.update(hooks, (n) => n + 1) },
                },
              ],
            },
          ],
          undefined,
          capability,
        )
        const prepared = yield* executor.prepare({
          state,
          settings,
          view: ConversationContext.empty(),
          sessionId: 'affinity',
        })
        const disposition = yield* executor.classifyResponse(prepared.request, prepared.agent, [])
        assert.strictEqual(disposition.type, 'deferred')
        assert.strictEqual(yield* Ref.get(hooks), 0)
        if (disposition.type !== 'deferred') return yield* Effect.die('Expected deferred')
        yield* executor
          .fetchDeferred(prepared.request, disposition.decision.handle)
          .pipe(Stream.runDrain)
        yield* executor.cancelDeferred(prepared.request, disposition.decision.handle)
        assert.strictEqual((yield* Ref.get(seen)).length, 2)
        assert.deepStrictEqual((yield* Ref.get(seen))[0], {
          handle: { request: 'provider-request' },
          options: prepared.request.options,
        })
        const unsupported = yield* runtime()
        assert.strictEqual(
          (yield* Effect.flip(unsupported.cancelDeferred(prepared.request, null))).reason._tag,
          'ModelUnsupported',
        )
        assert.strictEqual(
          (yield* Effect.flip(
            unsupported.fetchDeferred(prepared.request, null).pipe(Stream.runDrain),
          )).reason._tag,
          'ModelUnsupported',
        )
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it('classifies transient versus quota/context errors and preserves strictly increasing deferred deadlines', () => {
    assert.deepStrictEqual(Model.classify('Rate limit: prompt too long'), {
      retryable: true,
      overflow: false,
    })
    assert.deepStrictEqual(Model.classify('429 insufficient_quota'), {
      retryable: false,
      overflow: false,
    })
    assert.deepStrictEqual(Model.classify('413 (no body)', 'cerebras'), {
      retryable: false,
      overflow: true,
    })
    assert.strictEqual(Model.classify('stream ended without a terminal event').retryable, true)
    assert.strictEqual(Model.classify('unclassified diagnostic').retryable, false)
    const sdk = new AiError.AiError({
      module: 'SDK',
      method: 'request',
      reason: new AiError.InvalidRequestError({ description: 'prompt too long' }),
    })
    assert.deepStrictEqual(Model.classify(sdk), { retryable: false, overflow: true })
    const unknown = new AiError.AiError({
      module: 'SDK',
      method: 'stream',
      reason: new AiError.UnknownError({ description: 'stream ended before message_stop' }),
    })
    assert.strictEqual(Model.providerError(unknown).reason._tag, 'InternalProviderError')
    assert.strictEqual(
      DateTime.toEpochMillis(
        Model.pollAt(Time.fromEpochMillis(100), undefined, Duration.millis(-10)),
      ),
      90,
    )
    assert.strictEqual(
      DateTime.toEpochMillis(
        Model.pollAt(Time.fromEpochMillis(100), Time.fromEpochMillis(200), Duration.millis(-10)),
      ),
      201,
    )
  })
  it.effect(
    'native preliminary results replace previews, share progress pacing and remain final fallback',
    () =>
      Effect.gen(function* () {
        const tools = yield* binding(
          (_args, context) =>
            Effect.gen(function* () {
              yield* context.preliminary({
                content: [Prompt.textPart({ text: '123456' })],
                details: { stage: 1 },
              })
              yield* context.preliminary({
                content: [Prompt.textPart({ text: 'new' })],
                details: { stage: 2 },
              })
              return {}
            }),
          { output: { maxBytes: 3, retain: 'tail' } },
        )
        const executor = yield* runtime([{ name: 'tools', tools }])
        const result = yield* executor.tool(
          { id: 'c', name: 'echo', args: { n: 1 }, replay: 'unsafe' },
          yield* executor.resolve(state, settings),
        )
        assert.strictEqual(
          result.result.content?.[0]?.type === 'text' ? result.result.content[0].text : '',
          'new',
        )
        assert.deepStrictEqual(result.result.details, { stage: 2 })
        assert.strictEqual(result.outcome, 'completed')
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
})
