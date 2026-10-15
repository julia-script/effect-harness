import { assert, describe, it } from '@effect/vitest'
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Hook from '../src/Hook.js'
import * as Registry from '../src/Registry.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import * as StorageFormat from '../src/StorageFormat.js'
import { StorageError } from '../src/StorageError.js'
import { SessionError } from '../src/SessionError.js'
import * as Task from '../src/Task.js'
import { TaskExecution } from '../src/TaskExecution.js'
import * as Tool from '../src/Tool.js'
import { ToolExecution } from '../src/ToolExecution.js'
import * as ToolProgress from '../src/ToolProgress.js'
import * as Round from '../src/internal/ToolRound.js'
import * as Invocation from '../src/ToolInvocation.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Usage from '../src/Usage.js'
import * as Nested from '../src/internal/NestedInvocation.js'

const contract = {
  kind: 'nested.accounting',
  version: 1,
  input: Schema.Void,
  checkpoint: Schema.Struct({ phase: Schema.tag('execute') }),
  success: Schema.String,
  failure: Schema.String,
} as const
const unavailableModel = LanguageModel.make({
  generateText: () => Effect.die('No provider request is allowed'),
  streamText: () => Stream.die('No provider request is allowed'),
})

describe('nested result and no-entry accounting', () => {
  for (const mode of ['storage', 'session', 'hook'] as const)
    it.live(`native ${mode} mixed critical causes remain infrastructure failures`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const critical =
            mode === 'session'
              ? new SessionError({
                  reason: 'invalid',
                  operation: 'critical-probe',
                  message: 'critical typed branch',
                })
              : new StorageError({
                  reason: 'io',
                  operation: 'critical-probe',
                  message: 'critical typed branch',
                })
          const mixed = Cause.combine(Cause.fail(critical), Cause.die('critical defect branch'))
          const leaf = Tool.make('critical', { success: Schema.String })
          const toolkit = Toolkit.make(leaf)
          const runtime = yield* HarnessRuntime.make({
            tools: toolkit,
            hooks: [
              Hook.make({
                event: 'beforeTool',
                execute: () => (mode === 'hook' ? Effect.failCause(mixed) : Effect.void),
              }),
            ],
          }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, yield* unavailableModel),
            Effect.provide(toolkit.toLayer({ critical: () => Effect.failCause(mixed) })),
          )
          const criticalContract = { ...contract, kind: `nested.critical-${mode}` }
          const program = Task.make({
            ...criticalContract,
            initial: () => ({ phase: 'execute' as const }),
            phases: {
              execute: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  yield* Invocation.invoke(access, leaf, {}, { key: 'critical' })
                  return yield* Effect.die(
                    'Critical infrastructure cannot become a successful receipt',
                  )
                }),
            },
          })
          yield* runtime.registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'critical-program', version: 1 }, Toolkit.make()),
              program,
            ),
          )
          const root = yield* runtime.backend.root
          const parent = yield* runtime.scheduler.start(program, undefined, {
            conversationId: root,
          })
          const outcome = yield* runtime.scheduler.wait(parent).pipe(Effect.result)
          assert.isTrue(Result.isFailure(outcome))
          if (Result.isFailure(outcome)) {
            assert.include(outcome.failure.message, 'critical typed branch')
            assert.include(outcome.failure.message, 'critical defect branch')
          }
          const health = yield* runtime.scheduler.failure
          assert.isTrue(Option.isSome(health))
          if (Option.isSome(health)) {
            assert.include(health.value.message, 'critical typed branch')
            assert.include(health.value.message, 'critical defect branch')
            assert.isTrue(Cause.isCause(health.value.cause))
          }
          assert.lengthOf(
            yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root)),
            0,
          )
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
    )
  it.live(
    'all native fallback shapes stay distinct from file-shaped structured JSON and controls stay inert',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const url = new URL('https://example.test/image.png')
          const shapes = {
            empty: [],
            text: [Prompt.textPart({ text: 'single' })],
            string: [Prompt.filePart({ mediaType: 'image/png', data: 'encoded' })],
            url: [Prompt.filePart({ mediaType: 'image/png', data: url })],
            mixed: [
              Prompt.textPart({ text: 'first' }),
              Prompt.filePart({
                mediaType: 'application/octet-stream',
                data: new Uint8Array([4, 5]),
              }),
            ],
          } as const
          const fallback = Tool.makeResult('shapes', {
            parameters: Schema.Struct({ mode: Schema.String }),
          })
          const structured = Tool.makeResult('json-file', {
            success: Schema.Json,
            resultValidation: 'strict',
          })
          const toolkit = Toolkit.make(fallback, structured)
          const fileJson = {
            type: 'file',
            mediaType: 'image/png',
            data: 'ordinary JSON',
            extra: true,
          }
          const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, yield* unavailableModel),
            Effect.provide(
              toolkit.toLayer({
                shapes: ({ mode }) =>
                  Effect.sync(() => {
                    if (!Object.hasOwn(shapes, mode)) throw new Error('Unknown test shape')
                    const content = shapes[mode as keyof typeof shapes]
                    return {
                      content,
                      control: {
                        terminate: true as const,
                        handoff: 'next',
                        addTools: ['json-file'],
                      },
                      details: { mode },
                    }
                  }),
                'json-file': () => Effect.succeed({ content: [], structuredOutput: fileJson }),
              }),
            ),
          )
          const shapeContract = { ...contract, kind: 'nested.fallback-shapes' }
          const program = Task.make({
            ...shapeContract,
            initial: () => ({ phase: 'execute' as const }),
            phases: {
              execute: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  const before = yield* access.agent
                  for (const mode of ['empty', 'text', 'string', 'url', 'mixed'] as const) {
                    const receipt = yield* Invocation.invoke(
                      access,
                      fallback,
                      { mode },
                      { key: mode },
                    )
                    assert.strictEqual(receipt.status, 'succeeded')
                    assert.strictEqual(receipt.valueKind, 'content')
                    if (receipt.valueKind === 'content') {
                      let expected: unknown = shapes[mode]
                      if (mode === 'empty') expected = ''
                      else if (mode === 'text') expected = 'single'
                      else if (mode !== 'mixed') expected = shapes[mode][0]
                      assert.deepStrictEqual(
                        receipt.value,
                        expected,
                        `shape=${mode};actual=${JSON.stringify(receipt.value)};expected=${JSON.stringify(expected)}`,
                      )
                      if (mode === 'url') {
                        assert.isTrue(
                          typeof receipt.value === 'object' &&
                            receipt.value !== null &&
                            !Array.isArray(receipt.value) &&
                            'data' in receipt.value &&
                            receipt.value.data instanceof URL &&
                            receipt.value.data.href === url.href,
                        )
                      }
                    }
                    assert.notProperty(receipt, 'control')
                    assert.notProperty(receipt, 'spend')
                  }
                  const json = yield* Invocation.invoke(access, structured, {}, { key: 'json' })
                  assert.deepInclude(json, { status: 'succeeded', valueKind: 'decoded' })
                  if (json.valueKind === 'decoded' && 'value' in json)
                    assert.deepStrictEqual(json.value, fileJson)
                  assert.deepStrictEqual(yield* access.agent, before)
                  yield* access.commit((tx) =>
                    access.progress(tx, shapeContract, {
                      status: 'completing',
                      outcome: { status: 'completed', result: 'done' },
                    }),
                  )
                }),
            },
          })
          yield* runtime.registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'shapes-program', version: 1 }, Toolkit.make()),
              program,
            ),
          )
          const root = yield* runtime.backend.root
          const parent = yield* runtime.scheduler.start(program, undefined, {
            conversationId: root,
          })
          assert.deepStrictEqual(yield* runtime.scheduler.wait(parent), {
            status: 'completed',
            result: 'done',
          })
          assert.lengthOf(
            yield* Session.scanEntries(runtime.session, { conversationId: root }).pipe(
              Stream.runCollect,
            ),
            0,
          )
          assert.lengthOf(
            yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root)),
            6,
          )
          assert.lengthOf((yield* Session.usage(runtime.session, root)).costs, 0)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
  it.live(
    'final-only flood remains bounded and UTF8 counters survive report overflow without running commits',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const leaf = Tool.makeResult('flood', {
            success: Schema.String,
            resultValidation: 'strict',
            outputRetention: { retain: 'tail', maxBytes: 256, maxLines: 4 },
          })
          const toolkit = Toolkit.make(leaf)
          let childId: import('../src/Record.js').TaskId | undefined
          const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, yield* unavailableModel),
            Effect.provide(
              toolkit.toLayer({
                flood: () =>
                  Effect.gen(function* () {
                    const access = yield* ToolExecution
                    childId = access.taskId
                    for (let index = 0; index < 1000; index++) yield* access.output('😀\n')
                    yield* access.details({ local: 'last' })
                    const rejected = yield* access
                      .details({ oversized: 'x'.repeat(512) })
                      .pipe(Effect.result)
                    assert.isTrue(Result.isFailure(rejected))
                    if (Result.isFailure(rejected))
                      assert.strictEqual(rejected.failure._tag, 'ExecutionError')
                    yield* access.diagnostic({ severity: 'info', message: 'bounded' })
                    return { content: [], structuredOutput: 'done' }
                  }),
              }),
            ),
          )
          const root = yield* runtime.backend.root
          const floodContract = { ...contract, kind: 'nested.flood' }
          const program = Task.make({
            ...floodContract,
            initial: () => ({ phase: 'execute' as const }),
            phases: {
              execute: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  assert.deepInclude(
                    yield* Invocation.invoke(access, leaf, {}, { key: 'flood', progress: false }),
                    { status: 'succeeded', value: 'done' },
                  )
                  yield* access.commit((tx) =>
                    access.progress(tx, floodContract, {
                      status: 'completing',
                      outcome: { status: 'completed', result: 'done' },
                    }),
                  )
                }),
            },
          })
          yield* runtime.registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'program', version: 1 }, Toolkit.make()),
              program,
            ),
          )
          const parent = yield* runtime.scheduler.start(program, undefined, {
            conversationId: root,
          })
          assert.deepStrictEqual(yield* runtime.scheduler.wait(parent), {
            status: 'completed',
            result: 'done',
          })
          const progress = Option.getOrThrow(
            yield* Session.snapshot(
              runtime.session,
              ToolProgress.document,
              Round.target(root, Round.progressKey(childId!)),
            ),
          ).value
          assert.strictEqual(progress.status, 'completed')
          assert.strictEqual(progress.output.totalBytes, 5000)
          assert.strictEqual(progress.output.totalLines, 1000)
          assert.isAtMost(new TextEncoder().encode(progress.output.text).length, 256)
          assert.notInclude(progress.output.text, '\uFFFD')
          assert.deepStrictEqual(progress.details, { local: 'last' })
          assert.lengthOf(progress.diagnostics, 1)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  for (const mode of ['replace', 'block', 'invalid-arguments', 'invalid-output'] as const)
    it.live(`native hook ${mode} keeps final values and observed spend honest`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          let runs = 0
          const leaf = Tool.makeResult('hooked', {
            parameters: Schema.Struct({ text: Schema.String }),
            success: Schema.String,
            resultValidation: 'strict',
            replay: 'safe',
          })
          const toolkit = Toolkit.make(leaf)
          const runtime = yield* HarnessRuntime.make({
            tools: toolkit,
            hooks: [
              Hook.make({
                event: 'beforeTool',
                execute: ({ call }) => {
                  if (mode === 'block')
                    return Effect.succeed({ _tag: 'block', message: 'blocked by hook' } as const)
                  if (mode === 'invalid-arguments')
                    return Effect.succeed({
                      _tag: 'replaceArguments',
                      arguments: { text: 1 },
                    } as const)
                  const text = call.arguments.text
                  if (typeof text !== 'string')
                    return Effect.die('Expected the preceding accepted string')
                  return Effect.succeed({
                    _tag: 'replaceArguments',
                    arguments: { text: `${text}:first` },
                  } as const)
                },
              }),
              Hook.make({
                event: 'beforeTool',
                execute: ({ call }) => {
                  if (mode !== 'replace') return Effect.void
                  const text = call.arguments.text
                  if (typeof text !== 'string')
                    return Effect.die('Expected the preceding accepted string')
                  return Effect.succeed({
                    _tag: 'replaceArguments',
                    arguments: { text: `${text}:second` },
                  } as const)
                },
              }),
              Hook.make({
                event: 'afterTool',
                execute: ({ result }) => {
                  if (mode === 'invalid-output')
                    return Effect.succeed({ ...result, structuredOutput: 123 })
                  if (mode === 'replace')
                    return Effect.succeed({
                      ...result,
                      structuredOutput: 'redacted',
                      details: { final: true },
                      control: { terminate: true },
                    })
                  return Effect.void
                },
              }),
            ],
          }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, yield* unavailableModel),
            Effect.provide(
              toolkit.toLayer({
                hooked: ({ text }) =>
                  Effect.sync(() => {
                    runs++
                    if (mode === 'replace') assert.strictEqual(text, 'original:first:second')
                    return {
                      content: [],
                      structuredOutput: text,
                      details: { stale: true },
                      spend: { amount: 5, currency: 'USD', source: 'observed hook' },
                    }
                  }),
              }),
            ),
          )
          const root = yield* runtime.backend.root
          const program = Task.make({
            ...contract,
            initial: () => ({ phase: 'execute' as const }),
            phases: {
              execute: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  const receipt = yield* Invocation.invoke(
                    access,
                    leaf,
                    { text: 'original' },
                    { key: 'hook', progress: false },
                  )
                  let status = 'faulted'
                  if (mode === 'replace') status = 'succeeded'
                  if (mode === 'block') status = 'denied'
                  assert.strictEqual(receipt.status, status)
                  if (mode === 'replace') {
                    assert.deepStrictEqual(receipt.details, { final: true })
                    assert.deepInclude(receipt, { value: 'redacted' })
                    assert.notProperty(receipt, 'control')
                  } else assert.strictEqual(receipt.valueKind, 'unavailable')
                  yield* access.commit((tx) =>
                    access.progress(tx, contract, {
                      status: 'completing',
                      outcome: { status: 'completed', result: 'done' },
                    }),
                  )
                }),
            },
          })
          yield* runtime.registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'program', version: 1 }, Toolkit.make()),
              program,
            ),
          )
          const parent = yield* runtime.scheduler.start(program, undefined, {
            conversationId: root,
          })
          assert.deepStrictEqual(yield* runtime.scheduler.wait(parent), {
            status: 'completed',
            result: 'done',
          })
          assert.strictEqual(runs, mode === 'replace' || mode === 'invalid-output' ? 1 : 0)
          const receipts = yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root))
          assert.lengthOf(receipts, 1)
          const usage = receipts[0]!.usage
          if (mode === 'replace' || mode === 'invalid-output') {
            assert.strictEqual(usage?._tag, 'tool')
            if (usage?._tag === 'tool') assert.strictEqual(usage.cost?.amount, 5)
          } else assert.isUndefined(usage)
          assert.isTrue(Option.isNone(yield* runtime.scheduler.failure))
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
    )

  it.live(
    'all caller policies and model-only versus all-call filters use actual task permissions',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const defaults = Tool.make('default', { success: Schema.String })
          const both = Tool.make('both', { success: Schema.String, callers: ['tools', 'model'] })
          const modelOnly = Tool.make('model', { success: Schema.String, callers: ['model'] })
          const toolsOnly = Tool.make('tools', { success: Schema.String, callers: ['tools'] })
          const none = Tool.make('none', { success: Schema.String, callers: [] })
          const toolkit = Toolkit.make(defaults, both, modelOnly, toolsOnly, none)
          let allowed = 0
          let forbidden = 0
          const handler = () =>
            Effect.sync(() => {
              allowed++
              return 'ok'
            })
          const never = () =>
            Effect.sync(() => {
              forbidden++
              return 'forbidden'
            })
          const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, yield* unavailableModel),
            Effect.provide(
              toolkit.toLayer({
                default: handler,
                both: handler,
                tools: handler,
                model: never,
                none: never,
              }),
            ),
          )
          const program = Task.make({
            ...contract,
            initial: () => ({ phase: 'execute' as const }),
            phases: {
              execute: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  const agent = yield* access.agent
                  for (const declaration of [defaults, both, toolsOnly, modelOnly, none]) {
                    const receipt = yield* access.invocation.invokeTool(
                      declaration.name,
                      {},
                      { key: declaration.name, progress: false },
                    )
                    const accepted =
                      declaration !== modelOnly &&
                      declaration !== none &&
                      (agent.tools === undefined || agent.tools.includes(declaration.name))
                    assert.strictEqual(receipt.status, accepted ? 'succeeded' : 'denied')
                  }
                  yield* access.commit((tx) =>
                    access.progress(tx, contract, {
                      status: 'completing',
                      outcome: { status: 'completed', result: 'done' },
                    }),
                  )
                }),
            },
          })
          yield* runtime.registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'program', version: 1 }, Toolkit.make()),
              program,
            ),
          )
          for (const agent of [
            { modelTools: { mode: 'explicit' as const, names: [] } },
            { tools: [] },
            { tools: ['tools'], modelTools: { mode: 'explicit' as const, names: [] } },
          ]) {
            const root = yield* runtime.backend.create({ agent })
            const parent = yield* runtime.scheduler.start(program, undefined, {
              conversationId: root,
            })
            assert.deepStrictEqual(yield* runtime.scheduler.wait(parent), {
              status: 'completed',
              result: 'done',
            })
          }
          assert.strictEqual(allowed, 4)
          assert.strictEqual(forbidden, 0)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.live(
    'denials run no codecs, handlers, child facts or format activation; callback failures remain real',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let codecs = 0
          let handlers = 0
          class Label extends Context.Service<Label, string>()('nested/denial/Label') {}
          const string = Schema.String.pipe(
            Schema.middlewareEncoding((effect) =>
              Effect.flatMap(Label, () =>
                Effect.sync(() => {
                  codecs++
                }).pipe(Effect.andThen(effect)),
              ),
            ),
            Schema.middlewareDecoding((effect) =>
              Effect.flatMap(Label, () =>
                Effect.sync(() => {
                  codecs++
                }).pipe(Effect.andThen(effect)),
              ),
            ),
          )
          const declaration = Tool.make('forbidden', {
            parameters: Schema.Struct({ value: string }),
            success: string,
            callers: ['model'],
          })
          const toolkit = Toolkit.make(declaration)
          const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, yield* unavailableModel),
            Effect.provide(
              toolkit.toLayer({
                forbidden: () =>
                  Effect.sync(() => {
                    handlers++
                    return 'wrong'
                  }),
              }),
            ),
            Effect.provideService(Label, 'owner'),
          )
          const root = yield* runtime.backend.root
          const before = yield* StorageFormat.inspect
          let deniedCallbacks = 0
          const program = Task.make({
            ...contract,
            initial: () => ({ phase: 'execute' as const }),
            phases: {
              execute: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  const denied = yield* Invocation.invoke(
                    access,
                    declaration,
                    { value: 'input' },
                    { key: 'denied' },
                  ).pipe(Effect.provideService(Label, 'consumer'))
                  assert.deepInclude(denied, { status: 'denied', valueKind: 'unavailable' })
                  assert.strictEqual(codecs, 0)
                  for (const binding of [
                    { id: 'missing', version: 1 },
                    { id: 'native-runtime-tools', version: 999 },
                  ]) {
                    const receipt = yield* Invocation.invoke(
                      access,
                      declaration,
                      { value: 'input' },
                      { key: 'binding', binding },
                    ).pipe(Effect.provideService(Label, 'consumer'))
                    assert.strictEqual(receipt.status, 'denied')
                  }
                  const chosen = yield* access.invocation.withTool(
                    declaration,
                    () => Effect.die('use must not run'),
                    {
                      onDenied: () =>
                        Effect.sync(() => {
                          deniedCallbacks++
                          return 'fallback'
                        }),
                    },
                  )
                  assert.strictEqual(chosen, 'fallback')
                  assert.strictEqual(
                    yield* access.invocation
                      .withTool(declaration, () => Effect.succeed('bad'), {
                        onDenied: () => Effect.fail('denial callback failed'),
                      })
                      .pipe(Effect.flip),
                    'denial callback failed',
                  )
                  const failure = yield* access.invocation
                    .withTool(declaration, () => Effect.succeed('bad'))
                    .pipe(Effect.flip)
                  assert.deepInclude(failure, { _tag: 'InvocationError', reason: 'denied' })
                  assert.isTrue(
                    Option.isNone(
                      yield* access.snapshot(Nested.calls, Nested.target(access.taskId)),
                    ),
                  )
                  yield* access.commit((tx) =>
                    access.progress(tx, contract, {
                      status: 'completing',
                      outcome: { status: 'completed', result: 'denied' },
                    }),
                  )
                }),
            },
          })
          yield* runtime.registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'accounting', version: 1 }, Toolkit.make()),
              program,
            ),
          )
          const ref = yield* runtime.scheduler.start(program, undefined, { conversationId: root })
          assert.deepStrictEqual(yield* runtime.scheduler.wait(ref), {
            status: 'completed',
            result: 'denied',
          })
          assert.deepStrictEqual(yield* StorageFormat.inspect, before)
          assert.strictEqual(codecs, 0)
          assert.strictEqual(handlers, 0)
          assert.strictEqual(deniedCallbacks, 1)
          assert.lengthOf(
            yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root)),
            0,
          )
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.live(
    'ordinary transformed values, optional/null, native media fallback, declared failure and compound defects preserve channels',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const ordinary = Tool.make('ordinary', {
            parameters: Schema.Struct({ value: Schema.FiniteFromString }),
            success: Schema.FiniteFromString,
          })
          const optional = Tool.makeResult('optional', { success: Schema.String })
          const nullable = Tool.makeResult('nullable', {
            success: Schema.Null,
            resultValidation: 'strict',
          })
          const fallback = Tool.makeResult('fallback')
          const failed = Tool.makeResult('failed', {
            success: Schema.String,
            failure: Schema.String,
            resultValidation: 'strict',
          })
          const fatal = Tool.make('fatal', { success: Schema.String, failure: Schema.String })
          const toolkit = Toolkit.make(ordinary, optional, nullable, fallback, failed, fatal)
          const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, yield* unavailableModel),
            Effect.provide(
              toolkit.toLayer({
                ordinary: ({ value }) => Effect.succeed(value + 1),
                optional: () => Effect.succeed({ content: [], details: { ui: 'optional' } }),
                nullable: () => Effect.succeed({ content: [], structuredOutput: null }),
                fallback: () =>
                  Effect.succeed({
                    content: [
                      Prompt.filePart({ mediaType: 'image/png', data: new Uint8Array([1, 2, 3]) }),
                    ],
                    details: { ui: 'media' },
                    control: { terminate: true },
                  }),
                failed: () =>
                  Effect.fail({
                    content: [Prompt.textPart({ text: 'visible failure' })],
                    structuredOutput: 'domain',
                    spend: { amount: 3, currency: 'USD', source: 'declared' },
                  }),
                fatal: () =>
                  Effect.failCause(
                    Cause.combine(Cause.fail('domain branch'), Cause.die('defect branch')),
                  ),
              }),
            ),
          )
          const root = yield* runtime.backend.root
          const program = Task.make({
            ...contract,
            initial: () => ({ phase: 'execute' as const }),
            phases: {
              execute: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  const legacy = yield* Invocation.invoke(
                    access,
                    ordinary,
                    { value: 41 },
                    { key: 'legacy' },
                  )
                  assert.deepInclude(legacy, {
                    status: 'succeeded',
                    valueKind: 'decoded',
                    value: 42,
                  })
                  const omitted = yield* Invocation.invoke(
                    access,
                    optional,
                    {},
                    { key: 'optional' },
                  )
                  assert.deepInclude(omitted, {
                    status: 'succeeded',
                    valueKind: 'decoded',
                    value: undefined,
                  })
                  assert.deepInclude(
                    yield* Invocation.invoke(access, nullable, {}, { key: 'null' }),
                    { status: 'succeeded', valueKind: 'decoded', value: null },
                  )
                  const media = yield* Invocation.invoke(access, fallback, {}, { key: 'media' })
                  assert.strictEqual(media.valueKind, 'content')
                  if (media.valueKind === 'content')
                    assert.deepStrictEqual(
                      media.value,
                      Prompt.filePart({ mediaType: 'image/png', data: new Uint8Array([1, 2, 3]) }),
                    )
                  assert.deepStrictEqual(media.details, { ui: 'media' })
                  assert.notProperty(media, 'control')
                  assert.deepInclude(
                    yield* Invocation.invoke(access, failed, {}, { key: 'failed' }),
                    { status: 'failed', valueKind: 'decoded', error: 'domain' },
                  )
                  const died = yield* Invocation.invoke(access, fatal, {}, { key: 'fatal' })
                  assert.deepInclude(died, { status: 'faulted', valueKind: 'unavailable' })
                  assert.include(JSON.stringify(died.diagnostics), 'domain branch')
                  assert.include(JSON.stringify(died.diagnostics), 'defect branch')
                  for (const key of ['', '/', '__proto__', '1']) {
                    const invalid = yield* access.invocation
                      .invokeTool(ordinary.name, { value: '1' }, { key })
                      .pipe(Effect.flip)
                    assert.deepInclude(invalid, { _tag: 'InvocationError', reason: 'invalidKey' })
                  }
                  assert.deepInclude(
                    yield* Invocation.invoke(
                      access,
                      ordinary,
                      { value: 41 },
                      { key: 'constructor' },
                    ),
                    { value: 42 },
                  )
                  const receipts = yield* access.commit((tx) => Usage.receipts(tx, root))
                  assert.strictEqual(
                    receipts.filter((receipt) => receipt.identity._tag === 'nestedCall').length,
                    7,
                  )
                  yield* access.commit((tx) =>
                    access.progress(tx, contract, {
                      status: 'completing',
                      outcome: { status: 'completed', result: 'complete' },
                    }),
                  )
                }),
            },
          })
          yield* runtime.registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'accounting', version: 1 }, Toolkit.make()),
              program,
            ),
          )
          const ref = yield* runtime.scheduler.start(program, undefined, { conversationId: root })
          assert.deepStrictEqual(yield* runtime.scheduler.wait(ref), {
            status: 'completed',
            result: 'complete',
          })
          const receipts = yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root))
          assert.lengthOf(receipts, 7)
          assert.strictEqual(
            receipts.filter(
              (receipt) => receipt.usage?._tag === 'tool' && receipt.usage.cost?.amount === 3,
            ).length,
            1,
          )
          assert.isFalse(
            (yield* Session.scanEntries(runtime.session, { conversationId: root }).pipe(
              Stream.runCollect,
            )).some((entry) => entry.kind === 'tool.result'),
          )
          assert.isTrue(Option.isNone(yield* runtime.scheduler.failure))
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
})
