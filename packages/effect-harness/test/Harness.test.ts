import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Response from 'effect/ai/Response'
import * as Conversation from 'effect-harness/Conversation'
import * as Document from 'effect-harness/Document'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Hook from 'effect-harness/Hook'
import * as Extension from 'effect-harness/Extension'
import * as Model from 'effect-harness/Model'
import * as PromptSection from 'effect-harness/PromptSection'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Submission from 'effect-harness/Submission'
import * as Tool from 'effect-harness/Tool'
import { ToolExecution } from 'effect-harness/ToolExecution'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Transaction from 'effect-harness/Transaction'

const finish = (reason: Response.FinishReason): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
})
const plainModel = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: () => Effect.succeed([{ type: 'text', text: 'answer' }, finish('stop')]),
    streamText: () => Stream.empty,
  }),
)
const modelForTool = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: ({ prompt, tools }) => {
      if (tools.length === 0)
        return Effect.succeed([{ type: 'text', text: 'answer' }, finish('stop')])
      const result = prompt.content
        .flatMap((message) => (message.role === 'tool' ? message.content : []))
        .find((part) => part.type === 'tool-result')
      return Effect.succeed(
        result === undefined
          ? [
              {
                type: 'tool-call',
                id: 'call-1',
                name: 'work',
                params: { text: 'hello' },
                providerExecuted: false,
              },
              finish('tool-calls'),
            ]
          : [{ type: 'text', text: result.isFailure ? 'interrupted' : 'done' }, finish('stop')],
      )
    },
    streamText: () => Stream.empty,
  }),
)
const work = (replay: Tool.Replay = 'safe') =>
  Tool.make('work', {
    parameters: Schema.Struct({ text: Schema.String }),
    success: Schema.String,
    replay,
  })
const withMemory = <A, E, R>(program: Effect.Effect<A, E, R>) =>
  Effect.scoped(program).pipe(Effect.provide(Storage.layerMemory))
const client = (runtime: HarnessRuntime.HarnessRuntimeService) =>
  Harness.make.pipe(Effect.provideService(HarnessBackend, runtime.backend))
const openScope = Effect.fnUntraced(function* () {
  const scope = yield* Scope.make()
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
  return scope
})

describe('local runtime', () => {
  it.effect('the application client submits and waits without exposing Session', () =>
    withMemory(
      Effect.gen(function* () {
        const runtime = yield* HarnessRuntime.make()
        const harness = yield* client(runtime)
        assert.isFalse('session' in harness)
        const root = yield* harness.root
        const submission = yield* root.pipe(
          Conversation.submit({ type: 'input', content: 'hello', requestId: 'job-42' }),
        )
        const settled = yield* submission.pipe(Submission.wait())
        assert.strictEqual(settled.status, 'done')
        const repeated = yield* Conversation.submit(root, {
          type: 'input',
          content: 'hello',
          requestId: 'job-42',
        })
        assert.strictEqual(repeated.id, submission.id)
        assert.deepEqual(yield* Submission.wait(repeated), settled)
        const entries = yield* Stream.runCollect(
          Session.scanEntries(runtime.session, { conversationId: root.id, order: 'ascending' }),
        )
        assert.deepEqual(
          entries.map((entry) => entry.kind),
          ['input', 'assistant'],
        )
        yield* harness.waitForIdle
      }).pipe(Effect.provide(plainModel)),
    ),
  )

  it.effect('handler Layers execute tools and commit documents through invocation access', () => {
    const toolkit = Toolkit.make(work())
    const counter = Document.define({
      kind: 'counter',
      scope: 'session',
      version: 1,
      schema: Schema.Struct({ count: Schema.Natural }),
      initial: () => ({ count: 0 }),
    })
    const target = { scope: { _tag: 'session' } } as const
    return withMemory(
      Effect.gen(function* () {
        const runtime = yield* HarnessRuntime.make({ tools: toolkit })
        const harness = yield* client(runtime)
        const root = yield* harness.root
        const submission = yield* root.pipe(Conversation.submit({ type: 'input', content: 'work' }))
        assert.strictEqual((yield* Submission.wait(submission)).status, 'done')
        const snapshot = yield* Conversation.snapshot(root, counter, target)
        assert.strictEqual(Option.getOrThrow(snapshot).value.count, 1)
        const entries = yield* Stream.runCollect(
          Session.scanEntries(runtime.session, { conversationId: root.id, order: 'ascending' }),
        )
        assert.deepEqual(
          entries.map((entry) => entry.kind),
          ['input', 'assistant', 'tool.output', 'tool.result', 'assistant'],
        )
      }).pipe(
        Effect.provide([
          modelForTool,
          toolkit.toLayer({
            work: Effect.fnUntraced(function* ({ text }) {
              const execution = yield* ToolExecution
              yield* execution.output('working\n')
              yield* execution.commit((tx) =>
                Effect.gen(function* () {
                  yield* Transaction.ensureDocument(tx, counter, target)
                  yield* Transaction.updateDocument(tx, counter, target, ({ count }) => ({
                    count: count + 1,
                  }))
                }),
              )
              return text.toUpperCase()
            }),
          }),
        ]),
      ),
    )
  })

  for (const replay of ['safe', 'unsafe', 'downgraded'] as const)
    it.effect(
      `recovery ${replay === 'safe' ? 'repeats safe' : 'does not repeat unsafe'} interrupted tool execution`,
      () => {
        const toolkit = Toolkit.make(work(replay === 'downgraded' ? 'safe' : replay))
        const restoredToolkit = Toolkit.make(work(replay === 'downgraded' ? 'unsafe' : replay))
        let executions = 0
        return withMemory(
          Effect.gen(function* () {
            const entered = yield* Deferred.make<void>()
            const firstScope = yield* openScope()
            const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
              Effect.provideService(Scope.Scope, firstScope),
              Effect.provide(
                toolkit.toLayer({
                  work: () =>
                    Effect.gen(function* () {
                      executions++
                      yield* Deferred.succeed(entered, undefined)
                      return yield* Effect.never
                    }),
                }),
              ),
            )
            const harness = yield* client(runtime)
            const root = yield* harness.root
            const job = { type: 'input', content: 'recover', requestId: 'recover-42' } as const
            const submission = yield* Conversation.submit(root, job)
            yield* Deferred.await(entered)
            yield* Scope.close(firstScope, Exit.void)
            const replacement = yield* HarnessRuntime.make({ tools: restoredToolkit }).pipe(
              Effect.provide(
                restoredToolkit.toLayer({
                  work: () =>
                    Effect.sync(() => {
                      executions++
                      return 'hello'
                    }),
                }),
              ),
            )
            const next = yield* client(replacement)
            yield* Effect.yieldNow
            assert.strictEqual(executions, 1)
            const restoredRoot =
              replay === 'safe'
                ? yield* next.root
                : Option.getOrThrow(yield* next.conversation(submission.conversationId))
            // Opening the conversation recovers it without another submission or start call.
            assert.strictEqual((yield* replacement.backend.wait(submission.id)).status, 'done')
            const restored = yield* Conversation.submit(restoredRoot, job)
            assert.strictEqual(restored.id, submission.id)
            assert.strictEqual((yield* Submission.wait(restored)).status, 'done')
            assert.strictEqual(executions, replay === 'safe' ? 2 : 1)
            const entries = yield* Stream.runCollect(
              Session.scanEntries(replacement.session, { conversationId: restoredRoot.id }),
            )
            const final = entries.find((entry) => entry.kind === 'assistant')
            assert.isDefined(final)
          }).pipe(Effect.provide(modelForTool)),
        )
      },
    )

  it.effect(
    'different conversations run independently while inputs in one conversation remain ordered',
    () => {
      const toolkit = Toolkit.make(work())
      return withMemory(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
            Effect.provide(
              toolkit.toLayer({
                work: () =>
                  Deferred.succeed(entered, undefined).pipe(
                    Effect.andThen(Deferred.await(release)),
                    Effect.as('done'),
                  ),
              }),
            ),
          )
          const harness = yield* client(runtime)
          const root = yield* harness.root
          const first = yield* Conversation.submit(root, { type: 'input', content: 'first' })
          yield* Deferred.await(entered)
          const queued = yield* Conversation.submit(root, { type: 'input', content: 'second' })
          assert.strictEqual((yield* Submission.read(queued)).status, 'queued')
          const other = yield* harness.create({ agent: { tools: [] } })
          const second = yield* Conversation.submit(other, { type: 'input', content: 'other' })
          assert.strictEqual((yield* Submission.wait(second)).status, 'done')
          assert.strictEqual((yield* Submission.read(first)).status, 'placed')
          yield* Deferred.succeed(release, undefined)
          assert.strictEqual((yield* Submission.wait(first)).status, 'done')
          assert.strictEqual((yield* Submission.wait(queued)).status, 'done')
        }).pipe(Effect.provide(modelForTool)),
      )
    },
  )

  it.effect(
    'withdraws queued input and aborts active work without cancelling other conversations',
    () => {
      const toolkit = Toolkit.make(work())
      return withMemory(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>()
          const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
            Effect.provide(
              toolkit.toLayer({
                work: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
              }),
            ),
          )
          const harness = yield* client(runtime)
          const root = yield* harness.root
          const active = yield* Conversation.submit(root, { type: 'input', content: 'active' })
          yield* Deferred.await(entered)
          const queued = yield* Conversation.submit(root, { type: 'input', content: 'queued' })
          assert.strictEqual(yield* queued.pipe(Submission.withdraw()), 'aborted')
          assert.strictEqual((yield* Submission.wait(queued)).status, 'unanswered')
          assert.strictEqual(yield* Submission.withdraw(active), 'already_placed')
          const waiter = yield* Submission.wait(active).pipe(Effect.forkChild)
          yield* Fiber.interrupt(waiter)
          assert.strictEqual((yield* Submission.read(active)).status, 'placed')
          yield* root.pipe(Conversation.abort())
          const result = yield* Submission.wait(active)
          assert.strictEqual(result.status, 'unanswered')
          if (result.status === 'unanswered') assert.strictEqual(result.reason, 'aborted')
          yield* harness.waitForIdle
        }).pipe(Effect.provide(modelForTool)),
      )
    },
  )

  it.effect(
    'forks history and configuration while giving each conversation its own durable identity',
    () =>
      withMemory(
        Effect.gen(function* () {
          const runtime = yield* HarnessRuntime.make()
          const harness = yield* client(runtime)
          const root = yield* harness.root
          yield* root.pipe(Conversation.configure({ instructions: 'original' }))
          const submission = yield* Conversation.submit(root, { type: 'input', content: 'hello' })
          yield* Submission.wait(submission)
          const fork = yield* root.pipe(Conversation.fork())
          assert.notStrictEqual(fork.id, root.id)
          assert.strictEqual((yield* Conversation.agent(fork)).instructions, 'original')
          yield* Conversation.configure(fork, { instructions: 'fork' })
          assert.strictEqual((yield* Conversation.agent(root)).instructions, 'original')
          assert.strictEqual(
            (yield* Submission.wait(
              yield* Conversation.submit(fork, { type: 'input', content: 'next' }),
            )).status,
            'done',
          )
        }).pipe(Effect.provide(plainModel)),
      ),
  )

  it.effect('runs extension hooks and prompt sections with supplied services', () =>
    withMemory(
      Effect.gen(function* () {
        let seen = ''
        let yields = 0
        const section = PromptSection.make({
          key: 'project',
          render: () => Effect.succeed('project rules'),
        })
        const hook = Hook.make({
          event: 'onYield',
          execute: () =>
            Effect.sync(() => {
              yields++
            }),
        })
        const extension = Extension.make({ name: 'project', hooks: [hook], sections: [section] })
        const native = yield* LanguageModel.make({
          generateText: ({ prompt }) =>
            Effect.sync(() => {
              seen = prompt.content
                .filter((message) => message.role === 'system')
                .map((message) => message.content)
                .join('\n')
              return [{ type: 'text', text: 'answer' }, finish('stop')]
            }),
          streamText: () => Stream.empty,
        })
        const runtime = yield* HarnessRuntime.make({ extensions: [extension] }).pipe(
          Effect.provideService(LanguageModel.LanguageModel, native),
        )
        const harness = yield* client(runtime)
        const root = yield* harness.root
        yield* Submission.wait(
          yield* Conversation.submit(root, { type: 'input', content: 'hello' }),
        )
        assert.strictEqual(yields, 1)
        assert.include(seen, 'project rules')
      }),
    ),
  )

  it.effect('selects an already-built provider through model metadata', () =>
    withMemory(
      Effect.gen(function* () {
        const native = yield* LanguageModel.make({
          generateText: () => Effect.succeed([{ type: 'text', text: 'selected' }, finish('stop')]),
          streamText: () => Stream.empty,
        })
        const model = Model.make({
          definition: {
            ref: { provider: 'test', modelId: 'local' },
            capabilities: { tools: true, reasoning: false, images: false, structuredOutput: false },
          },
          languageModel: native,
          options: Schema.Struct({}),
          configure: () => Effect.succeed(Context.empty()),
        })
        const runtime = yield* HarnessRuntime.make({ models: [model] })
        const harness = yield* client(runtime)
        const root = yield* harness.root
        assert.strictEqual(
          (yield* Submission.wait(
            yield* Conversation.submit(root, { type: 'input', content: 'hello' }),
          )).status,
          'done',
        )
      }),
    ),
  )
  it.effect('streams persisted entries without gaps or duplicate replay', () =>
    withMemory(
      Effect.gen(function* () {
        const runtime = yield* HarnessRuntime.make()
        const harness = yield* client(runtime)
        const root = yield* harness.root
        const observing = yield* root.pipe(
          Conversation.entries(),
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild,
        )
        const submission = yield* Conversation.submit(root, { type: 'input', content: 'hello' })
        yield* Submission.wait(submission)
        const entries = yield* Fiber.join(observing)
        assert.deepEqual(
          entries.map((entry) => entry.kind),
          ['input', 'assistant'],
        )
        const cursor = entries[0]?.id
        assert.isDefined(cursor)
        const replay = yield* root.pipe(
          Conversation.entries({ after: cursor }),
          Stream.take(1),
          Stream.runCollect,
        )
        assert.strictEqual(replay[0]?.kind, 'assistant')
      }).pipe(Effect.provide(plainModel)),
    ),
  )

  it.effect('client document streams observe the initial revision and committed changes', () =>
    withMemory(
      Effect.gen(function* () {
        const document = Document.define({
          kind: 'stream-counter',
          scope: 'session',
          version: 1,
          schema: Schema.Struct({ count: Schema.Natural }),
          initial: () => ({ count: 0 }),
        })
        const target = { scope: { _tag: 'session' } } as const
        const runtime = yield* HarnessRuntime.make()
        const root = yield* (yield* client(runtime)).root
        yield* Session.commit(runtime.session, (tx) =>
          Transaction.ensureDocument(tx, document, target),
        )
        const observed = yield* Deferred.make<void>()
        const observing = yield* root.pipe(
          Conversation.watch(document, target),
          Stream.tap(() => Deferred.succeed(observed, undefined)),
          Stream.take(2),
          Stream.runCollect,
          Effect.ensuring(Deferred.succeed(observed, undefined)),
          Effect.forkChild,
        )
        yield* Deferred.await(observed)
        yield* Conversation.configure(root, { instructions: 'unrelated change' })
        yield* Session.commit(runtime.session, (tx) =>
          Transaction.updateDocument(tx, document, target, () => ({ count: 1 })),
        )
        const snapshots = yield* Fiber.join(observing)
        assert.deepEqual(
          snapshots.map((snapshot) => snapshot.value.count),
          [0, 1],
        )
      }).pipe(Effect.provide(plainModel)),
    ),
  )

  it.effect('constructor failure releases the Session so construction can be retried', () =>
    withMemory(
      Effect.gen(function* () {
        class Dependency extends Context.Service<Dependency, string>()('test/Dependency') {}
        const hook = Hook.make({
          event: 'afterResponse',
          execute: () => Effect.asVoid(Dependency),
        }).pipe(
          Hook.provide(
            Layer.effect(
              Dependency,
              Effect.fail(
                new Harness.HarnessError({
                  reason: 'failed',
                  operation: 'test',
                  message: 'construction failed',
                }),
              ),
            ),
          ),
        )
        const failed = yield* HarnessRuntime.make({ hooks: [hook] }).pipe(Effect.exit)
        assert.isTrue(Exit.isFailure(failed))
        const replacement = yield* HarnessRuntime.make()
        assert.strictEqual(yield* replacement.backend.root, 1)
      }).pipe(Effect.provide(plainModel)),
    ),
  )

  it.effect(
    'the local client Layer keeps captured handler resources alive until program shutdown',
    () => {
      class Tracker extends Context.Service<Tracker, { readonly query: Effect.Effect<string> }>()(
        'test/Tracker',
      ) {}
      const toolkit = Toolkit.make(work())
      let opened = false
      const tracker = Layer.effect(
        Tracker,
        Effect.acquireRelease(
          Effect.sync(() => {
            opened = true
            return {
              query: Effect.sync(() => {
                assert.isTrue(opened)
                return 'tracked'
              }),
            }
          }),
          () =>
            Effect.sync(() => {
              opened = false
            }),
        ),
      )
      const handlers = toolkit.toLayer(
        Effect.gen(function* () {
          const service = yield* Tracker
          return { work: () => service.query }
        }),
      )
      const runtime = Harness.layerLocal({ tools: toolkit }).pipe(
        Layer.provide(handlers.pipe(Layer.provide(tracker))),
        Layer.provide(modelForTool),
        Layer.provide(Storage.layerMemory),
      )
      return Effect.gen(function* () {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const harness = yield* Harness.Harness
            const root = yield* harness.root
            assert.strictEqual(
              (yield* Submission.wait(
                yield* Conversation.submit(root, { type: 'input', content: 'hello' }),
              )).status,
              'done',
            )
            assert.isTrue(opened)
          }).pipe(Effect.provide(runtime)),
        )
        assert.isFalse(opened)
      })
    },
  )

  it.effect(
    'domain failures become tool results while defects settle the input as unanswered',
    () => {
      class DomainError extends Schema.TaggedError<DomainError>()('DomainError', {
        message: Schema.String,
      }) {}
      const definition = Tool.make('work', {
        parameters: Schema.Struct({ text: Schema.String }),
        success: Schema.String,
        failure: DomainError,
      })
      const toolkit = Toolkit.make(definition)
      return withMemory(
        Effect.gen(function* () {
          const scope = yield* openScope()
          const first = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
            Effect.provideService(Scope.Scope, scope),
            Effect.provide(
              toolkit.toLayer({ work: () => new DomainError({ message: 'expected' }) }),
            ),
          )
          const harness = yield* client(first)
          assert.strictEqual(
            (yield* Submission.wait(
              yield* Conversation.submit(yield* harness.root, {
                type: 'input',
                content: 'domain error',
              }),
            )).status,
            'done',
          )
          yield* Scope.close(scope, Exit.void)
          const second = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
            Effect.provide(toolkit.toLayer({ work: () => Effect.die('unexpected defect') })),
          )
          const other = yield* client(second)
          const conversation = yield* other.create()
          const result = yield* Submission.wait(
            yield* Conversation.submit(conversation, { type: 'input', content: 'defect' }),
          )
          assert.strictEqual(result.status, 'unanswered')
          if (result.status === 'unanswered')
            assert.include(JSON.stringify(result.detail), 'unexpected defect')
        }).pipe(Effect.provide(modelForTool)),
      )
    },
  )
  it.effect('encodes schema-typed tool results before committing', () => {
    const toolkit = Toolkit.make(
      Tool.make('work', {
        parameters: Schema.Struct({ text: Schema.String }),
        success: Schema.DateFromString,
        replay: 'safe',
      }),
    )
    return withMemory(
      Effect.gen(function* () {
        const runtime = yield* HarnessRuntime.make({ tools: toolkit })
        const root = yield* (yield* client(runtime)).root
        const settled = yield* Submission.wait(
          yield* Conversation.submit(root, { type: 'input', content: 'typed result' }),
        )
        assert.strictEqual(settled.status, 'done')
        const entries = yield* Stream.runCollect(
          Session.scanEntries(runtime.session, { conversationId: root.id }),
        )
        const result = entries.find((entry) => entry.kind === 'tool.result')
        assert.include(JSON.stringify(result?.data), '2020-01-01T00:00:00.000Z')
      }).pipe(
        Effect.provide([
          modelForTool,
          toolkit.toLayer({ work: () => Effect.succeed(new Date('2020-01-01T00:00:00.000Z')) }),
        ]),
      ),
    )
  })

  it.effect('revokes invocation writes after a tool handler returns', () => {
    const toolkit = Toolkit.make(work())
    return withMemory(
      Effect.gen(function* () {
        const captured = yield* Deferred.make<typeof ToolExecution.Service>()
        const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
          Effect.provide(
            toolkit.toLayer({
              work: () =>
                Effect.gen(function* () {
                  const execution = yield* ToolExecution
                  yield* Deferred.succeed(captured, execution)
                  return 'done'
                }),
            }),
          ),
        )
        const root = yield* (yield* client(runtime)).root
        yield* Submission.wait(
          yield* Conversation.submit(root, { type: 'input', content: 'hello' }),
        )
        const execution = yield* Deferred.await(captured)
        const error = yield* execution.output('late output').pipe(Effect.flip)
        assert.strictEqual(error._tag, 'ExecutionError')
        if (error._tag === 'ExecutionError') assert.strictEqual(error.reason, 'revoked')
        const entries = yield* Stream.runCollect(
          Session.scanEntries(runtime.session, { conversationId: root.id }),
        )
        assert.isFalse(entries.some((entry) => entry.kind === 'tool.output'))
      }).pipe(Effect.provide(modelForTool)),
    )
  })
})
