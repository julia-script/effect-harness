import { awaitTransition } from './ModeledWorkflow.ts'
import * as TestClock from 'effect/testing/TestClock'
import { assertNone } from '@effect/vitest/utils'
import * as Option from 'effect/Option'
import * as Identity from '@effect-harness/durable/Identity'
import { assert, describe, it } from '@effect/vitest'
import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as Harness from '@effect-harness/harness/Executor'
import * as HarnessCompaction from '@effect-harness/harness/Compaction'
import { ToolError } from '@effect-harness/harness/ToolError'
import { type RegistryError } from '@effect-harness/harness/RegistryError'
import * as Invocation from '@effect-harness/harness/Invocation'
import * as Model from '@effect-harness/harness/Model'
import * as Registry from '@effect-harness/harness/Registry'
import * as Tool from '@effect-harness/harness/Tool'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as AiError from 'effect/ai/AiError'
import * as NativeModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as Response from 'effect/ai/Response'
import * as AiTool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Workflow from 'effect/workflow/Workflow'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Conversation from '@effect-harness/durable/Conversation'
import * as Executor from '@effect-harness/durable/Executor'
import * as Inbox from '@effect-harness/durable/Inbox'
import * as Ownership from '@effect-harness/durable/Ownership'
import * as Record from '@effect-harness/durable/Record'
import * as Session from '@effect-harness/durable/Session'
import * as Store from '@effect-harness/durable/Store'
import * as Directory from '@effect-harness/durable/SessionDirectory'
import * as Usage from '@effect-harness/durable/Usage'
import * as Memory from '@effect-harness/durable/storage/Memory'
import { Abort } from '@effect-harness/durable/workflow/Abort'
import { Compaction } from '@effect-harness/durable/workflow/Compaction'
import * as CompactionExecutor from '@effect-harness/durable/workflow/CompactionExecutor'
import * as Cancellation from '@effect-harness/durable/workflow/Cancellation'
import { Submission } from '@effect-harness/durable/workflow/Submission'
import * as Structured from '@effect-harness/durable/workflow/Structured'
import {
  ExecutionError,
  ExecutionErrorCodec,
  Storage,
} from '@effect-harness/durable/workflow/ExecutionError'

const modelRef = { provider: 'race', modelId: 'model' }
const finish = (reason: Response.FinishReason = 'stop'): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
})
const answer = (text = 'done'): Response.StreamPartEncoded[] => [
  { type: 'text-start', id: 'answer' },
  { type: 'text-delta', id: 'answer', delta: text },
  { type: 'text-end', id: 'answer' },
  finish(),
]
const failure = () =>
  new AiError.AiError({
    module: 'race',
    method: 'request',
    reason: new AiError.InvalidRequestError({ description: 'classified failure' }),
  })
const input = (requestId: string) => ({
  sessionId: Identity.SessionId.make('race'),
  conversationId: Record.ROOT_CONVERSATION_ID,
  requestId: Identity.RequestId.make(requestId),
  submission: {
    _tag: 'input' as const,
    type: 'input' as const,
    whenBusy: 'followUp' as const,
    message: Prompt.userMessage({ content: [Prompt.textPart({ text: requestId })] }),
  },
})
// Native memory engine timers are modeled. Each bounded clock advance follows an admitted operation or observed persisted retry, and the retry registration Deferred proves scheduling before policy changes.
const until = <E, R>(condition: Effect.Effect<boolean, E, R>) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 1000; attempt++) {
      if (yield* condition) return
      yield* TestClock.adjust('5 millis')
    }
    return yield* Effect.die('Admitted native transition did not settle')
  })
const pending = <A, E>(fiber: Fiber.Fiber<A, E>) => assert.isUndefined(fiber.pollUnsafe())
const Node = Workflow.make('race/custom/v1', {
  payload: {
    sessionId: Identity.SessionId,
    conversationId: Record.ConversationId,
    taskId: Record.TaskId,
    name: Schema.String,
  },
  success: Schema.Json,
  error: ExecutionErrorCodec,
  idempotencyKey: ({ taskId }) => String(taskId),
})
type Behavior = (name: string) => Effect.Effect<Record.Json, ExecutionError>
const runtime = (
  model: Model.Descriptor,
  registry: Layer.Layer<Registry.Registry, RegistryError> = Registry.layer([]),
  options: Conversation.Options = {},
  behavior: Behavior = () => Effect.succeed({ done: true }),
  clockRegistered?: Deferred.Deferred<void>,
) => {
  const config = Conversation.layerConfiguration({
    ...options,
    settings: options.settings ?? { compaction: { enabled: false }, retry: { enabled: false } },
  })
  const catalogue = Model.layer([model])
  const engine =
    clockRegistered === undefined
      ? WorkflowEngine.layerMemory
      : Layer.effect(
          WorkflowEngine.WorkflowEngine,
          Effect.gen(function* () {
            const engine = yield* WorkflowEngine.WorkflowEngine
            const scheduleClock = engine.scheduleClock
            const observed: typeof scheduleClock = (workflow, options) =>
              scheduleClock(workflow, options).pipe(
                Effect.tap(() => Deferred.succeed(clockRegistered, undefined)),
              )
            // Native execution supplies this same public engine object to its
            // body. Preserve that identity while observing the real delegate.
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                Object.assign(engine, { scheduleClock })
              }),
            )
            return Object.assign(engine, { scheduleClock: observed })
          }),
        ).pipe(Layer.provide(WorkflowEngine.layerMemory))
  const session = Session.layer.pipe(
    Layer.provideMerge(Memory.layer),
    Layer.provide(
      Conversation.layerCreation.pipe(Layer.provide(config), Layer.provide(BunCrypto.layer)),
    ),
  )
  const node = Node.toLayer((identity) =>
    Effect.gen(function* () {
      const session = yield* (yield* Directory.SessionDirectory).resolve(identity.sessionId)
      return yield* Structured.evaluate(identity, session, behavior(identity.name))
    }).pipe(
      Effect.mapError((error) =>
        error._tag === 'StorageError'
          ? new ExecutionError({ reason: new Storage({ message: error.message, cause: error }) })
          : error,
      ),
    ),
  )
  return Layer.mergeAll(Executor.layerExecutors, node.pipe(Layer.provide(Cancellation.layer))).pipe(
    Layer.provideMerge(engine),
    Layer.provideMerge(
      Directory.layerSingle(Identity.SessionId.make('race')).pipe(Layer.provideMerge(session)),
    ),
    Layer.provideMerge(config),
    Layer.provide(catalogue),
    Layer.provide(Harness.layer.pipe(Layer.provide(Layer.mergeAll(registry, catalogue)))),
    Layer.provideMerge(Ownership.layerDeclarations([...Executor.workflows, Node])),
  )
}
const descriptor = (model: NativeModel.LanguageModel): Model.Descriptor => ({
  ref: modelRef,
  model,
  estimate: () => 100,
  contextWindow: 100000,
  maxOutputTokens: 1000,
  configure: () => Effect.succeed(Context.empty()),
})
const initialize = Effect.fnUntraced(function* (session: Session.Service, history = false) {
  yield* session.root()
  return yield* session.transaction(
    Effect.fnUntraced(function* (tx) {
      const agent = yield* tx.doc(Conversation.AgentDoc, { owner: Record.ROOT_CONVERSATION_ID })
      agent.model = modelRef
      const ids: Record.EntryId[] = []
      if (history)
        for (let i = 1; i <= 3; i++) {
          for (const message of [
            Prompt.userMessage({ content: [Prompt.textPart({ text: `u${i}` })] }),
            Prompt.assistantMessage({ content: [Prompt.textPart({ text: `a${i}` })] }),
          ]) {
            const model = yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.Message))(message)
            ids.push(
              (yield* tx.appendEntry(Record.ROOT_CONVERSATION_ID, {
                kind: message.role === 'user' ? 'harness.user' : 'harness.assistant',
                model: [model],
              })).id,
            )
          }
        }
      return ids
    }),
  )
})
const reserve = (session: Session.Service, name: string, background = false) =>
  session.transaction(
    Effect.fnUntraced(function* (tx) {
      const task = {
        conversationId: Record.ROOT_CONVERSATION_ID,
        kind: Node._tag,
        version: 1,
        input: null,
        background,
        abortRequested: false,
        state: { status: 'pending' as const },
      }
      const taskId = yield* tx.createTask(task)
      const payload = {
        sessionId: Identity.SessionId.make('race'),
        conversationId: task.conversationId,
        taskId,
        name,
      }
      yield* Structured.bind(tx, { ...task, id: taskId }, Node, payload)
      return payload
    }),
  )

describe('GenerationRaces', () => {
  for (const operation of ['generation', 'compaction'] as const)
    for (const change of [
      'enable before decision',
      'disable before decision',
      'disable after timer admission',
    ] as const)
      it.effect(`${operation} reads current retry policy at decision (${change})`, () =>
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const clockRegistered = yield* Deferred.make<void>()
          let calls = 0
          const options: Model.RequestOptions[] = []
          const firstFailure = Effect.gen(function* () {
            if (++calls === 1) {
              yield* Deferred.succeed(entered, undefined)
              yield* Deferred.await(release)
              return yield* failure()
            }
          })
          const native = yield* NativeModel.make({
            generateText: () =>
              firstFailure.pipe(Effect.as([{ type: 'text' as const, text: 'summary' }, finish()])),
            streamText: () =>
              Stream.fromEffect(firstFailure).pipe(
                Stream.flatMap(() => Stream.fromIterable(answer())),
              ),
          })
          const model: Model.Descriptor = {
            ...descriptor(native),
            classify: () => ({ retryable: true, overflow: false }),
            configure: (value) =>
              Effect.sync(() => {
                options.push(value)
                return Context.empty()
              }),
          }
          const initiallyEnabled = change !== 'enable before decision'
          const policy = (enabled: boolean, timeoutMs: number) => ({
            stream: { timeoutMs },
            compaction: {
              enabled: operation === 'compaction',
              keepRecentTokens: 150,
              reserveTokens: 100,
            },
            retry: {
              enabled,
              maxRetries: 1,
              baseDelayMs: change === 'disable after timer admission' ? 1000 : 0,
            },
          })
          yield* Effect.gen(function* () {
            const session = yield* Session.Session
            const config = yield* Conversation.Configuration
            yield* initialize(session, operation === 'compaction')
            const execution =
              operation === 'generation'
                ? Submission.execute(input('retry-policy')).pipe(
                    Effect.map((result) => result.status === 'done'),
                  )
                : Compaction.execute(
                    yield* session.transaction((tx) =>
                      CompactionExecutor.make(
                        tx,
                        Identity.SessionId.make('race'),
                        Record.ROOT_CONVERSATION_ID,
                        'manual',
                      ),
                    ),
                  ).pipe(
                    Effect.result,
                    Effect.map((result) => result._tag === 'Success'),
                  )
            const result = yield* execution.pipe(Effect.forkScoped)
            yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
            yield* Deferred.await(entered)
            if (change === 'disable after timer admission') {
              yield* Deferred.succeed(release, undefined)
              yield* until(
                session.snapshot(Inbox.LiveDoc, { owner: Record.ROOT_CONVERSATION_ID }).pipe(
                  Effect.map(Option.getOrUndefined),
                  Effect.map((live) =>
                    operation === 'generation'
                      ? live?.value.generation?.retry !== undefined
                      : live?.value.compactions?.some((item) => item.retry !== undefined) === true,
                  ),
                  Effect.tap(() => TestClock.adjust('1 millis')),
                ),
              )
              pending(result)
              yield* Deferred.await(clockRegistered)
            }
            yield* config.updateSettings(policy(change === 'enable before decision', 999))
            yield* Deferred.succeed(release, undefined)
            const succeeds = change !== 'disable before decision'
            assert.strictEqual(yield* awaitTransition(Fiber.join(result)), succeeds)
            assert.strictEqual(calls, succeeds ? 2 : 1)
            if (succeeds)
              assert.deepStrictEqual(
                options.map((value) => value.options['timeoutMs']),
                operation === 'generation' ? [1234, 999] : [1234, 1234],
              )
            assert.isTrue(
              (yield* session.committed).tasks.every(
                (task) => task.state.status === 'terminal' && !task.abortRequested,
              ),
            )
          }).pipe(
            Effect.provide(
              runtime(
                model,
                Registry.layer([]),
                { settings: policy(initiallyEnabled, 1234) },
                undefined,
                clockRegistered,
              ),
            ),
          )
        }),
      )

  for (const enable of [false, true])
    it.effect(
      `overflow reads compaction policy when failure arrives (${enable ? 'enabled late' : 'disabled late'})`,
      () =>
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          let calls = 0
          let summaries = 0
          const native = yield* NativeModel.make({
            generateText: () =>
              Effect.sync(() => {
                summaries++
                return [{ type: 'text' as const, text: 'overflow summary' }, finish()]
              }),
            streamText: () =>
              Stream.unwrap(
                Effect.gen(function* () {
                  if (++calls === 1) {
                    yield* Deferred.succeed(entered, undefined)
                    yield* Deferred.await(release)
                    return Stream.fail(failure())
                  }
                  return Stream.fromIterable(answer())
                }),
              ),
          })
          yield* Effect.gen(function* () {
            const session = yield* Session.Session
            const config = yield* Conversation.Configuration
            yield* initialize(session, true)
            const result = yield* Submission.execute(input('overflow')).pipe(Effect.forkScoped)
            yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
            yield* Deferred.await(entered)
            yield* config.updateSettings({
              compaction: { enabled: enable, keepRecentTokens: 150, reserveTokens: 100 },
              retry: { enabled: true, maxRetries: 1, baseDelayMs: 0 },
            })
            yield* Deferred.succeed(release, undefined)
            assert.strictEqual(
              (yield* awaitTransition(Fiber.join(result))).status,
              enable ? 'done' : 'unanswered',
            )
            assert.strictEqual(summaries, enable ? 1 : 0)
            assert.strictEqual(calls, enable ? 2 : 1)
            assert.strictEqual(
              (yield* session.committed).entries.filter(
                (entry) => entry.entry.kind === 'harness.compaction',
              ).length,
              enable ? 1 : 0,
            )
          }).pipe(
            Effect.provide(
              runtime(
                { ...descriptor(native), classify: () => ({ retryable: true, overflow: true }) },
                Registry.layer([]),
                {
                  settings: {
                    compaction: { enabled: !enable, keepRecentTokens: 150, reserveTokens: 100 },
                    retry: { enabled: true, maxRetries: 1, baseDelayMs: 0 },
                  },
                },
              ),
            ),
          )
        }),
    )

  for (const race of [
    {
      name: 'equal cuts replace in placement order',
      firstKeep: 150,
      secondKeep: 150,
      finishFirst: true,
      staleSecond: false,
      winner: 'SECOND',
    },
    {
      name: 'an older cut loses to the already queued furthest cut',
      firstKeep: 150,
      secondKeep: 350,
      finishFirst: true,
      staleSecond: true,
      winner: 'FIRST',
    },
    {
      name: 'a further cut wins after the earlier cut',
      firstKeep: 350,
      secondKeep: 150,
      finishFirst: true,
      staleSecond: false,
      winner: 'SECOND',
    },
    {
      name: 'the older selected furthest cut wins when it finishes last',
      firstKeep: 150,
      secondKeep: 350,
      finishFirst: false,
      staleSecond: false,
      winner: 'FIRST',
    },
  ] as const)
    it.effect(`concurrent compaction: ${race.name}`, () =>
      Effect.gen(function* () {
        const answerEntered = yield* Deferred.make<void>()
        const answerRelease = yield* Deferred.make<void>()
        const firstEntered = yield* Deferred.make<void>()
        const secondEntered = yield* Deferred.make<void>()
        const firstRelease = yield* Deferred.make<void>()
        const secondRelease = yield* Deferred.make<void>()
        let summaries = 0
        const native = yield* NativeModel.make({
          generateText: () =>
            Effect.gen(function* () {
              const first = ++summaries === 1
              yield* Deferred.succeed(first ? firstEntered : secondEntered, undefined)
              yield* Deferred.await(first ? firstRelease : secondRelease)
              return [{ type: 'text', text: first ? 'FIRST' : 'SECOND' }, finish()]
            }),
          streamText: () =>
            Stream.fromEffect(
              Deferred.succeed(answerEntered, undefined).pipe(
                Effect.andThen(Deferred.await(answerRelease)),
              ),
            ).pipe(Stream.flatMap(() => Stream.fromIterable(answer()))),
        })
        yield* Effect.gen(function* () {
          const session = yield* Session.Session
          const config = yield* Conversation.Configuration
          yield* initialize(session, true)
          const primary = yield* Submission.execute(input('busy')).pipe(Effect.forkScoped)
          yield* Effect.addFinalizer(() =>
            Deferred.succeed(answerRelease, undefined).pipe(
              Effect.andThen(Deferred.succeed(firstRelease, undefined)),
              Effect.andThen(Deferred.succeed(secondRelease, undefined)),
            ),
          )
          yield* Deferred.await(answerEntered)
          const view = yield* Conversation.context(session, Record.ROOT_CONVERSATION_ID)
          const firstCut = HarnessCompaction.selectCut(view, race.firstKeep, () => 100)
          const secondCut = HarnessCompaction.selectCut(view, race.secondKeep, () => 100)
          assert.isDefined(firstCut)
          assert.isDefined(secondCut)
          const firstPayload = yield* session.transaction((tx) =>
            CompactionExecutor.make(
              tx,
              Identity.SessionId.make('race'),
              Record.ROOT_CONVERSATION_ID,
              'manual',
            ),
          )
          const first = yield* Compaction.execute(firstPayload).pipe(Effect.forkScoped)
          yield* Deferred.await(firstEntered)
          yield* config.updateSettings({
            compaction: { enabled: true, keepRecentTokens: race.secondKeep, reserveTokens: 100 },
            retry: { enabled: false },
          })
          const secondPayload = yield* session.transaction((tx) =>
            CompactionExecutor.make(
              tx,
              Identity.SessionId.make('race'),
              Record.ROOT_CONVERSATION_ID,
              'manual',
            ),
          )
          const second = yield* Compaction.execute(secondPayload).pipe(Effect.forkScoped)
          yield* Deferred.await(secondEntered)
          yield* Deferred.succeed(race.finishFirst ? firstRelease : secondRelease, undefined)
          const completedFirst = yield* awaitTransition(
            Fiber.join(race.finishFirst ? first : second),
          )
          assert.isDefined(completedFirst.submissionId)
          yield* until(
            session.committed.pipe(
              Effect.map((state) =>
                state.submissions.some(
                  (submission) =>
                    submission.id === completedFirst.submissionId && submission.status === 'queued',
                ),
              ),
              Effect.tap(() => TestClock.adjust('1 millis')),
            ),
          )
          yield* Deferred.succeed(race.finishFirst ? secondRelease : firstRelease, undefined)
          const firstResult = yield* awaitTransition(Fiber.join(first))
          const secondResult = yield* awaitTransition(Fiber.join(second))
          assert.isDefined(firstResult.submissionId)
          assert.isDefined(secondResult.submissionId)
          yield* until(
            session.committed.pipe(
              Effect.map(
                (state) =>
                  state.submissions.filter(
                    (submission) => submission.type === 'write' && submission.status === 'queued',
                  ).length === 2,
              ),
              Effect.tap(() => TestClock.adjust('1 millis')),
            ),
          )
          yield* Deferred.succeed(answerRelease, undefined)
          assert.strictEqual((yield* awaitTransition(Fiber.join(primary))).status, 'done')
          yield* Conversation.awaitIdle(session)
          const state = yield* session.committed
          const firstSubmission = state.submissions.find(
            (submission) => submission.id === firstResult.submissionId,
          )
          const secondSubmission = state.submissions.find(
            (submission) => submission.id === secondResult.submissionId,
          )
          assert.strictEqual(firstSubmission?.status, 'done')
          assert.strictEqual(secondSubmission?.status, race.staleSecond ? 'unanswered' : 'done')
          if (race.staleSecond) assert.strictEqual(secondSubmission?.reason, 'stale')
          const firstHead = state.entries.find((entry) => entry.entry.id === firstSubmission?.entry)
            ?.entry.head
          assert.strictEqual(
            firstHead,
            Option.isNone(firstCut) ? undefined : view.entries[firstCut.value]?.id,
          )
          if (!race.staleSecond) {
            const secondHead = state.entries.find(
              (entry) => entry.entry.id === secondSubmission?.entry,
            )?.entry.head
            assert.strictEqual(
              secondHead,
              Option.isNone(secondCut) ? undefined : view.entries[secondCut.value]?.id,
            )
          }
          const context = yield* Conversation.context(session, Record.ROOT_CONVERSATION_ID)
          assert.include(
            JSON.stringify(context.messages[0]),
            `<summary>\\n${race.winner}\\n</summary>`,
          )
          const usage = (yield* session
            .snapshot(Usage.UsageDoc, {
              owner: Record.ROOT_CONVERSATION_ID,
            })
            .pipe(Effect.map(Option.getOrUndefined)))?.value
          assert.strictEqual(
            Object.values(usage?.models ?? {}).reduce((sum, value) => sum + value.totalTokens, 0),
            6,
          )
          assert.strictEqual(summaries, 2)
        }).pipe(
          Effect.provide(
            runtime(descriptor(native), Registry.layer([]), {
              settings: {
                compaction: {
                  enabled: true,
                  keepRecentTokens: race.firstKeep,
                  reserveTokens: 100,
                },
                retry: { enabled: false },
              },
            }),
          ),
        )
      }),
    )

  for (const blocked of ['provider', 'tool', 'custom'] as const)
    it.effect(
      `abort withdraws and notifies queued inputs before joined ${blocked} cleanup; late ordinary work is awaited`,
      () =>
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>()
          const finalizing = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const lateEntered = yield* Deferred.make<void>()
          const lateRelease = yield* Deferred.make<void>()
          const stop = Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(
              Deferred.succeed(finalizing, undefined).pipe(Effect.andThen(Deferred.await(release))),
            ),
          )
          const native = yield* NativeModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              blocked === 'tool'
                ? Stream.fromIterable<Response.StreamPartEncoded>([
                    {
                      type: 'tool-call',
                      id: 'work',
                      name: 'work',
                      params: {},
                      providerExecuted: false,
                    },
                    finish('tool-calls'),
                  ])
                : Stream.fromEffect(blocked === 'provider' ? stop : Effect.never),
          })
          const toolkit = Toolkit.make(
            AiTool.make('work', {
              parameters: Schema.Struct({}),
              success: Invocation.Result,
              failure: ToolError,
            }),
          )
          const tools = yield* Tool.bind(toolkit, {
            work: {
              replay: 'safe',
              project: (value) => Tool.decodeResult('fixture', value),
            },
          }).pipe(Effect.provide(toolkit.toLayer({ work: () => stop })))
          const behavior: Behavior = (name) =>
            name === 'original'
              ? stop
              : Deferred.succeed(lateEntered, undefined).pipe(
                  Effect.andThen(Deferred.await(lateRelease)),
                  Effect.as({ settled: true }),
                )
          yield* Effect.gen(function* () {
            const session = yield* Session.Session
            const store = yield* Store.Store
            yield* initialize(session)
            const original = yield* Submission.execute(input('original')).pipe(Effect.forkScoped)
            if (blocked === 'custom') {
              yield* until(
                session.committed.pipe(
                  Effect.map((state) =>
                    state.tasks.some((task) => task.kind === 'harness.generation'),
                  ),
                  Effect.tap(() => TestClock.adjust('1 millis')),
                ),
              )
              const custom = yield* reserve(session, 'original')
              yield* Node.execute(custom, { discard: true })
            }
            yield* Deferred.await(entered)
            const queued = yield* Submission.execute(input('queued')).pipe(Effect.forkScoped)
            yield* until(
              session.committed.pipe(
                Effect.map((state) =>
                  state.submissions.some(
                    (item) => item.requestId === 'queued' && item.status === 'queued',
                  ),
                ),
                Effect.tap(() => TestClock.adjust('1 millis')),
              ),
            )
            const before = yield* session.committed
            const abort = yield* Abort.execute({
              sessionId: Identity.SessionId.make('race'),
              requestId: Identity.RequestId.make('abort'),
              target: {
                _tag: 'conversation' as const,
                type: 'conversation',
                id: Record.ROOT_CONVERSATION_ID,
              },
              background: false,
            }).pipe(Effect.forkScoped)
            yield* Effect.addFinalizer(() =>
              Deferred.succeed(release, undefined).pipe(
                Effect.andThen(Deferred.succeed(lateRelease, undefined)),
              ),
            )
            yield* Deferred.await(finalizing)
            const receipt = yield* awaitTransition(Fiber.join(queued))
            assert.strictEqual(receipt.status, 'unanswered')
            assert.strictEqual(receipt.reason, 'aborted')
            pending(abort)
            const journal = yield* store.journal(0)
            const markReceipt = journal.state.receipts.find((item) =>
              item.key.startsWith('workflow/abort/mark/'),
            )
            assert.isDefined(markReceipt)
            const frame = journal.frames.find((item) => item.seq === markReceipt?.seq)
            const marked =
              frame?.writes.flatMap((write) =>
                write.type === 'task' && write.value.abortRequested ? [write.value.id] : [],
              ) ?? []
            assert.deepStrictEqual(
              marked.toSorted((left, right) => left - right),
              before.tasks.map((task) => task.id).toSorted((left, right) => left - right),
            )
            assert.isTrue(
              frame?.writes.some(
                (write) =>
                  write.type === 'submission' &&
                  write.value.requestId === 'queued' &&
                  write.value.status === 'unanswered',
              ) ?? false,
            )
            const late = yield* reserve(session, 'late')
            const background = yield* reserve(session, 'background', true)
            yield* Deferred.succeed(release, undefined)
            yield* Deferred.await(lateEntered)
            assert.strictEqual(
              (yield* session.task(late.taskId).pipe(Effect.map(Option.getOrUndefined)))
                ?.abortRequested,
              false,
            )
            pending(abort)
            yield* Deferred.succeed(lateRelease, undefined)
            yield* awaitTransition(Fiber.join(abort))
            assert.strictEqual(
              (yield* session.task(late.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state
                .status,
              'terminal',
            )
            assert.strictEqual(
              (yield* session.task(background.taskId).pipe(Effect.map(Option.getOrUndefined)))
                ?.state.status,
              'pending',
            )
            assert.strictEqual(
              (yield* session.task(background.taskId).pipe(Effect.map(Option.getOrUndefined)))
                ?.abortRequested,
              false,
            )
            assertNone(yield* Node.poll(yield* Node.executionId(background)))
            assert.strictEqual((yield* awaitTransition(Fiber.join(original))).status, 'unanswered')
          }).pipe(
            Effect.provide(
              runtime(descriptor(native), Registry.layer([{ name: 'tools', tools }]), {}, behavior),
            ),
          )
        }),
    )

  it.effect(
    'interrupting an awaitIdle caller leaves the native execution and domain work intact',
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        let calls = 0
        let finalized = 0
        const native = yield* NativeModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.fromEffect(
              Effect.sync(() => {
                calls++
              }).pipe(
                Effect.andThen(Deferred.succeed(entered, undefined)),
                Effect.andThen(Deferred.await(release)),
              ),
            ).pipe(
              Stream.flatMap(() => Stream.fromIterable(answer())),
              Stream.ensuring(
                Effect.sync(() => {
                  finalized++
                }),
              ),
            ),
        })
        yield* Effect.gen(function* () {
          const session = yield* Session.Session
          yield* initialize(session)
          const result = yield* Submission.execute(input('idle')).pipe(Effect.forkScoped)
          yield* Deferred.await(entered)
          const before = yield* session.committed
          const engine = yield* WorkflowEngine.WorkflowEngine
          const joined = yield* Deferred.make<void>()
          const waiting = yield* Conversation.awaitIdle(session).pipe(
            Effect.provideService(WorkflowEngine.WorkflowEngine, {
              ...engine,
              execute: (workflow, options) =>
                Effect.gen(function* () {
                  const client = yield* engine
                    .execute(workflow, options)
                    .pipe(Effect.forkScoped({ startImmediately: true }))
                  yield* Deferred.succeed(joined, undefined)
                  return yield* awaitTransition(Fiber.join(client))
                }).pipe(Effect.scoped),
            }),
            Effect.forkScoped,
          )
          yield* Deferred.await(joined)
          yield* Fiber.interrupt(waiting)
          assert.deepStrictEqual(yield* session.committed, before)
          assert.strictEqual(finalized, 0)
          pending(result)
          yield* Deferred.succeed(release, undefined)
          assert.strictEqual((yield* awaitTransition(Fiber.join(result))).status, 'done')
          yield* Conversation.awaitIdle(session)
          assert.strictEqual(calls, 1)
          assert.strictEqual(finalized, 1)
        }).pipe(Effect.provide(runtime(descriptor(native))))
      }),
  )
})
