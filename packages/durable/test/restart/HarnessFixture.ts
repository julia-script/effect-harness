import * as Duration from 'effect/Duration'
import * as Identity from '@effect-harness/durable/Identity'
/** Executable test boundary: each worker owns a fresh SQL-backed native engine and real harness Layers. */
import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as BunFileSystem from '@effect/platform-bun/BunFileSystem'
import * as FileSystem from 'effect/FileSystem'
import * as BunRuntime from '@effect/platform-bun/BunRuntime'
import * as SqliteClient from '@effect/sql-sqlite-bun/SqliteClient'
import * as Harness from '@effect-harness/harness/Executor'
import { ToolError, ToolExecution } from '@effect-harness/harness/ToolError'
import * as Invocation from '@effect-harness/harness/Invocation'
import * as Model from '@effect-harness/harness/Model'
import * as Registry from '@effect-harness/harness/Registry'
import * as Tool from '@effect-harness/harness/Tool'
import * as Hook from '@effect-harness/harness/Hook'
import * as Context from 'effect/Context'
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Option from 'effect/Option'
import * as NativeModel from 'effect/ai/LanguageModel'
import * as AiError from 'effect/ai/AiError'
import * as Prompt from 'effect/ai/Prompt'
import * as Response from 'effect/ai/Response'
import * as AiTool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Stream from 'effect/Stream'
import * as ClusterWorkflowEngine from 'effect/cluster/ClusterWorkflowEngine'
import * as SingleRunner from 'effect/cluster/SingleRunner'
import * as Workflow from 'effect/workflow/Workflow'
import * as Activity from 'effect/workflow/Activity'
import * as Conversation from '@effect-harness/durable/Conversation'
import * as Inbox from '@effect-harness/durable/Inbox'
import * as Ownership from '@effect-harness/durable/Ownership'
import * as Record from '@effect-harness/durable/Record'
import * as Session from '@effect-harness/durable/Session'
import * as Store from '@effect-harness/durable/Store'
import type { StorageError } from '@effect-harness/durable/StorageError'
import * as Directory from '@effect-harness/durable/SessionDirectory'
import * as SqlStore from '../storage/TestStore.ts'
import * as DurableExecutor from '@effect-harness/durable/Executor'
import * as CompactionExecutor from '@effect-harness/durable/workflow/CompactionExecutor'
import * as SubmissionExecutor from '@effect-harness/durable/workflow/SubmissionExecutor'
import * as Cancellation from '@effect-harness/durable/workflow/Cancellation'
import * as Structured from '@effect-harness/durable/workflow/Structured'
import { Submission } from '@effect-harness/durable/workflow/Submission'
import { Abort } from '@effect-harness/durable/workflow/Abort'
import { Compaction } from '@effect-harness/durable/workflow/Compaction'
import { ExecutionErrorCodec } from '@effect-harness/durable/workflow/ExecutionError'

const Child = Workflow.make('restart/owned-child/v1', {
  payload: {
    sessionId: Identity.SessionId,
    conversationId: Record.ConversationId,
    taskId: Record.TaskId,
  },
  success: Schema.Json,
  error: ExecutionErrorCodec,
  idempotencyKey: ({ taskId }) => String(taskId),
})
const finish = (reason: Response.FinishReason): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 5, text: 5, reasoning: undefined },
  },
  response: undefined,
})
const answer = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: 'text-start', id: 'text' },
  { type: 'text-delta', id: 'text', delta: text },
  { type: 'text-end', id: 'text' },
  finish('stop'),
]
const input = (key: string, whenBusy?: 'steer' | 'followUp') => ({
  sessionId: Identity.SessionId.make('restart'),
  conversationId: Record.ROOT_CONVERSATION_ID,
  requestId: Identity.RequestId.make(key),
  submission: {
    _tag: 'input' as const,
    type: 'input' as const,
    message: Prompt.userMessage({ content: [Prompt.textPart({ text: key })] }),
    ...(whenBusy === undefined ? {} : { whenBusy }),
  },
})
const waitFor = <E, R>(condition: Effect.Effect<boolean, E, R>) =>
  Effect.repeat(condition, { until: (ready) => ready }).pipe(Effect.asVoid)

const main = Effect.gen(function* () {
  const filename = process.env['HARNESS_RESTART_DB']
  const scenario = process.env['HARNESS_RESTART_SCENARIO'] ?? 'request'
  const phase = process.env['HARNESS_RESTART_PHASE'] ?? 'start'
  const base = scenario === 'commit-deferred' ? 'deferred' : scenario.replace('-missing-model', '')
  if (filename === undefined) return yield* Effect.die('HARNESS_RESTART_DB is required')
  const database = SqliteClient.layer({ filename })
  yield* Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const auditFile = filename + '.audit.jsonl'
    const Audit = Schema.Struct({ phase: Schema.String, kind: Schema.String, data: Schema.String })
    const readAudit = fs.readFileString(auditFile).pipe(
      Effect.catchIf(
        (error) => error.reason._tag === 'NotFound',
        () => Effect.succeed(''),
      ),
      Effect.flatMap((text) =>
        Effect.forEach(text.split('\n').filter(Boolean), (line) =>
          Schema.decodeEffect(Schema.fromJsonString(Audit))(line),
        ),
      ),
      Effect.orDie,
    )
    const audit = (kind: string, data: unknown = null) =>
      fs
        .writeFileString(
          auditFile,
          JSON.stringify({ phase, kind, data: JSON.stringify(data) }) + '\n',
          { flag: 'a' },
        )
        .pipe(Effect.asVoid, Effect.orDie)
    const count = (kind: string) =>
      readAudit.pipe(Effect.map((rows) => rows.filter((row) => row.kind === kind).length))
    const first = phase === 'start'
    const toolScenario = scenario.startsWith('tool-') || scenario === 'hold' || scenario === 'abort'
    const native = yield* NativeModel.make({
      generateText: (options) =>
        Effect.gen(function* () {
          yield* audit('summary', options.prompt)
          if (
            first &&
            scenario !== 'compaction-retry' &&
            scenario !== 'compaction-queued' &&
            !scenario.includes('-commit-')
          )
            return yield* Effect.never
          if (
            first &&
            (scenario === 'compaction-retry' || scenario === 'compaction-commit-failure')
          )
            return yield* new AiError.AiError({
              module: 'fixture',
              method: 'generateText',
              reason: new AiError.InternalProviderError({ description: '503 Service Unavailable' }),
            })
          return [{ type: 'text' as const, text: 'persisted summary' }, finish('stop')]
        }),
      streamText: (options) =>
        Stream.unwrap(
          Effect.gen(function* () {
            yield* audit('request', options.prompt)
            const calls = yield* count('request')
            if (toolScenario && calls === 1)
              return Stream.fromIterable<Response.StreamPartEncoded>([
                {
                  type: 'tool-call',
                  id: 'call',
                  name: 'work',
                  params: { text: 'original' },
                  providerExecuted: false,
                },
                finish('tool-calls'),
              ])
            if (first && (scenario === 'retry' || scenario === 'commit-failure'))
              return Stream.fromIterable<Response.StreamPartEncoded>([
                { type: 'error', error: '503 Service Unavailable' },
                finish('error'),
              ])
            if (first && (base === 'deferred' || scenario === 'abort-deferred'))
              return Stream.fromIterable(answer('deferred-placeholder'))
            if (first && scenario === 'partial')
              return Stream.fromIterable<Response.StreamPartEncoded>([
                { type: 'text-start', id: 'text' },
                { type: 'text-delta', id: 'text', delta: 'committed partial' },
              ]).pipe(Stream.concat(Stream.never))
            if (first && !toolScenario && scenario !== 'commit-answer') return Stream.never
            return Stream.fromIterable(answer(`answer-${calls}`))
          }),
        ),
    })
    const fetchModel = yield* NativeModel.make({
      generateText: () => Effect.succeed([]),
      streamText: () => Stream.fromIterable(answer('fetched answer')),
    })
    const descriptor: Model.Descriptor = {
      ref: { provider: 'test', modelId: 'model' },
      model: native,
      contextWindow: scenario === 'compaction-blocking' ? 10 : 100000,
      maxOutputTokens: 1000,
      configure: (options) => audit('options', options).pipe(Effect.as(Context.empty())),
      deferred: {
        inspect: (parts) =>
          first &&
          (base === 'deferred' || scenario === 'abort-deferred') &&
          parts.some((part) => part.type === 'finish')
            ? Option.some({ handle: { job: 'pinned-job' }, pollAfterMs: Duration.millis(1200) })
            : Option.none(),
        fetch: (handle, options) =>
          Stream.unwrap(
            audit('fetch', { handle, options }).pipe(
              Effect.as(fetchModel.streamText({ prompt: Prompt.empty })),
            ),
          ),
        cancel: (handle) => audit('cancel-deferred', handle),
      },
    }
    const catalogue = Model.layer(!first && scenario.endsWith('missing-model') ? [] : [descriptor])
    const declaration = AiTool.make('work', {
      parameters: Schema.Struct({ text: Schema.String }),
      success: Invocation.Result,
      failure: ToolError,
    })
      .addDependency(Invocation.Invocation)
      .addDependency(Invocation.ToolCall)
      .addDependency(Ownership.Current)
    const toolkit = Toolkit.make(declaration)
    const currentSafe =
      scenario !== 'tool-unsafe' &&
      scenario !== 'tool-safe-unsafe' &&
      !(first && scenario === 'tool-unsafe-safe')
    const storedSafe = scenario !== 'tool-unsafe' && scenario !== 'tool-unsafe-safe'
    const bound = yield* Tool.bind(
      toolkit,
      {
        work: {
          replay: (first ? storedSafe : currentSafe) ? 'safe' : 'unsafe',
          project: (result) => Tool.decodeResult('fixture', result),
          output: { maxBytes: 32, maxLines: 2, retain: 'tail' },
          repair: (args) => audit('repair').pipe(Effect.as(args)),
        },
      },
      [Ownership.Current],
    ).pipe(
      Effect.provide(
        toolkit.toLayer({
          work: ({ text }) =>
            Effect.gen(function* () {
              const call = yield* Invocation.ToolCall
              const invocation = yield* Invocation.Invocation
              yield* audit('tool', { text, cwd: invocation.cwd })
              yield* call.output(first ? 'x'.repeat(200) + '\nold output\n' : 'new output\n')
              yield* call.details({ phase })
              yield* call.diagnostic({ kind: `diagnostic-${phase}` })
              if (scenario === 'hold') {
                yield* Ownership.memo('memo', Schema.String, audit('memo').pipe(Effect.as('kept')))
                yield* Structured.child(
                  Child,
                  (taskId) => ({
                    sessionId: Identity.SessionId.make('restart'),
                    conversationId: Record.ROOT_CONVERSATION_ID,
                    taskId,
                  }),
                  'owned',
                )
                return {}
              }
              if (first) return yield* Effect.never
              return {}
            }).pipe(
              Effect.mapError((error) =>
                error instanceof ToolError
                  ? error
                  : new ToolError({
                      reason: new ToolExecution({
                        name: 'work',
                        message: error.message,
                        cause: error,
                      }),
                    }),
              ),
            ),
        }),
      ),
    )
    const registry = Registry.layer([
      {
        name: 'fixture',
        tools: !first && scenario === 'tool-missing' ? [] : bound,
        sections: [
          {
            key: 'preamble',
            tag: false,
            render: () =>
              Effect.gen(function* () {
                yield* audit('section')
                if (first && scenario === 'prepare') return yield* Effect.never
                return first ? 'pinned preamble' : 'changed preamble'
              }),
          },
        ],
        hooks: [
          {
            operation: 'generation',
            handlers: {
              beforeRequest: () => audit('beforeRequest').pipe(Effect.as(undefined)),
              afterResponse: () => audit('afterResponse'),
            },
          },
          {
            operation: 'tool',
            handlers: {
              beforeTool: () =>
                audit('beforeTool').pipe(
                  Effect.andThen(
                    first && scenario === 'tool-before-intent'
                      ? Effect.never
                      : Effect.succeed(
                          Hook.ToolDecision.Args({ args: { text: first ? 'pinned' : 'changed' } }),
                        ),
                  ),
                ),
              afterTool: () => audit('afterTool').pipe(Effect.as(undefined)),
            },
          },
          {
            operation: 'compaction',
            handlers: {
              beforeCompact: () =>
                audit('beforeCompact').pipe(
                  Effect.andThen(
                    first && scenario === 'compaction-select'
                      ? Effect.never
                      : Effect.void.pipe(Effect.as(undefined)),
                  ),
                ),
            },
          },
        ],
      },
    ])
    const configuration = Conversation.layerConfiguration({
      cwd: first ? '/before' : '/after',
      settings: {
        stream: { timeoutMs: first ? 1234 : 999 },
        progress: { partialIntervalMs: 0, outputIntervalMs: 0 },
        retry: { enabled: true, maxRetries: 1, baseDelayMs: 1200 },
        compaction: {
          enabled: scenario === 'compaction-blocking',
          keepRecentTokens: 0,
          reserveTokens: 100,
        },
      },
    })
    const commitBoundary = new Map([
      ['commit-admission', 'workflow/submission/admit/'],
      ['commit-answer', 'workflow/generation/answer/'],
      ['commit-failure', 'workflow/generation/failure/'],
      ['commit-deferred', 'workflow/generation/deferred/'],
      ['compaction-commit-usage', 'workflow/compaction/usage/'],
      ['compaction-commit-placement', 'workflow/compaction/placement/'],
      ['compaction-commit-failure', 'workflow/compaction/failure/'],
    ]).get(scenario)
    const storage = Layer.effect(
      Store.Store,
      Effect.gen(function* () {
        const store = yield* SqlStore.make
        // The wrapper forwards both Store overloads without changing their results.
        const original = store.transact as <A, E, R>(
          change: (state: Record.State) => Effect.Effect<Store.Candidate<A>, E, R>,
          options?: Store.CommitOptions,
        ) => Effect.Effect<A, StorageError | E, R>
        const transact: typeof store.transact = <A, E, R>(
          change: (state: Record.State) => Effect.Effect<Store.Candidate<A>, E, R>,
          options?: Store.CommitOptions,
        ) =>
          original(change, options).pipe(
            Effect.tap(() =>
              first && commitBoundary !== undefined && options?.key?.startsWith(commitBoundary)
                ? audit('domain-commit', options.key).pipe(
                    Effect.andThen(Console.log('HARNESS_READY')),
                    Effect.andThen(Effect.never),
                  )
                : Effect.void,
            ),
          )
        return Store.Store.of({ ...store, transact })
      }),
    )
    const sessionLayer = Session.layer.pipe(
      Layer.provideMerge(storage),
      Layer.provide(
        Conversation.layerCreation.pipe(
          Layer.provide(configuration),
          Layer.provide(BunCrypto.layer),
        ),
      ),
    )
    const directory = Directory.layerSingle(Identity.SessionId.make('restart')).pipe(
      Layer.provideMerge(sessionLayer),
    )
    // Finish host admission before the engine can replay an unfinished SQL
    // Activity. A host transaction holding the domain semaphore while waiting
    // for that Activity's connection would block the Activity's domain commit.
    const directoryContext = yield* Layer.build(directory)
    const session = Context.get(directoryContext, Session.Session)
    yield* session.root()
    if (first)
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          const agent = yield* tx.doc(Conversation.AgentDoc, {
            owner: Record.ROOT_CONVERSATION_ID,
          })
          agent.model = descriptor.ref
          if (scenario.startsWith('compaction'))
            for (const text of ['old context', 'recent context']) {
              const message = yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.UserMessage))(
                Prompt.userMessage({ content: [Prompt.textPart({ text })] }),
              )
              yield* tx.appendEntry(Record.ROOT_CONVERSATION_ID, {
                kind: 'harness.user',
                model: [message],
              })
            }
        }),
      )
    const cluster = SingleRunner.layer({
      runnerStorage: 'memory',
      shardingConfig: {
        shardsPerGroup: 1,
        entityMessagePollInterval: '25 millis',
        entityReplyPollInterval: '25 millis',
      },
    }).pipe(Layer.provide(BunCrypto.layer))
    const engine = ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(cluster))
    const child = Child.toLayer((payload) =>
      Effect.gen(function* () {
        const session = yield* (yield* Directory.SessionDirectory).resolve(payload.sessionId)
        const exit = yield* Cancellation.run(
          payload,
          session,
          Activity.make({
            name: 'child/body',
            success: Schema.Json,
            error: ExecutionErrorCodec,
            execute: audit('child').pipe(
              Effect.andThen(first ? Effect.never : Effect.succeed({ status: 'completed' })),
            ),
          }),
        ).pipe(Effect.result)
        const outcome = exit._tag === 'Success' ? exit.success : { status: 'aborted' }
        return yield* Structured.complete(session, payload.taskId, outcome, payload.sessionId)
      }).pipe(
        Effect.mapError((error) =>
          error._tag === 'StorageError' ? SubmissionExecutor.storageError(error) : error,
        ),
      ),
    )
    const runtime: Layer.Layer<
      | Layer.Success<typeof DurableExecutor.layerExecutors>
      | Layer.Success<typeof engine>
      | Layer.Success<typeof directory>
      | Model.Catalog
      | Ownership.Declarations,
      Layer.Error<typeof engine> | Layer.Error<typeof directory> | Layer.Error<typeof registry>,
      SqlClient.SqlClient
    > = Layer.mergeAll(
      DurableExecutor.layerExecutors,
      child.pipe(Layer.provide(Cancellation.layer)),
    ).pipe(
      Layer.provideMerge(engine),
      Layer.provideMerge(Layer.succeedContext(directoryContext)),
      Layer.provide(configuration),
      Layer.provideMerge(catalogue),
      Layer.provide(Harness.layer.pipe(Layer.provide(Layer.mergeAll(registry, catalogue)))),
      Layer.provideMerge(Cancellation.layer),
      Layer.provideMerge(
        Ownership.layerDeclarations([...DurableExecutor.workflows, Child] as const),
      ),
    )
    yield* Effect.gen(function* () {
      const receipt =
        scenario.startsWith('compaction') &&
        scenario !== 'compaction-blocking' &&
        scenario !== 'compaction-queued'
          ? Effect.gen(function* () {
              const state = yield* session.committed
              const existing = state.tasks.find((task) => task.kind === 'harness.compaction')
              const payload =
                existing === undefined
                  ? yield* session.transaction((tx) =>
                      CompactionExecutor.make(
                        tx,
                        Identity.SessionId.make('restart'),
                        Record.ROOT_CONVERSATION_ID,
                        'manual',
                        undefined,
                        'pinned instruction',
                      ),
                    )
                  : yield* Schema.decodeUnknownEffect(Compaction.payloadSchema)(
                      (yield* Schema.decodeUnknownEffect(Ownership.Binding)(existing.input))
                        .payload,
                    )
              const executing = Compaction.execute(payload)
              return scenario === 'compaction-commit-failure'
                ? yield* Effect.result(executing)
                : yield* executing
            })
          : Submission.execute(input('primary'))
      if (!first && scenario.startsWith('abort'))
        yield* Abort.execute({
          sessionId: Identity.SessionId.make('restart'),
          requestId: Identity.RequestId.make('abort-reconcile'),
          target: {
            _tag: 'conversation' as const,
            type: 'conversation',
            id: Record.ROOT_CONVERSATION_ID,
          },
          background: false,
        })
      const running = yield* receipt.pipe(Effect.forkScoped)
      if (first) {
        if (commitBoundary !== undefined) return yield* Effect.never
        if (scenario === 'compaction-queued') {
          yield* waitFor(
            count('request').pipe(
              Effect.map((n) => n > 0),
              Effect.tap(() => Effect.sleep('5 millis')),
            ),
          )
          const payload = yield* session.transaction((tx) =>
            CompactionExecutor.make(
              tx,
              Identity.SessionId.make('restart'),
              Record.ROOT_CONVERSATION_ID,
              'manual',
            ),
          )
          yield* Compaction.execute(payload)
        }
        yield* waitFor(
          Effect.gen(function* () {
            if (scenario === 'tool-before-intent') return (yield* count('beforeTool')) > 0
            if (scenario === 'compaction-select') return (yield* count('beforeCompact')) > 0
            if (scenario === 'prepare') return (yield* count('section')) > 0
            const state = yield* session.committed
            const live = (yield* session
              .snapshot(Inbox.LiveDoc, {
                owner: Record.ROOT_CONVERSATION_ID,
              })
              .pipe(Effect.map(Option.getOrUndefined)))?.value
            if (scenario === 'partial')
              return (JSON.stringify(live?.generation?.message) ?? '').includes('committed partial')
            if (scenario === 'retry') return live?.generation?.retry !== undefined
            if (base === 'deferred' || scenario === 'abort-deferred')
              return live?.generation?.deferred !== undefined
            if (scenario === 'compaction-queued')
              return state.submissions.some(
                (submission) => submission.type === 'write' && submission.status === 'queued',
              )
            if (scenario === 'compaction-retry') return live?.compactions?.[0]?.retry !== undefined
            if (scenario.startsWith('compaction')) return (yield* count('summary')) > 0
            if (scenario === 'hold')
              return (
                state.tasks.some((task) => task.state.status === 'completing') &&
                (yield* count('child')) > 0
              )
            if (toolScenario) {
              const slot = live?.tools?.[0]
              return (
                slot?.details !== undefined &&
                (slot.output?.includes('old output') ?? false) &&
                (slot.diagnostics?.some((item) => item.kind === 'diagnostic-start') ?? false) &&
                (yield* count('tool')) > 0
              )
            }
            return (yield* count('request')) > 0
          }).pipe(Effect.tap(() => Effect.sleep('5 millis'))),
        )
        if (scenario === 'inbox') {
          yield* Submission.execute(input('steering', 'steer'), { discard: true })
          yield* Submission.execute(input('follow-up', 'followUp'), { discard: true })
          yield* waitFor(
            session.committed.pipe(
              Effect.map((state) => state.submissions.length === 3),
              Effect.tap(() => Effect.sleep('5 millis')),
            ),
          )
        }
        if (scenario.startsWith('abort'))
          yield* Cancellation.mark(session, {
            _tag: 'conversation' as const,
            kind: 'conversation',
            id: Record.ROOT_CONVERSATION_ID,
          })
        yield* Console.log('HARNESS_READY')
        return yield* Effect.never
      }
      const outcome = yield* Fiber.join(running)
      yield* waitFor(
        session.committed.pipe(
          Effect.map(
            (state) =>
              state.submissions.every(
                (submission) => submission.status === 'done' || submission.status === 'unanswered',
              ) && state.tasks.every((task) => task.state.status === 'terminal'),
          ),
          Effect.tap(() => Effect.sleep('5 millis')),
        ),
      )
      // Domain placement may precede a discard client's final native admission Activity.
      // Poll each ordinary Workflow to completion before declaring the worker settled.
      for (const submission of (yield* session.committed).submissions) {
        if (submission.requestId === undefined)
          return yield* Effect.die('Fixture submissions require persistent request identities')
        const executionId = yield* Submission.executionId({
          sessionId: Identity.SessionId.make('restart'),
          conversationId: submission.conversationId,
          requestId: submission.requestId,
          submission:
            submission.type === 'input'
              ? {
                  _tag: 'input' as const,
                  type: 'input',
                  message: Prompt.userMessage({ content: [] }),
                }
              : { _tag: 'write' as const, type: 'write', entry: { kind: 'poll-identity-only' } },
        })
        yield* waitFor(
          Submission.poll(executionId).pipe(
            Effect.map((result) => Option.isSome(result) && result.value._tag === 'Complete'),
            Effect.tap(() => Effect.sleep('5 millis')),
          ),
        )
      }
      yield* Console.log(`HARNESS_DONE:${JSON.stringify(outcome)}`)
    }).pipe(Effect.provide(runtime))
  }).pipe(Effect.provide(Layer.mergeAll(database, BunFileSystem.layer)))
})
BunRuntime.runMain(Effect.scoped(main))
