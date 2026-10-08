import { awaitTransition } from './ModeledWorkflow.ts'
import { assertFailure } from '@effect/vitest/utils'
// effect-review-allow P8-tests-import-public-specifiers: these adversarial tests exercise private storage validation seams that intentionally have no public package export; all public behavior uses package specifiers.
import * as Identity from '@effect-harness/durable/Identity'
import { assert, describe, it } from '@effect/vitest'
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as FileSystem from 'effect/FileSystem'
import * as Path from 'effect/Path'
import * as SqlClient from 'effect/sql/SqlClient'
import * as ClusterWorkflowEngine from 'effect/cluster/ClusterWorkflowEngine'
import * as SingleRunner from 'effect/cluster/SingleRunner'
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Harness from '@effect-harness/harness/Executor'
import * as Model from '@effect-harness/harness/Model'
import * as Registry from '@effect-harness/harness/Registry'
import * as Tool from '@effect-harness/harness/Tool'
import * as Invocation from '@effect-harness/harness/Invocation'
import { ToolError, ToolExecution } from '@effect-harness/harness/ToolError'
import * as NativeModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as Response from 'effect/ai/Response'
import * as AiTool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Stream from 'effect/Stream'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Activity from 'effect/workflow/Activity'
import * as Workflow from 'effect/workflow/Workflow'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Directory from '@effect-harness/durable/SessionDirectory'
import * as Conversation from '@effect-harness/durable/Conversation'
import * as Inbox from '@effect-harness/durable/Inbox'
import * as Executors from '@effect-harness/durable/Executor'
import { Submission } from '@effect-harness/durable/workflow/Submission'
import { Generation } from '@effect-harness/durable/workflow/Generation'
import { ToolCall } from '@effect-harness/durable/workflow/ToolCall'
import { Compaction } from '@effect-harness/durable/workflow/Compaction'
import * as CompactionExecutor from '@effect-harness/durable/workflow/CompactionExecutor'
import * as Ownership from '@effect-harness/durable/Ownership'
import * as Record from '@effect-harness/durable/Record'
import * as Session from '@effect-harness/durable/Session'
import * as Store from '@effect-harness/durable/Store'
import * as Backend from '../../src/storage/internal/backend.ts'
import * as SqlStore from '../storage/TestStore.ts'
import * as SnapshotStore from '@effect-harness/durable/storage/SnapshotStore'
import * as KeyValueStore from 'effect/persistence/KeyValueStore'
import * as Document from '@effect-harness/durable/Document'
import { rejected, NotFound, Closed } from '@effect-harness/durable/StorageError'
import * as Cancellation from '@effect-harness/durable/workflow/Cancellation'
import * as Structured from '@effect-harness/durable/workflow/Structured'
import { ExecutionError, Storage, Aborted } from '@effect-harness/durable/workflow/ExecutionError'

const User = Workflow.make('test/session-lifecycle/v1', {
  payload: {
    sessionId: Identity.SessionId,
    conversationId: Record.ConversationId,
    taskId: Record.TaskId,
  },
  success: Schema.Json,
  error: ExecutionError,
  idempotencyKey: ({ taskId }) => String(taskId),
})
const persisted = Effect.sync(() => {
  let snapshot: Backend.Snapshot = { state: Record.emptyState(), frames: [] }
  let closes = 0
  const backend: Backend.Backend = {
    load: Effect.sync(() => snapshot),
    committed: Effect.sync(() => snapshot),
    save: (next) =>
      Effect.sync(() => {
        snapshot = next
      }),
    atomic: (effect) => effect,
  }
  const open = Effect.gen(function* () {
    const store = yield* Backend.make(
      backend,
      Effect.sync(() => {
        closes++
      }),
    )
    return yield* Session.make.pipe(Effect.provideService(Store.Store, store))
  })
  return { open, state: Effect.sync(() => snapshot.state), closes: Effect.sync(() => closes) }
})
const pending = <A, E>(fiber: Fiber.Fiber<A, E>) =>
  Effect.sync(() => fiber.pollUnsafe() === undefined)
const reserve = (session: Session.Service) =>
  session.transaction(
    Effect.fnUntraced(function* (tx) {
      const projection = {
        conversationId: Record.ROOT_CONVERSATION_ID,
        kind: User._tag,
        version: 1,
        input: null,
        background: false,
        abortRequested: false,
        memos: { committed: 'kept' },
        state: { status: 'running' as const, checkpoint: { position: 'pinned' } },
      }
      const taskId = yield* tx.createTask(projection)
      const payload = {
        sessionId: Identity.SessionId.make('lifecycle'),
        conversationId: Record.ROOT_CONVERSATION_ID,
        taskId,
      }
      yield* Structured.bind(tx, { ...projection, id: taskId }, User, payload)
      return payload
    }),
  )
// Polls inspect actual cached native completion; memory cases drive admitted timers through awaitTransition, while SQL cases retain their real worker clock.
const until = <E, R>(predicate: Effect.Effect<boolean, E, R>) =>
  predicate.pipe(Effect.repeat({ until: (ready) => ready }), Effect.asVoid)

describe('GenerationLifecycle', () => {
  it.effect(
    'recovery initialization is atomic, idempotent and does not rerun conversation creation',
    () =>
      Effect.gen(function* () {
        const token = Document.defineUnsafe({
          kind: 'lifecycle.recover',
          version: 1,
          scope: 'conversation',
          history: 'latest',
          fork: 'initial',
          schema: Schema.Struct({ identity: Schema.optionalKey(Schema.String) }),
          initial: (): { identity?: string } => ({}),
        })
        const storage = yield* persisted
        const legacyScope = yield* Effect.acquireRelease(Scope.make(), (owned, exit) =>
          Scope.close(owned, exit),
        )
        const legacy = yield* Scope.provide(storage.open, legacyScope)
        const conversation = yield* legacy.root()
        const initial = yield* storage.state
        yield* legacy.initialize(conversation.id)
        assert.deepStrictEqual(yield* storage.state, initial)
        yield* awaitTransition(Scope.close(legacyScope, Exit.void))
        let created = 0
        const restoredScope = yield* Effect.acquireRelease(Scope.make(), (owned, exit) =>
          Scope.close(owned, exit),
        )
        const restored = yield* Scope.provide(
          storage.open.pipe(
            Effect.provideService(Session.CreationHook, {
              run: () =>
                Effect.sync(() => {
                  created++
                }),
              recover: (tx, owner) =>
                Effect.gen(function* () {
                  const doc = yield* tx.doc(token, { owner: owner.id })
                  doc.identity ??= 'pinned recovery identity'
                }),
            }),
          ),
          restoredScope,
        )
        yield* restored.initialize(conversation.id)
        const recovered = yield* storage.state
        yield* restored.initialize(conversation.id)
        assert.deepStrictEqual(yield* storage.state, recovered)
        assert.strictEqual(created, 0)
        assert.strictEqual(
          (yield* restored
            .snapshot(token, { owner: conversation.id })
            .pipe(Effect.map(Option.getOrUndefined)))?.value.identity,
          'pinned recovery identity',
        )
        const missing = yield* restored
          .initialize(Record.ConversationId.make(999))
          .pipe(Effect.result)
        assertFailure(missing, rejected('Conversation is absent', NotFound))
        assert.deepStrictEqual(yield* storage.state, recovered)
        yield* awaitTransition(Scope.close(restoredScope, Exit.void))
        const failing = yield* storage.open.pipe(
          Effect.provideService(Session.CreationHook, {
            run: () => Effect.void,
            recover: (tx, owner) =>
              Effect.gen(function* () {
                const doc = yield* tx.doc(token, { owner: owner.id })
                doc.identity = 'must roll back'
                return yield* rejected('recovery rejected')
              }),
          }),
        )
        assertFailure(
          yield* failing.initialize(conversation.id).pipe(Effect.result),
          rejected('recovery rejected'),
        )
        assert.deepStrictEqual(yield* storage.state, recovered)
      }),
  )

  for (const useActivity of [false, true])
    it.effect(
      `close joins finalizers and resumes the same native ${useActivity ? 'Activity' : 'body'}`,
      () =>
        Effect.gen(function* () {
          const storage = yield* persisted
          const firstScope = yield* Effect.acquireRelease(Scope.make(), (owned, exit) =>
            Scope.close(owned, exit),
          )
          const first = yield* Scope.provide(storage.open, firstScope)
          yield* first.root()
          const payload = yield* reserve(first)
          const token = Document.defineUnsafe({
            kind: 'lifecycle.observed',
            version: 1,
            scope: 'conversation',
            history: 'latest',
            fork: 'initial',
            schema: Schema.Struct({ kept: Schema.Boolean }),
            initial: () => ({ kept: true }),
          })
          yield* first.transaction((tx) => tx.doc(token, { owner: payload.conversationId }))
          const watch = yield* first
            .watchDoc(token, { owner: payload.conversationId })
            .pipe(Effect.map(Option.getOrUndefined))
          if (watch === undefined) return yield* Effect.die('Expected existing document watch')
          const before = yield* storage.state
          const current = yield* Ref.make(first)
          const entered = yield* Deferred.make<void>()
          const finalizing = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          let calls = 0
          let finalized = 0
          const executor = User.toLayer((identity) =>
            Effect.gen(function* () {
              const session = yield* (yield* Directory.SessionDirectory).resolve(identity.sessionId)
              const work = Effect.gen(function* () {
                calls++
                if (calls > 1) return { answer: 'resumed' }
                yield* Deferred.succeed(entered, undefined)
                return yield* Effect.never.pipe(
                  Effect.ensuring(
                    Deferred.succeed(finalizing, undefined).pipe(
                      Effect.andThen(Deferred.await(release)),
                      Effect.andThen(
                        Effect.sync(() => {
                          finalized++
                        }),
                      ),
                    ),
                  ),
                )
              })
              const operation = useActivity
                ? Activity.make({
                    name: 'body',
                    success: Schema.Json,
                    error: ExecutionError,
                    execute: Cancellation.activity(identity, session, work).pipe(
                      Effect.mapError(
                        (error) =>
                          new ExecutionError({
                            reason: new Storage({ message: error.message, cause: error }),
                          }),
                      ),
                    ),
                  })
                : work
              return yield* Structured.evaluate(identity, session, operation)
            }).pipe(
              Effect.mapError((error) =>
                error._tag === 'StorageError'
                  ? new ExecutionError({
                      reason: new Storage({ message: error.message, cause: error }),
                    })
                  : error,
              ),
            ),
          )
          const directory = Layer.succeed(Directory.SessionDirectory, {
            resolve: () => Ref.get(current),
          })
          const runtime = executor.pipe(
            Layer.provideMerge(WorkflowEngine.layerMemory),
            Layer.provide(directory),
            Layer.provide(Cancellation.layer),
            Layer.provide(Ownership.layerDeclarations([User])),
          )
          yield* Effect.gen(function* () {
            const client = yield* User.execute(payload).pipe(Effect.forkScoped)
            yield* Deferred.await(entered)
            const scopeClosing = yield* Scope.close(firstScope, Exit.void).pipe(Effect.forkScoped)
            const closeWaiter = yield* first.awaitClosed.pipe(Effect.forkScoped)
            yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
            yield* Deferred.await(finalizing)
            assert.isTrue(yield* first.isClosed)
            assertFailure(
              yield* first.committed.pipe(Effect.result),
              rejected('Session is closed', Closed),
            )
            assert.strictEqual(
              (yield* first.onClose(Effect.void).pipe(Effect.result))._tag,
              'Failure',
            )
            assert.strictEqual(
              (yield* first
                .transaction((tx) => tx.appendEntry(payload.conversationId, { kind: 'late' }))
                .pipe(Effect.result))._tag,
              'Failure',
            )
            assert.isTrue(yield* pending(closeWaiter))
            assert.strictEqual(yield* storage.closes, 0)
            assert.strictEqual(yield* watch.closed, 'session_closed')
            assert.isTrue(yield* pending(client))
            yield* Fiber.interrupt(closeWaiter)
            const otherWaiter = yield* first.awaitClosed.pipe(Effect.forkScoped)
            yield* Deferred.succeed(release, undefined)
            yield* Fiber.join(otherWaiter)
            yield* Fiber.join(scopeClosing)
            assert.strictEqual(finalized, 1)
            assert.strictEqual(yield* storage.closes, 1)
            assert.deepStrictEqual(yield* storage.state, before)
            const id = yield* User.executionId(payload)
            yield* until(
              User.poll(id).pipe(
                Effect.map((result) => Option.isSome(result) && result.value._tag === 'Suspended'),
                Effect.delay('1 millis'),
              ),
            )
            assert.isTrue(yield* pending(client))
            const reopened = yield* storage.open
            yield* Ref.set(current, reopened)
            yield* User.resume(id)
            assert.deepStrictEqual(yield* Fiber.join(client), {
              status: 'completed',
              result: { answer: 'resumed' },
            })
            assert.strictEqual(calls, 2)
            const task = yield* reopened
              .task(payload.taskId)
              .pipe(Effect.map(Option.getOrUndefined))
            assert.strictEqual(task?.state.status, 'terminal')
            assert.strictEqual(task?.abortRequested, false)
            assert.isUndefined(task?.memos)
          }).pipe(
            Effect.ensuring(Deferred.succeed(release, undefined)),
            Effect.provide(runtime),
            // One clock driver owns body progress and native runtime finalization.
            // Nested drivers can strand Activity shutdown on a later polling timer.
            awaitTransition,
          )
        }),
    )

  for (const engineKind of ['memory', 'sqlite'] as const)
    for (const boundary of [
      'prepare',
      'request',
      'tool-intent',
      'tool',
      'compact-select',
      'compact-request',
    ] as const)
      // Native SQL worker acquisition and transaction notifications progress outside TestClock.
      it.live(`actual harness public close preserves and resumes ${boundary} (${engineKind})`, () =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'harness-lifecycle-' })
          const database = yield* Layer.build(
            SqliteClient.layer({ filename: path.join(directory, 'state.sqlite') }),
          )
          const sql = Context.get(database, SqlClient.SqlClient)
          const reader = yield* Layer.build(
            SqliteClient.layer({
              filename: path.join(directory, 'state.sqlite'),
              readonly: true,
            }),
          )
          const readerSql = Context.get(reader, SqlClient.SqlClient)
          yield* Effect.gen(function* () {
            const sqlStorage = {
              open: Effect.gen(function* () {
                const store = yield* SqlStore.make
                return yield* Session.make.pipe(Effect.provideService(Store.Store, store))
              }),
              state: Effect.gen(function* () {
                const values = yield* KeyValueStore.KeyValueStore
                const saved = yield* KeyValueStore.toSchemaStore(
                  values,
                  SnapshotStore.Snapshot,
                ).get('@effect-harness/durable/session')
                if (Option.isNone(saved)) return yield* Effect.die('Saved snapshot missing')
                return saved.value.state
              }).pipe(
                Effect.provide(
                  KeyValueStore.layerSql().pipe(
                    Layer.provide(Layer.succeed(SqlClient.SqlClient, readerSql)),
                  ),
                ),
              ),
            }
            const storage = engineKind === 'sqlite' ? sqlStorage : yield* persisted
            const entered = yield* Deferred.make<void>()
            let firstPhase = true
            let finalized = 0
            let toolCalls = 0
            let modelCalls = 0
            const blocked = <A>(name: string, value: A) =>
              boundary === name && firstPhase
                ? Deferred.succeed(entered, undefined).pipe(
                    Effect.andThen(Effect.never),
                    Effect.ensuring(
                      Effect.sync(() => {
                        finalized++
                      }),
                    ),
                  )
                : Effect.succeed(value)
            const finish = (reason: Response.FinishReason): Response.FinishPartEncoded => ({
              type: 'finish',
              reason,
              usage: {
                inputTokens: {
                  total: 10,
                  uncached: 10,
                  cacheRead: undefined,
                  cacheWrite: undefined,
                },
                outputTokens: { total: 5, text: 5, reasoning: undefined },
              },
              response: undefined,
            })
            const native = yield* NativeModel.make({
              generateText: () =>
                blocked('compact-request', [
                  { type: 'text' as const, text: 'summary' },
                  finish('stop'),
                ]),
              streamText: () =>
                Stream.unwrap(
                  Effect.sync(() => {
                    modelCalls++
                    return modelCalls
                  }).pipe(
                    Effect.map((call) => {
                      if (boundary === 'request' && firstPhase)
                        return Stream.fromIterable<Response.StreamPartEncoded>([
                          { type: 'text-start', id: 'text' },
                          { type: 'text-delta', id: 'text', delta: 'kept partial' },
                        ]).pipe(
                          Stream.concat(Stream.fromEffect(blocked('request', finish('stop')))),
                        )
                      if (boundary.startsWith('tool') && call === 1)
                        return Stream.fromIterable<Response.StreamPartEncoded>([
                          {
                            type: 'tool-call',
                            id: 'call',
                            name: 'work',
                            params: {},
                            providerExecuted: false,
                          },
                          finish('tool-calls'),
                        ])
                      return Stream.fromIterable<Response.StreamPartEncoded>([
                        { type: 'text-start', id: 'text' },
                        { type: 'text-delta', id: 'text', delta: 'answer' },
                        { type: 'text-end', id: 'text' },
                        finish('stop'),
                      ])
                    }),
                  ),
                ),
            })
            const catalogue = Model.layer([
              {
                ref: { provider: 'test', modelId: 'model' },
                model: native,
                contextWindow: 100000,
                maxOutputTokens: 1000,
                configure: () => Effect.succeed(Context.empty()),
              },
            ])
            const declaration = AiTool.make('work', {
              parameters: Schema.Struct({}),
              success: Invocation.Result,
              failure: ToolError,
            })
              .addDependency(Invocation.ToolCall)
              .addDependency(Ownership.Current)
            const toolkit = Toolkit.make(declaration)
            const tools = yield* Tool.bind(
              toolkit,
              {
                work: {
                  replay: 'safe',
                  project: (value) => Tool.decodeResult('fixture', value),
                },
              },
              [Ownership.Current],
            ).pipe(
              Effect.provide(
                toolkit.toLayer({
                  work: () =>
                    Effect.gen(function* () {
                      toolCalls++
                      const call = yield* Invocation.ToolCall
                      yield* call.output('kept tool output')
                      yield* call.details({ kept: true })
                      yield* Ownership.memo('kept', Schema.String, Effect.succeed('memo'))
                      return yield* blocked('tool', {})
                    }).pipe(
                      Effect.mapError(
                        (error) =>
                          new ToolError({
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
                name: 'lifecycle',
                tools,
                sections: [
                  { key: 'preamble', tag: false, render: () => blocked('prepare', 'preamble') },
                ],
                hooks: [
                  {
                    operation: 'tool',
                    handlers: { beforeTool: () => blocked('tool-intent', undefined) },
                  },
                  {
                    operation: 'compaction',
                    handlers: { beforeCompact: () => blocked('compact-select', undefined) },
                  },
                ],
              },
            ])
            const configuration = Conversation.layerConfiguration({
              settings: {
                progress: { partialIntervalMs: 0, outputIntervalMs: 0 },
                compaction: { enabled: false, reserveTokens: 100, keepRecentTokens: 0 },
              },
            })
            const creation = yield* Layer.build(
              Conversation.layerCreation.pipe(Layer.provide(configuration)),
            )
            const open = storage.open.pipe(
              Effect.provideService(
                Session.CreationHook,
                Context.get(creation, Session.CreationHook),
              ),
            )
            const firstScope = yield* Effect.acquireRelease(Scope.make(), (owned, exit) =>
              Scope.close(owned, exit),
            )
            const first = yield* Scope.provide(open, firstScope)
            yield* first.root()
            yield* first.transaction(
              Effect.fnUntraced(function* (tx) {
                const agent = yield* tx.doc(Conversation.AgentDoc, {
                  owner: Record.ROOT_CONVERSATION_ID,
                })
                agent.model = { provider: 'test', modelId: 'model' }
                if (boundary.startsWith('compact'))
                  for (const text of ['old context', 'recent context'])
                    yield* tx.appendEntry(Record.ROOT_CONVERSATION_ID, {
                      kind: 'harness.user',
                      model: [
                        yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.UserMessage))(
                          Prompt.userMessage({ content: [Prompt.textPart({ text })] }),
                        ),
                      ],
                    })
              }),
            )
            const current = yield* Ref.make(first)
            const engine =
              engineKind === 'sqlite'
                ? ClusterWorkflowEngine.layer.pipe(
                    Layer.provideMerge(
                      SingleRunner.layer({
                        runnerStorage: 'memory',
                        shardingConfig: {
                          shardsPerGroup: 1,
                          entityMessagePollInterval: '10 millis',
                          entityReplyPollInterval: '10 millis',
                        },
                      }),
                    ),
                  )
                : WorkflowEngine.layerMemory
            const runtime = Executors.layer.pipe(
              Layer.provideMerge(engine),
              Layer.provide(configuration),
              Layer.provide(catalogue),
              Layer.provide(Harness.layer.pipe(Layer.provide(Layer.mergeAll(registry, catalogue)))),
              Layer.provide(
                Layer.succeed(Directory.SessionDirectory, { resolve: () => Ref.get(current) }),
              ),
            )
            yield* Effect.gen(function* () {
              const compaction = boundary.startsWith('compact')
                ? yield* first.transaction((tx) =>
                    CompactionExecutor.make(
                      tx,
                      Identity.SessionId.make('lifecycle'),
                      Record.ROOT_CONVERSATION_ID,
                      'manual',
                    ),
                  )
                : undefined
              const input = {
                sessionId: Identity.SessionId.make('lifecycle'),
                conversationId: Record.ROOT_CONVERSATION_ID,
                requestId: Identity.RequestId.make(boundary),
                submission: {
                  _tag: 'input' as const,
                  type: 'input' as const,
                  message: Prompt.userMessage({
                    content: [Prompt.textPart({ text: 'question' })],
                  }),
                },
              }
              if (compaction === undefined) yield* Submission.execute(input, { discard: true })
              else yield* Compaction.execute(compaction, { discard: true })
              yield* Deferred.await(entered)
              if (boundary === 'request')
                yield* until(
                  first.snapshot(Inbox.LiveDoc, { owner: Record.ROOT_CONVERSATION_ID }).pipe(
                    Effect.map(Option.getOrUndefined),
                    Effect.map((live) =>
                      (JSON.stringify(live?.value.generation?.message) ?? '').includes(
                        'kept partial',
                      ),
                    ),
                    Effect.tap(() => Effect.sleep('1 millis')),
                  ),
                )
              const before = yield* storage.state
              yield* Scope.close(firstScope, Exit.void)
              assert.strictEqual(finalized, 1)
              assert.deepStrictEqual(yield* storage.state, before)
              assert.isTrue(
                before.tasks.every(
                  (task) => !task.abortRequested && task.state.status !== 'terminal',
                ),
              )
              let taskKind = 'harness.generation'
              if (boundary.startsWith('tool')) taskKind = 'harness.tool'
              else if (boundary.startsWith('compact')) taskKind = 'harness.compaction'
              const task = before.tasks.find((item) => item.kind === taskKind)
              assert.isDefined(task)
              if (boundary === 'tool') assert.deepStrictEqual(task?.memos, { kept: 'memo' })
              const reopened = yield* open
              yield* Ref.set(current, reopened)
              firstPhase = false
              for (const running of before.tasks.toReversed()) {
                const binding = yield* Schema.decodeUnknownEffect(Ownership.Binding)(running.input)
                let declaration: typeof Generation | typeof ToolCall | typeof Compaction =
                  Generation
                if (running.kind === 'harness.tool') declaration = ToolCall
                else if (running.kind === 'harness.compaction') declaration = Compaction
                yield* declaration.resume(binding.executionId)
              }
              if (compaction === undefined)
                assert.strictEqual((yield* Submission.execute(input)).status, 'done')
              else yield* Compaction.execute(compaction)
              yield* until(
                reopened.committed.pipe(
                  Effect.map(
                    (state) =>
                      state.tasks.every((item) => item.state.status === 'terminal') &&
                      state.submissions.every((item) => item.status === 'done'),
                  ),
                  Effect.tap(() => Effect.sleep('1 millis')),
                ),
              )
              assert.isTrue((yield* reopened.committed).tasks.every((item) => !item.abortRequested))
              if (boundary === 'tool') assert.strictEqual(toolCalls, 2)
            }).pipe(Effect.provide(runtime))
          }).pipe(Effect.provideService(SqlClient.SqlClient, sql))
        }).pipe(Effect.provide(NodeServices.layer)),
      )

  for (const transactional of [false, true])
    // Native SQL worker acquisition and transaction notifications progress outside TestClock.
    it.live(
      `SQLite native Activity close rolls back and resumes (${transactional ? 'transactional hook' : 'provider body'})`,
      () =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'session-lifecycle-' })
          const database = SqliteClient.layer({ filename: path.join(directory, 'state.sqlite') })
          yield* Effect.gen(function* () {
            const physical = Effect.gen(function* () {
              const values = yield* KeyValueStore.KeyValueStore
              const saved = yield* KeyValueStore.toSchemaStore(values, SnapshotStore.Snapshot).get(
                '@effect-harness/durable/session',
              )
              if (Option.isNone(saved)) return yield* Effect.die('Saved snapshot missing')
              return saved.value.state
            }).pipe(Effect.provide(KeyValueStore.layerSql()))
            const open = Effect.gen(function* () {
              const store = yield* SqlStore.make
              return yield* Session.make.pipe(Effect.provideService(Store.Store, store))
            })
            const firstScope = yield* Effect.acquireRelease(Scope.make(), (owned, exit) =>
              Scope.close(owned, exit),
            )
            const first = yield* Scope.provide(open, firstScope)
            yield* first.root()
            const payload = yield* reserve(first)
            const before = yield* physical
            const current = yield* Ref.make(first)
            const entered = yield* Deferred.make<void>()
            let calls = 0
            let cleaned = 0
            const executor = User.toLayer((identity) =>
              Effect.gen(function* () {
                const session = yield* (yield* Directory.SessionDirectory).resolve(
                  identity.sessionId,
                )
                const work = Effect.gen(function* () {
                  calls++
                  if (calls > 1) return { answer: 'resumed SQL' }
                  const wait = Deferred.succeed(entered, undefined).pipe(
                    Effect.andThen(Effect.never),
                    Effect.ensuring(
                      Effect.sync(() => {
                        cleaned++
                      }),
                    ),
                  )
                  if (transactional)
                    return yield* session.transaction(
                      Effect.fnUntraced(function* (tx) {
                        yield* tx.appendEntry(identity.conversationId, { kind: 'uncommitted' })
                        return yield* wait
                      }),
                    )
                  return yield* wait
                })
                const activity = Activity.make({
                  name: 'SQL body',
                  success: Schema.Json,
                  error: ExecutionError,
                  execute: Cancellation.activity(identity, session, work).pipe(
                    Effect.mapError(
                      (error) =>
                        new ExecutionError({
                          reason: new Storage({ message: error.message, cause: error }),
                        }),
                    ),
                  ),
                })
                return yield* Structured.evaluate(identity, session, activity)
              }).pipe(
                Effect.mapError((error) =>
                  error._tag === 'StorageError'
                    ? new ExecutionError({
                        reason: new Storage({ message: error.message, cause: error }),
                      })
                    : error,
                ),
              ),
            )
            const engine = ClusterWorkflowEngine.layer.pipe(
              Layer.provideMerge(
                SingleRunner.layer({
                  runnerStorage: 'memory',
                  shardingConfig: {
                    shardsPerGroup: 1,
                    entityMessagePollInterval: '10 millis',
                    entityReplyPollInterval: '10 millis',
                  },
                }),
              ),
            )
            const runtime = executor.pipe(
              Layer.provideMerge(engine),
              Layer.provide(Cancellation.layer),
              Layer.provide(Ownership.layerDeclarations([User])),
              Layer.provide(
                Layer.succeed(Directory.SessionDirectory, { resolve: () => Ref.get(current) }),
              ),
            )
            yield* Effect.gen(function* () {
              const id = yield* User.execute(payload, { discard: true })
              yield* Deferred.await(entered)
              yield* Scope.close(firstScope, Exit.void)
              assert.strictEqual(cleaned, 1)
              assert.deepStrictEqual(yield* physical, before)
              yield* until(
                User.poll(id).pipe(
                  Effect.map(
                    (result) => Option.isSome(result) && result.value._tag === 'Suspended',
                  ),
                  Effect.tap(() => Effect.sleep('1 millis')),
                ),
              )
              const reopened = yield* open
              yield* Ref.set(current, reopened)
              yield* User.resume(id)
              assert.deepStrictEqual(yield* User.execute(payload), {
                status: 'completed',
                result: { answer: 'resumed SQL' },
              })
              assert.strictEqual(calls, 2)
              assert.isFalse(
                (yield* reopened.task(payload.taskId).pipe(Effect.map(Option.getOrUndefined)))
                  ?.abortRequested ?? true,
              )
              assert.notMatch(JSON.stringify(yield* physical), /uncommitted/)
            }).pipe(Effect.provide(runtime))
          }).pipe(Effect.provide(database))
        }).pipe(Effect.provide(NodeServices.layer)),
    )

  // Native SQL worker acquisition and transaction notifications progress outside TestClock.
  it.effect('owner Scope shutdown joins body cleanup without recording an abort or outcome', () =>
    Effect.gen(function* () {
      const storage = yield* persisted
      const scope = yield* Scope.make()
      const session = yield* Scope.provide(storage.open, scope)
      yield* session.root()
      const payload = yield* reserve(session)
      const before = yield* storage.state
      const entered = yield* Deferred.make<void>()
      let finalized = false
      const running = yield* Cancellation.run(
        payload,
        session,
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(
            Effect.sync(() => {
              finalized = true
            }),
          ),
        ),
      ).pipe(Effect.provide(Cancellation.layer), Effect.exit, Effect.forkScoped)
      yield* awaitTransition(Deferred.await(entered))
      yield* awaitTransition(Scope.close(scope, Exit.succeed(undefined)))
      assert.isTrue(finalized)
      const result = yield* awaitTransition(Fiber.join(running))
      assert.isTrue(Exit.isFailure(result))
      if (Exit.isFailure(result)) assert.isTrue(Cause.hasInterruptsOnly(result.cause))
      assert.deepStrictEqual(yield* storage.state, before)
    }),
  )

  it.effect('durable abort still requires a physical mark and joins active body finalizers', () =>
    Effect.gen(function* () {
      const storage = yield* persisted
      const session = yield* storage.open
      yield* session.root()
      const payload = yield* reserve(session)
      const entered = yield* Deferred.make<void>()
      const finalizing = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      yield* Effect.gen(function* () {
        const running = yield* Cancellation.run(
          payload,
          session,
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(
              Deferred.succeed(finalizing, undefined).pipe(Effect.andThen(Deferred.await(release))),
            ),
          ),
        ).pipe(Effect.result, Effect.forkScoped)
        yield* awaitTransition(Deferred.await(entered))
        const state = yield* session.committed
        yield* Cancellation.cancel(payload.sessionId, {
          tasks: state.tasks,
          conversations: state.conversations,
        })
        assert.isTrue(yield* pending(running))
        const marked = yield* Cancellation.mark(session, {
          _tag: 'task' as const,
          kind: 'task',
          id: payload.taskId,
        })
        const cancelWaiter = yield* Cancellation.cancel(payload.sessionId, marked).pipe(
          Effect.forkScoped,
        )
        yield* awaitTransition(Deferred.await(finalizing))
        assert.isTrue(yield* pending(cancelWaiter))
        yield* Deferred.succeed(release, undefined)
        yield* awaitTransition(Fiber.join(cancelWaiter))
        const result = yield* awaitTransition(Fiber.join(running))
        assertFailure(
          result,
          new ExecutionError({ reason: new Aborted({ message: 'Task has a durable abort mark' }) }),
        )
        assert.strictEqual(
          (yield* session.task(payload.taskId).pipe(Effect.map(Option.getOrUndefined)))
            ?.abortRequested,
          true,
        )
        assert.strictEqual(yield* session.isClosed, false)
      }).pipe(
        Effect.ensuring(Deferred.succeed(release, undefined)),
        Effect.provide(Cancellation.layer),
      )
    }),
  )
})
