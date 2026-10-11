import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Scheduler from 'effect/Scheduler'
import * as Scope from 'effect/Scope'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as Events from 'effect-harness/AgentEvent'
import * as View from 'effect-harness/ConversationView'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import type * as Observation from 'effect-harness/Observation'
import type { ObservationError } from 'effect-harness/ObservationError'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as StorageFormat from 'effect-harness/StorageFormat'
import * as Graph from 'effect-harness/TaskGraph'
import * as Transaction from 'effect-harness/Transaction'
import * as Conversation from 'effect-harness/Conversation'
import * as Registry from 'effect-harness/Registry'
import * as Task from 'effect-harness/Task'
import { TaskExecution } from 'effect-harness/TaskExecution'
import * as Tool from 'effect-harness/Tool'
import { ToolExecution } from 'effect-harness/ToolExecution'
import * as ToolInvocation from 'effect-harness/ToolInvocation'
import * as Toolkit from 'effect-harness/Toolkit'
import type * as OutputRetention from 'effect-harness/OutputRetention'
import * as ContextPlan from 'effect-harness/ContextPlan'
import * as Projection from 'effect-harness/ContextProjection'
import * as RunControl from 'effect-harness/RunControl'
import * as Submission from 'effect-harness/Submission'
import type * as Response from 'effect/ai/Response'
import * as AiError from 'effect/ai/AiError'
import * as Clock from 'effect/Clock'
import * as TestClock from 'effect/testing/TestClock'
import * as Model from 'effect-harness/Model'
import * as Settings from 'effect-harness/Settings'
import * as Hook from 'effect-harness/Hook'
import * as CompactionTask from 'effect-harness/CompactionTask'
import { CompactionRequest } from 'effect-harness/CompactionRequest'
import * as Prepared from 'effect-harness/PreparedRequest'
import * as Usage from 'effect-harness/Usage'
import * as ToolProgress from 'effect-harness/ToolProgress'
import type { Failure as NativeTaskFailure } from 'effect-harness/TaskError'
import * as Document from 'effect-harness/Document'
import { StorageError } from 'effect-harness/StorageError'

const bun = 'Bun' in globalThis
const services = bun ? (await import('@effect/platform-bun')).BunServices.layer : NodeServices.layer
const root = Record.ROOT_CONVERSATION_ID
const HydrationProbe = Context.Reference<boolean>('test/ObservationReview/HydrationProbe', {
  defaultValue: () => false,
})
type Backend = 'memory' | 'sqlite' | 'jsonl'

const nativeLayer = Effect.fn(function* (backend: Backend) {
  const fs = yield* FileSystem.FileSystem
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'observation-review-' })
  if (backend === 'memory') return Storage.layerMemory
  if (backend === 'jsonl') return Storage.layerJsonl({ filePath: `${directory}/store.jsonl` })
  const sql = bun
    ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
    : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient'))).layer
  return Storage.layerSql.pipe(Layer.provide(sql({ filename: `${directory}/store.sqlite` })))
})

interface Replica {
  readonly revision: { readonly epoch: string; readonly cursor: number }
  readonly value: unknown
}
interface Port<S extends Replica, F> {
  readonly stream: Stream.Stream<F, Observation.Failure>
  readonly snapshot: Effect.Effect<S, Observation.Failure>
  readonly initial: (frame: F) => S
  readonly reduce: (state: S, frame: F) => Effect.Effect<S, ObservationError>
}

const noYieldScheduler: Scheduler.Scheduler = {
  executionMode: 'async',
  shouldYield: () => false,
  makeDispatcher: () => new Scheduler.MixedScheduler().makeDispatcher(),
}

const atomicReplicaRace = Effect.fn(function* <S extends Replica, F>(
  backend: Backend,
  makePort: (client: Harness.HarnessService) => Effect.Effect<Port<S, F>, Observation.Failure>,
) {
  const native = yield* Layer.build(yield* nativeLayer(backend))
  const storage = Context.get(native, Storage.Storage)
  const format = Context.get(native, StorageFormat.StorageFormat)
  const hydrated = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
  let hold = false
  const wrapped: Storage.Storage['Service'] = {
    ...storage,
    scanTasks: (query) =>
      Stream.unwrap(
        Effect.gen(function* () {
          // Collect real native facts first: a removed permit must expose mixed facts.
          const tasks = yield* storage.scanTasks(query).pipe(Stream.runCollect)
          if (hold && (yield* HydrationProbe)) {
            hold = false
            yield* Deferred.succeed(hydrated, undefined)
            yield* Deferred.await(release)
          }
          return Stream.fromIterable(tasks)
        }),
      ),
  }
  const model = yield* LanguageModel.make({
    generateText: () => Effect.die('Observation must not invoke a provider'),
    streamText: () => Stream.die('Observation must not invoke a provider'),
  })
  const runtime = yield* HarnessRuntime.make().pipe(
    Effect.provideContext(
      Context.add(Context.add(native, Storage.Storage, wrapped), StorageFormat.StorageFormat, {
        ...format,
        storage: wrapped,
      }),
    ),
    Effect.provideService(LanguageModel.LanguageModel, model),
  )
  const owner = yield* Session.commit(runtime.session, (tx) =>
    Effect.gen(function* () {
      yield* Transaction.ensureRoot(tx)
      return yield* Transaction.createTask(tx, {
        conversationId: root,
        kind: 'observation.review.hydration',
        version: 1,
        input: null,
        background: false,
        abortRequested: false,
        state: { status: 'pending', checkpoint: { phase: 'before' } },
      })
    }),
  )
  const clients = yield* Effect.all([Harness.make, Harness.make]).pipe(
    Effect.provideService(HarnessBackend, runtime.backend),
  )
  const port = yield* makePort(clients[0])
  const second = yield* makePort(clients[1])
  const before = yield* second.snapshot
  const consumer = yield* Scope.make()
  yield* Effect.addFinalizer((exit) => Scope.close(consumer, exit))
  hold = true
  const attaching = yield* Effect.gen(function* () {
    const pull = yield* Stream.toPull(port.stream).pipe(
      Effect.provideService(Scope.Scope, consumer),
    )
    return { pull, frames: yield* pull }
  }).pipe(Effect.provideService(HydrationProbe, true), Effect.forkChild)
  yield* Deferred.await(hydrated)
  let admitted = false
  const writer = yield* Session.commit(runtime.session, (tx) =>
    Effect.gen(function* () {
      admitted = true
      yield* Transaction.putTask(tx, {
        ...owner,
        state: { status: 'running', checkpoint: { phase: 'after' } },
      })
      const owned = yield* Transaction.createConversation(tx, {
        ownership: { _tag: 'task', taskId: owner.id },
      })
      const entry = yield* Transaction.appendEntry(tx, root, {
        kind: 'observation.review.atomic',
        data: { ownedConversation: owned.id, task: owner.id },
      })
      return { owned, entry }
    }),
  ).pipe(
    // Admission reaches its first actual IO synchronously if the permit is absent.
    // This chooses the adversarial schedule; all oracles below compare public state.
    Effect.provideService(Scheduler.Scheduler, noYieldScheduler),
    Effect.forkChild({ startImmediately: true }),
  )
  if (admitted) yield* Fiber.join(writer)
  yield* Deferred.succeed(release, undefined)
  const attached = yield* Fiber.join(attaching)
  const pull = attached.pull
  const initial = port.initial(attached.frames[0])
  assert.deepStrictEqual(
    initial.value,
    before.value,
    'FULL initial state must precede queued commit',
  )
  const committed = yield* Fiber.join(writer)
  const current = yield* second.snapshot
  let replica = initial
  while (replica.revision.cursor < current.revision.cursor)
    for (const frame of yield* pull) replica = yield* port.reduce(replica, frame)
  assert.deepStrictEqual(replica, current, 'FULL folded state must equal coherent current snapshot')
  assert.strictEqual(current.revision.cursor, initial.revision.cursor + 1)
  const actual = yield* storage.scanEntries({ conversationId: root }).pipe(Stream.runCollect)
  assert.deepStrictEqual(
    actual.filter((entry) => entry.kind === 'observation.review.atomic'),
    [committed.entry],
  )
  const persisted = yield* storage.task(owner.id)
  assert.isTrue(Option.isSome(persisted))
  if (Option.isSome(persisted)) assert.strictEqual(persisted.value.state.status, 'running')
  const lateScope = yield* Scope.make()
  const latePull = yield* Stream.toPull(second.stream).pipe(
    Effect.provideService(Scope.Scope, lateScope),
  )
  assert.deepStrictEqual(
    second.initial((yield* latePull)[0]),
    current,
    'FULL reused-mount late join',
  )
  yield* Scope.close(lateScope, Exit.void)
  yield* Scope.close(consumer, Exit.void)
  const detached = yield* second.snapshot
  assert.notStrictEqual(detached.revision.epoch, current.revision.epoch)
  assert.deepStrictEqual(detached.value, current.value, 'FULL independently rehydrated state')
})

const observationPorts = {
  graph: (client: Harness.HarnessService): Effect.Effect<Port<Graph.Snapshot, Graph.Frame>> =>
    Effect.succeed({
      stream: Graph.watch(client),
      snapshot: Graph.snapshot(client),
      initial: (frame) =>
        frame._tag === 'Snapshot' ? frame : assert.fail('Initial graph Snapshot required'),
      reduce: Graph.reduce,
    }),
  view: Effect.fn(function* (client: Harness.HarnessService) {
    const conversation = Option.getOrThrow(yield* client.conversation(root))
    return {
      stream: View.watch(conversation),
      snapshot: View.snapshot(conversation),
      initial: (frame: View.Frame) =>
        frame._tag === 'Snapshot' ? frame : assert.fail('Initial view Snapshot required'),
      reduce: View.reduce,
    } satisfies Port<View.Snapshot, View.Frame>
  }),
  events: Effect.fn(function* (client: Harness.HarnessService) {
    const conversation = Option.getOrThrow(yield* client.conversation(root))
    return {
      stream: Events.watch(conversation),
      snapshot: View.snapshot(conversation),
      initial: (frame: Events.Frame) =>
        frame._tag === 'Snapshot' ? frame : assert.fail('Initial event Snapshot required'),
      reduce: Events.reduce,
    } satisfies Port<View.Snapshot, Events.Frame>
  }),
}

const retainedWindowLifecycle = Effect.fn(function* (
  backend: Backend,
  retention: OutputRetention.Policy['retain'],
) {
  const storageContext = yield* Layer.build(yield* nativeLayer(backend))
  const first = yield* Deferred.make<void>()
  const next = yield* Deferred.make<void>()
  const second = yield* Deferred.make<void>()
  const finish = yield* Deferred.make<void>()
  yield* Effect.addFinalizer(() =>
    Effect.all([Deferred.succeed(next, undefined), Deferred.succeed(finish, undefined)]),
  )
  let handlerCalls = 0
  const leaf = Tool.makeResult('review-retained-window', {
    replay: 'safe',
    progress: { minInterval: 0, bytesPerSecond: 1_000_000 },
    outputRetention: { retain: retention, maxBytes: retention === 'full' ? 64 : 6, maxLines: 10 },
  })
  const kit = Toolkit.make(leaf)
  const contract = {
    kind: 'review.retained.parent',
    version: 1,
    input: Schema.Void,
    checkpoint: Schema.Struct({ phase: Schema.Literal('execute') }),
    success: Schema.Void,
    failure: Schema.String,
  }
  const parent = Task.make({
    ...contract,
    initial: (): Task.Checkpoint<typeof contract> => ({ phase: 'execute' }),
    phases: {
      execute: () =>
        Effect.gen(function* () {
          const execution = yield* TaskExecution
          const result = yield* ToolInvocation.invoke(execution, leaf, {}, { key: 'real-window' })
          assert.strictEqual(result.status, 'succeeded')
          yield* execution.commit((tx) =>
            execution.progress(tx, contract, {
              status: 'completing',
              outcome: { status: 'completed', result: undefined },
            }),
          )
        }),
    },
  })
  const registry = yield* Registry.make
  yield* registry.install(
    Registry.withTask(
      Registry.toolkit({ id: 'review.retained', version: 1 }, Toolkit.make()),
      parent,
    ),
  )
  const model = yield* LanguageModel.make({
    generateText: () => Effect.die('Retained observations must not call a provider'),
    streamText: () => Stream.die('Retained observations must not call a provider'),
  })
  const makeRuntime = (scope: Scope.Scope) =>
    HarnessRuntime.make({ registry, tools: kit }).pipe(
      Effect.provideContext(storageContext),
      Effect.provideService(Scope.Scope, scope),
      Effect.provideService(LanguageModel.LanguageModel, model),
      Effect.provide(
        kit.toLayer({
          'review-retained-window': () =>
            Effect.gen(function* () {
              handlerCalls++
              const execution = yield* ToolExecution
              yield* execution.output('alpha\n')
              yield* execution.diagnostic({
                severity: 'info',
                code: 'review',
                message: 'actual first report',
              })
              yield* execution.details({ stage: 'first', replaced: true })
              yield* Deferred.succeed(first, undefined)
              yield* Deferred.await(next)
              yield* execution.output('beta\n')
              yield* execution.details({ stage: 'second' })
              yield* execution.details(null)
              yield* Deferred.succeed(second, undefined)
              yield* Deferred.await(finish)
              return {
                content: [
                  Prompt.filePart({
                    mediaType: 'application/octet-stream',
                    data: new Uint8Array([13, 0, 255]),
                  }),
                ],
                details: { terminal: true },
              }
            }),
        }),
      ),
    )
  const runtimeScope = yield* Scope.make()
  yield* Effect.addFinalizer((exit) => Scope.close(runtimeScope, exit))
  const runtime = yield* makeRuntime(runtimeScope)
  const client = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, runtime.backend))
  const conversation = yield* client.root
  let observer = yield* Scope.make()
  yield* Effect.addFinalizer((exit) => Scope.close(observer, exit))
  let pull = yield* Stream.toPull(Events.watch(conversation)).pipe(
    Effect.provideService(Scope.Scope, observer),
  )
  const initial = (yield* pull)[0]
  if (initial._tag !== 'Snapshot') return assert.fail('Actual initial Snapshot required')
  let replica = initial
  const progressEvents: Array<Extract<Events.Event, { _tag: 'Progress' }>> = []
  const observeRetainedStage = Effect.fn(function* () {
    const current = yield* View.snapshot(conversation)
    while (replica.revision.cursor < current.revision.cursor)
      for (const frame of yield* pull) {
        if (frame._tag === 'Batch')
          for (const event of frame.events)
            if (event._tag === 'Progress') progressEvents.push(event)
        replica = yield* Events.reduce(replica, frame)
      }
    assert.deepStrictEqual(replica, current, 'FULL retained native batch fold')
    yield* Scope.close(observer, Exit.void)
    observer = yield* Scope.make()
    pull = yield* Stream.toPull(Events.watch(conversation)).pipe(
      Effect.provideService(Scope.Scope, observer),
    )
    const remounted = (yield* pull)[0]
    if (remounted._tag !== 'Snapshot') return assert.fail('Remount Snapshot required')
    assert.notStrictEqual(remounted.revision.epoch, current.revision.epoch)
    assert.deepStrictEqual(
      remounted.value,
      current.value,
      'FULL last-detach/remount retained value',
    )
    replica = remounted
    return current.value
  })
  const ref = yield* runtime.scheduler.start(parent, undefined, { conversationId: conversation.id })
  yield* Deferred.await(first)
  const firstValue = yield* observeRetainedStage()
  assert.strictEqual(firstValue.nested.length, 1)
  const firstNested = Option.getOrThrow(Option.fromUndefinedOr(firstValue.nested[0]))
  const firstProgress = Option.getOrThrow(Option.fromUndefinedOr(firstValue.progress[0])).value
  assert.strictEqual(firstNested.availability, 'pending')
  assert.strictEqual(firstProgress.output.text, 'alpha\n')
  assert.deepStrictEqual(firstProgress.details, { stage: 'first', replaced: true })
  assert.deepStrictEqual(firstProgress.diagnostics, [
    { severity: 'info', code: 'review', message: 'actual first report' },
  ])
  yield* Deferred.succeed(next, undefined)
  yield* Deferred.await(second)
  const secondValue = yield* observeRetainedStage()
  const secondProgress = Option.getOrThrow(Option.fromUndefinedOr(secondValue.progress[0])).value
  const output = secondProgress.output
  const windows = {
    head: {
      text: 'alpha\n',
      retainedBytes: 6,
      skippedBytes: 5,
      retainedLines: 1,
      skippedLines: 1,
      truncated: true,
    },
    tail: {
      text: 'beta\n',
      retainedBytes: 5,
      skippedBytes: 6,
      retainedLines: 1,
      skippedLines: 1,
      truncated: true,
    },
    full: {
      text: 'alpha\nbeta\n',
      retainedBytes: 11,
      skippedBytes: 0,
      retainedLines: 2,
      skippedLines: 0,
      truncated: false,
    },
  }
  assert.deepStrictEqual(output, { totalBytes: 11, totalLines: 2, ...windows[retention] })
  assert.strictEqual(secondProgress.details, null)
  assert.isTrue(
    progressEvents.some((event) =>
      Schema.toEquivalence(Schema.Json)(event.value.details ?? null, { stage: 'second' }),
    ),
    'Acknowledged replaced details must be in an actual committed native batch',
  )
  yield* Deferred.succeed(finish, undefined)
  assert.strictEqual((yield* runtime.scheduler.wait(ref)).status, 'completed')
  const terminal = yield* observeRetainedStage()
  const terminalProgress = Option.getOrThrow(Option.fromUndefinedOr(terminal.progress[0])).value
  const terminalNested = Option.getOrThrow(Option.fromUndefinedOr(terminal.nested[0]))
  const receipt = Option.getOrThrow(Option.fromUndefinedOr(terminalNested.receipts[0]))
  assert.strictEqual(terminalProgress.status, 'completed')
  assert.deepStrictEqual(terminalProgress.output, output)
  assert.strictEqual(terminalNested.availability, 'available')
  assert.deepStrictEqual(
    receipt.result.structuredOutput,
    Prompt.filePart({
      mediaType: 'application/octet-stream',
      data: new Uint8Array([13, 0, 255]),
    }),
  )
  assert.deepStrictEqual(terminal.usage, yield* Conversation.usage(conversation))
  for (const id of [ref.id, terminalNested.taskId])
    assert.deepStrictEqual(
      yield* Session.scanDocuments(runtime.session, { scope: { _tag: 'task', taskId: id } }).pipe(
        Stream.runCollect,
      ),
      [],
    )
  yield* Scope.close(observer, Exit.void)
  yield* Scope.close(runtimeScope, Exit.void)
  const coldScope = yield* Scope.make()
  yield* Effect.addFinalizer((exit) => Scope.close(coldScope, exit))
  const cold = yield* makeRuntime(coldScope)
  const coldClient = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, cold.backend))
  const coldSnapshot = yield* View.snapshot(yield* coldClient.root)
  assert.deepStrictEqual(coldSnapshot.value, terminal)
  assert.strictEqual(handlerCalls, 1, 'Cold public observation must not replay the actual tool')
})

const canonicalRecordedContext = Effect.fn(function* (
  session: Session.Session,
  conversationId: Record.ConversationId,
) {
  const stored = yield* Session.snapshot(
    session,
    RunControl.contextProtocol,
    RunControl.target(conversationId),
  )
  return Option.isSome(stored)
    ? yield* ContextPlan.readWithProtocol(session, conversationId, stored.value.value)
    : yield* Projection.read(session, conversationId)
})

const legacyForkOracle = Effect.fn(function* (backend: Backend, mode: ContextPlan.LegacyMode) {
  const native = yield* Layer.build(yield* nativeLayer(backend))
  let providers = 0
  const model = yield* LanguageModel.make({
    generateText: () => {
      providers++
      return Effect.die('History observation cannot execute a provider')
    },
    streamText: () => {
      providers++
      return Stream.die('History observation cannot execute a provider')
    },
  })
  const open = (scope: Scope.Scope) =>
    HarnessRuntime.make().pipe(
      Effect.provideContext(native),
      Effect.provideService(LanguageModel.LanguageModel, model),
      Effect.provideService(Scope.Scope, scope),
    )
  const runtimeScope = yield* Scope.make()
  yield* Effect.addFinalizer((exit) => Scope.close(runtimeScope, exit))
  const runtime = yield* open(runtimeScope)
  const client = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, runtime.backend))
  const parent = yield* client.create({ agent: { instructions: 'EXPLICIT_HISTORICAL_AGENT' } })
  const observer = yield* Scope.make()
  yield* Effect.addFinalizer((exit) => Scope.close(observer, exit))
  const pull = yield* Stream.toPull(Events.watch(parent)).pipe(
    Effect.provideService(Scope.Scope, observer),
  )
  const initial = (yield* pull)[0]
  if (initial._tag !== 'Snapshot') return assert.fail('Actual historical Snapshot required')
  let replica = initial
  const foldHistorical = Effect.fn(function* () {
    const current = yield* View.snapshot(parent)
    while (replica.revision.cursor < current.revision.cursor)
      for (const frame of yield* pull) replica = yield* Events.reduce(replica, frame)
    assert.deepStrictEqual(replica, current, 'FULL historical event fold')
    assert.deepStrictEqual(
      current.value.context,
      yield* canonicalRecordedContext(runtime.session, parent.id),
    )
    assert.deepStrictEqual(current.value.agent, yield* Conversation.agent(parent))
    return current
  })
  const legacy = yield* Session.commit(runtime.session, (tx) =>
    Effect.gen(function* () {
      const a = yield* Transaction.appendEntry(tx, parent.id, {
        kind: 'input',
        head: 'self',
        model: [{ role: 'user', content: 'LEGACY_A' }],
      })
      const b = yield* Transaction.appendEntry(tx, parent.id, {
        kind: 'assistant',
        head: 'self',
        model: [{ role: 'assistant', content: 'LEGACY_B' }],
      })
      return { a, b }
    }),
  )
  const before = yield* foldHistorical()
  assert.deepStrictEqual(
    before.value.context.activeEntries.map((e) => e.id),
    [legacy.b.id],
  )
  const protocol = yield* ContextPlan.activateProtocol(
    { conversationId: parent.id, tail: legacy.b.id },
    mode,
  )
  yield* Session.commit(runtime.session, (tx) =>
    Effect.gen(function* () {
      yield* Transaction.ensureDocument(
        tx,
        RunControl.contextProtocol,
        RunControl.target(parent.id),
      )
      yield* Transaction.setDocument(
        tx,
        RunControl.contextProtocol,
        RunControl.target(parent.id),
        protocol,
      )
    }),
  )
  const recorded = yield* foldHistorical()
  const expectedLegacy = mode === 'preserveManaged' ? [legacy.a.id, legacy.b.id] : [legacy.b.id]
  assert.deepStrictEqual(
    recorded.value.context.activeEntries.map((e) => e.id),
    expectedLegacy,
    'G13 literal recorded legacy mode selects actual entry IDs',
  )
  const explicitHonorAll = yield* ContextPlan.readWithProtocol(
    runtime.session,
    parent.id,
    protocol,
    { legacyMode: 'honorAll' },
  )
  assert.deepStrictEqual(
    explicitHonorAll.activeEntries.map((e) => e.id),
    [legacy.b.id],
  )
  const later = yield* Session.commit(runtime.session, (tx) =>
    Effect.gen(function* () {
      const head = yield* Transaction.appendEntry(tx, parent.id, {
        kind: 'input',
        head: 'self',
        model: [{ role: 'user', content: 'POST_ACTIVATION' }],
      })
      const edit = yield* Transaction.appendEntry(tx, parent.id, {
        kind: 'edit',
        edits: [
          {
            _tag: 'replace',
            target: head.id,
            messages: [{ role: 'user', content: 'EDITED_POST_ACTIVATION' }],
          },
        ],
      })
      return { head, edit }
    }),
  )
  const edited = yield* foldHistorical()
  assert.deepStrictEqual(
    edited.value.context.activeEntries.map((e) => e.id),
    [later.head.id, later.edit.id],
  )
  assert.strictEqual(edited.value.context.bounds.head?.head, later.head.id)
  assert.isTrue(JSON.stringify(edited.value.context.prompt).includes('EDITED_POST_ACTIVATION'))
  yield* Conversation.configure(parent, { instructions: 'LATER_PARENT_AGENT' })
  yield* foldHistorical()
  const oldFork = yield* Conversation.fork(parent, { at: legacy.b.id })
  const newFork = yield* Conversation.fork(parent, { at: later.edit.id })
  const oldState = yield* View.snapshot(oldFork)
  assert.deepStrictEqual(
    oldState.value.context.activeEntries.map((e) => e.id),
    expectedLegacy,
  )
  const newState = yield* View.snapshot(newFork)
  assert.deepStrictEqual(
    newState.value.context.activeEntries.map((e) => e.id),
    [later.head.id, later.edit.id],
  )
  for (const child of [oldFork, newFork]) {
    const state = yield* View.snapshot(child)
    assert.deepStrictEqual(
      state.value.context,
      yield* canonicalRecordedContext(runtime.session, child.id),
    )
    assert.deepStrictEqual(state.value.agent, { instructions: 'EXPLICIT_HISTORICAL_AGENT' })
    assert.deepStrictEqual(state.value.agent, yield* Conversation.agent(child))
    const childProtocol = Option.getOrThrow(
      yield* Session.snapshot(
        runtime.session,
        RunControl.contextProtocol,
        RunControl.target(child.id),
      ),
    )
    assert.deepStrictEqual(
      childProtocol.value,
      protocol,
      'Actual fork retains original protocol origin/cutoff',
    )
    assert.deepStrictEqual(state.value.run, {})
    assert.deepStrictEqual(state.value.inbox, { items: [] })
    assert.deepStrictEqual(state.value.live, {})
    assert.deepStrictEqual(state.value.generations, [])
    assert.deepStrictEqual(state.value.nested, [])
    assert.deepStrictEqual(state.value.compactions, { items: [] })
    assert.deepStrictEqual(state.value.usage, yield* Conversation.usage(child))
  }
  yield* Conversation.configure(newFork, { instructions: 'EXPLICIT_CHILD_OVERRIDE' })
  const childOverride = yield* View.snapshot(newFork)
  assert.deepStrictEqual(childOverride.value.agent, { instructions: 'EXPLICIT_CHILD_OVERRIDE' })
  assert.deepStrictEqual(childOverride.value.agent, yield* Conversation.agent(newFork))
  yield* Session.commit(runtime.session, (tx) =>
    Transaction.appendEntry(tx, parent.id, {
      kind: 'input',
      model: [{ role: 'user', content: 'AFTER_BOTH_FORK_CUTOFFS' }],
    }),
  )
  yield* Conversation.configure(parent, { instructions: 'FINAL_PARENT_AGENT' })
  yield* foldHistorical()
  assert.deepStrictEqual((yield* View.snapshot(oldFork)).value, oldState.value)
  assert.deepStrictEqual((yield* View.snapshot(newFork)).value, childOverride.value)
  yield* Scope.close(observer, Exit.void)
  yield* Scope.close(runtimeScope, Exit.void)
  const coldScope = yield* Scope.make()
  yield* Effect.addFinalizer((exit) => Scope.close(coldScope, exit))
  const coldRuntime = yield* open(coldScope)
  const cold = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, coldRuntime.backend))
  for (const [id, expected] of [
    [oldFork.id, oldState.value],
    [newFork.id, childOverride.value],
  ] as const) {
    const handle = Option.getOrThrow(yield* cold.conversation(id))
    const state = yield* View.snapshot(handle)
    assert.deepStrictEqual(state.value, expected, 'FULL readonly historical fork reopen')
    assert.deepStrictEqual(
      state.value.context,
      yield* canonicalRecordedContext(coldRuntime.session, id),
    )
    assert.deepStrictEqual(state.value.agent, yield* Conversation.agent(handle))
  }
  assert.strictEqual(providers, 0)
})

const nativeFinishPart = (reason: 'stop' | 'tool-calls'): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 3, uncached: 3, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
})

const nativeMountResources = Effect.fn(function* (
  backend: Backend,
  ending: 'detach' | 'close' | 'poison',
  checkpoint: (phase: string) => void = () => {},
) {
  const native = yield* Layer.build(yield* nativeLayer(backend))
  const storage = Context.get(native, Storage.Storage)
  const format = Context.get(native, StorageFormat.StorageFormat)
  let armed = false
  const wrapped = Storage.Storage.of({
    ...storage,
    commit: (writes) => {
      if (!armed) return storage.commit(writes)
      armed = false
      return storage.commit(writes).pipe(
        Effect.andThen(
          Effect.fail(
            new StorageError({
              reason: 'uncertain',
              operation: 'review.real-native-commit',
              message: 'Actual native writes persisted before uncertainty',
            }),
          ),
        ),
      )
    },
  })
  const owner = yield* Scope.make()
  yield* Effect.addFinalizer((exit) => Scope.close(owner, exit))
  const model = yield* LanguageModel.make({
    generateText: () => Effect.die('Mount resource observation must not call a provider'),
    streamText: () => Stream.die('Mount resource observation must not call a provider'),
  })
  const runtime = yield* HarnessRuntime.make().pipe(
    Effect.provideContext(
      native.pipe(
        Context.add(Storage.Storage, wrapped),
        Context.add(StorageFormat.StorageFormat, { ...format, storage: wrapped }),
      ),
    ),
    Effect.provideService(LanguageModel.LanguageModel, model),
    Effect.provideService(Scope.Scope, owner),
  )
  const client = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, runtime.backend))
  const conversation = yield* client.root
  checkpoint('baseline')
  const failed = yield* Events.watch(conversation, { capacity: 0 }).pipe(
    Stream.runHead,
    Effect.exit,
  )
  assert.isTrue(Exit.isFailure(failed), 'Actual invalid-capacity attach fails before allocation')
  checkpoint('failed-attach')
  const expired = yield* Scope.make()
  yield* Scope.close(expired, Exit.void)
  yield* Stream.toPull(Events.watch(conversation)).pipe(
    Effect.provideService(Scope.Scope, expired),
    Effect.exit,
  )
  checkpoint('expired-scope')
  const scopes = [yield* Scope.make(), yield* Scope.make(), yield* Scope.make()]
  yield* Effect.addFinalizer((exit) => Effect.forEach(scopes, (scope) => Scope.close(scope, exit)))
  const graphPull = yield* Stream.toPull(Graph.watch(client)).pipe(
    Effect.provideService(Scope.Scope, scopes[0]!),
  )
  const viewPull = yield* Stream.toPull(View.watch(conversation)).pipe(
    Effect.provideService(Scope.Scope, scopes[1]!),
  )
  const eventPull = yield* Stream.toPull(Events.watch(conversation)).pipe(
    Effect.provideService(Scope.Scope, scopes[2]!),
  )
  const graphInitial = (yield* graphPull)[0]
  const viewInitial = (yield* viewPull)[0]
  const eventInitial = (yield* eventPull)[0]
  if (
    graphInitial._tag !== 'Snapshot' ||
    viewInitial._tag !== 'Snapshot' ||
    eventInitial._tag !== 'Snapshot'
  )
    return assert.fail('Actual graph/view/event initial snapshots required')
  assert.deepStrictEqual(viewInitial, eventInitial)
  checkpoint('mounted')
  if (ending === 'detach') {
    yield* Effect.forEach(scopes, (scope) => Scope.close(scope, Exit.void))
    checkpoint('detached')
    for (let index = 0; index < 140; index++)
      yield* Session.commit(runtime.session, (tx) =>
        Transaction.appendEntry(tx, conversation.id, {
          kind: 'review.no-dormant-observer',
          data: { index },
        }),
      )
    checkpoint('quiet-after-commits')
    const freshScope = yield* Scope.make()
    const freshPull = yield* Stream.toPull(Events.watch(conversation)).pipe(
      Effect.provideService(Scope.Scope, freshScope),
    )
    const fresh = (yield* freshPull)[0]
    if (fresh._tag !== 'Snapshot') return assert.fail('Actual fresh resource snapshot required')
    assert.notStrictEqual(fresh.revision.epoch, eventInitial.revision.epoch)
    assert.strictEqual(
      fresh.value.entries.filter((entry) => entry.kind === 'review.no-dormant-observer').length,
      140,
    )
    checkpoint('fresh-join')
    yield* Scope.close(freshScope, Exit.void)
    checkpoint('fresh-detached')
  } else if (ending === 'close') {
    for (let index = 0; index < 5; index++)
      yield* Session.commit(runtime.session, (tx) =>
        Transaction.appendEntry(tx, conversation.id, {
          kind: 'review.normal-close-resource',
          data: { index },
        }),
      )
    const viewFinal = yield* View.snapshot(conversation)
    const graphFinal = yield* Graph.snapshot(client)
    yield* Scope.close(owner, Exit.void)
    let graph = graphInitial
    let view = viewInitial
    let events = eventInitial
    for (;;) {
      const frame = yield* graphPull.pipe(Effect.exit)
      if (Exit.isFailure(frame)) break
      for (const value of frame.value) graph = yield* Graph.reduce(graph, value)
    }
    for (;;) {
      const frame = yield* viewPull.pipe(Effect.exit)
      if (Exit.isFailure(frame)) break
      for (const value of frame.value) view = yield* View.reduce(view, value)
    }
    for (;;) {
      const frame = yield* eventPull.pipe(Effect.exit)
      if (Exit.isFailure(frame)) break
      for (const value of frame.value) events = yield* Events.reduce(events, value)
    }
    assert.deepStrictEqual(graph, graphFinal)
    assert.deepStrictEqual(view, viewFinal)
    assert.deepStrictEqual(events, viewFinal)
    checkpoint('normal-closed')
    yield* Effect.forEach(scopes, (scope) => Scope.close(scope, Exit.void))
    checkpoint('normal-detached')
  } else {
    armed = true
    const poisoned = yield* Session.commit(runtime.session, (tx) =>
      Transaction.appendEntry(tx, conversation.id, {
        kind: 'review.real-poison-resource',
        data: { durable: true },
      }),
    ).pipe(Effect.exit)
    assert.isTrue(Exit.isFailure(poisoned))
    assert.isTrue(Exit.isFailure(yield* graphPull.pipe(Effect.exit)))
    assert.isTrue(Exit.isFailure(yield* viewPull.pipe(Effect.exit)))
    assert.isTrue(Exit.isFailure(yield* eventPull.pipe(Effect.exit)))
    const durable = yield* storage
      .scanEntries({ conversationId: conversation.id })
      .pipe(Stream.runCollect)
    assert.strictEqual(
      durable.filter((entry) => entry.kind === 'review.real-poison-resource').length,
      1,
    )
    checkpoint('poison-closed')
    yield* Effect.forEach(scopes, (scope) => Scope.close(scope, Exit.void))
    checkpoint('poison-detached')
  }
  yield* Scope.close(owner, Exit.void)
  checkpoint('all-closed')
})

const nativeBorrowedLeaseResources = Effect.fn(function* (
  backend: Backend,
  checkpoint: (phase: string) => void = () => {},
) {
  const native = yield* Layer.build(yield* nativeLayer(backend))
  const boundReady = yield* Deferred.make<Conversation.Conversation>()
  const returnHandler = yield* Deferred.make<void>()
  const callbackEntered = yield* Deferred.make<void>()
  const releaseCallback = yield* Deferred.make<void>()
  yield* Effect.addFinalizer(() =>
    Effect.all([
      Deferred.succeed(returnHandler, undefined),
      Deferred.succeed(releaseCallback, undefined),
    ]),
  )
  const leaf = Tool.makeResult('review-real-borrowed-lease', {
    replay: 'safe',
    success: Schema.String,
    callers: ['tools'],
  })
  const toolkit = Toolkit.make(leaf)
  const contract = {
    kind: 'review.actual-lease-parent',
    version: 1,
    input: Schema.Void,
    checkpoint: Schema.Struct({ phase: Schema.Literal('execute') }),
    success: Schema.Void,
    failure: Schema.String,
  }
  const parent = Task.make({
    ...contract,
    initial: (): Task.Checkpoint<typeof contract> => ({ phase: 'execute' }),
    phases: {
      execute: () =>
        Effect.gen(function* () {
          const execution = yield* TaskExecution
          const receipt = yield* ToolInvocation.invoke(
            execution,
            leaf,
            {},
            { key: 'actual-resource-child' },
          )
          assert.strictEqual(receipt.status, 'succeeded')
          yield* execution.commit((tx) =>
            execution.progress(tx, contract, {
              status: 'completing',
              outcome: { status: 'completed', result: undefined },
            }),
          )
        }),
    },
  })
  const registry = yield* Registry.make
  yield* registry.install(
    Registry.withTask(
      Registry.toolkit({ id: 'review.actual-lease', version: 1 }, Toolkit.make()),
      parent,
    ),
  )
  const model = yield* LanguageModel.make({
    generateText: () => Effect.die('Lease observation must not call a provider'),
    streamText: () => Stream.die('Lease observation must not call a provider'),
  })
  const runtime = yield* HarnessRuntime.make({ registry, tools: toolkit }).pipe(
    Effect.provideContext(native),
    Effect.provideService(LanguageModel.LanguageModel, model),
    Effect.provide(
      toolkit.toLayer({
        'review-real-borrowed-lease': () =>
          Effect.gen(function* () {
            const execution = yield* ToolExecution
            const bound = yield* execution
              .subagent!.conversation(execution.conversationId)
              .pipe(Effect.orDie)
            yield* Deferred.succeed(boundReady, bound)
            yield* Deferred.await(returnHandler)
            return {
              content: [Prompt.textPart({ text: 'Real borrowed handler result' })],
              structuredOutput: 'ACTUAL',
            }
          }),
      }),
    ),
  )
  const client = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, runtime.backend))
  const conversation = yield* client.root
  checkpoint('baseline')
  const ref = yield* runtime.scheduler.start(parent, undefined, { conversationId: conversation.id })
  const bound = yield* Deferred.await(boundReady)
  const revoked = yield* View.watch(bound).pipe(Stream.runDrain, Effect.forkScoped)
  let closed = 0
  let delivered = 0
  const outerScope = yield* Scope.Scope
  const observer = yield* View.watchWith(bound, (frame) =>
    Effect.uninterruptible(
      Effect.gen(function* () {
        assert.notStrictEqual(yield* Scope.Scope, outerScope)
        assert.strictEqual(frame.target.conversationId, conversation.id)
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            closed++
          }),
        )
        yield* Deferred.succeed(callbackEntered, undefined)
        yield* Deferred.await(releaseCallback)
        return frame
      }),
    ),
  ).pipe(
    Stream.tap(() =>
      Effect.sync(() => {
        delivered++
      }),
    ),
    Stream.runDrain,
    Effect.forkScoped,
  )
  yield* Effect.addFinalizer(() =>
    Effect.all([
      Deferred.succeed(returnHandler, undefined),
      Deferred.succeed(releaseCallback, undefined),
    ]),
  )
  yield* Deferred.await(callbackEntered)
  checkpoint('callback-held')
  yield* Deferred.succeed(returnHandler, undefined)
  yield* Fiber.await(revoked)
  assert.isTrue(Exit.isFailure(yield* View.snapshot(bound).pipe(Effect.exit)))
  assert.strictEqual(closed, 0)
  assert.strictEqual(delivered, 0)
  assert.isTrue((yield* runtime.scheduler.inspect(ref.id)).invoking)
  checkpoint('revoked-held')
  yield* Deferred.succeed(releaseCallback, undefined)
  assert.isTrue(Exit.isFailure(yield* Fiber.await(observer)))
  assert.strictEqual(closed, 1)
  assert.strictEqual(delivered, 0)
  checkpoint('callback-released')
  assert.strictEqual((yield* runtime.scheduler.wait(ref)).status, 'completed')
  const final = yield* View.snapshot(conversation)
  assert.strictEqual(final.value.nested.length, 1)
  assert.strictEqual(final.value.nested[0]?.availability, 'available')
  assert.strictEqual(final.value.nested[0]?.receipts[0]?.result.structuredOutput, 'ACTUAL')
  checkpoint('task-retired')
})

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  for (const ending of ['detach', 'close', 'poison'] as const)
    it.live(`independent actual mount resources ${ending}: ${backend}`, () =>
      Effect.scoped(nativeMountResources(backend, ending)).pipe(Effect.provide(services)),
    )
  it.live(`independent actual held borrowed callback resources: ${backend}`, () =>
    Effect.scoped(nativeBorrowedLeaseResources(backend)).pipe(Effect.provide(services)),
  )
}

const progressRetryClearOracle = Effect.fn(function* (backend: Backend) {
  const native = yield* Layer.build(yield* nativeLayer(backend))
  const firstReady = yield* Deferred.make<void>()
  const firstRelease = yield* Deferred.make<void>()
  const retryReady = yield* Deferred.make<void>()
  const retryRelease = yield* Deferred.make<void>()
  yield* Effect.addFinalizer(() =>
    Effect.all([
      Deferred.succeed(firstRelease, undefined),
      Deferred.succeed(retryRelease, undefined),
    ]),
  )
  const retirementDocument = Document.define({
    kind: 'review.retry.retirement-marker',
    version: 1,
    scope: 'task',
    schema: Schema.Struct({ marker: Schema.Boolean }),
    initial: () => ({ marker: true }),
  })
  const contract = {
    kind: 'review.progress.retry-clear',
    version: 1,
    input: Schema.Void,
    checkpoint: Schema.Struct({ phase: Schema.Literals(['first', 'retry']) }),
    success: Schema.Void,
    failure: Schema.String,
  }
  const program = Task.make({
    ...contract,
    initial: (): Task.Checkpoint<typeof contract> => ({ phase: 'first' }),
    phases: {
      first: () =>
        Effect.gen(function* () {
          const execution = yield* TaskExecution
          const writer = yield* ToolProgress.make<NativeTaskFailure>(
            { commit: execution.commit },
            {
              conversationId: execution.conversationId,
              callId: `actual-task:${execution.taskId}`,
              attempt: 0,
              policy: { minInterval: 0, bytesPerSecond: 1000000 },
            },
          )
          yield* execution.commit((tx) =>
            Transaction.ensureDocument(tx, retirementDocument, {
              scope: { _tag: 'task', taskId: execution.taskId },
            }),
          )
          yield* writer.output('first\n')
          yield* writer.diagnostic({
            severity: 'info',
            code: 'first',
            message: 'Actual previous attempt report',
          })
          yield* writer.details({ stage: 'first' })
          yield* Deferred.succeed(firstReady, undefined)
          yield* Deferred.await(firstRelease)
          yield* writer.settle('interrupted', (tx) =>
            execution.progress(tx, contract, {
              status: 'pending',
              checkpoint: { phase: 'retry' },
            }),
          )
        }),
      retry: () =>
        Effect.gen(function* () {
          const execution = yield* TaskExecution
          const writer = yield* ToolProgress.make<NativeTaskFailure>(
            { commit: execution.commit },
            {
              conversationId: execution.conversationId,
              callId: `actual-task:${execution.taskId}`,
              attempt: 1,
              recovery: 'retry',
              policy: { minInterval: 0, bytesPerSecond: 1000000 },
            },
          )
          yield* Deferred.succeed(retryReady, undefined)
          yield* Deferred.await(retryRelease)
          yield* writer.output('new\n')
          yield* writer.details({ stage: 'replacement' })
          yield* writer.settle('completed', (tx) =>
            execution.progress(tx, contract, {
              status: 'completing',
              outcome: { status: 'completed', result: undefined },
            }),
          )
        }),
    },
  })
  const registry = yield* Registry.make
  yield* registry.install(
    Registry.withTask(
      Registry.toolkit({ id: 'review.progress.retry-clear', version: 1 }, Toolkit.make()),
      program,
    ),
  )
  const model = yield* LanguageModel.make({
    generateText: () => Effect.die('Retry writer must not call provider'),
    streamText: () => Stream.die('Retry writer must not call provider'),
  })
  const runtime = yield* HarnessRuntime.make({ registry }).pipe(
    Effect.provideContext(native),
    Effect.provideService(LanguageModel.LanguageModel, model),
  )
  const client = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, runtime.backend))
  const conversation = yield* client.root
  let consumer = yield* Scope.make()
  yield* Effect.addFinalizer((exit) => Scope.close(consumer, exit))
  let pull = yield* Stream.toPull(Events.watch(conversation)).pipe(
    Effect.provideService(Scope.Scope, consumer),
  )
  const initial = (yield* pull)[0]
  if (initial._tag !== 'Snapshot') return assert.fail('Actual retry initial Snapshot required')
  let replica = initial
  const clearedRetryState = Effect.fn(function* () {
    const snapshot = yield* View.snapshot(conversation)
    while (replica.revision.cursor < snapshot.revision.cursor)
      for (const frame of yield* pull) replica = yield* Events.reduce(replica, frame)
    assert.deepStrictEqual(replica, snapshot, 'FULL actual retry state fold')
    yield* Scope.close(consumer, Exit.void)
    consumer = yield* Scope.make()
    pull = yield* Stream.toPull(Events.watch(conversation)).pipe(
      Effect.provideService(Scope.Scope, consumer),
    )
    const fresh = (yield* pull)[0]
    if (fresh._tag !== 'Snapshot') return assert.fail('Actual retry remount Snapshot required')
    assert.notStrictEqual(fresh.revision.epoch, snapshot.revision.epoch)
    assert.deepStrictEqual(fresh.value, snapshot.value, 'FULL actual retry last detach/remount')
    replica = fresh
    return snapshot.value
  })
  const ref = yield* runtime.scheduler.start(program, undefined, {
    conversationId: conversation.id,
  })
  yield* Deferred.await(firstReady)
  const first = yield* clearedRetryState()
  const previous = first.progress[0]!.value
  assert.strictEqual(previous.attempt, 0)
  assert.deepStrictEqual(previous.details, { stage: 'first' })
  assert.strictEqual(previous.diagnostics.length, 1)
  assert.strictEqual(previous.output.text, 'first\n')
  assert.isAbove(
    (yield* Session.scanDocuments(runtime.session, {
      scope: { _tag: 'task', taskId: ref.id },
    }).pipe(Stream.runCollect)).length,
    0,
  )
  yield* Deferred.succeed(firstRelease, undefined)
  yield* Deferred.await(retryReady)
  const cleared = yield* clearedRetryState()
  const actual = cleared.progress[0]!.value
  assert.strictEqual(actual.callId, previous.callId)
  assert.strictEqual(actual.attempt, 1, 'G10 actual newer retry clears prior reports')
  assert.strictEqual(actual.status, 'running')
  assert.isFalse(
    Object.hasOwn(actual, 'details'),
    'Genuine native retry removes prior details field',
  )
  assert.deepStrictEqual(actual.diagnostics, [], 'Genuine native retry clears prior diagnostics')
  assert.deepStrictEqual(actual.output, {
    text: '',
    totalBytes: 0,
    totalLines: 0,
    retainedBytes: 0,
    retainedLines: 0,
    skippedBytes: 0,
    skippedLines: 0,
    truncated: false,
  })
  yield* Deferred.succeed(retryRelease, undefined)
  assert.strictEqual((yield* runtime.scheduler.wait(ref)).status, 'completed')
  const final = yield* clearedRetryState()
  assert.strictEqual(final.progress[0]?.value.status, 'completed')
  assert.strictEqual(final.progress[0]?.value.output.text, 'new\n')
  assert.deepStrictEqual(final.progress[0]?.value.details, { stage: 'replacement' })
  assert.deepStrictEqual(final.progress[0]?.value.diagnostics, [])
  assert.deepStrictEqual(
    yield* Session.scanDocuments(runtime.session, { scope: { _tag: 'task', taskId: ref.id } }).pipe(
      Stream.runCollect,
    ),
    [],
  )
  assert.deepStrictEqual(final.usage, yield* Conversation.usage(conversation))
})

for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  it.live(`independent genuine retry clears details and diagnostics: ${backend}`, () =>
    Effect.scoped(progressRetryClearOracle(backend)).pipe(Effect.provide(services)),
  )

type CompactionMode = 'manual' | 'blocking' | 'background' | 'overflow'
type SummaryOutcome =
  | 'success'
  | 'declined'
  | 'stale'
  | 'paidFailure'
  | 'partial'
  | 'unknown'
  | 'empty'
  | 'unpriced'
  | 'retry'

const compactionWireReply = (empty: boolean): Response.PartEncoded[] => [
  ...(empty ? [] : [{ type: 'text' as const, text: 'Actual native summary of selected history' }]),
  nativeFinishPart('stop'),
]

const nativeCompactionObserver = Effect.fn(function* (
  backend: Backend,
  mode: CompactionMode,
  outcome: SummaryOutcome,
  generationFirst = false,
  ownedChild = false,
) {
  const native = yield* Layer.build(yield* nativeLayer(backend))
  const summaryEntered = yield* Deferred.make<void>()
  const releaseSummary = yield* Deferred.make<void>()
  const releaseRetryFailure = yield* Deferred.make<void>()
  const generationEntered = yield* Deferred.make<void>()
  const releaseGeneration = yield* Deferred.make<void>()
  const childReady = yield* Deferred.make<Conversation.Conversation>()
  const parentRelease = yield* Deferred.make<void>()
  yield* Effect.addFinalizer(() =>
    Effect.all([
      Deferred.succeed(releaseSummary, undefined),
      Deferred.succeed(releaseRetryFailure, undefined),
      Deferred.succeed(releaseGeneration, undefined),
      Deferred.succeed(parentRelease, undefined),
    ]),
  )
  const parentContract = {
    kind: 'review.compaction.accounting-parent',
    version: 1,
    input: Schema.Void,
    checkpoint: Schema.Struct({ phase: Schema.Literal('execute') }),
    success: Schema.Void,
    failure: Schema.String,
  }
  const parentTask = Task.make({
    ...parentContract,
    initial: (): Task.Checkpoint<typeof parentContract> => ({ phase: 'execute' }),
    phases: {
      execute: () =>
        Effect.gen(function* () {
          const execution = yield* TaskExecution
          const child = yield* execution.commit((tx) => execution.createConversation(tx))
          const bound = yield* execution.subagent!.conversation(child.id)
          yield* Deferred.succeed(childReady, bound)
          yield* Deferred.await(parentRelease)
          yield* execution.commit((tx) =>
            execution.progress(tx, parentContract, {
              status: 'completing',
              outcome: { status: 'completed', result: undefined },
            }),
          )
        }),
    },
  })
  const registry = yield* Registry.make
  yield* registry.install(
    Registry.withTask(
      Registry.toolkit({ id: 'review.compaction.accounting-parent', version: 1 }, Toolkit.make()),
      parentTask,
    ),
  )
  let summaries = 0
  let generations = 0
  const policies: { requestId: string; attempt: number }[] = []
  const overflow = AiError.make({
    module: 'ObservationReview',
    method: 'streamText',
    reason: new AiError.InvalidRequestError({
      description: 'Actual declared context overflow',
      metadata: { review: { overflow: true }, usage: { promptTokens: 7, completionTokens: 2 } },
    }),
  })
  const languageModel = yield* LanguageModel.make({
    generateText: () =>
      Effect.gen(function* () {
        summaries++
        const policy = Option.getOrThrow(yield* Effect.serviceOption(CompactionRequest))
        const prepared = Option.getOrThrow(yield* Effect.serviceOption(Prepared.PreparedRequest))
        policies.push({ requestId: policy.requestId, attempt: policy.attempt })
        assert.deepStrictEqual(prepared.current.tools, [])
        assert.strictEqual(prepared.current.requestId, policy.requestId)
        assert.strictEqual(policy.cacheRetention, 'none')
        assert.strictEqual(policy.deferred, false)
        yield* Deferred.succeed(summaryEntered, undefined)
        if (outcome === 'retry' && summaries === 1) {
          yield* Deferred.await(releaseRetryFailure)
          return yield* AiError.make({
            module: 'ObservationReview',
            method: 'summary',
            reason: new AiError.InternalProviderError({
              description: 'Actual paid retry failure',
              metadata: { usage: { promptTokens: 11, completionTokens: 3 } },
            }),
          })
        }
        yield* Deferred.await(releaseSummary)
        if (outcome === 'paidFailure' || outcome === 'partial' || outcome === 'unknown')
          return yield* AiError.make({
            module: 'ObservationReview',
            method: 'summary',
            reason: new AiError.InternalProviderError({
              description: 'Actual native summary failure',
              metadata:
                outcome === 'unknown'
                  ? {}
                  : {
                      usage: {
                        promptTokens: 11,
                        ...(outcome === 'partial' ? {} : { completionTokens: 3 }),
                      },
                    },
            }),
          })
        return compactionWireReply(outcome === 'empty')
      }),
    streamText: () =>
      Stream.unwrap(
        Effect.sync(() => {
          generations++
          if (mode === 'overflow' && generations === 1) return Stream.fail(overflow)
          return Stream.concat(
            Stream.fromIterable<Response.StreamPartEncoded>([
              { type: 'text-start', id: 'real-compaction-generation' },
              {
                type: 'text-delta',
                id: 'real-compaction-generation',
                delta: 'Unrelated live generation',
              },
            ]),
            Stream.fromEffect(
              Deferred.succeed(generationEntered, undefined).pipe(
                Effect.andThen(Deferred.await(releaseGeneration)),
                Effect.as(nativeFinishPart('stop')),
              ),
            ),
          )
        }),
      ),
  })
  const capability = yield* Model.makeCompaction({
    binding: { id: 'observation-review/summary', version: 1 },
    configure: (configured) => Effect.succeed(configured),
    isOverflow: (error) =>
      error.reason._tag === 'InvalidRequestError' &&
      Schema.is(Schema.Struct({ overflow: Schema.Literal(true) }))(error.reason.metadata.review),
  })
  const descriptor = Model.make({
    definition: {
      ref: { provider: 'observation-review', modelId: 'compaction' },
      contextWindow: { manual: 1000000, blocking: 100, background: 350, overflow: 1000000 }[mode],
      maxOutputTokens: 16,
      capabilities: { tools: false, images: false, reasoning: false, structuredOutput: false },
      ...(outcome === 'unpriced'
        ? {}
        : {
            pricing: {
              inputToken: 2,
              outputToken: 5,
              currency: 'REVIEW',
              source: 'literal-fixture-price',
            },
          }),
    },
    languageModel,
    options: Schema.Struct({}),
    compaction: capability,
    configure: () => Effect.succeed(Context.empty()),
  })
  const settings = yield* Settings.make({
    compaction: {
      enabled: mode !== 'manual',
      reserveTokens: 20,
      keepRecentTokens: 10,
      backgroundTokens: mode === 'background' ? 300 : 0,
    },
    progress: { partialIntervalMs: 0 },
    retry: { enabled: outcome === 'retry', maxRetries: 1, baseDelayMs: 100, maxAgentDelayMs: 100 },
  })
  const hooks =
    outcome === 'declined'
      ? [
          Hook.make({
            event: 'beforeCompact',
            execute: () =>
              Deferred.succeed(summaryEntered, undefined).pipe(
                Effect.andThen(Deferred.await(releaseSummary)),
                Effect.as({ _tag: 'decline' as const }),
              ),
          }),
        ]
      : []
  const runtime = yield* HarnessRuntime.make({ models: [descriptor], hooks, registry }).pipe(
    Effect.provideContext(native),
    Effect.provideService(Settings.Settings, settings),
  )
  const client = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, runtime.backend))
  const rootConversation = yield* client.root
  const parentRef = ownedChild
    ? yield* runtime.scheduler.start(parentTask, undefined, { conversationId: rootConversation.id })
    : undefined
  const conversation = ownedChild ? yield* Deferred.await(childReady) : rootConversation
  const seeded = yield* Session.commit(runtime.session, (tx) =>
    Effect.gen(function* () {
      const records: Record.Entry[] = []
      for (const text of ['old history '.repeat(40), 'recent history '.repeat(12)])
        records.push(
          yield* Transaction.appendEntry(tx, conversation.id, {
            kind: 'input',
            model: yield* Schema.encodeEffect(Schema.Array(Schema.toCodecJson(Prompt.Message)))([
              Prompt.userMessage({ content: [Prompt.textPart({ text })] }),
            ]),
          }),
        )
      return records
    }),
  )
  const pull = yield* Stream.toPull(Events.watch(conversation, { capacity: 4096 }))
  const initial = (yield* pull)[0]
  if (initial._tag !== 'Snapshot') return assert.fail('Actual compaction initial Snapshot required')
  let replica = initial
  const events: Events.Event[] = []
  const foldCompactionState = Effect.fn(function* () {
    const current = yield* View.snapshot(conversation)
    while (replica.revision.cursor < current.revision.cursor)
      for (const frame of yield* pull) {
        if (frame._tag === 'Batch') events.push(...frame.events)
        replica = yield* Events.reduce(replica, frame)
      }
    assert.deepStrictEqual(replica, current, 'FULL actual compaction state fold')
    assert.deepStrictEqual(
      current.value.usage,
      yield* Conversation.usage(conversation),
      'G12 public observer equals original authoritative accounting',
    )
    assert.deepStrictEqual(
      current.value.context,
      yield* canonicalRecordedContext(runtime.session, conversation.id),
    )
    return current.value
  })
  const manual = mode === 'manual' ? yield* Conversation.compact(conversation) : undefined
  const submission =
    mode === 'manual'
      ? undefined
      : yield* Conversation.submit(conversation, {
          type: 'input',
          content: 'Actual automatic compaction admission',
        })
  yield* Deferred.await(summaryEntered)
  const admitted = yield* foldCompactionState()
  if (outcome !== 'declined') {
    assert.strictEqual(
      admitted.compactions.items.length,
      1,
      'G11 actual independently observed compaction status',
    )
    assert.strictEqual(
      admitted.compactions.items[0]?.blocking,
      mode === 'blocking' || mode === 'overflow',
    )
    assert.strictEqual(
      admitted.compactions.items[0]?.reason,
      { manual: 'manual', blocking: 'threshold', background: 'threshold', overflow: 'overflow' }[
        mode
      ],
    )
  }
  const taskId = manual?.id ?? admitted.compactions.items[0]?.taskId
  if (outcome === 'declined' && mode !== 'manual') {
    assert.deepStrictEqual(
      admitted.compactions.items,
      [],
      'Declined automatic preflight admits no fabricated compaction',
    )
    yield* Deferred.succeed(releaseSummary, undefined)
  }
  if (outcome === 'stale')
    yield* Session.commit(runtime.session, (tx) =>
      Transaction.appendEntry(tx, conversation.id, {
        kind: 'edit',
        edits: [
          {
            _tag: 'replace',
            target: seeded[0]!.id,
            messages: [{ role: 'user', content: 'Genuine selected history edit' }],
          },
        ],
      }),
    )
  if (outcome === 'retry') {
    const before = yield* Clock.currentTimeMillis
    yield* Deferred.succeed(releaseRetryFailure, undefined)
    yield* View.watch(conversation).pipe(
      Stream.mapEffect(() => View.snapshot(conversation)),
      Stream.filter((frame) =>
        frame.value.compactions.items.some((item) => item.phase === 'retry'),
      ),
      Stream.take(1),
      Stream.runDrain,
    )
    const retryState = yield* foldCompactionState()
    assert.strictEqual(retryState.compactions.items[0]?.retryAt, before + 100)
    yield* TestClock.adjust(99)
    assert.strictEqual(summaries, 1)
    yield* TestClock.adjust(1)
  }
  const simultaneous =
    mode === 'manual'
      ? yield* Conversation.submit(conversation, {
          type: 'input',
          content: 'Genuine simultaneous generation',
        })
      : submission
  if (mode === 'manual' || mode === 'background') {
    yield* Deferred.await(generationEntered)
    yield* View.watch(conversation).pipe(
      Stream.mapEffect(() => View.snapshot(conversation)),
      Stream.filter((frame) => frame.value.live.generation?.partial !== undefined),
      Stream.take(1),
      Stream.runDrain,
    )
    const held = yield* foldCompactionState()
    assert.isDefined(held.live.generation?.partial)
    if (outcome !== 'declined')
      assert.isTrue(held.compactions.items.some((item) => item.taskId === taskId))
    if (generationFirst) {
      yield* Deferred.succeed(releaseGeneration, undefined)
      yield* Submission.wait(Option.getOrThrow(Option.fromUndefinedOr(simultaneous)))
      const cleared = yield* foldCompactionState()
      assert.deepStrictEqual(cleared.live, {})
      if (outcome !== 'declined')
        assert.isTrue(cleared.compactions.items.some((item) => item.taskId === taskId))
    }
  }
  yield* Deferred.succeed(releaseSummary, undefined)
  if (manual !== undefined)
    yield* CompactionTask.wait(manual).pipe(Effect.provideService(HarnessBackend, runtime.backend))
  if (taskId !== undefined) yield* runtime.scheduler.waitForTask(taskId)
  if ((mode === 'manual' || mode === 'background') && !generationFirst) {
    const summaryCleared = yield* foldCompactionState()
    assert.deepStrictEqual(summaryCleared.compactions.items, [])
    assert.isDefined(
      summaryCleared.live.generation?.partial,
      'Summary cleanup retains unrelated real generation',
    )
  }
  yield* Deferred.succeed(releaseGeneration, undefined)
  if (simultaneous !== undefined) yield* Submission.wait(simultaneous)
  if (!ownedChild) yield* client.waitForIdle
  const final = yield* foldCompactionState()
  assert.deepStrictEqual(final.compactions.items, [])
  assert.deepStrictEqual(final.live, {})
  const receipts = yield* Session.commit(runtime.session, (tx) =>
    Usage.receipts(tx, conversation.id),
  )
  const summaryReceipts = receipts.filter(
    (r) => r.identity._tag === 'requestAttempt' && r.identity.purpose === 'compaction',
  )
  if (outcome === 'declined') assert.deepStrictEqual(summaryReceipts, [])
  else {
    assert.strictEqual(summaryReceipts.length, outcome === 'retry' ? 2 : 1)
    assert.strictEqual(
      new Set(summaryReceipts.map((r) => JSON.stringify(r.identity))).size,
      summaryReceipts.length,
    )
    for (const receipt of summaryReceipts) {
      assert.isTrue(
        policies.some(
          (p) =>
            receipt.identity._tag === 'requestAttempt' &&
            p.requestId === receipt.identity.requestId &&
            p.attempt === receipt.identity.attempt,
        ),
      )
      if (['paidFailure', 'partial', 'unknown', 'empty', 'stale'].includes(outcome)) {
        assert.strictEqual(receipt.outcome._tag, 'failed')
        assert.isUndefined(
          receipt.linkedEntryId,
          'Native failed/no-entry attempt has no fabricated entry',
        )
      }
    }
  }
  if (outcome === 'unknown') assert.strictEqual(final.usage.unknownAttempts, 1)
  if (outcome === 'partial') {
    const receipt = summaryReceipts[0]!
    assert.isDefined(receipt.usage)
    if (receipt.usage?._tag === 'model') {
      assert.strictEqual(receipt.usage.tokens.input, 11)
      assert.isUndefined(receipt.usage.tokens.output)
      assert.isUndefined(receipt.usage.cost)
    }
  }
  if (outcome === 'paidFailure') {
    const receipt = summaryReceipts[0]!
    assert.strictEqual(receipt.usage?.cost?.amount, 37)
  }
  if (outcome === 'unpriced') assert.isAbove(final.usage.unpricedModels, 0)
  if (outcome === 'retry') {
    assert.deepStrictEqual(
      policies.map((p) => p.attempt),
      [0, 1],
    )
    assert.strictEqual(policies[0]?.requestId, policies[1]?.requestId)
  }
  const count = final.entries.filter((entry) => entry.kind === 'compaction').length
  assert.strictEqual(count, ['success', 'retry', 'unpriced'].includes(outcome) ? 1 : 0)
  if (taskId !== undefined) {
    assert.strictEqual(
      events.filter((e) => e._tag === 'CompactionStart' && e.item.taskId === taskId).length,
      1,
    )
    assert.strictEqual(
      events.filter((e) => e._tag === 'CompactionEnd' && e.taskId === taskId).length,
      1,
    )
  }
  const own = yield* Conversation.usage(conversation)
  const succeeds = ['success', 'retry', 'unpriced'].includes(outcome)
  let expectedGenerations = 1
  if (mode === 'overflow' && succeeds) expectedGenerations = 2
  assert.strictEqual(
    generations,
    expectedGenerations,
    'Actual provider invocations match native admission semantics',
  )
  const expectedSummary = {
    success: { responses: 1, input: 3, output: 1, cost: 11 },
    declined: { responses: 0, input: 0, output: 0, cost: 0 },
    stale: { responses: 1, input: 3, output: 1, cost: 11 },
    paidFailure: { responses: 1, input: 11, output: 3, cost: 37 },
    partial: { responses: 1, input: 11, output: undefined, cost: 0 },
    unknown: { responses: 0, input: 0, output: 0, cost: 0 },
    empty: { responses: 1, input: 3, output: 1, cost: 11 },
    unpriced: { responses: 1, input: 3, output: 1, cost: 0 },
    retry: { responses: 2, input: 14, output: 4, cost: 48 },
  }[outcome]
  const generationInput =
    mode === 'overflow' ? 7 + 3 * (expectedGenerations - 1) : 3 * expectedGenerations
  const generationOutput = mode === 'overflow' ? 2 + (expectedGenerations - 1) : expectedGenerations
  const expectedResponses = expectedGenerations + expectedSummary.responses
  assert.strictEqual(own.models.length, expectedResponses === 0 ? 0 : 1)
  if (expectedResponses > 0) {
    const total = own.models[0]!
    assert.strictEqual(total.responses, expectedResponses, 'G12 literal known response count')
    assert.strictEqual(
      total.tokens.input,
      generationInput + expectedSummary.input,
      'G12 literal input subtotal',
    )
    assert.strictEqual(
      total.tokens.output,
      expectedSummary.output === undefined ? undefined : generationOutput + expectedSummary.output,
      'G12 partial counter cannot invent an output total',
    )
    assert.deepStrictEqual(
      total.tokenCoverage?.output,
      {
        known: generationOutput + (expectedSummary.output ?? 0),
        unknown: outcome === 'partial' ? 1 : 0,
      },
      'G12 literal partial/known output coverage',
    )
  }
  assert.strictEqual(own.unknownAttempts ?? 0, outcome === 'unknown' ? 1 : 0)
  let expectedUnpriced = 0
  if (outcome === 'unpriced') expectedUnpriced = expectedResponses
  else if (outcome === 'partial') expectedUnpriced = 1
  assert.strictEqual(own.unpricedModels, expectedUnpriced)
  const generationCost =
    mode === 'overflow' ? 24 + 11 * (expectedGenerations - 1) : 11 * expectedGenerations
  const expectedCost = outcome === 'unpriced' ? 0 : generationCost + expectedSummary.cost
  assert.strictEqual(own.costs.length, expectedCost === 0 ? 0 : 1)
  if (expectedCost > 0) {
    assert.strictEqual(own.costs[0]?.currency, 'REVIEW')
    assert.strictEqual(
      own.costs[0]?.model,
      expectedCost,
      'G12 literal original known charge counted once',
    )
    assert.strictEqual(own.costs[0]?.tool, 0)
  }
  const whole = yield* client.usage()
  assert.deepStrictEqual(own, whole, 'Actual root receipts are counted exactly once in session')
  const reset = yield* Conversation.reset(conversation)
  yield* Submission.wait(reset)
  if (!ownedChild) yield* client.waitForIdle
  const afterReset = yield* foldCompactionState()
  assert.deepStrictEqual(
    afterReset.usage,
    own,
    'Actual context reset preserves paid/unknown accounting',
  )
  assert.deepStrictEqual(yield* client.usage(), whole)
  if (parentRef !== undefined) {
    const rootUsage = yield* Conversation.usage(rootConversation)
    assert.deepStrictEqual(rootUsage.models, [], 'Supervisor does not recharge child summary')
    const ordinary = Option.getOrThrow(yield* client.conversation(conversation.id))
    yield* Deferred.succeed(parentRelease, undefined)
    assert.strictEqual((yield* runtime.scheduler.wait(parentRef)).status, 'completed')
    const afterRetirement = yield* View.snapshot(ordinary)
    assert.deepStrictEqual(afterRetirement.value.usage, own)
    assert.deepStrictEqual(yield* Conversation.usage(rootConversation), rootUsage)
    assert.deepStrictEqual(
      yield* client.usage(),
      whole,
      'Parent retirement cannot recharge child spend',
    )
    assert.deepStrictEqual(
      yield* Session.scanDocuments(runtime.session, {
        scope: { _tag: 'task', taskId: parentRef.id },
      }).pipe(Stream.runCollect),
      [],
    )
  }
})

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  for (const mode of ['manual', 'blocking', 'background', 'overflow'] as const)
    it.live(`independent actual ${mode} compaction success: ${backend}`, () =>
      Effect.scoped(nativeCompactionObserver(backend, mode, 'success')).pipe(
        Effect.provide(services),
      ),
    )
  for (const outcome of [
    'declined',
    'stale',
    'paidFailure',
    'partial',
    'unknown',
    'empty',
    'unpriced',
  ] as const)
    it.live(`independent actual manual compaction ${outcome}: ${backend}`, () =>
      Effect.scoped(nativeCompactionObserver(backend, 'manual', outcome, true)).pipe(
        Effect.provide(services),
      ),
    )
  for (const mode of ['manual', 'background'] as const)
    it.live(`independent actual ${mode} compaction generation cleanup: ${backend}`, () =>
      Effect.scoped(nativeCompactionObserver(backend, mode, 'success', true)).pipe(
        Effect.provide(services),
      ),
    )
  it.effect(`independent actual manual compaction exact retry: ${backend}`, () =>
    Effect.scoped(nativeCompactionObserver(backend, 'manual', 'retry', true)).pipe(
      Effect.provide(services),
    ),
  )
  for (const outcome of [
    'success',
    'paidFailure',
    'partial',
    'unknown',
    'empty',
    'unpriced',
  ] as const)
    it.live(`independent actual owned child compaction ${outcome}: ${backend}`, () =>
      Effect.scoped(nativeCompactionObserver(backend, 'manual', outcome, true, true)).pipe(
        Effect.provide(services),
      ),
    )
  for (const mode of ['blocking', 'background', 'overflow'] as const) {
    for (const outcome of [
      'declined',
      'stale',
      'paidFailure',
      'partial',
      'unknown',
      'empty',
      'unpriced',
    ] as const)
      it.live(`independent actual ${mode} compaction ${outcome}: ${backend}`, () =>
        Effect.scoped(nativeCompactionObserver(backend, mode, outcome, true)).pipe(
          Effect.provide(services),
        ),
      )
    it.effect(`independent actual ${mode} compaction exact retry: ${backend}`, () =>
      Effect.scoped(nativeCompactionObserver(backend, mode, 'retry', true)).pipe(
        Effect.provide(services),
      ),
    )
  }
}

const nativeRoundOrder = Effect.fn(function* (
  backend: Backend,
  mode: Tool.ExecutionMode,
  fault: boolean,
  cancel = false,
) {
  const native = yield* Layer.build(yield* nativeLayer(backend))
  const enteredFirst = yield* Deferred.make<void>()
  const enteredSecond = yield* Deferred.make<void>()
  const enteredLeaf = yield* Deferred.make<void>()
  const releaseFirst = yield* Deferred.make<void>()
  const releaseSecond = yield* Deferred.make<void>()
  const releaseLeaf = yield* Deferred.make<void>()
  yield* Effect.addFinalizer(() =>
    Effect.all([
      Deferred.succeed(releaseFirst, undefined),
      Deferred.succeed(releaseSecond, undefined),
      Deferred.succeed(releaseLeaf, undefined),
    ]),
  )
  const firstTool = Tool.makeResult('review-order-first', {
    success: Schema.String,
    replay: 'safe',
    executionMode: mode,
  })
  const secondTool = Tool.makeResult('review-order-second', {
    success: Schema.String,
    replay: 'safe',
    executionMode: mode,
  })
  const leafTool = Tool.makeResult('review-order-leaf', {
    success: Schema.String,
    replay: 'safe',
    callers: ['tools'],
  })
  const toolkit = Toolkit.make(firstTool, secondTool, leafTool)
  let firstId: Record.TaskId | undefined
  let secondId: Record.TaskId | undefined
  let leafId: Record.TaskId | undefined
  let secondCalls = 0
  let requests = 0
  const model = yield* LanguageModel.make({
    generateText: () => Effect.die('Only actual native streaming used'),
    streamText: () =>
      Stream.unwrap(
        Effect.sync(() => {
          requests++
          if (requests === 1)
            return Stream.fromIterable<Response.StreamPartEncoded>([
              {
                type: 'tool-call',
                id: 'review-provider-first',
                name: firstTool.name,
                params: {},
                providerExecuted: false,
              },
              {
                type: 'tool-call',
                id: 'review-provider-second',
                name: secondTool.name,
                params: {},
                providerExecuted: false,
              },
              nativeFinishPart('tool-calls'),
            ])
          return Stream.fromIterable<Response.StreamPartEncoded>([
            { type: 'text-start', id: 'review-successor' },
            { type: 'text-delta', id: 'review-successor', delta: 'Actual successor answer' },
            { type: 'text-end', id: 'review-successor' },
            nativeFinishPart('stop'),
          ])
        }),
      ),
  })
  const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
    Effect.provideContext(native),
    Effect.provideService(LanguageModel.LanguageModel, model),
    Effect.provide(
      toolkit.toLayer({
        'review-order-first': () =>
          Effect.gen(function* () {
            const execution = yield* ToolExecution
            firstId = execution.taskId
            yield* execution.output('held outer first\n')
            yield* execution.details({ actual: 'first pending' })
            yield* Deferred.succeed(enteredFirst, undefined)
            const child = yield* ToolInvocation.invoke(
              execution,
              leafTool,
              {},
              { key: 'real-descendant' },
            ).pipe(Effect.orDie)
            assert.strictEqual(child.status, 'succeeded')
            yield* Deferred.await(releaseFirst)
            if (fault) return yield* Effect.die('Actual first native handler defect')
            yield* execution.output('outer first final\n')
            return {
              content: [Prompt.textPart({ text: 'Real first result' })],
              structuredOutput: 'FIRST',
              control: { terminate: true },
            }
          }),
        'review-order-second': () =>
          Effect.gen(function* () {
            secondCalls++
            secondId = (yield* ToolExecution).taskId
            yield* Deferred.succeed(enteredSecond, undefined)
            yield* Deferred.await(releaseSecond)
            return {
              content: [Prompt.textPart({ text: 'Real second result' })],
              structuredOutput: 'SECOND',
              control: { terminate: true },
            }
          }),
        'review-order-leaf': () =>
          Effect.gen(function* () {
            leafId = (yield* ToolExecution).taskId
            yield* Deferred.succeed(enteredLeaf, undefined)
            yield* Deferred.await(releaseLeaf)
            return {
              content: [Prompt.textPart({ text: 'Real nested result' })],
              structuredOutput: 'LEAF',
            }
          }),
      }),
    ),
  )
  const client = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, runtime.backend))
  const conversation = yield* client.root
  const consumer = yield* Scope.make()
  yield* Effect.addFinalizer((exit) => Scope.close(consumer, exit))
  const pull = yield* Stream.toPull(Events.watch(conversation, { capacity: 4096 })).pipe(
    Effect.provideService(Scope.Scope, consumer),
  )
  const initial = (yield* pull)[0]
  if (initial._tag !== 'Snapshot') return assert.fail('Actual ordered Snapshot required')
  let replica = initial
  const events: Events.Event[] = []
  const foldOrderedState = Effect.fn(function* () {
    const current = yield* View.snapshot(conversation)
    while (replica.revision.cursor < current.revision.cursor)
      for (const frame of yield* pull) {
        if (frame._tag === 'Batch') events.push(...frame.events)
        replica = yield* Events.reduce(replica, frame)
      }
    assert.deepStrictEqual(replica, current, 'FULL actual native tool-order state fold')
    return current.value
  })
  const firstSubmission = yield* Conversation.submit(conversation, {
    type: 'input',
    content: 'Run real ordered tools',
  })
  yield* Deferred.await(enteredFirst)
  yield* Deferred.await(enteredLeaf)
  const actualFirstId = Option.getOrThrow(Option.fromUndefinedOr(firstId))
  const actualLeafId = Option.getOrThrow(Option.fromUndefinedOr(leafId))
  const queued = cancel
    ? undefined
    : yield* Conversation.submit(conversation, {
        type: 'input',
        content: 'Queued successor run',
        whenBusy: 'followUp',
      })
  const held = yield* foldOrderedState()
  const oldRun = Option.getOrThrow(Option.fromUndefinedOr(held.run.active)).taskId
  const round = Option.getOrThrow(Option.fromUndefinedOr(held.rounds[0]))
  assert.strictEqual(round.mode, mode)
  assert.strictEqual(round.slots[0]?.taskId, actualFirstId)
  if (cancel) {
    assert.strictEqual(secondCalls, 0)
    assert.isUndefined(round.slots[1]?.taskId)
    yield* Conversation.abort(conversation)
    yield* Submission.wait(firstSubmission)
    yield* client.waitForIdle
    const aborted = yield* foldOrderedState()
    const unstarted = aborted.rounds[0]?.slots[1]
    assert.isDefined(unstarted)
    assert.isUndefined(unstarted?.taskId, 'Never admitted call has no fabricated task ID')
    assert.isUndefined(unstarted?.entryId, 'Never admitted call has no fabricated result entry')
    assert.strictEqual(secondCalls, 0)
    assert.isUndefined(secondId)
    const leafEnd = events.findIndex(
      (e) => e._tag === 'NestedEnd' && e.nested.taskId === actualLeafId,
    )
    const firstEnd = events.findIndex(
      (e) => e._tag === 'ToolEnd' && e.slot.taskId === actualFirstId,
    )
    assert.isAtLeast(leafEnd, 0)
    assert.isAbove(firstEnd, leafEnd)
    assert.deepStrictEqual(aborted.graph.tasks, [])
    assert.deepStrictEqual(aborted.live, {})
    const next = yield* Conversation.submit(conversation, {
      type: 'input',
      content: 'Actual successor after abort',
    })
    yield* Submission.wait(next)
    yield* client.waitForIdle
    const after = yield* foldOrderedState()
    const endIndex = events.findIndex((e) => e._tag === 'RunEnd' && e.membership.taskId === oldRun)
    const nextIndex = events.findIndex(
      (e) => e._tag === 'RunStart' && e.membership.taskId !== oldRun,
    )
    assert.isAtLeast(endIndex, 0)
    assert.isAbove(nextIndex, endIndex)
    assert.strictEqual(requests, 2)
    assert.deepStrictEqual(after.usage, yield* Conversation.usage(conversation))
    return
  }
  if (mode === 'parallel') {
    yield* Deferred.await(enteredSecond)
    yield* Deferred.succeed(releaseSecond, undefined)
    yield* runtime.scheduler.waitForTask(Option.getOrThrow(Option.fromUndefinedOr(secondId)))
    const reverse = yield* foldOrderedState()
    assert.isDefined(reverse.rounds[0]?.slots[1]?.result)
    assert.isUndefined(reverse.rounds[0]?.slots[0]?.result)
  } else {
    assert.strictEqual(secondCalls, 0)
    assert.isUndefined(round.slots[1]?.taskId)
    assert.isUndefined(round.slots[1]?.result)
  }
  yield* Deferred.succeed(releaseLeaf, undefined)
  yield* runtime.scheduler.waitForTask(actualLeafId)
  const nestedEnded = yield* foldOrderedState()
  assert.isTrue(
    nestedEnded.nested.some((n) => n.taskId === actualLeafId && n.availability === 'available'),
  )
  assert.isUndefined(nestedEnded.rounds[0]?.slots[0]?.result)
  yield* Deferred.succeed(releaseFirst, undefined)
  if (mode === 'sequential') {
    yield* Deferred.await(enteredSecond)
    yield* Deferred.succeed(releaseSecond, undefined)
  }
  yield* Submission.wait(firstSubmission)
  yield* Submission.wait(Option.getOrThrow(Option.fromUndefinedOr(queued)))
  yield* client.waitForIdle
  const final = yield* foldOrderedState()
  const ends = events.filter((e) => e._tag === 'ToolEnd')
  const actualSecondId = secondId
  if (fault) {
    assert.strictEqual(secondCalls, 1)
    assert.isDefined(actualSecondId)
    assert.isTrue(final.terminal.some((t) => t.taskId === actualFirstId && t.status === 'failed'))
    const failedSlot = final.rounds[0]?.slots[0]
    assert.strictEqual(failedSlot?.taskId, actualFirstId)
    assert.isUndefined(failedSlot?.entryId, 'Actual failed slot has no fabricated result entry')
  } else {
    const ids = [actualFirstId, Option.getOrThrow(Option.fromUndefinedOr(actualSecondId))]
    assert.deepStrictEqual(
      ends.map((end) => end.slot.taskId),
      mode === 'parallel' ? ids.toReversed() : ids,
      'G09 actual native round ToolEnd order',
    )
  }
  for (const end of ends) {
    const entryId = end.slot.entryId
    if (entryId === undefined) continue
    const linked = events.findIndex((e) => e._tag === 'Message' && e.entry.id === entryId)
    assert.isAtLeast(linked, 0)
    assert.isBelow(events.indexOf(end), linked, 'Real outer end precedes its linked result Message')
  }
  const nestedEnd = events.findIndex(
    (e) => e._tag === 'NestedEnd' && e.nested.taskId === actualLeafId,
  )
  const outerEnd = events.findIndex((e) => e._tag === 'ToolEnd' && e.slot.taskId === actualFirstId)
  assert.isAtLeast(nestedEnd, 0)
  assert.isAbove(outerEnd, nestedEnd, 'Actual descendant end precedes its enclosing outer end')
  const oldTurnEnd = events.findIndex((e) => e._tag === 'TurnEnd' && e.generation.taskId === oldRun)
  const oldRunEnd = events.findIndex((e) => e._tag === 'RunEnd' && e.membership.taskId === oldRun)
  const successorStart = events.findIndex(
    (e) => e._tag === 'RunStart' && e.membership.taskId !== oldRun,
  )
  const successorTurn = events.findIndex(
    (e) => e._tag === 'TurnStart' && e.generation.taskId !== oldRun,
  )
  assert.isAtLeast(oldTurnEnd, 0)
  assert.isAtLeast(oldRunEnd, 0)
  assert.isAbove(successorStart, oldRunEnd, 'Actual old RunEnd precedes successor RunStart')
  assert.isAbove(successorTurn, oldTurnEnd, 'Actual old TurnEnd precedes successor TurnStart')
  for (const r of final.rounds)
    for (const slot of r.slots) {
      if (slot.taskId !== undefined)
        assert.isTrue(Option.isSome(yield* Session.task(runtime.session, slot.taskId)))
      if (slot.entryId !== undefined)
        assert.isTrue(
          Option.isSome(
            yield* Session.entry(runtime.session, slot.entryId, {
              conversationId: conversation.id,
            }),
          ),
        )
    }
  assert.strictEqual(requests, 2)
  assert.deepStrictEqual(final.live, {})
  assert.deepStrictEqual(final.run, {})
  assert.deepStrictEqual(final.graph.tasks, [])
  assert.deepStrictEqual(final.usage, yield* Conversation.usage(conversation))
})

for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  it.live(`independent actual sequential unstarted abort order: ${backend}`, () =>
    Effect.scoped(nativeRoundOrder(backend, 'sequential', false, true)).pipe(
      Effect.provide(services),
    ),
  )

for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  for (const scenario of [
    { mode: 'parallel', fault: false },
    { mode: 'sequential', fault: false },
    { mode: 'sequential', fault: true },
  ] as const)
    it.live(
      `independent actual ${scenario.mode} tool order fault=${scenario.fault}: ${backend}`,
      () =>
        Effect.scoped(nativeRoundOrder(backend, scenario.mode, scenario.fault)).pipe(
          Effect.provide(services),
        ),
    )

for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  for (const mode of ['preserveManaged', 'honorAll'] as const)
    it.live(`independent recorded ${mode} and stored agent fork: ${backend}`, () =>
      Effect.scoped(legacyForkOracle(backend, mode)).pipe(Effect.provide(services)),
    )

for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  for (const retain of ['head', 'tail', 'full'] as const)
    it.live(`independent actual retained ${retain} native batches: ${backend}`, () =>
      Effect.scoped(retainedWindowLifecycle(backend, retain)).pipe(Effect.provide(services)),
    )

for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  describe(`independent full atomic state: ${backend}`, () => {
    it.live('graph stays coherent across hydration and a two-handle commit', () =>
      Effect.scoped(atomicReplicaRace(backend, observationPorts.graph)).pipe(
        Effect.provide(services),
      ),
    )
    it.live('view stays coherent across hydration and a two-handle commit', () =>
      Effect.scoped(atomicReplicaRace(backend, observationPorts.view)).pipe(
        Effect.provide(services),
      ),
    )
    it.live('events stay coherent across hydration and a two-handle commit', () =>
      Effect.scoped(atomicReplicaRace(backend, observationPorts.events)).pipe(
        Effect.provide(services),
      ),
    )
  })
