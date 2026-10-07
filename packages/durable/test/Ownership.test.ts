import { assertFailure } from '@effect/vitest/utils'
import * as TestClock from 'effect/testing/TestClock'
import * as Identity from '@effect-harness/durable/Identity'
import { ResourceScope, withLayer } from '@effect-harness/durable/testing/Storage'
import * as Scope from 'effect/Scope'
import { assert, describe, it } from '@effect/vitest'
import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as Option from 'effect/Option'
import * as DurableDeferred from 'effect/workflow/DurableDeferred'
import * as Activity from 'effect/workflow/Activity'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Cause from 'effect/Cause'
import { StorageError } from '@effect-harness/durable/StorageError'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Document from '@effect-harness/durable/Document'
import * as Ownership from '@effect-harness/durable/Ownership'
import * as Conversation from '@effect-harness/durable/Conversation'
import * as Record from '@effect-harness/durable/Record'
import * as Session from '@effect-harness/durable/Session'
import * as Memory from '@effect-harness/durable/storage/Memory'
import * as Cancellation from '@effect-harness/durable/workflow/Cancellation'
import * as Structured from '@effect-harness/durable/workflow/Structured'
import { Generation } from '@effect-harness/durable/workflow/Generation'
import { ToolCall } from '@effect-harness/durable/workflow/ToolCall'
import { Compaction } from '@effect-harness/durable/workflow/Compaction'
import {
  ExecutionError,
  ExecutionErrorCodec,
  Storage,
  InvalidArguments,
  InvalidState,
} from '@effect-harness/durable/workflow/ExecutionError'

const Notes = Document.defineUnsafe({
  kind: 'test/owned-notes',
  version: 1,
  scope: 'task',
  schema: Schema.Struct({ note: Schema.String }),
  initial: () => ({ note: 'old' }),
})

const Node = Workflow.make('test/ordinary-owned-work', {
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
type Payload = typeof Node.payloadSchema.Type
const identity = (
  taskId: Record.TaskId,
  conversationId = Record.ROOT_CONVERSATION_ID,
): Ownership.Identity => ({
  sessionId: Identity.SessionId.make('ownership'),
  conversationId,
  taskId,
})
const reserve = Effect.fnUntraced(function* (
  session: Session.Service,
  name: string,
  options?: {
    readonly owner?: Record.TaskId
    readonly conversationId?: Record.ConversationId
    readonly background?: boolean
  },
) {
  return yield* session.transaction(
    Effect.fnUntraced(function* (tx) {
      const projection: Omit<Record.Task, 'id'> = {
        conversationId: options?.conversationId ?? Record.ROOT_CONVERSATION_ID,
        kind: Node._tag,
        version: 1,
        input: null,
        background: options?.background ?? false,
        abortRequested: false,
        state: { status: 'pending' },
        ...(options?.owner === undefined ? {} : { owner: options.owner }),
      }
      const taskId = yield* tx.createTask(projection)
      const payload = { ...identity(taskId, projection.conversationId), name }
      yield* Structured.bind(tx, { ...projection, id: taskId }, Node, payload)
      return payload
    }),
  )
})
const until = <E, R>(predicate: Effect.Effect<boolean, E, R>) =>
  Effect.gen(function* () {
    for (let i = 0; i < 300; i++) {
      if (yield* predicate) return
      // Advance modeled polling time only after the predicate observes an admitted transition.
      yield* TestClock.adjust('5 millis')
    }
    return yield* Effect.die('Ownership condition did not settle')
  })
const rejection = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  tag: string,
  reason: string,
  message: string,
) =>
  effect.pipe(
    Effect.flip,
    Effect.tap((error) =>
      Effect.sync(() => {
        assert.isTrue(error instanceof ExecutionError || error instanceof StorageError)
        if (error instanceof ExecutionError || error instanceof StorageError) {
          assert.strictEqual(error._tag, tag)
          assert.strictEqual(error.reason._tag, reason)
          assert.strictEqual(error.message, message)
        }
      }),
    ),
  )
type Services =
  | Ownership.Current
  | Ownership.Declarations
  | Cancellation.Cancellation
  | WorkflowEngine.WorkflowEngine
  | WorkflowEngine.WorkflowInstance
type Behavior = (
  payload: Payload,
  session: Session.Service,
) => Effect.Effect<Record.Json, ExecutionError, Services>
const setup = (behaviors: ReadonlyMap<string, Behavior>) => {
  const executor = Node.toLayer(
    Effect.fnUntraced(
      function* (payload) {
        const session = yield* Session.Session
        const behavior =
          behaviors.get(payload.name) ?? (() => Effect.succeed({ status: 'completed' }))
        const exit = yield* Cancellation.run(payload, session, behavior(payload, session)).pipe(
          Effect.exit,
        )
        if (Exit.isFailure(exit) && !exit.cause.reasons.some((reason) => reason._tag === 'Fail'))
          return yield* Effect.failCause(exit.cause)
        const outcome: Record.Json = Exit.isSuccess(exit) ? exit.value : { status: 'aborted' }
        return yield* Structured.complete(session, payload.taskId, outcome, payload.sessionId).pipe(
          Effect.mapError((error) =>
            error._tag === 'StorageError'
              ? new ExecutionError({
                  reason: new Storage({ message: error.message, cause: error }),
                })
              : error,
          ),
        )
      },
      Effect.mapError((error) =>
        error._tag === 'StorageError'
          ? new ExecutionError({ reason: new Storage({ message: error.message, cause: error }) })
          : error,
      ),
    ),
  )
  return executor.pipe(
    Layer.provideMerge(WorkflowEngine.layerMemory),
    Layer.provideMerge(Cancellation.layer),
    Layer.provideMerge(Ownership.layerDeclarations([Node])),
    Layer.provide(BunCrypto.layer),
  )
}
const invoke = (payload: Payload) => Node.execute(payload)
const withSetup = <A, E, R>(
  behaviors: ReadonlyMap<string, Behavior>,
  effect: Effect.Effect<A, E, R>,
) =>
  withLayer(
    effect.pipe(Effect.provide(setup(behaviors))),
    Session.layer.pipe(Layer.provideMerge(Memory.layer)),
  )

describe('Ownership', () => {
  it.effect(
    'awaitIdle drives restored declarations across ownerless roots and excludes background subtrees',
    () =>
      Effect.gen(function* () {
        const release = yield* Deferred.make<void>()
        const calls: string[] = []
        let available = false
        const behaviors = new Map<string, Behavior>([
          [
            'ordinary',
            () =>
              Effect.sync(() => {
                calls.push('ordinary')
              }).pipe(Effect.andThen(Deferred.await(release)), Effect.as({ status: 'completed' })),
          ],
          [
            'failure',
            () =>
              Effect.sync(() => {
                calls.push('failure')
                return { status: 'failed' }
              }),
          ],
          ['background', () => Effect.die('Background must not be driven')],
          ['below-background', () => Effect.die('Background subtree must not be driven')],
        ])
        yield* withSetup(
          behaviors,
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
            const session = yield* Session.Session
            const declarations = yield* Ownership.Declarations
            yield* session.root()
            const ordinary = yield* reserve(session, 'ordinary')
            const other = yield* session.transaction((tx) =>
              tx.createConversation({
                ownership: { _tag: 'ownerless' as const, kind: 'ownerless' },
              }),
            )
            yield* reserve(session, 'failure', { conversationId: other.id })
            const background = yield* reserve(session, 'background', { background: true })
            const owned = yield* session.transaction((tx) =>
              tx.createConversation({
                ownership: { _tag: 'task' as const, kind: 'task', taskId: background.taskId },
              }),
            )
            const below = yield* reserve(session, 'below-background', {
              conversationId: owned.id,
            })
            const waiting = yield* Conversation.awaitIdle(session).pipe(
              Effect.provideService(Ownership.Declarations, {
                ...declarations,
                get: (name) => (available ? declarations.get(name) : Option.none()),
              }),
              Effect.forkScoped,
            )
            // Native awaitIdle scans on its captured clock; this negative window proves unavailable declarations cannot start after a full scan interval.
            yield* TestClock.adjust('40 millis')
            assert.deepStrictEqual(calls, [])
            assert.isUndefined(waiting.pollUnsafe())
            available = true
            yield* until(Effect.sync(() => calls.length === 2))
            assert.isUndefined(waiting.pollUnsafe())
            yield* Deferred.succeed(release, undefined)
            yield* until(Effect.sync(() => waiting.pollUnsafe() !== undefined))
            yield* Fiber.join(waiting).pipe(Effect.timeout('3 seconds'))
            assert.strictEqual(
              (yield* session.task(ordinary.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state
                .status,
              'terminal',
            )
            assert.strictEqual(
              (yield* session.task(below.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state
                .status,
              'pending',
            )
            const blocked = yield* reserve(session, 'unregistered')
            const closing = yield* Conversation.awaitIdle(session, blocked.conversationId).pipe(
              Effect.provideService(Ownership.Declarations, {
                ...declarations,
                get: () => Option.none(),
              }),
              Effect.result,
              Effect.forkScoped,
            )
            yield* Scope.close(yield* ResourceScope, Exit.void)
            assert.strictEqual((yield* Fiber.join(closing))._tag, 'Failure')
          }),
        )
      }),
  )
  it.effect('rejects every task and conversation owner ancestor before persisting a join', () =>
    withSetup(
      new Map(),
      Effect.gen(function* () {
        const session = yield* Session.Session
        yield* session.root()
        const grandparent = yield* reserve(session, 'grandparent')
        const parent = yield* reserve(session, 'parent', { owner: grandparent.taskId })
        const child = yield* reserve(session, 'child', { owner: parent.taskId })
        const owned = yield* session.transaction((tx) =>
          tx.createConversation({
            ownership: { _tag: 'task' as const, kind: 'task', taskId: grandparent.taskId },
          }),
        )
        const conversational = yield* reserve(session, 'conversational', {
          conversationId: owned.id,
        })
        for (const task of [child, conversational]) {
          const result = yield* Structured.join(session, task.taskId, [grandparent.taskId]).pipe(
            Effect.result,
          )
          assertFailure(
            result,
            new ExecutionError({
              reason: new InvalidState({ message: 'A task cannot await itself or its owner' }),
            }),
          )
          assert.deepStrictEqual(
            (yield* session.task(task.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state,
            { status: 'pending' },
          )
        }
      }),
    ),
  )
  it.effect(
    'terminal-aborted owners and unmarked background owners permit fresh independent admission',
    () =>
      withSetup(
        new Map(),
        Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          const ended = yield* reserve(session, 'ended')
          const retained = yield* session.transaction((tx) =>
            tx.createConversation({
              ownership: { _tag: 'task' as const, kind: 'task', taskId: ended.taskId },
            }),
          )
          const outer = yield* reserve(session, 'outer')
          const nested = yield* session.transaction((tx) =>
            tx.createConversation({
              ownership: { _tag: 'task' as const, kind: 'task', taskId: outer.taskId },
            }),
          )
          const background = yield* reserve(session, 'background', {
            conversationId: nested.id,
            background: true,
          })
          const surviving = yield* session.transaction((tx) =>
            tx.createConversation({
              ownership: { _tag: 'task' as const, kind: 'task', taskId: background.taskId },
            }),
          )
          yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              const endedTask = (yield* tx
                .task(ended.taskId)
                .pipe(Effect.map(Option.getOrUndefined)))!
              const outerTask = (yield* tx
                .task(outer.taskId)
                .pipe(Effect.map(Option.getOrUndefined)))!
              yield* tx.write({
                _tag: 'task' as const,
                type: 'task',
                value: {
                  ...endedTask,
                  abortRequested: true,
                  state: { status: 'terminal', outcome: { status: 'aborted' } },
                },
              })
              yield* tx.write({
                _tag: 'task' as const,
                type: 'task',
                value: { ...outerTask, abortRequested: true },
              })
            }),
          )
          for (const conversation of [retained, surviving]) {
            assert.isDefined(yield* reserve(session, 'fresh', { conversationId: conversation.id }))
            assert.isDefined(
              yield* session.transaction((tx) =>
                tx.createSubmission({
                  _tag: 'InputQueued' as const,
                  conversationId: conversation.id,
                  type: 'input',
                  status: 'queued',
                }),
              ),
            )
          }
        }),
      ),
  )

  it.effect(
    'conversation abort crosses ordinary terminal ancestry while task abort does not reactivate it',
    () =>
      withSetup(
        new Map(),
        Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          const parent = yield* reserve(session, 'parent')
          const ended = yield* reserve(session, 'ended', { owner: parent.taskId })
          const owned = yield* session.transaction((tx) =>
            tx.createConversation({
              ownership: { _tag: 'task' as const, kind: 'task', taskId: ended.taskId },
            }),
          )
          yield* Structured.complete(
            session,
            ended.taskId,
            { status: 'completed' },
            Identity.SessionId.make('ownership'),
          )
          const below = yield* reserve(session, 'below', { conversationId: owned.id })
          const graph = yield* session.committed
          for (const background of [false, true]) {
            assert.deepStrictEqual(
              Option.getOrUndefined(
                Ownership.reach(
                  graph,
                  { _tag: 'task' as const, kind: 'task', id: ended.taskId },
                  background,
                ),
              ),
              { tasks: [], conversations: [] },
            )
            assert.isFalse(
              Option.getOrUndefined(
                Ownership.reach(
                  graph,
                  { _tag: 'task' as const, kind: 'task', id: parent.taskId },
                  background,
                ),
              )!.tasks.some((task) => task.id === below.taskId),
            )
          }
          const reached = Option.getOrUndefined(
            Ownership.reach(graph, {
              _tag: 'conversation' as const,
              kind: 'conversation',
              id: parent.conversationId,
            }),
          )!
          assert.isTrue(reached.tasks.some((task) => task.id === below.taskId))
          assert.isFalse(reached.tasks.some((task) => task.id === ended.taskId))
          assert.isTrue(reached.conversations.some((conversation) => conversation.id === owned.id))
        }),
      ),
  )

  it.effect('holds the native result, memos and documents until a late child drains', () =>
    Effect.gen(function* () {
      const gate = yield* Deferred.make<void>()
      let child: Payload | undefined
      const behaviors = new Map<string, Behavior>([
        [
          'child',
          () => Deferred.await(gate).pipe(Effect.as({ status: 'completed', result: 'child' })),
        ],
        [
          'parent',
          (payload, session) =>
            Effect.gen(function* () {
              yield* Ownership.memo('kept', Schema.String, Effect.succeed('memo'))
              yield* session.transaction((tx) =>
                tx.doc(Notes, { owner: payload.taskId }).pipe(
                  Effect.map((draft) => {
                    draft.note = 'held'
                  }),
                ),
              )
              child = yield* reserve(session, 'child', { owner: payload.taskId })
              return { status: 'completed', result: 'parent' }
            }).pipe(Effect.orDie),
        ],
      ])
      yield* withSetup(
        behaviors,
        Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          const parent = yield* reserve(session, 'parent')
          const fiber = yield* invoke(parent).pipe(Effect.forkScoped)
          yield* until(
            session.task(parent.taskId).pipe(
              Effect.map(Option.getOrUndefined),
              Effect.map((task) => task?.state.status === 'completing'),
            ),
          )
          assert.deepStrictEqual(
            (yield* session.task(parent.taskId).pipe(Effect.map(Option.getOrUndefined)))?.memos,
            { kept: 'memo' },
          )
          assert.strictEqual(
            (yield* session
              .snapshot(Notes, { owner: parent.taskId })
              .pipe(Effect.map(Option.getOrUndefined)))?.value.note,
            'held',
          )
          assert.isUndefined(yield* Effect.sync(() => fiber.pollUnsafe()))
          assert.isDefined(child)
          yield* Deferred.succeed(gate, undefined)
          assert.deepStrictEqual(yield* Fiber.join(fiber), {
            status: 'completed',
            result: 'parent',
          })
          assert.isUndefined(
            (yield* session.task(parent.taskId).pipe(Effect.map(Option.getOrUndefined)))?.memos,
          )
          assert.isUndefined(
            yield* session
              .snapshot(Notes, { owner: parent.taskId })
              .pipe(Effect.map(Option.getOrUndefined)),
          )
          assert.strictEqual(
            (yield* session.task(child!.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state
              .status,
            'terminal',
          )
        }),
      )
    }),
  )

  it.effect(
    'ordinary child helper has a stable receipt and native join returns outcomes in input order',
    () =>
      Effect.gen(function* () {
        let ids: Record.TaskId[] = []
        const behaviors = new Map<string, Behavior>([
          ['bad', () => Effect.succeed({ status: 'failed', result: 'bad' })],
          ['good', () => Effect.succeed({ status: 'completed', result: 'good' })],
          [
            'parent',
            (payload, session) =>
              Effect.gen(function* () {
                const first = yield* Structured.child(
                  Node,
                  (taskId) => ({ ...payload, taskId, name: 'good' }),
                  'good',
                )
                const replay = yield* Structured.child(
                  Node,
                  (taskId) => ({ ...payload, taskId, name: 'good' }),
                  'good',
                )
                assert.strictEqual(first.id, replay.id)
                const second = yield* Structured.child(
                  Node,
                  (taskId) => ({ ...payload, taskId, name: 'bad' }),
                  'bad',
                )
                ids = [first.id, second.id]
                const outcomes = yield* Structured.join(
                  session,
                  payload.taskId,
                  [second.id, first.id],
                  'allSettled',
                )
                return { status: 'completed', outcomes }
              }).pipe(Effect.orDie),
          ],
        ])
        yield* withSetup(
          behaviors,
          Effect.gen(function* () {
            const session = yield* Session.Session
            yield* session.root()
            const parent = yield* reserve(session, 'parent')
            assert.deepStrictEqual(yield* invoke(parent), {
              status: 'completed',
              outcomes: [
                { status: 'failed', result: 'bad' },
                { status: 'completed', result: 'good' },
              ],
            })
            for (const id of ids)
              assert.isFalse(
                (yield* session.task(id).pipe(Effect.map(Option.getOrUndefined)))?.abortRequested,
              )
          }),
        )
      }),
  )

  it.effect(
    'fail-fast cancels and joins only awaited siblings while an outside child still holds the parent',
    () =>
      Effect.gen(function* () {
        const outsideGate = yield* Deferred.make<void>()
        let outside: Payload | undefined
        let sibling: Payload | undefined
        const behaviors = new Map<string, Behavior>([
          ['bad', () => Effect.succeed({ status: 'failed' })],
          ['sibling', () => Effect.never],
          ['outside', () => Deferred.await(outsideGate).pipe(Effect.as({ status: 'completed' }))],
          [
            'parent',
            (payload, session) =>
              Effect.gen(function* () {
                const bad = yield* reserve(session, 'bad', { owner: payload.taskId })
                sibling = yield* reserve(session, 'sibling', { owner: payload.taskId })
                outside = yield* reserve(session, 'outside', { owner: payload.taskId })
                const outcomes = yield* Structured.join(
                  session,
                  payload.taskId,
                  [bad.taskId, sibling.taskId],
                  'failFast',
                )
                return { status: 'completed', outcomes }
              }).pipe(Effect.orDie),
          ],
        ])
        yield* withSetup(
          behaviors,
          Effect.gen(function* () {
            const session = yield* Session.Session
            yield* session.root()
            const parent = yield* reserve(session, 'parent')
            const fiber = yield* invoke(parent).pipe(Effect.forkScoped)
            yield* until(
              session.task(parent.taskId).pipe(
                Effect.map(Option.getOrUndefined),
                Effect.map((task) => task?.state.status === 'completing'),
              ),
            )
            assert.isTrue(
              (yield* session.task(sibling!.taskId).pipe(Effect.map(Option.getOrUndefined)))
                ?.abortRequested,
            )
            assert.isFalse(
              (yield* session.task(outside!.taskId).pipe(Effect.map(Option.getOrUndefined)))
                ?.abortRequested,
            )
            assert.isFalse(
              (yield* session.task(parent.taskId).pipe(Effect.map(Option.getOrUndefined)))
                ?.abortRequested,
            )
            yield* Deferred.succeed(outsideGate, undefined)
            assert.deepStrictEqual(yield* Fiber.join(fiber), {
              status: 'completed',
              outcomes: [{ status: 'failed' }, { status: 'aborted' }],
            })
          }),
        )
      }),
  )

  it.effect(
    'an abort mark is committed before live interruption, joins finalizers and blocks late children',
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>()
        const cleanup = yield* Deferred.make<void>()
        const finishCleanup = yield* Deferred.make<void>()
        const behaviors = new Map<string, Behavior>([
          [
            'work',
            () =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(
                  Deferred.succeed(cleanup, undefined).pipe(
                    Effect.andThen(Deferred.await(finishCleanup)),
                  ),
                ),
              ),
          ],
        ])
        yield* withSetup(
          behaviors,
          Effect.gen(function* () {
            const session = yield* Session.Session
            yield* session.root()
            const task = yield* reserve(session, 'work')
            const fiber = yield* invoke(task).pipe(Effect.forkScoped)
            yield* Deferred.await(entered)
            const reached = yield* Cancellation.mark(session, {
              _tag: 'task' as const,
              kind: 'task',
              id: task.taskId,
            })
            const cancellation = yield* Cancellation.cancel(
              Identity.SessionId.make('ownership'),
              reached,
            ).pipe(Effect.forkScoped)
            yield* Deferred.await(cleanup)
            assert.isTrue(
              (yield* session.committed).tasks.find((value) => value.id === task.taskId)
                ?.abortRequested,
            )
            assert.isUndefined(yield* Effect.sync(() => cancellation.pollUnsafe()))
            yield* rejection(
              reserve(session, 'late', { owner: task.taskId }),
              'StorageError',
              'Invalid',
              'Invalid task owner',
            )
            yield* Deferred.succeed(finishCleanup, undefined)
            yield* Fiber.join(cancellation)
            yield* until(Effect.sync(() => fiber.pollUnsafe() !== undefined))
            assert.deepStrictEqual(yield* Fiber.join(fiber), { status: 'aborted' })
          }),
        )
      }),
  )

  it.effect(
    'a completing abort retains the held result and bottom-up traversal fences background subtrees',
    () =>
      Effect.gen(function* () {
        const gate = yield* Deferred.make<void>()
        let child: Payload | undefined
        const behaviors = new Map<string, Behavior>([
          ['child', () => Deferred.await(gate).pipe(Effect.as({ status: 'completed' }))],
          [
            'parent',
            (payload, session) =>
              reserve(session, 'child', { owner: payload.taskId }).pipe(
                Effect.tap((value) =>
                  Effect.sync(() => {
                    child = value
                  }),
                ),
                Effect.as({ status: 'completed', result: 'held' }),
                Effect.orDie,
              ),
          ],
        ])
        yield* withSetup(
          behaviors,
          Effect.gen(function* () {
            const session = yield* Session.Session
            yield* session.root()
            const background = yield* reserve(session, 'background', { background: true })
            const parent = yield* reserve(session, 'parent')
            const fiber = yield* invoke(parent).pipe(Effect.forkScoped)
            yield* until(
              session.task(parent.taskId).pipe(
                Effect.map(Option.getOrUndefined),
                Effect.map((task) => task?.state.status === 'completing'),
              ),
            )
            const reached = yield* Cancellation.mark(session, {
              _tag: 'conversation' as const,
              kind: 'conversation',
              id: parent.conversationId,
            })
            assert.deepStrictEqual(
              reached.tasks.map((task) => task.id),
              [child!.taskId, parent.taskId],
            )
            assert.isFalse(
              (yield* session.task(background.taskId).pipe(Effect.map(Option.getOrUndefined)))
                ?.abortRequested,
            )
            yield* Cancellation.cancel(Identity.SessionId.make('ownership'), reached)
            assert.deepStrictEqual(yield* Fiber.join(fiber), {
              status: 'completed',
              result: 'held',
            })
            const explicit = yield* Cancellation.mark(session, {
              _tag: 'task' as const,
              kind: 'task',
              id: background.taskId,
            })
            assert.deepStrictEqual(
              explicit.tasks.map((task) => task.id),
              [background.taskId],
            )
          }),
        )
      }),
  )

  it.effect(
    'keeps holding for new ordinary work in an owned conversation and ignores its background root',
    () =>
      Effect.gen(function* () {
        const firstGate = yield* Deferred.make<void>()
        const secondGate = yield* Deferred.make<void>()
        const secondStarted = yield* Deferred.make<void>()
        let owned: Record.ConversationId | undefined
        const behaviors = new Map<string, Behavior>([
          ['first', () => Deferred.await(firstGate).pipe(Effect.as({ status: 'completed' }))],
          [
            'second',
            () =>
              Deferred.succeed(secondStarted, undefined).pipe(
                Effect.andThen(Deferred.await(secondGate)),
                Effect.as({ status: 'completed' }),
              ),
          ],
          [
            'parent',
            (payload, session) =>
              Effect.gen(function* () {
                const conversation = yield* session.transaction((tx) =>
                  tx.createConversation({
                    ownership: { _tag: 'task' as const, kind: 'task', taskId: payload.taskId },
                  }),
                )
                owned = conversation.id
                yield* reserve(session, 'first', { conversationId: owned })
                return { status: 'completed' }
              }).pipe(Effect.orDie),
          ],
        ])
        yield* withSetup(
          behaviors,
          Effect.gen(function* () {
            const session = yield* Session.Session
            yield* session.root()
            const parent = yield* reserve(session, 'parent')
            const fiber = yield* invoke(parent).pipe(Effect.forkScoped)
            yield* until(
              session.task(parent.taskId).pipe(
                Effect.map(Option.getOrUndefined),
                Effect.map((task) => task?.state.status === 'completing'),
              ),
            )
            yield* reserve(session, 'second', { conversationId: owned! })
            const background = yield* reserve(session, 'background', {
              conversationId: owned!,
              background: true,
            })
            yield* Deferred.succeed(firstGate, undefined)
            yield* Deferred.await(secondStarted)
            assert.strictEqual(
              (yield* session.task(parent.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state
                .status,
              'completing',
            )
            yield* Deferred.succeed(secondGate, undefined)
            assert.deepStrictEqual(yield* Fiber.join(fiber), { status: 'completed' })
            assert.strictEqual(
              (yield* session.task(background.taskId).pipe(Effect.map(Option.getOrUndefined)))
                ?.state.status,
              'pending',
            )
          }),
        )
      }),
  )

  it.effect(
    'queued submissions hold an owned conversation before a generation projection exists',
    () =>
      Effect.gen(function* () {
        const dispatchEntered = yield* Deferred.make<void>()
        const dispatchFinish = yield* Deferred.make<void>()
        const behaviors = new Map<string, Behavior>([
          [
            'parent',
            (payload, session) =>
              Effect.gen(function* () {
                const conversation = yield* session.transaction((tx) =>
                  tx.createConversation({
                    ownership: { _tag: 'task' as const, kind: 'task', taskId: payload.taskId },
                  }),
                )
                yield* session.transaction((tx) =>
                  tx.createSubmission({
                    _tag: 'WriteQueued' as const,
                    type: 'write',
                    conversationId: conversation.id,
                    status: 'queued',
                  }),
                )
                return { status: 'completed' }
              }).pipe(Effect.orDie),
          ],
        ])
        const drain = Structured.layerDrainConversations(
          (session, owner, conversation, submissions) =>
            Effect.gen(function* () {
              assert.strictEqual(owner.state.status, 'completing')
              yield* Deferred.succeed(dispatchEntered, undefined)
              yield* Deferred.await(dispatchFinish)
              yield* session.transaction(
                Effect.fnUntraced(function* (tx) {
                  const entry = yield* tx.appendEntry(conversation.id, { kind: 'dispatched' })
                  for (const submission of submissions)
                    yield* tx.placeSubmission(submission.id, entry.id)
                }),
              )
            }),
        )
        yield* withLayer(
          Effect.gen(function* () {
            const session = yield* Session.Session
            yield* session.root()
            const parent = yield* reserve(session, 'parent')
            const fiber = yield* invoke(parent).pipe(Effect.forkScoped)
            yield* Deferred.await(dispatchEntered)
            assert.strictEqual(
              (yield* session.task(parent.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state
                .status,
              'completing',
            )
            yield* Deferred.succeed(dispatchFinish, undefined)
            assert.deepStrictEqual(yield* Fiber.join(fiber), { status: 'completed' })
          }).pipe(Effect.provide(setup(behaviors).pipe(Layer.provideMerge(drain)))),
          Session.layer.pipe(Layer.provideMerge(Memory.layer)),
        )
      }),
  )

  it.effect(
    'observes an external durable mark without owner-local cancellation and preserves memo reads during cleanup',
    () =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>()
        let captured: Ownership.Current['Service'] | undefined
        let read: string | undefined
        const behaviors = new Map<string, Behavior>([
          [
            'work',
            () =>
              Effect.gen(function* () {
                captured = yield* Ownership.Current
                yield* Ownership.memo('kept', Schema.String, Effect.succeed('old'))
                yield* Deferred.succeed(started, undefined)
                return yield* Effect.never.pipe(
                  Effect.onInterrupt(() =>
                    Effect.gen(function* () {
                      read = yield* Ownership.memo(
                        'kept',
                        Schema.String,
                        Effect.die('producer must not run'),
                      )
                      const error = yield* Ownership.memo(
                        'late',
                        Schema.String,
                        Effect.succeed('new'),
                      ).pipe(Effect.flip)
                      assert.instanceOf(error, ExecutionError)
                      if (error instanceof ExecutionError) {
                        assert.strictEqual(error.reason._tag, 'Aborted')
                        assert.strictEqual(
                          error.message,
                          'Task no longer accepts invocation writes',
                        )
                      }
                    }).pipe(Effect.orDie),
                  ),
                )
              }).pipe(Effect.orDie),
          ],
        ])
        yield* withSetup(
          behaviors,
          Effect.gen(function* () {
            const session = yield* Session.Session
            yield* session.root()
            const task = yield* reserve(session, 'work')
            const fiber = yield* invoke(task).pipe(Effect.forkScoped)
            yield* Deferred.await(started)
            yield* Cancellation.mark(session, {
              _tag: 'task' as const,
              kind: 'task',
              id: task.taskId,
            })
            yield* until(Effect.sync(() => fiber.pollUnsafe() !== undefined))
            assert.deepStrictEqual(yield* Fiber.join(fiber), { status: 'aborted' })
            assert.strictEqual(read, 'old')
            yield* rejection(
              captured!.check,
              'ExecutionError',
              'Closed',
              'Task invocation has ended',
            )
          }),
        )
      }),
  )

  it.effect(
    'validates foreign joins, self/owner cycles and explicitly crosses a terminal background ancestor',
    () =>
      withSetup(
        new Map(),
        Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          const root = yield* reserve(session, 'root')
          const child = yield* reserve(session, 'child', { owner: root.taskId })
          const foreign = yield* reserve(session, 'foreign')
          yield* rejection(
            Structured.join(session, root.taskId, [root.taskId]),
            'ExecutionError',
            'InvalidState',
            'A task cannot await itself or its owner',
          )
          yield* rejection(
            Structured.join(session, root.taskId, [Record.TaskId.make(9999)]),
            'ExecutionError',
            'InvalidState',
            'Awaited task 9999 is absent',
          )
          yield* rejection(
            Structured.join(session, child.taskId, [root.taskId]),
            'ExecutionError',
            'InvalidState',
            'A task cannot await itself or its owner',
          )
          yield* rejection(
            Structured.join(session, root.taskId, [foreign.taskId], 'failFast'),
            'ExecutionError',
            'InvalidState',
            'Fail-fast requires directly owned tasks',
          )
          const background = yield* reserve(session, 'background', { background: true })
          const owned = yield* session.transaction((tx) =>
            tx.createConversation({
              ownership: { _tag: 'task' as const, kind: 'task', taskId: background.taskId },
            }),
          )
          yield* Structured.complete(
            session,
            background.taskId,
            { status: 'completed' },
            Identity.SessionId.make('ownership'),
          )
          const below = yield* reserve(session, 'below', { conversationId: owned.id })
          const state = yield* session.committed
          assert.isFalse(
            Option.getOrUndefined(
              Ownership.reach(state, {
                _tag: 'conversation' as const,
                kind: 'conversation',
                id: root.conversationId,
              }),
            )!.tasks.some((task) => task.id === below.taskId),
          )
          assert.isTrue(
            Option.getOrUndefined(
              Ownership.reach(
                state,
                { _tag: 'conversation' as const, kind: 'conversation', id: root.conversationId },
                true,
              ),
            )!.tasks.some((task) => task.id === below.taskId),
          )
        }),
      ),
  )

  it.effect(
    'native deferred suspension and replay preserve a single child binding and parent wakeup',
    () =>
      Effect.gen(function* () {
        const gate = DurableDeferred.make('ownership/native-gate')
        let childId: Record.TaskId | undefined
        const behaviors = new Map<string, Behavior>([
          ['child', () => DurableDeferred.await(gate).pipe(Effect.as({ status: 'completed' }))],
          [
            'parent',
            (payload, session) =>
              Effect.gen(function* () {
                const child = yield* Structured.child(
                  Node,
                  (taskId) => ({ ...payload, taskId, name: 'child' }),
                  'native-child',
                )
                childId = child.id
                const outcomes = yield* Structured.join(session, payload.taskId, [child.id])
                return { status: 'completed', outcomes }
              }).pipe(Effect.orDie),
          ],
        ])
        yield* withSetup(
          behaviors,
          Effect.gen(function* () {
            const session = yield* Session.Session
            yield* session.root()
            const parent = yield* reserve(session, 'parent')
            const executionId = yield* Node.execute(parent, { discard: true })
            yield* until(
              Node.poll(executionId).pipe(
                Effect.map((result) => Option.isSome(result) && result.value._tag === 'Suspended'),
              ),
            )
            const child = (yield* session.task(childId!).pipe(Effect.map(Option.getOrUndefined)))!
            const binding = yield* Schema.decodeUnknownEffect(Ownership.Binding)(child.input)
            const token = DurableDeferred.tokenFromExecutionId(gate, {
              workflow: Node,
              executionId: binding.executionId,
            })
            yield* DurableDeferred.succeed(gate, { token, value: undefined })
            const resumed = yield* Node.execute(parent).pipe(Effect.forkScoped)
            yield* until(Effect.sync(() => resumed.pollUnsafe() !== undefined))
            assert.deepStrictEqual(yield* Fiber.join(resumed), {
              status: 'completed',
              outcomes: [{ status: 'completed' }],
            })
            assert.strictEqual((yield* session.committed).tasks.length, 2)
          }),
        )
      }),
  )

  it.effect(
    'normal terminal publication during a live body does not turn its receipt into an abort',
    () =>
      withSetup(
        new Map(),
        Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          const task = yield* reserve(session, 'normal')
          const value = yield* Cancellation.run(
            task,
            session,
            Effect.gen(function* () {
              yield* Structured.complete(
                session,
                task.taskId,
                { status: 'completed' },
                task.sessionId,
              )
              // Keep the native cancellation monitor alive for more than two 20ms scans after terminal commitment; it must not reinterpret success as abort.
              yield* TestClock.adjust('50 millis')
              return 'receipt'
            }),
          )
          assert.strictEqual(value, 'receipt')
        }),
      ),
  )

  it.effect(
    'authoring evaluate maps success, typed errors and defects inside ordinary native handlers',
    () =>
      Effect.gen(function* () {
        const Custom = Workflow.make('test/custom-native-author', {
          payload: Node.payloadSchema,
          success: Schema.Json,
          error: ExecutionErrorCodec,
          idempotencyKey: ({ taskId }) => String(taskId),
        })
        const custom = Custom.toLayer(
          Effect.fnUntraced(function* (payload) {
            const session = yield* Session.Session
            const body =
              payload.name === 'completed'
                ? Effect.succeed('value')
                : Effect.fail(
                    new ExecutionError({ reason: new InvalidArguments({ message: 'bad' }) }),
                  )
            return yield* Structured.evaluate(
              payload,
              session,
              payload.name === 'faulted' ? Effect.die(new Error('defect')) : body,
            ).pipe(
              Effect.mapError((error) =>
                error._tag === 'StorageError'
                  ? new ExecutionError({
                      reason: new Storage({ message: error.message, cause: error }),
                    })
                  : error,
              ),
            )
          }),
        ).pipe(Layer.provideMerge(setup(new Map())))
        yield* withLayer(
          Effect.gen(function* () {
            const session = yield* Session.Session
            yield* session.root()
            for (const status of ['completed', 'failed', 'faulted']) {
              const payload = yield* reserve(session, status)
              yield* session.transaction(
                Effect.fnUntraced(function* (tx) {
                  const task = (yield* tx
                    .task(payload.taskId)
                    .pipe(Effect.map(Option.getOrUndefined)))!
                  yield* Structured.bind(tx, task, Custom, payload)
                }),
              )
              const result = yield* Custom.execute(payload)
              assert.strictEqual(
                typeof result === 'object' && result !== null && !Array.isArray(result)
                  ? Reflect.get(result, 'status')
                  : undefined,
                status,
              )
              assert.strictEqual(
                (yield* session.task(payload.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state
                  .status,
                'terminal',
              )
            }
          }).pipe(Effect.provide(custom)),
          Session.layer.pipe(Layer.provideMerge(Memory.layer)),
        )
      }),
  )

  it.effect(
    'rejects a new directly owned child in its finishing commit, and only orphans missing code after abort',
    () =>
      withSetup(
        new Map(),
        Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          const parent = yield* reserve(session, 'parent')
          const rejected = yield* session
            .transaction(
              Effect.fnUntraced(function* (tx) {
                const graph = yield* Ownership.readGraph(tx)
                const task = graph.tasks.find((task) => task.id === parent.taskId)!
                const childId = yield* tx.createTask({
                  conversationId: parent.conversationId,
                  owner: parent.taskId,
                  kind: Node._tag,
                  version: 1,
                  input: null,
                  background: false,
                  abortRequested: false,
                  state: { status: 'pending' },
                })
                const child: Record.Task = {
                  id: childId,
                  conversationId: parent.conversationId,
                  owner: parent.taskId,
                  kind: Node._tag,
                  version: 1,
                  input: null,
                  background: false,
                  abortRequested: false,
                  state: { status: 'pending' },
                }
                yield* Structured.hold(
                  tx,
                  task,
                  { status: 'completed' },
                  { ...graph, tasks: [...graph.tasks, child] },
                )
              }),
            )
            .pipe(Effect.exit)
          assert.isTrue(Exit.isFailure(rejected))
          if (Exit.isFailure(rejected)) {
            const failure = Cause.squash(rejected.cause)
            assert.instanceOf(failure, StorageError)
            if (failure instanceof StorageError) {
              assert.strictEqual(failure.reason._tag, 'Invalid')
              assert.strictEqual(
                failure.message,
                'New owned work requires a live non-aborting owner',
              )
            }
          }
          assert.strictEqual((yield* session.committed).tasks.length, 1)
          const missing = yield* reserve(session, 'missing', { owner: parent.taskId })
          yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              const task = (yield* tx.task(missing.taskId).pipe(Effect.map(Option.getOrUndefined)))!
              const binding = yield* Schema.decodeUnknownEffect(Ownership.Binding)(task.input)
              yield* tx.write({
                _tag: 'task' as const,
                type: 'task',
                value: { ...task, input: { ...binding, workflow: 'missing/declaration' } },
              })
            }),
          )
          const held = { status: 'completed', result: 'kept' }
          const blocked = yield* Structured.complete(
            session,
            parent.taskId,
            held,
            parent.sessionId,
          ).pipe(Effect.result)
          assertFailure(
            blocked,
            new ExecutionError({
              reason: new InvalidState({
                message: `Workflow missing/declaration is not declared; task ${missing.taskId} remains blocked`,
              }),
            }),
          )
          assert.strictEqual(
            (yield* session.task(missing.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state
              .status,
            'pending',
          )
          assert.deepStrictEqual(
            (yield* session.task(parent.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state,
            {
              status: 'completing',
              outcome: held,
            },
          )
          yield* Cancellation.mark(session, {
            _tag: 'task' as const,
            kind: 'task',
            id: missing.taskId,
          })
          yield* Structured.drain(session, parent.taskId, parent.sessionId)
          const result = [
            (yield* session.task(missing.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state
              .outcome,
          ]
          assert.strictEqual(
            typeof result[0] === 'object' && result[0] !== null && !Array.isArray(result[0])
              ? Reflect.get(result[0], 'status')
              : undefined,
            'orphaned',
          )
        }),
      ),
  )

  it.effect('a held nested tool failure triggers fail-fast before the failing child drains', () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>()
      const cleanup = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let bad: Payload | undefined
      let sibling: Payload | undefined
      const behaviors = new Map<string, Behavior>([
        [
          'grandchild',
          () =>
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(
                Deferred.succeed(cleanup, undefined).pipe(Effect.andThen(Deferred.await(release))),
              ),
            ),
        ],
        [
          'bad',
          (payload, session) =>
            Effect.gen(function* () {
              const grandchild = yield* reserve(session, 'grandchild', { owner: payload.taskId })
              yield* Node.execute(grandchild, { discard: true })
              yield* Deferred.await(started)
              return { execution: { outcome: 'failed' }, receipt: { status: 'failed' } }
            }).pipe(Effect.orDie),
        ],
        ['sibling', () => Effect.never],
        [
          'parent',
          (payload, session) =>
            Effect.gen(function* () {
              bad = yield* reserve(session, 'bad', { owner: payload.taskId })
              sibling = yield* reserve(session, 'sibling', { owner: payload.taskId })
              const outcomes = yield* Structured.join(
                session,
                payload.taskId,
                [bad.taskId, sibling.taskId],
                'failFast',
              )
              return { status: 'completed', outcomes }
            }).pipe(Effect.orDie),
        ],
      ])
      yield* withSetup(
        behaviors,
        Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          const parent = yield* reserve(session, 'parent')
          const fiber = yield* invoke(parent).pipe(Effect.forkScoped)
          yield* Deferred.await(cleanup)
          yield* until(
            session.task(sibling!.taskId).pipe(
              Effect.map(Option.getOrUndefined),
              Effect.map((task) => task?.state.status === 'terminal'),
            ),
          )
          assert.strictEqual(
            (yield* session.task(bad!.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state
              .status,
            'completing',
          )
          assert.strictEqual(
            (yield* session.task(parent.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state
              .status,
            'waiting',
          )
          assert.isFalse(
            (yield* session.task(bad!.taskId).pipe(Effect.map(Option.getOrUndefined)))
              ?.abortRequested,
          )
          yield* Deferred.succeed(release, undefined)
          assert.isDefined(yield* Fiber.join(fiber))
          assert.strictEqual(
            (yield* session.task(bad!.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state
              .status,
            'terminal',
          )
        }),
      )
    }),
  )

  it.effect('session closure fails the scoped cancellation monitor and joins body cleanup', () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>()
      let cleaned = false
      yield* withSetup(
        new Map(),
        Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          const task = yield* reserve(session, 'close')
          const fiber = yield* Cancellation.run(
            task,
            session,
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(
                Effect.sync(() => {
                  cleaned = true
                }),
              ),
            ),
          ).pipe(Effect.exit, Effect.forkScoped)
          yield* Deferred.await(entered)
          yield* Scope.close(yield* ResourceScope, Exit.void)
          const closed = yield* Fiber.join(fiber)
          assert.isTrue(Exit.isFailure(closed))
          // Resource closure interrupts the admitted body; fiber IDs depend on scheduling.
          if (Exit.isFailure(closed)) assert.isTrue(Cause.hasInterruptsOnly(closed.cause))
          assert.isTrue(cleaned)
        }),
      )
    }),
  )

  it.effect(
    'an owner-local capability cannot cancel an invocation without a physical durable mark',
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        yield* withSetup(
          new Map(),
          Effect.gen(function* () {
            const session = yield* Session.Session
            yield* session.root()
            const task = yield* reserve(session, 'fence')
            const fiber = yield* Cancellation.run(
              task,
              session,
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.as('still running'),
              ),
            ).pipe(Effect.forkScoped)
            yield* Deferred.await(entered)
            const reached = Option.getOrUndefined(
              Ownership.reach(yield* session.committed, {
                _tag: 'task' as const,
                kind: 'task',
                id: task.taskId,
              }),
            )!
            yield* Cancellation.cancel(task.sessionId, reached)
            assert.isUndefined(yield* Effect.sync(() => fiber.pollUnsafe()))
            yield* Deferred.succeed(release, undefined)
            assert.strictEqual(yield* Fiber.join(fiber), 'still running')
          }),
        )
      }),
  )

  it.effect('heterogeneous builtin declarations retain their exact empty schema requirements', () =>
    Effect.gen(function* () {
      const support = Layer.mergeAll(
        Cancellation.layer,
        Ownership.layerDeclarations([Generation, ToolCall, Compaction]),
      )
      yield* Effect.gen(function* () {
        const declarations = yield* Ownership.Declarations
        assert.strictEqual(Option.getOrUndefined(declarations.get(Generation._tag)), Generation)
      }).pipe(Effect.provide(support))
    }),
  )

  it.effect(
    'suspends the native parent on missing code and resumes retained owned work after registration returns',
    () =>
      Effect.gen(function* () {
        let available = false
        const executions: string[] = []
        const original = yield* Ownership.Declarations.pipe(
          Effect.provide(Ownership.layerDeclarations([Node])),
        )
        const declarations = Layer.succeed(
          Ownership.Declarations,
          Ownership.Declarations.of({
            ...original,
            get: (name) => (available ? original.get(name) : Option.none()),
          }),
        )
        const session = yield* Session.Session
        yield* session.root()
        const parent = yield* reserve(session, 'parent')
        const child = yield* reserve(session, 'child', { owner: parent.taskId })
        yield* session.transaction((tx) => tx.doc(Notes, { owner: child.taskId }))
        const handler = Node.toLayer((payload) =>
          Structured.evaluate(
            payload,
            session,
            Activity.make({
              name: 'body',
              success: Schema.Json,
              execute: Effect.sync(() => {
                executions.push(payload.name)
                return payload.name
              }),
            }),
          ).pipe(
            Effect.mapError((error) =>
              error._tag === 'StorageError'
                ? new ExecutionError({
                    reason: new Storage({ message: error.message, cause: error }),
                  })
                : error,
            ),
          ),
        ).pipe(Layer.provideMerge(declarations), Layer.provideMerge(Cancellation.layer))
        yield* Effect.gen(function* () {
          const executionId = yield* Node.execute(parent, { discard: true })
          yield* until(
            Node.poll(executionId).pipe(
              Effect.map((value) => value._tag === 'Some' && value.value._tag === 'Suspended'),
            ),
          )
          assert.strictEqual(
            (yield* session.task(child.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state
              .status,
            'pending',
          )
          assert.strictEqual(
            (yield* session.task(parent.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state
              .status,
            'completing',
          )
          assert.isDefined(
            yield* session
              .snapshot(Notes, { owner: child.taskId })
              .pipe(Effect.map(Option.getOrUndefined)),
          )
          assert.deepStrictEqual(executions, ['parent'])
          available = true
          yield* Node.resume(executionId)
          const result = yield* Node.execute(parent)
          assert.deepStrictEqual(result, { status: 'completed', result: 'parent' })
          assert.deepStrictEqual(executions, ['parent', 'child'])
          assert.strictEqual(
            (yield* session.task(child.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state
              .status,
            'terminal',
          )
          assert.isUndefined(
            yield* session
              .snapshot(Notes, { owner: child.taskId })
              .pipe(Effect.map(Option.getOrUndefined)),
          )
        }).pipe(Effect.provide(handler))
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            Session.layer.pipe(Layer.provideMerge(Memory.layer)),
            WorkflowEngine.layerMemory,
            BunCrypto.layer,
          ),
        ),
      ),
  )

  it.effect('rejects an incomplete custom native child once and preserves the owner hold', () =>
    Effect.gen(function* () {
      let executions = 0
      const Incomplete = Workflow.make('test/incomplete-native-child', {
        payload: Node.payloadSchema,
        success: Schema.Json,
        error: ExecutionErrorCodec,
        idempotencyKey: ({ taskId }) => String(taskId),
      })
      const incomplete = Incomplete.toLayer(() =>
        Effect.sync(() => {
          executions++
          return { status: 'completed' }
        }),
      ).pipe(
        Layer.provideMerge(WorkflowEngine.layerMemory),
        Layer.provideMerge(Session.layer.pipe(Layer.provideMerge(Memory.layer))),
        Layer.provideMerge(Cancellation.layer),
        Layer.provideMerge(Ownership.layerDeclarations([Incomplete])),
        Layer.provide(BunCrypto.layer),
      )
      yield* Effect.gen(function* () {
        const session = yield* Session.Session
        yield* session.root()
        const parent = yield* reserve(session, 'parent')
        const child = yield* reserve(session, 'child', { owner: parent.taskId })
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const task = (yield* tx.task(child.taskId).pipe(Effect.map(Option.getOrUndefined)))!
            yield* Structured.bind(tx, task, Incomplete, child)
          }),
        )
        const outcome = { status: 'completed', result: 'held' }
        const result = yield* Structured.complete(
          session,
          parent.taskId,
          outcome,
          parent.sessionId,
        ).pipe(Effect.result, Effect.timeout('2 seconds'))
        const expected = new ExecutionError({
          reason: new InvalidState({
            message: `Native execution ${child.taskId} ended before its domain projection settled`,
          }),
        })
        assertFailure(result, expected)
        assert.strictEqual(executions, 1)
        assert.deepStrictEqual(
          (yield* session.task(parent.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state,
          {
            status: 'completing',
            outcome,
          },
        )
        assert.strictEqual(
          (yield* session.task(child.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state.status,
          'pending',
        )
        // A retry observes the cached native result and still rejects without
        // invoking the child again or spinning a domain execution loop.
        const replay = yield* Structured.drain(session, parent.taskId, parent.sessionId).pipe(
          Effect.result,
          Effect.timeout('2 seconds'),
        )
        assertFailure(replay, expected)
        assert.strictEqual(executions, 1)
      }).pipe(Effect.provide(incomplete))
    }),
  )
})
