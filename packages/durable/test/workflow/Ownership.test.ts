import { assert, describe, it } from '@effect/vitest'
import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as Option from 'effect/Option'
import * as DurableDeferred from 'effect/workflow/DurableDeferred'
import * as Activity from 'effect/workflow/Activity'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Document from '../../src/Document.ts'
import * as Ownership from '../../src/Ownership.ts'
import * as Conversation from '../../src/Conversation.ts'
import * as Record from '../../src/Record.ts'
import * as Session from '../../src/Session.ts'
import * as Memory from '../../src/storage/Memory.ts'
import * as Cancellation from '../../src/workflow/Cancellation.ts'
import * as Structured from '../../src/workflow/Structured.ts'
import { Generation } from '../../src/workflow/Generation.ts'
import { ToolCall } from '../../src/workflow/ToolCall.ts'
import { Compaction } from '../../src/workflow/Compaction.ts'
import { ExecutionError } from '../../src/workflow/ExecutionError.ts'

const Notes = Document.define({
  kind: 'test/owned-notes',
  version: 1,
  scope: 'task',
  schema: Schema.Struct({ note: Schema.String }),
  initial: () => ({ note: 'old' }),
})

const Node = Workflow.make('test/ordinary-owned-work', {
  payload: {
    sessionId: Schema.String,
    conversationId: Record.ConversationId,
    taskId: Record.TaskId,
    name: Schema.String,
  },
  success: Schema.Json,
  error: ExecutionError,
  idempotencyKey: ({ taskId }) => String(taskId),
})
type Payload = typeof Node.payloadSchema.Type
const identity = (
  taskId: Record.TaskId,
  conversationId = Record.ROOT_CONVERSATION_ID,
): Ownership.Identity => ({ sessionId: 'ownership', conversationId, taskId })
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
      yield* Effect.sleep('5 millis')
    }
    return yield* Effect.die('Ownership condition did not settle')
  })
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
  const sessionLayer = Session.layer.pipe(Layer.provideMerge(Memory.layer))
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
              ? new ExecutionError({ reason: 'storage', message: error.message })
              : error,
          ),
        )
      },
      Effect.mapError((error) =>
        error._tag === 'StorageError'
          ? new ExecutionError({ reason: 'storage', message: error.message })
          : error,
      ),
    ),
  )
  return executor.pipe(
    Layer.provideMerge(WorkflowEngine.layerMemory),
    Layer.provideMerge(sessionLayer),
    Layer.provideMerge(Cancellation.layer),
    Layer.provideMerge(Ownership.layerDeclarations([Node])),
    Layer.provide(BunCrypto.layer),
  )
}
const invoke = (payload: Payload) => Node.execute(payload)
const withSetup = <A, E, R>(
  behaviors: ReadonlyMap<string, Behavior>,
  effect: Effect.Effect<A, E, R>,
) => Effect.scoped(effect.pipe(Effect.provide(setup(behaviors))))

describe('native structured ownership', () => {
  it.live(
    'awaitIdle drives restored declarations across ownerless roots and excludes background subtrees',
    () =>
      Effect.scoped(
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
                }).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.as({ status: 'completed' }),
                ),
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
              const session = yield* Session.Session
              const declarations = yield* Ownership.Declarations
              yield* session.root()
              const ordinary = yield* reserve(session, 'ordinary')
              const other = yield* session.transaction((tx) =>
                tx.createConversation({ ownership: { kind: 'ownerless' } }),
              )
              yield* reserve(session, 'failure', { conversationId: other.id })
              const background = yield* reserve(session, 'background', { background: true })
              const owned = yield* session.transaction((tx) =>
                tx.createConversation({ ownership: { kind: 'task', taskId: background.taskId } }),
              )
              const below = yield* reserve(session, 'below-background', {
                conversationId: owned.id,
              })
              const waiting = yield* Conversation.awaitIdle(session).pipe(
                Effect.provideService(Ownership.Declarations, {
                  ...declarations,
                  get: (name) => (available ? declarations.get(name) : undefined),
                }),
                Effect.forkScoped,
              )
              yield* Effect.sleep('40 millis')
              assert.deepStrictEqual(calls, [])
              assert.isUndefined(waiting.pollUnsafe())
              available = true
              yield* until(Effect.sync(() => calls.length === 2))
              assert.isUndefined(waiting.pollUnsafe())
              yield* Deferred.succeed(release, undefined)
              yield* Fiber.join(waiting).pipe(Effect.timeout('3 seconds'))
              assert.strictEqual((yield* session.task(ordinary.taskId))?.state.status, 'terminal')
              assert.strictEqual((yield* session.task(below.taskId))?.state.status, 'pending')
              const blocked = yield* reserve(session, 'unregistered')
              const closing = yield* Conversation.awaitIdle(session, blocked.conversationId).pipe(
                Effect.provideService(Ownership.Declarations, {
                  ...declarations,
                  get: () => undefined,
                }),
                Effect.result,
                Effect.forkScoped,
              )
              yield* session.close
              assert.strictEqual((yield* Fiber.join(closing))._tag, 'Failure')
            }),
          )
        }),
      ),
  )
  it.live('rejects every task and conversation owner ancestor before persisting a join', () =>
    withSetup(
      new Map(),
      Effect.gen(function* () {
        const session = yield* Session.Session
        yield* session.root()
        const grandparent = yield* reserve(session, 'grandparent')
        const parent = yield* reserve(session, 'parent', { owner: grandparent.taskId })
        const child = yield* reserve(session, 'child', { owner: parent.taskId })
        const owned = yield* session.transaction((tx) =>
          tx.createConversation({ ownership: { kind: 'task', taskId: grandparent.taskId } }),
        )
        const conversational = yield* reserve(session, 'conversational', {
          conversationId: owned.id,
        })
        for (const task of [child, conversational]) {
          const result = yield* Structured.join(session, task.taskId, [grandparent.taskId]).pipe(
            Effect.result,
          )
          assert.strictEqual(result._tag, 'Failure')
          assert.deepStrictEqual((yield* session.task(task.taskId))?.state, { status: 'pending' })
        }
      }),
    ),
  )
  it.live(
    'terminal-aborted owners and unmarked background owners permit fresh independent admission',
    () =>
      withSetup(
        new Map(),
        Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          const ended = yield* reserve(session, 'ended')
          const retained = yield* session.transaction((tx) =>
            tx.createConversation({ ownership: { kind: 'task', taskId: ended.taskId } }),
          )
          const outer = yield* reserve(session, 'outer')
          const nested = yield* session.transaction((tx) =>
            tx.createConversation({ ownership: { kind: 'task', taskId: outer.taskId } }),
          )
          const background = yield* reserve(session, 'background', {
            conversationId: nested.id,
            background: true,
          })
          const surviving = yield* session.transaction((tx) =>
            tx.createConversation({ ownership: { kind: 'task', taskId: background.taskId } }),
          )
          yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              const endedTask = (yield* tx.task(ended.taskId))!
              const outerTask = (yield* tx.task(outer.taskId))!
              yield* tx.write({
                type: 'task',
                value: {
                  ...endedTask,
                  abortRequested: true,
                  state: { status: 'terminal', outcome: { status: 'aborted' } },
                },
              })
              yield* tx.write({ type: 'task', value: { ...outerTask, abortRequested: true } })
            }),
          )
          for (const conversation of [retained, surviving]) {
            assert.isDefined(yield* reserve(session, 'fresh', { conversationId: conversation.id }))
            assert.isDefined(
              yield* session.transaction((tx) =>
                tx.createSubmission({
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

  it.live(
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
            tx.createConversation({ ownership: { kind: 'task', taskId: ended.taskId } }),
          )
          yield* Structured.complete(session, ended.taskId, { status: 'completed' }, 'ownership')
          const below = yield* reserve(session, 'below', { conversationId: owned.id })
          const graph = yield* session.committed
          for (const background of [false, true]) {
            assert.deepStrictEqual(
              Ownership.reach(graph, { kind: 'task', id: ended.taskId }, background),
              { tasks: [], conversations: [] },
            )
            assert.isFalse(
              Ownership.reach(graph, { kind: 'task', id: parent.taskId }, background)!.tasks.some(
                (task) => task.id === below.taskId,
              ),
            )
          }
          const reached = Ownership.reach(graph, {
            kind: 'conversation',
            id: parent.conversationId,
          })!
          assert.isTrue(reached.tasks.some((task) => task.id === below.taskId))
          assert.isFalse(reached.tasks.some((task) => task.id === ended.taskId))
          assert.isTrue(reached.conversations.some((conversation) => conversation.id === owned.id))
        }),
      ),
  )

  it.live('holds the native result, memos and documents until a late child drains', () =>
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
            session
              .task(parent.taskId)
              .pipe(Effect.map((task) => task?.state.status === 'completing')),
          )
          assert.deepStrictEqual((yield* session.task(parent.taskId))?.memos, { kept: 'memo' })
          assert.strictEqual(
            (yield* session.snapshot(Notes, { owner: parent.taskId }))?.value.note,
            'held',
          )
          assert.isUndefined(yield* Effect.sync(() => fiber.pollUnsafe()))
          assert.isDefined(child)
          yield* Deferred.succeed(gate, undefined)
          assert.deepStrictEqual(yield* Fiber.join(fiber), {
            status: 'completed',
            result: 'parent',
          })
          assert.isUndefined((yield* session.task(parent.taskId))?.memos)
          assert.isUndefined(yield* session.snapshot(Notes, { owner: parent.taskId }))
          assert.strictEqual((yield* session.task(child!.taskId))?.state.status, 'terminal')
        }),
      )
    }),
  )

  it.live(
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
            for (const id of ids) assert.isFalse((yield* session.task(id))?.abortRequested)
          }),
        )
      }),
  )

  it.live(
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
              session
                .task(parent.taskId)
                .pipe(Effect.map((task) => task?.state.status === 'completing')),
            )
            assert.isTrue((yield* session.task(sibling!.taskId))?.abortRequested)
            assert.isFalse((yield* session.task(outside!.taskId))?.abortRequested)
            assert.isFalse((yield* session.task(parent.taskId))?.abortRequested)
            yield* Deferred.succeed(outsideGate, undefined)
            assert.deepStrictEqual(yield* Fiber.join(fiber), {
              status: 'completed',
              outcomes: [{ status: 'failed' }, { status: 'aborted' }],
            })
          }),
        )
      }),
  )

  it.live(
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
            const reached = yield* Cancellation.mark(session, { kind: 'task', id: task.taskId })
            const cancellation = yield* Cancellation.cancel('ownership', reached).pipe(
              Effect.forkScoped,
            )
            yield* Deferred.await(cleanup)
            assert.isTrue(
              (yield* session.committed).tasks.find((value) => value.id === task.taskId)
                ?.abortRequested,
            )
            assert.isUndefined(yield* Effect.sync(() => cancellation.pollUnsafe()))
            assert.isTrue(
              Exit.isFailure(
                yield* reserve(session, 'late', { owner: task.taskId }).pipe(Effect.exit),
              ),
            )
            yield* Deferred.succeed(finishCleanup, undefined)
            yield* Fiber.join(cancellation)
            assert.deepStrictEqual(yield* Fiber.join(fiber), { status: 'aborted' })
          }),
        )
      }),
  )

  it.live(
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
              session
                .task(parent.taskId)
                .pipe(Effect.map((task) => task?.state.status === 'completing')),
            )
            const reached = yield* Cancellation.mark(session, {
              kind: 'conversation',
              id: parent.conversationId,
            })
            assert.deepStrictEqual(
              reached.tasks.map((task) => task.id),
              [child!.taskId, parent.taskId],
            )
            assert.isFalse((yield* session.task(background.taskId))?.abortRequested)
            yield* Cancellation.cancel('ownership', reached)
            assert.deepStrictEqual(yield* Fiber.join(fiber), {
              status: 'completed',
              result: 'held',
            })
            const explicit = yield* Cancellation.mark(session, {
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

  it.live(
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
                  tx.createConversation({ ownership: { kind: 'task', taskId: payload.taskId } }),
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
              session
                .task(parent.taskId)
                .pipe(Effect.map((task) => task?.state.status === 'completing')),
            )
            yield* reserve(session, 'second', { conversationId: owned! })
            const background = yield* reserve(session, 'background', {
              conversationId: owned!,
              background: true,
            })
            yield* Deferred.succeed(firstGate, undefined)
            yield* Deferred.await(secondStarted)
            assert.strictEqual((yield* session.task(parent.taskId))?.state.status, 'completing')
            yield* Deferred.succeed(secondGate, undefined)
            assert.deepStrictEqual(yield* Fiber.join(fiber), { status: 'completed' })
            assert.strictEqual((yield* session.task(background.taskId))?.state.status, 'pending')
          }),
        )
      }),
  )

  it.live(
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
                  tx.createConversation({ ownership: { kind: 'task', taskId: payload.taskId } }),
                )
                yield* session.transaction((tx) =>
                  tx.createSubmission({
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
        yield* Effect.scoped(
          Effect.gen(function* () {
            const session = yield* Session.Session
            yield* session.root()
            const parent = yield* reserve(session, 'parent')
            const fiber = yield* invoke(parent).pipe(Effect.forkScoped)
            yield* Deferred.await(dispatchEntered)
            assert.strictEqual((yield* session.task(parent.taskId))?.state.status, 'completing')
            yield* Deferred.succeed(dispatchFinish, undefined)
            assert.deepStrictEqual(yield* Fiber.join(fiber), { status: 'completed' })
          }).pipe(Effect.provide(setup(behaviors).pipe(Layer.provideMerge(drain)))),
        )
      }),
  )

  it.live(
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
                      assert.isTrue(
                        Exit.isFailure(
                          yield* Ownership.memo('late', Schema.String, Effect.succeed('new')).pipe(
                            Effect.exit,
                          ),
                        ),
                      )
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
            yield* Cancellation.mark(session, { kind: 'task', id: task.taskId })
            assert.deepStrictEqual(yield* Fiber.join(fiber), { status: 'aborted' })
            assert.strictEqual(read, 'old')
            assert.isTrue(Exit.isFailure(yield* captured!.check.pipe(Effect.exit)))
          }),
        )
      }),
  )

  it.live(
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
          for (const ids of [[root.taskId], [yield* Schema.decodeEffect(Record.TaskId)(9999)]])
            assert.isTrue(
              Exit.isFailure(yield* Structured.join(session, root.taskId, ids).pipe(Effect.exit)),
            )
          assert.isTrue(
            Exit.isFailure(
              yield* Structured.join(session, child.taskId, [root.taskId]).pipe(Effect.exit),
            ),
          )
          assert.isTrue(
            Exit.isFailure(
              yield* Structured.join(session, root.taskId, [foreign.taskId], 'failFast').pipe(
                Effect.exit,
              ),
            ),
          )
          const background = yield* reserve(session, 'background', { background: true })
          const owned = yield* session.transaction((tx) =>
            tx.createConversation({ ownership: { kind: 'task', taskId: background.taskId } }),
          )
          yield* Structured.complete(
            session,
            background.taskId,
            { status: 'completed' },
            'ownership',
          )
          const below = yield* reserve(session, 'below', { conversationId: owned.id })
          const state = yield* session.committed
          assert.isFalse(
            Ownership.reach(state, { kind: 'conversation', id: root.conversationId })!.tasks.some(
              (task) => task.id === below.taskId,
            ),
          )
          assert.isTrue(
            Ownership.reach(
              state,
              { kind: 'conversation', id: root.conversationId },
              true,
            )!.tasks.some((task) => task.id === below.taskId),
          )
        }),
      ),
  )

  it.live(
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
            const child = (yield* session.task(childId!))!
            const binding = yield* Schema.decodeUnknownEffect(Ownership.Binding)(child.input)
            const token = DurableDeferred.tokenFromExecutionId(gate, {
              workflow: Node,
              executionId: binding.executionId,
            })
            yield* DurableDeferred.succeed(gate, { token, value: undefined })
            assert.deepStrictEqual(yield* Node.execute(parent), {
              status: 'completed',
              outcomes: [{ status: 'completed' }],
            })
            assert.strictEqual((yield* session.committed).tasks.length, 2)
          }),
        )
      }),
  )

  it.live(
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
              yield* Effect.sleep('50 millis')
              return 'receipt'
            }),
          )
          assert.strictEqual(value, 'receipt')
        }),
      ),
  )

  it.live(
    'authoring evaluate maps success, typed errors and defects inside ordinary native handlers',
    () => {
      const Custom = Workflow.make('test/custom-native-author', {
        payload: Node.payloadSchema,
        success: Schema.Json,
        error: ExecutionError,
        idempotencyKey: ({ taskId }) => String(taskId),
      })
      const custom = Custom.toLayer(
        Effect.fnUntraced(function* (payload) {
          const session = yield* Session.Session
          const body =
            payload.name === 'completed'
              ? Effect.succeed('value')
              : Effect.fail(new ExecutionError({ reason: 'invalid_arguments', message: 'bad' }))
          return yield* Structured.evaluate(
            payload,
            session,
            payload.name === 'faulted' ? Effect.die(new Error('defect')) : body,
          ).pipe(
            Effect.mapError((error) =>
              error._tag === 'StorageError'
                ? new ExecutionError({ reason: 'storage', message: error.message })
                : error,
            ),
          )
        }),
      ).pipe(Layer.provideMerge(setup(new Map())))
      return Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          for (const status of ['completed', 'failed', 'faulted']) {
            const payload = yield* reserve(session, status)
            yield* session.transaction(
              Effect.fnUntraced(function* (tx) {
                const task = (yield* tx.task(payload.taskId))!
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
            assert.strictEqual((yield* session.task(payload.taskId))?.state.status, 'terminal')
          }
        }).pipe(Effect.provide(custom)),
      )
    },
  )

  it.live(
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
          assert.strictEqual((yield* session.committed).tasks.length, 1)
          const missing = yield* reserve(session, 'missing', { owner: parent.taskId })
          yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              const task = (yield* tx.task(missing.taskId))!
              const binding = yield* Schema.decodeUnknownEffect(Ownership.Binding)(task.input)
              yield* tx.write({
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
          assert.strictEqual(blocked._tag, 'Failure')
          if (blocked._tag === 'Failure') assert.match(blocked.failure.message, /remains blocked/)
          assert.strictEqual((yield* session.task(missing.taskId))?.state.status, 'pending')
          assert.deepStrictEqual((yield* session.task(parent.taskId))?.state, {
            status: 'completing',
            outcome: held,
          })
          yield* Cancellation.mark(session, { kind: 'task', id: missing.taskId })
          yield* Structured.drain(session, parent.taskId, parent.sessionId)
          const result = [(yield* session.task(missing.taskId))?.state.outcome]
          assert.strictEqual(
            typeof result[0] === 'object' && result[0] !== null && !Array.isArray(result[0])
              ? Reflect.get(result[0], 'status')
              : undefined,
            'orphaned',
          )
        }),
      ),
  )

  it.live('a held nested tool failure triggers fail-fast before the failing child drains', () =>
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
            session
              .task(sibling!.taskId)
              .pipe(Effect.map((task) => task?.state.status === 'terminal')),
          )
          assert.strictEqual((yield* session.task(bad!.taskId))?.state.status, 'completing')
          assert.strictEqual((yield* session.task(parent.taskId))?.state.status, 'waiting')
          assert.isFalse((yield* session.task(bad!.taskId))?.abortRequested)
          yield* Deferred.succeed(release, undefined)
          assert.isDefined(yield* Fiber.join(fiber))
          assert.strictEqual((yield* session.task(bad!.taskId))?.state.status, 'terminal')
        }),
      )
    }),
  )

  it.live('session closure fails the scoped cancellation monitor and joins body cleanup', () =>
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
          yield* session.close
          assert.isTrue(Exit.isFailure(yield* Fiber.join(fiber)))
          assert.isTrue(cleaned)
        }),
      )
    }),
  )

  it.live(
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
            const reached = Ownership.reach(yield* session.committed, {
              kind: 'task',
              id: task.taskId,
            })!
            yield* Cancellation.cancel(task.sessionId, reached)
            assert.isUndefined(yield* Effect.sync(() => fiber.pollUnsafe()))
            yield* Deferred.succeed(release, undefined)
            assert.strictEqual(yield* Fiber.join(fiber), 'still running')
          }),
        )
      }),
  )

  it.effect(
    'heterogeneous builtin declarations retain their exact empty schema requirements',
    () => {
      const support: Layer.Layer<Ownership.Declarations | Cancellation.Cancellation> =
        Layer.mergeAll(
          Cancellation.layer,
          Ownership.layerDeclarations([Generation, ToolCall, Compaction]),
        )
      return Effect.gen(function* () {
        const declarations = yield* Ownership.Declarations
        assert.strictEqual(declarations.get(Generation._tag), Generation)
      }).pipe(Effect.provide(support))
    },
  )

  it.live(
    'suspends the native parent on missing code and resumes retained owned work after registration returns',
    () =>
      Effect.scoped(
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
              get: (name) => (available ? original.get(name) : undefined),
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
                  ? new ExecutionError({ reason: 'storage', message: error.message })
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
            assert.strictEqual((yield* session.task(child.taskId))?.state.status, 'pending')
            assert.strictEqual((yield* session.task(parent.taskId))?.state.status, 'completing')
            assert.isDefined(yield* session.snapshot(Notes, { owner: child.taskId }))
            assert.deepStrictEqual(executions, ['parent'])
            available = true
            yield* Node.resume(executionId)
            const result = yield* Node.execute(parent)
            assert.deepStrictEqual(result, { status: 'completed', result: 'parent' })
            assert.deepStrictEqual(executions, ['parent', 'child'])
            assert.strictEqual((yield* session.task(child.taskId))?.state.status, 'terminal')
            assert.isUndefined(yield* session.snapshot(Notes, { owner: child.taskId }))
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
      ),
  )

  it.live('rejects an incomplete custom native child once and preserves the owner hold', () => {
    let executions = 0
    const Incomplete = Workflow.make('test/incomplete-native-child', {
      payload: Node.payloadSchema,
      success: Schema.Json,
      error: ExecutionError,
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
    return Effect.scoped(
      Effect.gen(function* () {
        const session = yield* Session.Session
        yield* session.root()
        const parent = yield* reserve(session, 'parent')
        const child = yield* reserve(session, 'child', { owner: parent.taskId })
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const task = (yield* tx.task(child.taskId))!
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
        assert.strictEqual(result._tag, 'Failure')
        if (result._tag === 'Failure') {
          assert.strictEqual(result.failure._tag, 'ExecutionError')
          assert.strictEqual(
            result.failure._tag === 'ExecutionError' ? result.failure.reason : undefined,
            'invalid_state',
          )
          assert.match(result.failure.message, /ended before its domain projection settled/)
        }
        assert.strictEqual(executions, 1)
        assert.deepStrictEqual((yield* session.task(parent.taskId))?.state, {
          status: 'completing',
          outcome,
        })
        assert.strictEqual((yield* session.task(child.taskId))?.state.status, 'pending')
        // A retry observes the cached native result and still rejects without
        // invoking the child again or spinning a domain execution loop.
        const replay = yield* Structured.drain(session, parent.taskId, parent.sessionId).pipe(
          Effect.result,
          Effect.timeout('2 seconds'),
        )
        assert.strictEqual(replay._tag, 'Failure')
        assert.strictEqual(executions, 1)
      }).pipe(Effect.provide(incomplete)),
    )
  })
})
