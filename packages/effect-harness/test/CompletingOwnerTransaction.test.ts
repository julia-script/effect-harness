import * as TemporaryStorage from './fixtures/Storage.js'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Document from 'effect-harness/Document'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'

const root = Record.ROOT_CONVERSATION_ID
const outcome = { status: 'completed', result: 10 } as const
const draft = (conversationId = root): Transaction.TaskCreate => ({
  conversationId,
  kind: 'completing-owner',
  version: 1,
  input: null,
  background: false,
  abortRequested: false,
  state: { status: 'running', checkpoint: { phase: 'work' } },
})
const held = (task: Record.Task): Record.Task => ({
  ...task,
  state: { status: 'completing', outcome },
})
const receipt = (task: Record.Task): Record.Task => ({
  ...task,
  state: { status: 'terminal', outcome },
})
const document = Document.define({
  kind: 'completing-owner.retained',
  version: 1,
  scope: 'task',
  schema: Schema.Struct({ count: Schema.Natural }),
  initial: () => ({ count: 0 }),
})
const target = (taskId: Record.TaskId): Document.Target => ({
  scope: { _tag: 'task', taskId },
})
const some = <A>(value: Option.Option<A>): A => {
  assert.isTrue(Option.isSome(value))
  return Option.getOrThrow(value)
}
const assertConflict = (failure: Session.Failure, operation = 'task.owner') => {
  assert.strictEqual(failure._tag, 'SessionError')
  if (failure._tag === 'SessionError') {
    assert.strictEqual(failure.reason, 'conflict')
    assert.strictEqual(failure.operation, operation)
  }
}
const initialize = (session: Session.Session) =>
  Session.commit(session, (tx) =>
    Effect.gen(function* () {
      yield* Transaction.ensureRoot(tx)
      const parent = yield* Transaction.createTask(tx, draft())
      yield* Transaction.ensureDocument(tx, document, target(parent.id))
      return parent
    }),
  )

for (const backend of ['temporary', 'jsonl'] as const) {
  const storage = Layer.unwrap(
    Effect.gen(function* () {
      if (backend === 'temporary') return TemporaryStorage.layer
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'completing-owner-' })

      return Storage.layerJsonl({ filePath: `${directory}/storage.jsonl` })
    }),
  ).pipe(Layer.provide(NodeServices.layer))

  describe(`completing owner transactions: ${backend}`, () => {
    it.effect(
      'a caught late admission failure preserves earlier admitted conversation drafts',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const session = yield* Session.make()
            const parent = yield* initialize(session)
            const admitted = yield* Session.commit(session, (tx) =>
              Effect.gen(function* () {
                const options = { ownership: { _tag: 'task', taskId: parent.id } } as const
                const admitted = yield* Transaction.createConversation(tx, options)
                yield* Transaction.putTask(tx, held(parent))
                assertConflict(yield* Transaction.createConversation(tx, options).pipe(Effect.flip))
                assertConflict(
                  yield* Transaction.createTask(tx, { ...draft(), owner: parent.id }).pipe(
                    Effect.flip,
                  ),
                )
                return admitted
              }),
            )
            assert.deepStrictEqual(
              some(yield* Session.conversation(session, admitted.id)),
              admitted,
            )
            assert.deepStrictEqual(some(yield* Session.task(session, parent.id)), held(parent))
            assert.strictEqual(
              (yield* Session.scanTasks(session).pipe(Stream.runCollect)).length,
              1,
            )
            assert.strictEqual(
              (yield* Session.scanConversations(session).pipe(Stream.runCollect)).length,
              2,
            )
          }),
        ).pipe(Effect.provide(storage)),
    )

    for (const persisted of [false, true]) {
      it.effect(
        `preserves admission and later lifecycle with ${persisted ? 'stored' : 'new'} owner`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const session = yield* Session.make()
              const previous = persisted ? yield* initialize(session) : undefined
              const { parent, child, conversation, foreground } = yield* Session.commit(
                session,
                (tx) =>
                  Effect.gen(function* () {
                    yield* Transaction.ensureRoot(tx)
                    const parent = previous ?? (yield* Transaction.createTask(tx, draft()))
                    yield* Transaction.ensureDocument(tx, document, target(parent.id))
                    const child = yield* Transaction.createTask(tx, {
                      ...draft(),
                      owner: parent.id,
                    })
                    const conversation = yield* Transaction.createConversation(tx, {
                      ownership: { _tag: 'task', taskId: parent.id },
                    })
                    const foreground = yield* Transaction.createTask(tx, draft(conversation.id))
                    yield* Transaction.putTask(tx, held(parent))
                    return { parent, child, conversation, foreground }
                  }),
              )
              assert.deepStrictEqual(some(yield* Session.task(session, parent.id)), held(parent))
              assert.deepStrictEqual(some(yield* Session.task(session, child.id)), child)
              assert.deepStrictEqual(some(yield* Session.task(session, foreground.id)), foreground)
              assert.deepStrictEqual(
                some(yield* Session.conversation(session, conversation.id)).owner,
                {
                  conversationId: root,
                  taskId: parent.id,
                },
              )
              assert.isTrue(
                Option.isSome(yield* Session.snapshot(session, document, target(parent.id))),
              )
              yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* Transaction.putTask(tx, receipt(child))
                  yield* Transaction.putTask(tx, receipt(foreground))
                  yield* Transaction.putTask(tx, receipt(parent))
                }),
              )
              assert.isTrue(
                Option.isNone(yield* Session.snapshot(session, document, target(parent.id))),
              )
              assert.strictEqual(
                (yield* Session.scanTasks(session).pipe(Stream.runCollect)).length,
                3,
              )
              assert.strictEqual(
                (yield* Session.scanConversations(session, { ownerTaskId: parent.id }).pipe(
                  Stream.runCollect,
                )).length,
                1,
              )
            }),
          ).pipe(Effect.provide(storage)),
      )
    }

    for (const transition of ['terminal', 'abort', 'version'] as const) {
      it.effect(`rolls back admitted work and mixed writes after ${transition}`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const session = yield* Session.make()
            const parent = yield* initialize(session)
            const before = some(yield* Session.snapshot(session, document, target(parent.id)))
            const failure = yield* Session.commit(session, (tx) =>
              Effect.gen(function* () {
                yield* Transaction.setDocument(tx, document, target(parent.id), { count: 99 })
                yield* Transaction.appendEntry(tx, root, { kind: 'completing-owner.rollback' })
                yield* Transaction.createTask(tx, { ...draft(), owner: parent.id })
                yield* Transaction.createConversation(tx, {
                  ownership: { _tag: 'task', taskId: parent.id },
                })
                if (transition === 'terminal') yield* Transaction.putTask(tx, receipt(parent))
                else if (transition === 'abort') yield* Transaction.requestTaskAbort(tx, parent.id)
                else yield* Transaction.putTask(tx, { ...held(parent), version: 2 })
              }),
            ).pipe(Effect.flip)
            assertConflict(failure, transition === 'version' ? 'task.write' : 'task.owner')
            assert.deepStrictEqual(some(yield* Session.task(session, parent.id)), parent)
            assert.deepStrictEqual(
              some(yield* Session.snapshot(session, document, target(parent.id))),
              before,
            )
            assert.strictEqual(
              (yield* Session.scanTasks(session).pipe(Stream.runCollect)).length,
              1,
            )
            assert.strictEqual(
              (yield* Session.scanConversations(session).pipe(Stream.runCollect)).length,
              1,
            )
            assert.strictEqual(
              (yield* Session.scanEntries(session, { conversationId: root }).pipe(
                Stream.runCollect,
              )).length,
              0,
            )
          }),
        ).pipe(Effect.provide(storage)),
      )
    }

    for (const state of ['completing', 'terminal'] as const) {
      it.effect(
        `rejects new admission to an already ${state} owner and rolls back earlier writes`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const session = yield* Session.make()
              const parent = yield* initialize(session)
              yield* Session.commit(session, (tx) =>
                Transaction.putTask(tx, state === 'completing' ? held(parent) : receipt(parent)),
              )
              for (const operation of ['conversation', 'child'] as const) {
                const failure = yield* Session.commit(session, (tx) =>
                  Effect.gen(function* () {
                    yield* Transaction.appendEntry(tx, root, { kind: 'completing-owner.forbidden' })
                    yield* Transaction.createTask(tx, draft())
                    if (operation === 'conversation')
                      yield* Transaction.createConversation(tx, {
                        ownership: { _tag: 'task', taskId: parent.id },
                      })
                    else yield* Transaction.createTask(tx, { ...draft(), owner: parent.id })
                  }),
                ).pipe(Effect.flip)
                assertConflict(failure)
                assert.strictEqual(
                  (yield* Session.scanTasks(session).pipe(Stream.runCollect)).length,
                  1,
                )
                assert.strictEqual(
                  (yield* Session.scanConversations(session).pipe(Stream.runCollect)).length,
                  1,
                )
                assert.strictEqual(
                  (yield* Session.scanEntries(session, { conversationId: root }).pipe(
                    Stream.runCollect,
                  )).length,
                  0,
                )
              }
            }),
          ).pipe(Effect.provide(storage)),
      )
    }
  })
}
