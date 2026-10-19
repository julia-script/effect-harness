import * as TemporaryStorage from './fixtures/Storage.js'
import { assert, describe, it } from '@effect/vitest'
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Record from '../src/Record.js'
import * as Registry from '../src/Registry.js'
import * as Session from '../src/Session.js'

import * as TaskSupervision from '../src/TaskSupervision.js'
import * as Transaction from '../src/Transaction.js'

const root = Record.ROOT_CONVERSATION_ID
const open = Effect.gen(function* () {
  const session = yield* Session.make()
  yield* Session.commit(session, Transaction.ensureRoot)
  const registry = yield* Registry.make
  const schedulerScope = yield* Scope.make()
  yield* Effect.addFinalizer(() => Scope.close(schedulerScope, Exit.void))
  const scheduler = yield* Scope.provide(
    TaskSupervision.make(session).pipe(Effect.provideService(Registry.Registry, registry)),
    schedulerScope,
  )
  return { session, scheduler, schedulerScope }
})
const create = (session: Session.Session) =>
  Session.commit(session, (tx) =>
    Transaction.createTask(tx, {
      conversationId: root,
      kind: 'graph.missing',
      version: 1,
      input: null,
      background: false,
      abortRequested: false,
      state: { status: 'pending', checkpoint: { phase: 'run', private: 'never exposed' } },
    }),
  )

describe('native ownership graph observation', () => {
  it.effect(
    'coherent initial snapshot, atomic ownership batch, no rollback frames and retained terminal edges',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, scheduler } = yield* open
          const parent = yield* create(session)
          const pull = yield* scheduler.watchGraph.pipe(Stream.rechunk(1), Stream.toPull)
          const initial = (yield* pull)[0]
          assert.strictEqual(initial.seq, undefined)
          assert.strictEqual(initial.snapshot.tasks.length, 1)
          assert.strictEqual(initial.snapshot.tasks[0]!.phase, 'run')
          assert.isFalse('checkpoint' in initial.snapshot.tasks[0]!)
          yield* Session.commit(session, (tx) =>
            Transaction.requestTaskAbort(tx, parent.id).pipe(
              Effect.andThen(Effect.fail('rollback')),
            ),
          ).pipe(Effect.flip)
          const conversation = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              const conversation = yield* Transaction.createConversation(tx, {
                ownership: { _tag: 'task', taskId: parent.id },
              })
              yield* Transaction.createTask(tx, {
                conversationId: conversation.id,
                kind: 'graph.background',
                version: 1,
                input: null,
                background: true,
                abortRequested: false,
                state: { status: 'pending', checkpoint: { phase: 'run' } },
              })
              return conversation
            }),
          )
          const committed = (yield* pull)[0]
          assert.strictEqual(committed.snapshot.tasks.length, 2)
          assert.deepStrictEqual(
            committed.snapshot.tasks.find((node) => node.id === parent.id)?.ownedConversations,
            [conversation.id],
          )
          assert.isFalse(committed.snapshot.tasks[0]!.abortRequested)
          yield* Session.commit(session, (tx) =>
            Transaction.putTask(tx, {
              ...parent,
              state: { status: 'terminal', outcome: { status: 'completed', result: null } },
            }),
          )
          const terminal = (yield* pull)[0]
          assert.isAbove(terminal.seq!, committed.seq!)
          assert.strictEqual(terminal.snapshot.tasks.length, 1)
          assert.strictEqual(
            terminal.snapshot.conversations.find((item) => item.id === conversation.id)?.owner
              ?.taskId,
            parent.id,
          )
          assert.deepStrictEqual(yield* scheduler.graph, terminal.snapshot)
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )

  it.effect(
    'slow observer receives overflow, reattachment sees all current work and scheduler close revokes streams',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, scheduler, schedulerScope } = yield* open
          const pull = yield* scheduler.watchGraph.pipe(Stream.rechunk(1), Stream.toPull)
          yield* pull
          for (let index = 0; index < 200; index++) yield* create(session)
          let overflow = false
          for (let index = 0; index < 210; index++) {
            const exit = yield* pull.pipe(Effect.exit)
            if (Exit.isFailure(exit)) {
              const error = Cause.findErrorOption(exit.cause)
              assert.isTrue(Option.isSome(error))
              if (Option.isSome(error) && 'reason' in error.value)
                assert.strictEqual(error.value.reason, 'overflow')
              overflow = true
              break
            }
          }
          assert.isTrue(overflow)
          const fresh = yield* scheduler.watchGraph.pipe(Stream.rechunk(1), Stream.toPull)
          assert.strictEqual((yield* fresh)[0].snapshot.tasks.length, 200)
          yield* Scope.close(schedulerScope, Exit.void)
          const closed = yield* fresh.pipe(Effect.exit)
          assert.isTrue(Exit.isFailure(closed))
          if (Exit.isFailure(closed)) {
            const error = Cause.findErrorOption(closed.cause)
            assert.isTrue(Option.isSome(error))
            if (Option.isSome(error)) assert.strictEqual(error.value._tag, 'TaskError')
          }
          assert.strictEqual(
            (yield* Session.scanTasks(session).pipe(Stream.runCollect)).length,
            200,
          )
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )

  it.effect('unknown task inspection is typed and does not write records', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { session, scheduler } = yield* open
        const id = yield* Schema.decodeEffect(Record.TaskId)(999)
        assert.strictEqual((yield* scheduler.inspect(id).pipe(Effect.flip))._tag, 'TaskError')
        assert.deepStrictEqual(yield* Session.scanTasks(session).pipe(Stream.runCollect), [])
      }),
    ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
})
