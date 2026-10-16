import * as TemporaryStorage from './fixtures/Storage.js'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Struct from 'effect/Struct'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as TaskPolicy from 'effect-harness/TaskPolicy'
import * as Transaction from 'effect-harness/Transaction'

const root = Record.ROOT_CONVERSATION_ID
const draft = (): Transaction.TaskCreate => ({
  conversationId: root,
  kind: 'policy.test',
  version: 1,
  input: { count: 1 },
  background: false,
  abortRequested: false,
  state: { status: 'running', checkpoint: { phase: 'work' } },
})

for (const backend of ['temporary', 'jsonl'] as const) {
  it.effect(
    `persists restart/abort policies and upgrades legacy defaults on ${backend} reopen`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'task-policy-' })
          const layer = Storage.layerJsonl({ filePath: `${directory}/store.jsonl` })
          const cycle = Effect.gen(function* () {
            const storage = yield* Storage.Storage
            const tasks = yield* Effect.scoped(
              Effect.gen(function* () {
                const session = yield* Session.make()
                return yield* Session.commit(session, (tx) =>
                  Effect.gen(function* () {
                    yield* Transaction.ensureRoot(tx)
                    const legacy = yield* Transaction.createTask(tx, draft())
                    const abandon = yield* Transaction.createTask(tx, {
                      ...draft(),
                      restartPolicy: 'abandon',
                      schedulerFormat: { version: 1, minimumReader: 1 },
                      memos: { ['__proto__']: { saved: true }, toString: 'owned' },
                    })
                    const requested = yield* Transaction.createTask(tx, {
                      ...draft(),
                      restartPolicy: 'abandon',
                    })
                    yield* Transaction.requestTaskAbort(tx, requested.id, 'user stopped work')
                    return { legacy, abandon, requested }
                  }),
                )
              }),
            )
            const reopen = Effect.scoped(
              Effect.gen(function* () {
                const session = yield* Session.make()
                const read = (id: Record.TaskId) =>
                  Session.task(session, id).pipe(Effect.map(Option.getOrThrow))
                const legacy = yield* read(tasks.legacy.id)
                const abandon = yield* read(tasks.abandon.id)
                const requested = yield* read(tasks.requested.id)
                assert.strictEqual(TaskPolicy.restartPolicy(legacy), 'resume')
                assert.strictEqual(legacy.restartPolicy, undefined)
                assert.deepEqual(abandon.schedulerFormat, { version: 1, minimumReader: 1 })
                assert.deepEqual(abandon.memos, {
                  ['__proto__']: { saved: true },
                  toString: 'owned',
                })
                assert.deepEqual(TaskPolicy.abortIntent(requested), {
                  _tag: 'request',
                  reason: 'user stopped work',
                })
                yield* Session.commit(session, (tx) =>
                  Effect.gen(function* () {
                    yield* Transaction.putTask(tx, TaskPolicy.recover(legacy))
                    yield* Transaction.putTask(tx, TaskPolicy.recover(abandon))
                    yield* Transaction.putTask(tx, TaskPolicy.recover(requested))
                  }),
                )
                const restarted = yield* read(abandon.id)
                assert.strictEqual(restarted.state.status, 'pending')
                assert.deepEqual(restarted.abortIntent, { _tag: 'restart' })
                assert.isTrue(restarted.abortRequested)
                assert.isFalse((yield* read(legacy.id)).abortRequested)
                assert.deepEqual((yield* read(requested.id)).abortIntent, requested.abortIntent)
                yield* Session.commit(session, (tx) =>
                  Effect.gen(function* () {
                    yield* Transaction.putTask(tx, TaskPolicy.recover(restarted))
                    yield* Transaction.requestTaskAbort(tx, restarted.id, 'explicit cancellation')
                    yield* Transaction.requestTaskAbort(
                      tx,
                      restarted.id,
                      'later reason must not replace first',
                    )
                  }),
                )
                assert.deepEqual((yield* read(abandon.id)).abortIntent, {
                  _tag: 'request',
                  reason: 'explicit cancellation',
                })
              }),
            )
            if (backend === 'temporary') yield* reopen
            else yield* reopen.pipe(Effect.provide(layer))
            assert.isTrue(Option.isSome(yield* storage.task(tasks.legacy.id)))
          })
          if (backend === 'temporary') yield* cycle.pipe(Effect.provide(TemporaryStorage.layer))
          else yield* cycle.pipe(Effect.provide(layer))
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )
}

describe('persisted task policy invariants', () => {
  it.effect(
    'keeps owner, policy, held outcome and terminal receipt immutable, even when errors are caught',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.make()
          const { task, owner } = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.ensureRoot(tx)
              const owner = yield* Transaction.createTask(tx, draft())
              return {
                owner,
                task: yield* Transaction.createTask(tx, {
                  ...draft(),
                  owner: owner.id,
                  restartPolicy: 'abandon',
                }),
              }
            }),
          )
          yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              const entry = yield* Transaction.appendEntry(tx, root, { kind: 'policy.before' })
              for (const change of [
                { owner: task.id },
                { background: true },
                { kind: 'different' },
                { version: 2 },
                { input: null },
                { restartPolicy: 'resume' as const },
              ]) {
                const error = yield* Transaction.putTask(tx, { ...task, ...change }).pipe(
                  Effect.flip,
                )
                assert.strictEqual(error._tag, 'SessionError')
              }
              assert.isTrue(Option.isSome(yield* Transaction.entry(tx, entry.id)))
              yield* Transaction.requestTaskAbort(tx, task.id, 'first')
              const marked = Option.getOrThrow(yield* Transaction.task(tx, task.id))
              for (const change of [
                { abortRequested: false, abortIntent: undefined },
                { abortIntent: { _tag: 'restart' as const } },
                { abortIntent: { _tag: 'request' as const, reason: 'changed' } },
              ]) {
                const { abortIntent: _intent, ...rest } = marked
                const candidate =
                  change.abortIntent === undefined
                    ? { ...rest, abortRequested: false }
                    : { ...marked, ...change }
                assert.strictEqual(
                  (yield* Transaction.putTask(tx, candidate).pipe(Effect.flip))._tag,
                  'SessionError',
                )
              }
              const held = TaskPolicy.withState(marked, {
                status: 'completing',
                outcome: { status: 'completed', result: 3 },
              })
              yield* Transaction.putTask(tx, held)
              assert.strictEqual(
                (yield* Transaction.putTask(tx, marked).pipe(Effect.flip))._tag,
                'SessionError',
              )
              assert.strictEqual(
                (yield* Transaction.putTask(
                  tx,
                  TaskPolicy.withState(held, {
                    status: 'terminal',
                    outcome: { status: 'completed', result: 9 },
                  }),
                ).pipe(Effect.flip))._tag,
                'SessionError',
              )
              const terminal = TaskPolicy.withState(held, { ...held.state, status: 'terminal' })
              yield* Transaction.putTask(tx, terminal)
              yield* Transaction.requestTaskAbort(tx, task.id, 'terminal does not change')
              yield* Transaction.putTask(tx, terminal)
              assert.strictEqual(
                (yield* Transaction.putTask(tx, held).pipe(Effect.flip))._tag,
                'SessionError',
              )
            }),
          )
          assert.strictEqual(
            Option.getOrThrow(yield* Session.task(session, owner.id)).state.status,
            'running',
          )
          assert.strictEqual(
            Option.getOrThrow(yield* Session.task(session, task.id)).state.status,
            'terminal',
          )
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )

  it.effect(
    'validates markers/intent and honestly demonstrates legacy readers discard additive policy',
    () =>
      Effect.gen(function* () {
        const task = {
          ...draft(),
          id: Record.TaskId.make(2),
          restartPolicy: 'abandon' as const,
          schedulerFormat: { version: 1 as const, minimumReader: 1 as const },
        }
        const json = Schema.fromJsonString(Record.Task)
        assert.deepEqual(
          yield* Schema.encodeEffect(json)(task).pipe(Effect.flatMap(Schema.decodeEffect(json))),
          task,
        )
        for (const candidate of [
          { ...task, abortIntent: { _tag: 'request' } },
          { ...task, schedulerFormat: { version: 2, minimumReader: 1 } },
          { ...task, schedulerFormat: { version: 1, minimumReader: 2 } },
        ])
          assert.strictEqual(
            (yield* Schema.decodeUnknownEffect(Record.Task)(candidate).pipe(Effect.flip))._tag,
            'SchemaError',
          )
        const legacySchema = Record.Task.mapFields(
          Struct.omit(['restartPolicy', 'abortIntent', 'schedulerFormat']),
        )
        const downgraded = yield* Schema.decodeEffect(legacySchema)(task)
        assert.isFalse(Object.hasOwn(downgraded, 'restartPolicy'))
        assert.strictEqual(TaskPolicy.restartPolicy(downgraded), 'resume')
        const held = TaskPolicy.withState(task, {
          status: 'completing',
          outcome: { status: 'completed', result: 1 },
        })
        assert.deepEqual(TaskPolicy.recover(held).state, held.state)
        assert.deepEqual(TaskPolicy.recover(held).abortIntent, { _tag: 'restart' })
        const terminal = TaskPolicy.withState(task, {
          status: 'terminal',
          outcome: { status: 'completed', result: 1 },
        })
        assert.strictEqual(TaskPolicy.recover(terminal), terminal)
      }),
  )

  it.effect('raw adapter writes also reject terminal receipt and ownership changes', () =>
    Effect.gen(function* () {
      const storage = yield* Storage.Storage
      const task = { ...draft(), id: Record.TaskId.make(2) }
      yield* storage.commit([{ _tag: 'task', value: task }])
      assert.strictEqual(
        (yield* storage
          .commit([{ _tag: 'task', value: { ...task, background: true } }])
          .pipe(Effect.flip))._tag,
        'StorageError',
      )
      const terminal = TaskPolicy.withState(task, {
        status: 'terminal',
        outcome: { status: 'aborted' },
      })
      yield* storage.commit([{ _tag: 'task', value: terminal }])
      assert.strictEqual(
        (yield* storage.commit([{ _tag: 'task', value: task }]).pipe(Effect.flip))._tag,
        'StorageError',
      )
    }).pipe(Effect.provide(TemporaryStorage.layer)),
  )
})
