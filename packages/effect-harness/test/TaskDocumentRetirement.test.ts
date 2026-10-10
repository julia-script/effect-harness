import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Pull from 'effect/Pull'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Document from 'effect-harness/Document'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import { StorageError } from 'effect-harness/StorageError'
import * as Tool from 'effect-harness/Tool'
import { ToolExecution } from 'effect-harness/ToolExecution'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Transaction from 'effect-harness/Transaction'

const root = Record.ROOT_CONVERSATION_ID
const schema = Schema.Struct({ count: Schema.Natural })
const document = Document.family({
  kind: 'retirement.task',
  version: 1,
  scope: 'task',
  schema,
  initial: () => ({ count: 0 }),
})
const conversationDocument = Document.define({
  kind: 'retirement.conversation',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  schema,
  initial: () => ({ count: 0 }),
})
const conversationTarget = {
  scope: { _tag: 'conversation', conversationId: root },
} satisfies Document.Target
const target = (taskId: Record.TaskId, key = 'saved'): Document.Target => ({
  scope: { _tag: 'task', taskId },
  key,
})
const draft = (): Transaction.TaskCreate => ({
  conversationId: root,
  kind: 'retirement',
  version: 1,
  input: null,
  background: false,
  abortRequested: false,
  state: { status: 'running', checkpoint: { phase: 'work' } },
})
const terminal = (task: Record.Task, outcome: Record.TaskOutcome): Record.Task => ({
  ...task,
  state: { status: 'terminal', outcome },
})
const outcomes = [
  { status: 'completed', result: null },
  { status: 'failed', error: { message: 'failed' } },
  { status: 'aborted', reason: 'cancelled' },
  { status: 'orphaned', reason: 'abandoned' },
  { status: 'faulted', error: { message: 'faulted' } },
] as const satisfies ReadonlyArray<Record.TaskOutcome>
const some = <A>(value: Option.Option<A>): A => {
  assert.isTrue(Option.isSome(value))
  return Option.getOrThrow(value)
}
const initialize = (session: Session.Session) =>
  Session.commit(session, (tx) =>
    Effect.gen(function* () {
      yield* Transaction.ensureRoot(tx)
      const task = yield* Transaction.createTask(tx, draft())
      const saved = yield* Transaction.ensureDocument(tx, document, target(task.id))
      yield* Transaction.ensureDocument(tx, document, target(task.id, 'unloaded'))
      yield* Transaction.ensureDocument(tx, conversationDocument, conversationTarget)
      const entry = yield* Transaction.appendEntry(tx, root, { kind: 'retirement.before' })
      return { task, saved, entry }
    }),
  )
const withMemory = <A, E>(program: Effect.Effect<A, E, Storage.Storage | Scope.Scope>) =>
  Effect.scoped(program).pipe(Effect.provide(Storage.layerMemory))
const assertConflict = (failure: Session.Failure) => {
  assert.strictEqual(failure._tag, 'SessionError')
  if (failure._tag === 'SessionError') {
    assert.strictEqual(failure.reason, 'conflict')
    assert.strictEqual(failure.operation, 'document.scope')
  }
}
const rejectWrites = (tx: Transaction.Transaction, taskId: Record.TaskId) =>
  Effect.gen(function* () {
    for (const key of ['saved', 'missing']) {
      assertConflict(
        yield* Transaction.ensureDocument(tx, document, target(taskId, key)).pipe(Effect.flip),
      )
      assertConflict(
        yield* Transaction.setDocument(tx, document, target(taskId, key), { count: 99 }).pipe(
          Effect.flip,
        ),
      )
      assertConflict(
        yield* Transaction.updateDocument(tx, document, target(taskId, key), () => {
          assert.fail('A terminal owner must be checked before running the replacement')
          return { count: 99 }
        }).pipe(Effect.flip),
      )
    }
  })

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  describe(`task document retirement: ${backend}`, () => {
    for (const outcome of outcomes) {
      it.effect(`retires ${outcome.status} documents atomically and survives reopen`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'task-retirement-' })
            const layer =
              backend === 'sqlite'
                ? Storage.layerSql.pipe(
                    Layer.provide(SqliteClient.layer({ filename: `${directory}/storage.sqlite` })),
                  )
                : Storage.layerJsonl({ filePath: `${directory}/storage.jsonl` })
            const run = Effect.gen(function* () {
              const storage = yield* Storage.Storage
              const initialized = yield* Effect.scoped(
                Effect.gen(function* () {
                  const session = yield* Session.make()
                  const initialized = yield* initialize(session)
                  const { task, saved, entry } = initialized
                  const other = yield* Session.commit(session, (tx) =>
                    Effect.gen(function* () {
                      const other = yield* Transaction.createTask(tx, draft())
                      yield* Transaction.ensureDocument(tx, document, target(other.id))
                      return other
                    }),
                  )
                  // Populate the committed cache and subscribe to the old incarnation.
                  const before = some(yield* Session.snapshot(session, document, target(task.id)))
                  const pull = yield* Session.watch(session, document, target(task.id)).pipe(
                    Stream.toPull,
                  )
                  assert.strictEqual((yield* pull)[0].record.id, saved.record.id)
                  const commits = yield* Session.commits(session).pipe(Stream.toPull)
                  const committed = yield* commits.pipe(Effect.forkChild)
                  yield* Effect.yieldNow
                  let stagedId: Record.DocumentId | undefined
                  yield* Session.commit(session, (tx) =>
                    Effect.gen(function* () {
                      yield* Transaction.updateDocument(
                        tx,
                        document,
                        target(task.id),
                        ({ count }) => ({ count: count + 1 }),
                      )
                      stagedId = (yield* Transaction.ensureDocument(
                        tx,
                        document,
                        target(task.id, 'new'),
                      )).record.id
                      yield* Transaction.putTask(tx, terminal(task, outcome))
                      assert.isTrue(
                        Option.isNone(yield* Transaction.snapshot(tx, document, target(task.id))),
                      )
                      assert.strictEqual(
                        (yield* Transaction.scanDocuments(tx, {
                          scope: target(task.id).scope,
                        }).pipe(Stream.runCollect)).length,
                        0,
                      )
                      yield* rejectWrites(tx, task.id)
                    }),
                  )
                  const batch = (yield* Fiber.join(committed))[0]
                  assert.strictEqual(
                    batch.changes.filter((write) => write._tag === 'task').length,
                    1,
                  )
                  assert.strictEqual(
                    batch.changes.filter((write) => write._tag === 'document.retire').length,
                    2,
                  )
                  const ended = yield* pull.pipe(Effect.exit)
                  assert.isTrue(Exit.isFailure(ended))
                  if (Exit.isFailure(ended)) assert.isTrue(Pull.isDoneCause(ended.cause))
                  assert.isTrue(
                    Option.isNone(yield* Session.snapshot(session, document, target(task.id))),
                  )
                  assert.strictEqual(
                    (yield* storage
                      .scanDocuments({ scope: target(task.id).scope })
                      .pipe(Stream.runCollect)).length,
                    0,
                  )
                  assert.isTrue(Option.isNone(yield* storage.document(saved.record.id)))
                  const beforeSeq = some(yield* storage.entry(entry.id)).commitSeq
                  const historical = yield* storage
                    .scanDocuments({ scope: target(task.id).scope, at: beforeSeq })
                    .pipe(Stream.runCollect)
                  assert.strictEqual(historical.length, 2)
                  assert.isTrue(historical.every((record) => record.retiredAt === batch.seq))
                  const replacement = batch.changes.find(
                    (write) => write._tag === 'document.change',
                  )
                  assert.deepEqual(replacement?.content, {
                    _tag: 'base',
                    version: 1,
                    value: { count: 1 },
                  })
                  assert.deepEqual(before.value, { count: 0 })
                  assert.deepEqual(saved.value, { count: 0 })
                  if (stagedId !== undefined)
                    assert.isTrue(Option.isNone(yield* storage.document(stagedId)))
                  assert.isTrue(
                    Option.isSome(yield* Session.snapshot(session, document, target(other.id))),
                  )
                  assert.deepEqual(
                    some(
                      yield* Session.snapshotAsOf(
                        session,
                        conversationDocument,
                        conversationTarget,
                        entry.id,
                      ),
                    ).value,
                    { count: 0 },
                  )
                  assert.deepEqual(
                    some(yield* Session.task(session, task.id)).state.outcome,
                    outcome,
                  )
                  return initialized
                }),
              )
              // A new Session has no cached state to hide an incorrect persisted retirement.
              const reopened = yield* Session.make()
              assert.isTrue(
                Option.isNone(
                  yield* Session.snapshot(reopened, document, target(initialized.task.id)),
                ),
              )
              yield* Session.commit(reopened, (tx) => rejectWrites(tx, initialized.task.id))
              return initialized
            })
            if (backend === 'memory') return yield* run.pipe(Effect.provide(Storage.layerMemory))
            const initialized = yield* run.pipe(Effect.provide(layer))
            // Rebuild the backend too, proving disk recovery rather than just Session cache eviction.
            yield* Effect.gen(function* () {
              const session = yield* Session.make()
              assert.isTrue(
                Option.isNone(
                  yield* Session.snapshot(session, document, target(initialized.task.id)),
                ),
              )
              assert.deepEqual(
                some(yield* Session.task(session, initialized.task.id)).state.outcome,
                outcome,
              )
              yield* Session.commit(session, (tx) => rejectWrites(tx, initialized.task.id))
            }).pipe(Effect.provide(layer))
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
      )
    }
  })
}

describe('task retirement failures and legacy state', () => {
  it.effect('normal Harness settlement retires documents created by a running tool', () =>
    withMemory(
      Effect.gen(function* () {
        const tools = Toolkit.make(Tool.make('work', { success: Schema.String, replay: 'unsafe' }))
        const finish = {
          type: 'finish' as const,
          reason: 'stop' as const,
          usage: {
            inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
            outputTokens: { total: 1, text: 1, reasoning: undefined },
          },
          response: undefined,
        }
        const model = yield* LanguageModel.make({
          generateText: ({ prompt }) =>
            Effect.succeed(
              prompt.content.some((message) => message.role === 'tool')
                ? [{ type: 'text', text: 'done' }, finish]
                : [
                    {
                      type: 'tool-call',
                      id: 'retirement-call',
                      name: 'work',
                      params: {},
                      providerExecuted: false,
                    },
                    { ...finish, reason: 'tool-calls' as const },
                  ],
            ),
          streamText: () => Stream.empty,
        })
        let owner: Record.TaskId | undefined
        const runtime = yield* HarnessRuntime.make({ tools }).pipe(
          Effect.provideService(LanguageModel.LanguageModel, model),
          Effect.provide(
            tools.toLayer({
              work: Effect.fn('work')(function* () {
                const execution = yield* ToolExecution
                owner = execution.taskId
                yield* execution.commit((tx) =>
                  Transaction.ensureDocument(tx, document, target(execution.taskId)),
                )
                return 'worked'
              }),
            }),
          ),
        )
        const job = yield* runtime.backend.submit({
          conversationId: yield* runtime.backend.root,
          draft: { type: 'input', content: 'go' },
        })
        assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
        assert.isDefined(owner)
        if (owner === undefined) return
        assert.strictEqual(
          some(yield* Session.task(runtime.session, owner)).state.status,
          'terminal',
        )
        assert.isTrue(
          Option.isNone(yield* Session.snapshot(runtime.session, document, target(owner))),
        )
        const taskId = owner
        yield* Session.commit(runtime.session, (tx) => rejectWrites(tx, taskId))
      }),
    ),
  )

  it.effect(
    'rejects legacy terminal acquisition before migration and preserves detached reads',
    () =>
      withMemory(
        Effect.gen(function* () {
          const storage = yield* Storage.Storage
          const taskId = yield* storage.mintId<Record.TaskId>()
          const id = yield* storage.mintId<Record.DocumentId>()
          const task = terminal({ ...draft(), id: taskId }, outcomes[0])
          yield* storage.commit([
            { _tag: 'conversation', value: { id: root } },
            { _tag: 'task', value: task },
            {
              _tag: 'document.create',
              record: { id, kind: document.definition.kind, ...target(taskId) },
              content: { _tag: 'base', version: 1, value: { count: 7 } },
            },
          ])
          const session = yield* Session.make()
          const upgrading = Document.family({
            ...document.definition,
            version: 2,
            migrations: {
              1: () => {
                assert.fail('Terminal acquisition must not run a migration')
                return Effect.succeed({ count: 999 })
              },
            },
          })
          yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* rejectWrites(tx, taskId)
              assertConflict(
                yield* Transaction.ensureDocument(tx, upgrading, target(taskId)).pipe(Effect.flip),
              )
              assertConflict(
                yield* Transaction.updateDocument(
                  tx,
                  upgrading,
                  target(taskId),
                  (value) => value,
                ).pipe(Effect.flip),
              )
              yield* Transaction.appendEntry(tx, root, { kind: 'legacy.caught' })
            }),
          )
          assert.deepEqual(some(yield* Session.snapshot(session, document, target(taskId))).value, {
            count: 7,
          })
          assert.strictEqual(some(yield* storage.document(id)).version, 1)
          assert.strictEqual(
            (yield* storage.scanDocuments({ scope: target(taskId).scope }).pipe(Stream.runCollect))
              .length,
            1,
          )
          yield* Session.commit(session, (tx) =>
            Transaction.retireDocument(tx, document, target(taskId)),
          )
          assert.isTrue(Option.isNone(yield* Session.snapshot(session, document, target(taskId))))
        }),
      ),
  )

  it.effect('settles a task and all new/recreated incarnations in its creation transaction', () =>
    withMemory(
      Effect.gen(function* () {
        const session = yield* Session.make()
        const { task, first, second } = yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.ensureRoot(tx)
            const task = yield* Transaction.createTask(tx, draft())
            const first = yield* Transaction.ensureDocument(tx, document, target(task.id))
            yield* Transaction.retireDocument(tx, document, target(task.id))
            const second = yield* Transaction.ensureDocument(tx, document, target(task.id))
            yield* Transaction.putTask(tx, terminal(task, outcomes[0]))
            yield* rejectWrites(tx, task.id)
            return { task, first, second }
          }),
        )
        const storage = yield* Storage.Storage
        assert.isTrue(Option.isNone(yield* storage.document(first.record.id)))
        assert.isTrue(Option.isNone(yield* storage.document(second.record.id)))
        assert.isTrue(Option.isNone(yield* Session.snapshot(session, document, target(task.id))))
        assert.strictEqual(some(yield* Session.task(session, task.id)).state.status, 'terminal')
      }),
    ),
  )

  for (const mode of ['callback', 'io'] as const) {
    it.effect(`rolls back ${mode} failure without ending watches or evicting cached values`, () =>
      withMemory(
        Effect.gen(function* () {
          const storage = yield* Storage.Storage
          let failCommit = false
          const failure = new StorageError({
            reason: 'io',
            operation: 'test.commit',
            message: 'injected',
          })
          const wrapped: Storage.Storage['Service'] = {
            ...storage,
            commit: (writes) =>
              Effect.suspend(() => (failCommit ? Effect.fail(failure) : storage.commit(writes))),
          }
          const session = yield* Session.make().pipe(
            Effect.provideService(Storage.Storage, wrapped),
          )
          const { task } = yield* initialize(session)
          const pull = yield* Session.watch(session, document, target(task.id)).pipe(Stream.toPull)
          yield* pull
          assert.strictEqual(
            some(yield* Session.snapshot(session, document, target(task.id))).value.count,
            0,
          )
          failCommit = mode === 'io'
          yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.putTask(tx, terminal(task, outcomes[0]))
              if (mode === 'callback') return yield* Effect.fail('rollback')
            }),
          ).pipe(Effect.flip)
          failCommit = false
          assert.strictEqual(some(yield* Session.task(session, task.id)).state.status, 'running')
          assert.strictEqual(
            some(yield* Session.snapshot(session, document, target(task.id))).value.count,
            0,
          )
          yield* Session.commit(session, (tx) =>
            Transaction.updateDocument(tx, document, target(task.id), () => ({ count: 8 })),
          )
          assert.strictEqual((yield* pull)[0].value.count, 8)
        }),
      ),
    )
  }

  for (const mode of ['scan', 'content', 'invalid'] as const) {
    it.effect(`caught ${mode} settlement error stages neither task nor partial retirement`, () =>
      withMemory(
        Effect.gen(function* () {
          const storage = yield* Storage.Storage
          const failure = new StorageError({
            reason: 'io',
            operation: 'test.retire',
            message: 'injected',
          })
          let fail = false
          let reads = 0
          const wrapped: Storage.Storage['Service'] = {
            ...storage,
            scanDocuments: (query) =>
              fail && mode === 'scan'
                ? Stream.concat(storage.scanDocuments(query), Stream.fail(failure))
                : storage.scanDocuments(query),
            document: (id, at) =>
              Effect.suspend(() => {
                reads++
                return fail && mode === 'content' && reads === 2
                  ? Effect.fail(failure)
                  : storage.document(id, at)
              }),
          }
          const session = yield* Session.make().pipe(
            Effect.provideService(Storage.Storage, wrapped),
          )
          const { task } = yield* initialize(session)
          yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              // Keep one new draft in the overlay before preparation fails after a stored read.
              yield* Transaction.ensureDocument(tx, document, target(task.id, 'new'))
              fail = true
              reads = 0
              const next =
                mode === 'invalid'
                  ? {
                      ...terminal(task, outcomes[0]),
                      conversationId: Record.ConversationId.make(999),
                    }
                  : terminal(task, outcomes[0])
              yield* Transaction.putTask(tx, next).pipe(Effect.flip)
              fail = false
              assert.strictEqual(some(yield* Transaction.task(tx, task.id)).state.status, 'running')
              assert.strictEqual(
                some(yield* Transaction.snapshot(tx, document, target(task.id, 'new'))).value.count,
                0,
              )
              assert.strictEqual(
                (yield* Transaction.scanDocuments(tx, { scope: target(task.id).scope }).pipe(
                  Stream.runCollect,
                )).length,
                3,
              )
              yield* Transaction.appendEntry(tx, root, { kind: 'retirement.caught' })
            }),
          )
          assert.strictEqual(some(yield* Session.task(session, task.id)).state.status, 'running')
          assert.strictEqual(
            (yield* Session.scanDocuments(session, { scope: target(task.id).scope }).pipe(
              Stream.runCollect,
            )).length,
            3,
          )
          yield* Session.commit(session, (tx) =>
            Transaction.putTask(tx, terminal(task, outcomes[0])),
          )
          assert.strictEqual(
            (yield* Session.scanDocuments(session, { scope: target(task.id).scope }).pipe(
              Stream.runCollect,
            )).length,
            0,
          )
        }),
      ),
    )
  }

  it.effect(
    'allows documents during completing/abort-requested work and rejects missing owners',
    () =>
      withMemory(
        Effect.gen(function* () {
          const session = yield* Session.make()
          const { task } = yield* initialize(session)
          yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.putTask(tx, {
                ...task,
                abortRequested: true,
                state: { status: 'completing', outcome: outcomes[2] },
              })
              yield* Transaction.ensureDocument(tx, document, target(task.id, 'new'))
              yield* Transaction.updateDocument(tx, document, target(task.id), () => ({ count: 3 }))
              const failure = yield* Transaction.ensureDocument(
                tx,
                document,
                target(Record.TaskId.make(999)),
              ).pipe(Effect.flip)
              assert.strictEqual(failure._tag, 'SessionError')
              if (failure._tag === 'SessionError') assert.strictEqual(failure.reason, 'notFound')
            }),
          )
          assert.strictEqual(
            some(yield* Session.snapshot(session, document, target(task.id))).value.count,
            3,
          )
          yield* Session.commit(session, (tx) =>
            Transaction.putTask(tx, terminal({ ...task, abortRequested: true }, outcomes[2])),
          )
          assert.isTrue(Option.isNone(yield* Session.snapshot(session, document, target(task.id))))
        }),
      ),
  )
})
