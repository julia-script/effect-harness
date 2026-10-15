import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import type * as Context from 'effect/Context'
import * as Cause from 'effect/Cause'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Stream from 'effect/Stream'
import * as ConversationInitializer from '../src/ConversationInitializer.js'
import * as Identity from '../src/Identity.js'
import * as Record from '../src/Record.js'
import * as RunControl from '../src/RunControl.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import { StorageError } from '../src/StorageError.js'
import * as Transaction from '../src/Transaction.js'
import * as Inbox from '../src/internal/Inbox.js'

const root = Record.ROOT_CONVERSATION_ID
const ownerless = { ownership: { _tag: 'ownerless' } } as const
const initializer = ConversationInitializer.make({
  execute: (tx, record) => Inbox.initialize(tx, record.id),
})
const open = Effect.gen(function* () {
  const session = yield* Session.make({ initializers: [initializer] })
  yield* Session.commit(session, Transaction.ensureRoot)
  return session
})
const memory = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.scoped(effect).pipe(Effect.provide(Storage.layerMemory))
const task = (tx: Transaction.Transaction, conversationId = root) =>
  Transaction.createTask(tx, {
    conversationId,
    kind: 'inbox.test',
    version: 1,
    input: null,
    background: false,
    abortRequested: false,
    state: { status: 'running', checkpoint: { phase: 'work' } },
  })
const draft = (mode: Inbox.Draft['mode'], kind: string): Inbox.Draft => ({
  mode,
  payload: { kind },
})
const admit = (session: Session.Session, mode: Inbox.Draft['mode'], kind: string) =>
  Session.commit(session, (tx) => Inbox.admit(tx, root, draft(mode, kind)))
const queue = (session: Session.Session) =>
  Session.snapshot(session, Inbox.document, Inbox.target(root)).pipe(
    Effect.map((value) => Option.getOrThrow(value).value.items),
  )
const membership = (session: Session.Session) =>
  Session.snapshot(session, RunControl.document, Inbox.target(root)).pipe(
    Effect.map((value) => Option.getOrThrow(value).value.active),
  )
const entries = (session: Session.Session) =>
  Session.scanEntries(session, { conversationId: root, order: 'ascending' }).pipe(Stream.runCollect)
const start = (session: Session.Session) =>
  Session.commit(session, (tx) =>
    Effect.gen(function* () {
      const run = yield* task(tx)
      const selected = yield* Inbox.select(tx, root, { boundary: 'final', nextTaskId: run.id })
      return { run, selected }
    }),
  )

describe('inactive inbox boundary foundation', () => {
  for (const followUp of ['one', 'all'] as const)
    for (const steering of ['one', 'all'] as const) {
      it.effect(
        `final: independent ${followUp}/${steering} modes, writes first and users in ID order`,
        () =>
          memory(
            Effect.gen(function* () {
              const session = yield* open
              const s1 = yield* admit(session, 'steer', 's1')
              const f1 = yield* admit(session, 'followUp', 'f1')
              const w1 = yield* admit(session, 'write', 'w1')
              const f2 = yield* admit(session, 'followUp', 'f2')
              const w2 = yield* admit(session, 'write', 'w2')
              const s2 = yield* admit(session, 'steer', 's2')
              const selected = yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  const run = yield* task(tx)
                  return yield* Inbox.select(tx, root, {
                    boundary: 'final',
                    nextTaskId: run.id,
                    modes: { followUp, steering },
                  })
                }),
              )
              const userIds = [
                s1.id,
                f1.id,
                ...(followUp === 'all' ? [f2.id] : []),
                ...(steering === 'all' ? [s2.id] : []),
              ].sort((a, b) => a - b)
              assert.deepEqual(
                selected.placed.map((value) => value.id),
                [w1.id, w2.id, ...userIds],
              )
              assert.deepEqual(selected.active?.submissionIds, userIds)
              assert.deepEqual(
                (yield* queue(session)).map((value) => value.submissionId),
                [f2.id, s2.id].filter((id) => !userIds.includes(id)),
              )
              assert.deepEqual(
                (yield* entries(session)).slice(0, 2).map((entry) => entry.kind),
                ['w1', 'w2'],
              )
              assert.strictEqual(
                (yield* Session.scanTasks(session).pipe(Stream.runCollect)).length,
                1,
              )
            }),
          ),
      )
    }

  it.effect(
    'postTools selects only steers and retains original run membership; defaults are one',
    () =>
      memory(
        Effect.gen(function* () {
          const session = yield* open
          const original = yield* admit(session, 'followUp', 'original')
          const { run } = yield* start(session)
          const f1 = yield* admit(session, 'followUp', 'f1')
          const s1 = yield* admit(session, 'steer', 's1')
          const s2 = yield* admit(session, 'steer', 's2')
          const w1 = yield* admit(session, 'write', 'w1')
          const selected = yield* Session.commit(session, (tx) =>
            Inbox.select(tx, root, { boundary: 'postTools', taskId: run.id }),
          )
          assert.deepEqual(
            selected.placed.map((value) => value.id),
            [w1.id, s1.id],
          )
          assert.deepEqual(selected.active, { taskId: run.id, submissionIds: [original.id, s1.id] })
          assert.deepEqual(
            (yield* queue(session)).map((item) => item.submissionId),
            [f1.id, s2.id],
          )
          const next = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              const nextRun = yield* task(tx)
              return yield* Inbox.select(tx, root, {
                boundary: 'final',
                taskId: run.id,
                nextTaskId: nextRun.id,
              })
            }),
          )
          assert.notStrictEqual(next.active?.taskId, run.id)
          assert.deepEqual(next.active?.submissionIds, [f1.id, s2.id])
        }),
      ),
  )

  it.effect(
    'postTools all mode drains steers without consuming follow-ups or interrupting a tool fiber',
    () =>
      memory(
        Effect.gen(function* () {
          const session = yield* open
          yield* admit(session, 'followUp', 'original')
          const { run } = yield* start(session)
          const entered = yield* Deferred.make<void>()
          const finish = yield* Deferred.make<void>()
          let interrupted = false
          const tool = yield* Effect.gen(function* () {
            yield* Deferred.succeed(entered, undefined)
            yield* Deferred.await(finish)
          }).pipe(
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                interrupted = true
              }),
            ),
            Effect.forkChild,
          )
          yield* Deferred.await(entered)
          const f1 = yield* admit(session, 'followUp', 'f1')
          const s1 = yield* admit(session, 'steer', 's1')
          const s2 = yield* admit(session, 'steer', 's2')
          assert.isFalse(interrupted)
          assert.strictEqual((yield* entries(session)).length, 1)
          yield* Deferred.succeed(finish, undefined)
          yield* Fiber.join(tool)
          const selected = yield* Session.commit(session, (tx) =>
            Inbox.select(tx, root, {
              boundary: 'postTools',
              taskId: run.id,
              modes: { followUp: 'all', steering: 'all' },
            }),
          )
          assert.deepEqual(
            selected.placed.map((value) => value.id),
            [s1.id, s2.id],
          )
          assert.deepEqual(
            (yield* queue(session)).map((item) => item.submissionId),
            [f1.id],
          )
          assert.isFalse(interrupted)
        }),
      ),
  )

  it.effect(
    'a queued reset converts postTools into final only through the context seam; later head edits can be stale',
    () =>
      memory(
        Effect.gen(function* () {
          const session = yield* open
          yield* admit(session, 'followUp', 'old')
          const { run } = yield* start(session)
          const oldHead = (yield* entries(session))[0]!.id
          const follow = yield* admit(session, 'followUp', 'next')
          const reset = yield* Session.commit(session, (tx) =>
            Inbox.admit(tx, root, { mode: 'write', payload: { kind: 'reset', head: 'self' } }),
          )
          const stale = yield* Session.commit(session, (tx) =>
            Inbox.admit(tx, root, { mode: 'write', payload: { kind: 'compact', head: oldHead } }),
          )
          const selected = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              const next = yield* task(tx)
              return yield* Inbox.select(tx, root, {
                boundary: 'postTools',
                taskId: run.id,
                nextTaskId: next.id,
                context: { head: oldHead, resetsRun: (payload) => payload.kind === 'reset' },
              })
            }),
          )
          assert.strictEqual(selected.boundary, 'final')
          assert.deepEqual(
            selected.placed.map((value) => value.id),
            [reset.id, follow.id],
          )
          assert.deepEqual(
            selected.stale.map((value) => value.id),
            [stale.id],
          )
          assert.deepEqual(
            (yield* entries(session)).map((entry) => entry.kind),
            ['old', 'reset', 'next'],
          )
        }),
      ),
  )

  it.effect('incidental self-heads are passive writes, and idle writes create no run', () =>
    memory(
      Effect.gen(function* () {
        const session = yield* open
        yield* Session.commit(session, (tx) =>
          Inbox.admit(tx, root, { mode: 'write', payload: { kind: 'audit', head: 'self' } }),
        )
        const selected = yield* Session.commit(session, (tx) =>
          Inbox.select(tx, root, { boundary: 'final' }),
        )
        assert.isUndefined(selected.active)
        assert.strictEqual(selected.placed.length, 1)
        assert.strictEqual((yield* Session.scanTasks(session).pipe(Stream.runCollect)).length, 0)
      }),
    ),
  )

  it.effect(
    'deduplicates requests before queue mutation, even after placement/withdrawal; cross-type conflict rolls back',
    () =>
      memory(
        Effect.gen(function* () {
          const session = yield* open
          const requestId = Identity.RequestId.make('same')
          const input = { ...draft('followUp', 'original'), requestId }
          const first = yield* Session.commit(session, (tx) => Inbox.admit(tx, root, input))
          const duplicate = yield* Session.commit(session, (tx) =>
            Inbox.admit(tx, root, { ...input, payload: { kind: 'changed' } }),
          )
          assert.deepEqual(duplicate, first)
          yield* Session.commit(session, (tx) =>
            Inbox.admit(tx, root, { ...draft('write', 'bad'), requestId }).pipe(Effect.flip),
          )
          assert.strictEqual((yield* queue(session)).length, 1)
          yield* start(session)
          assert.strictEqual(
            (yield* Session.commit(session, (tx) => Inbox.admit(tx, root, input))).id,
            first.id,
          )
          assert.strictEqual((yield* queue(session)).length, 0)
          const other = yield* admit(session, 'steer', 'withdraw')
          assert.isTrue(yield* Session.commit(session, (tx) => Inbox.withdraw(tx, root, other.id)))
          assert.isFalse(yield* Session.commit(session, (tx) => Inbox.withdraw(tx, root, first.id)))
          assert.strictEqual((yield* entries(session)).length, 1)
        }),
      ),
  )

  it.effect(
    'caught selection failure discards appended writes, receipts and documents but preserves earlier caller drafts',
    () =>
      memory(
        Effect.gen(function* () {
          const session = yield* open
          const write = yield* admit(session, 'write', 'rollback.write')
          const input = yield* admit(session, 'followUp', 'rollback.input')
          yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.appendEntry(tx, root, { kind: 'caller.kept' })
              const failure = yield* Inbox.select(tx, root, { boundary: 'final' }).pipe(Effect.flip)
              assert.strictEqual(failure._tag, 'SessionError')
              assert.deepEqual(
                (yield* Transaction.scanEntries(tx, { conversationId: root }).pipe(
                  Stream.runCollect,
                )).map((entry) => entry.kind),
                ['caller.kept'],
              )
              assert.strictEqual(
                Option.getOrThrow(yield* Transaction.submission(tx, write.id)).status,
                'queued',
              )
              assert.strictEqual(
                Option.getOrThrow(yield* Transaction.submission(tx, input.id)).status,
                'queued',
              )
            }),
          )
          assert.deepEqual(
            (yield* entries(session)).map((entry) => entry.kind),
            ['caller.kept'],
          )
          assert.strictEqual((yield* queue(session)).length, 2)
          yield* start(session)
          assert.deepEqual(
            (yield* entries(session)).map((entry) => entry.kind),
            ['caller.kept', 'rollback.write', 'rollback.input'],
          )
        }),
      ),
  )

  it.effect(
    'caught context failure rolls back earlier writes; final enclosing failure rolls back successful helper selection',
    () =>
      memory(
        Effect.gen(function* () {
          const session = yield* open
          yield* admit(session, 'write', 'first')
          yield* admit(session, 'write', 'second')
          yield* Session.commit(session, (tx) =>
            Inbox.select(tx, root, {
              boundary: 'final',
              context: {
                resetsRun: (payload) => {
                  if (payload.kind === 'second') throw new Error('bad classification')
                  return false
                },
              },
            }).pipe(Effect.flip),
          )
          assert.strictEqual((yield* entries(session)).length, 0)
          yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Inbox.select(tx, root, { boundary: 'final' })
              return yield* Effect.fail('caller rolled back')
            }),
          ).pipe(Effect.flip)
          assert.strictEqual((yield* queue(session)).length, 2)
          assert.strictEqual((yield* entries(session)).length, 0)
        }),
      ),
  )

  it.effect('forks initialize empty queue and membership rather than copying parent work', () =>
    memory(
      Effect.gen(function* () {
        const session = yield* open
        yield* admit(session, 'followUp', 'placed')
        yield* start(session)
        yield* admit(session, 'steer', 'queued')
        const at = (yield* entries(session))[0]!.id
        const fork = yield* Session.commit(session, (tx) =>
          Transaction.forkConversation(tx, root, at, ownerless),
        )
        const childQueue = Option.getOrThrow(
          yield* Session.snapshot(session, Inbox.document, Inbox.target(fork.id)),
        )
        const childControl = Option.getOrThrow(
          yield* Session.snapshot(session, RunControl.document, Inbox.target(fork.id)),
        )
        assert.deepEqual(childQueue.value, { items: [] })
        assert.deepEqual(childControl.value, {})
        assert.strictEqual(childQueue.record.history, 'latest')
        assert.strictEqual(childControl.record.fork, 'initial')
        assert.strictEqual((yield* queue(session)).length, 1)
        assert.isDefined(yield* membership(session))
      }),
    ),
  )

  it.effect(
    'reconciles settled queue receipts and terminal/missing run memberships; stale boundary owners cannot place writes',
    () =>
      memory(
        Effect.gen(function* () {
          const session = yield* open
          const original = yield* admit(session, 'followUp', 'original')
          const { run } = yield* start(session)
          const queued = yield* admit(session, 'steer', 'queued')
          yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.putSubmission(tx, {
                _tag: 'InputUnanswered',
                type: 'input',
                status: 'unanswered',
                id: queued.id,
                conversationId: root,
                reason: 'external withdrawal',
              })
              yield* Transaction.putSubmission(tx, {
                _tag: 'InputUnanswered',
                type: 'input',
                status: 'unanswered',
                id: original.id,
                conversationId: root,
                entry: Option.getOrThrow(
                  Option.fromUndefinedOr(
                    Option.getOrThrow(yield* Transaction.submission(tx, original.id)).entry,
                  ),
                ),
                reason: 'settled',
              })
              yield* Transaction.putTask(tx, {
                ...run,
                state: { status: 'terminal', outcome: { status: 'completed', result: null } },
              })
              yield* Inbox.reconcile(tx, root)
            }),
          )
          assert.deepEqual(yield* queue(session), [])
          assert.isUndefined(yield* membership(session))
          yield* admit(session, 'write', 'must.wait')
          yield* Session.commit(session, (tx) =>
            Inbox.select(tx, root, { boundary: 'postTools', taskId: run.id }).pipe(Effect.flip),
          )
          assert.strictEqual((yield* entries(session)).length, 1)
          yield* Session.commit(session, (tx) =>
            Transaction.setDocument(tx, RunControl.document, Inbox.target(root), {
              active: { taskId: Record.TaskId.make(999999), submissionIds: [original.id] },
            }),
          )
          yield* Session.commit(session, (tx) => Inbox.reconcile(tx, root))
          assert.isUndefined(yield* membership(session))
        }),
      ),
  )

  for (const operation of ['admit', 'withdraw', 'reconcile'] as const) {
    it.effect(
      `caught ${operation} failure restores earlier document drafts and submission receipts`,
      () =>
        memory(
          Effect.gen(function* () {
            const storage = yield* Storage.Storage
            let remaining: number | undefined
            const faulty: Context.Service.Shape<typeof Storage.Storage> = {
              ...storage,
              conversation: (id) =>
                Effect.suspend(() => {
                  if (remaining !== undefined && --remaining === 0)
                    return Effect.fail(
                      new StorageError({
                        reason: 'io',
                        operation: 'inbox.test',
                        message: 'injected late read failure',
                      }),
                    )
                  return storage.conversation(id)
                }),
            }
            const session = yield* open.pipe(Effect.provideService(Storage.Storage, faulty))
            const queued = yield* admit(session, 'followUp', 'queued')
            yield* Session.commit(session, (tx) =>
              Effect.gen(function* () {
                const before = Option.getOrThrow(
                  yield* Transaction.snapshot(tx, Inbox.document, Inbox.target(root)),
                ).value
                yield* Transaction.setDocument(tx, RunControl.document, Inbox.target(root), {})
                if (operation === 'reconcile')
                  yield* Transaction.putSubmission(tx, {
                    _tag: 'InputUnanswered',
                    type: 'input',
                    status: 'unanswered',
                    id: queued.id,
                    conversationId: root,
                    reason: 'prior caller draft',
                  })
                remaining = operation === 'withdraw' ? 3 : 4
                if (operation === 'admit')
                  yield* Inbox.admit(tx, root, draft('write', 'rollback')).pipe(Effect.flip)
                if (operation === 'withdraw')
                  yield* Inbox.withdraw(tx, root, queued.id).pipe(Effect.flip)
                if (operation === 'reconcile') yield* Inbox.reconcile(tx, root).pipe(Effect.flip)
                remaining = undefined
                assert.deepEqual(
                  Option.getOrThrow(
                    yield* Transaction.snapshot(tx, Inbox.document, Inbox.target(root)),
                  ).value,
                  before,
                )
                assert.strictEqual(
                  Option.getOrThrow(yield* Transaction.submission(tx, queued.id)).status,
                  operation === 'reconcile' ? 'unanswered' : 'queued',
                )
                assert.strictEqual(
                  (yield* Transaction.scanSubmissions(tx).pipe(Stream.runCollect)).length,
                  1,
                )
                yield* Transaction.appendEntry(tx, root, { kind: 'caller.survives' })
              }),
            )
            assert.strictEqual((yield* queue(session)).length, 1)
            assert.strictEqual((yield* entries(session)).length, 1)
          }),
        ),
    )
  }

  for (const failure of ['defect', 'interruption'] as const) {
    it.effect(
      `${failure} after admission drafts leaves no overlay changes and releases the parent permit`,
      () =>
        memory(
          Effect.gen(function* () {
            const storage = yield* Storage.Storage
            const reached = yield* Deferred.make<void>()
            let reads: number | undefined
            const faulty: Context.Service.Shape<typeof Storage.Storage> = {
              ...storage,
              conversation: (id) =>
                Effect.suspend(() => {
                  if (reads !== undefined && --reads === 0)
                    return Effect.gen(function* () {
                      yield* Deferred.succeed(reached, undefined)
                      if (failure === 'defect') return yield* Effect.die('injected defect')
                      return yield* Effect.never
                    })
                  return storage.conversation(id)
                }),
            }
            const session = yield* open.pipe(Effect.provideService(Storage.Storage, faulty))
            yield* Session.commit(session, (tx) =>
              Effect.gen(function* () {
                yield* Transaction.setDocument(tx, Inbox.document, Inbox.target(root), {
                  items: [],
                })
                reads = 4
                const child = yield* Inbox.admit(tx, root, draft('write', 'rollback')).pipe(
                  Effect.forkChild,
                )
                yield* Deferred.await(reached)
                if (failure === 'interruption') yield* Fiber.interrupt(child)
                const exit = yield* Fiber.await(child)
                assert.strictEqual(exit._tag, 'Failure')
                if (exit._tag === 'Failure')
                  assert.isTrue(
                    failure === 'defect'
                      ? Cause.hasDies(exit.cause)
                      : Cause.hasInterrupts(exit.cause),
                  )
                reads = undefined
                assert.strictEqual(
                  (yield* Transaction.scanSubmissions(tx).pipe(Stream.runCollect)).length,
                  0,
                )
                assert.deepEqual(
                  Option.getOrThrow(
                    yield* Transaction.snapshot(tx, Inbox.document, Inbox.target(root)),
                  ).value,
                  { items: [] },
                )
                yield* Transaction.appendEntry(tx, root, { kind: 'caller.survives' })
              }),
            )
            assert.deepEqual(yield* queue(session), [])
            assert.strictEqual((yield* entries(session)).length, 1)
          }),
        ),
    )
  }

  for (const ordering of [
    'admission first',
    'selection first',
    'withdrawal first',
    'placement first',
  ] as const) {
    it.effect(`concurrent boundary uses Session commit order: ${ordering}`, () =>
      memory(
        Effect.gen(function* () {
          const session = yield* open
          const initial = yield* admit(session, 'followUp', 'initial')
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const first = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Deferred.succeed(entered, undefined)
              yield* Deferred.await(release)
              if (ordering === 'admission first')
                return yield* Inbox.admit(tx, root, draft('followUp', 'arrival'))
              if (ordering === 'withdrawal first')
                return yield* Inbox.withdraw(tx, root, initial.id)
              const run = yield* task(tx)
              return yield* Inbox.select(tx, root, {
                boundary: 'final',
                nextTaskId: run.id,
                modes: { followUp: 'all' },
              })
            }),
          ).pipe(Effect.forkChild)
          yield* Deferred.await(entered)
          const second = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              if (ordering === 'placement first') return yield* Inbox.withdraw(tx, root, initial.id)
              if (ordering === 'selection first')
                return yield* Inbox.admit(tx, root, draft('followUp', 'arrival'))
              const run = yield* task(tx)
              return yield* Inbox.select(tx, root, {
                boundary: 'final',
                nextTaskId: run.id,
                modes: { followUp: 'all' },
              })
            }),
          ).pipe(Effect.forkChild)
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(first)
          yield* Fiber.join(second)
          const expectedEntries = {
            'admission first': 2,
            'selection first': 1,
            'withdrawal first': 0,
            'placement first': 1,
          }[ordering]
          assert.strictEqual((yield* entries(session)).length, expectedEntries)
          assert.strictEqual((yield* queue(session)).length, ordering === 'selection first' ? 1 : 0)
        }),
      ),
    )
  }
})

for (const backend of ['memory', 'jsonl', 'sqlite'] as const) {
  it.effect(
    `${backend}: reopen before and after selection retains receipts/membership without duplicate placement`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'inbox-boundary-' })
          const disk =
            backend === 'sqlite'
              ? Storage.layerJournal().pipe(
                  Layer.provide(SqlEventJournal.layer()),
                  Layer.provide(SqliteClient.layer({ filename: `${directory}/state.sqlite` })),
                )
              : Storage.layerJsonl({ filePath: `${directory}/state.jsonl` })
          const before = Effect.scoped(
            Effect.gen(function* () {
              const session = yield* open
              const input = yield* admit(session, 'followUp', 'persisted.input')
              yield* admit(session, 'write', 'persisted.write')
              return input.id
            }),
          )
          const select = Effect.scoped(
            Effect.gen(function* () {
              const session = yield* open
              assert.strictEqual((yield* queue(session)).length, 2)
              const { run, selected } = yield* start(session)
              assert.strictEqual(selected.placed.length, 2)
              return run.id
            }),
          )
          const after = (input: Record.SubmissionId, run: Record.TaskId) =>
            Effect.scoped(
              Effect.gen(function* () {
                const session = yield* open
                assert.deepEqual(yield* membership(session), {
                  taskId: run,
                  submissionIds: [input],
                })
                assert.deepEqual(
                  (yield* entries(session)).map((entry) => entry.kind),
                  ['persisted.write', 'persisted.input'],
                )
                yield* Session.commit(session, (tx) =>
                  Inbox.select(tx, root, { boundary: 'postTools', taskId: run }),
                )
                assert.strictEqual((yield* entries(session)).length, 2)
                yield* Session.commit(session, (tx) =>
                  Inbox.select(tx, root, { boundary: 'final', taskId: run }),
                )
                assert.isUndefined(yield* membership(session))
              }),
            )
          if (backend === 'memory')
            yield* Effect.gen(function* () {
              const input = yield* before
              const run = yield* select
              yield* after(input, run)
            }).pipe(Effect.provide(Storage.layerMemory))
          else {
            const input = yield* before.pipe(Effect.provide(disk))
            const run = yield* select.pipe(Effect.provide(disk))
            yield* after(input, run).pipe(Effect.provide(disk))
          }
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )
}
