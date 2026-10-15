import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import type * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as TestClock from 'effect/testing/TestClock'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as ToolProgress from '../src/ToolProgress.ts'
import * as Transaction from 'effect-harness/Transaction'

const root = Record.ROOT_CONVERSATION_ID
const retention = { retain: 'tail' as const, maxBytes: 12, maxLines: 2 }
const policy = { minInterval: 100000, bytesPerSecond: 102400 }
const target = {
  scope: { _tag: 'conversation' as const, conversationId: root },
  key: 'native-call',
}
const open = Effect.gen(function* () {
  const session = yield* Session.make()
  yield* Session.commit(session, Transaction.ensureRoot)
  const gate: ToolProgress.Gate = { commit: (change) => Session.commit(session, change) }
  return { session, gate }
})
const saved = Effect.fnUntraced(function* (session: Session.Session) {
  return (yield* Effect.fromOption(yield* Session.snapshot(session, ToolProgress.document, target)))
    .value
})
const turns = Effect.forEach(Array.from({ length: 16 }), () => Effect.yieldNow, { discard: true })

describe('native progress settlement and recovery', () => {
  it.effect(
    'a fork initializes inherited progress independently instead of leaking or replaying a parent call',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, gate } = yield* open
          const writer = yield* ToolProgress.make(gate, {
            conversationId: root,
            callId: 'native-call',
            attempt: 0,
            retention,
            policy,
          })
          yield* writer.output('parent')
          const receipt = yield* writer.settle('completed', (tx) =>
            Transaction.appendEntry(tx, root, { kind: 'fork.boundary', data: null }),
          )
          const child = yield* Session.commit(session, (tx) =>
            Transaction.forkConversation(tx, root, receipt.id, {
              ownership: { _tag: 'ownerless' },
            }),
          )
          const fresh = yield* ToolProgress.make(gate, {
            conversationId: child.id,
            callId: 'native-call',
            attempt: 0,
            retention,
            policy,
          })
          assert.strictEqual((yield* fresh.snapshot).output.totalBytes, 0)
          assert.strictEqual((yield* fresh.snapshot).status, 'running')
          yield* fresh.output('child')
          yield* fresh.settle('completed', () => Effect.void)
          assert.strictEqual((yield* saved(session)).output.text, 'parent')
          const found = yield* Session.snapshot(session, ToolProgress.document, {
            scope: { _tag: 'conversation', conversationId: child.id },
            key: 'native-call',
          })
          assert.strictEqual(
            Option.isSome(found) ? found.value.value.output.text : undefined,
            'child',
          )
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    it.effect(
      `${backend}: final/cancel progress and receipt share one durable batch; unsafe and safe recovery replay`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const folder = yield* fs.makeTempDirectoryScoped()
            const memory = yield* Layer.build(Storage.layerMemory)
            const storage =
              backend === 'sqlite'
                ? Storage.layerJournal().pipe(
                    Layer.provide(SqlEventJournal.layer()),
                    Layer.provide(SqliteClient.layer({ filename: `${folder}/progress.sqlite` })),
                  )
                : Storage.layerJsonl({ filePath: `${folder}/progress.jsonl` })
            const use = <A, E>(effect: Effect.Effect<A, E, Storage.Storage | Scope.Scope>) =>
              backend === 'memory'
                ? effect.pipe(Effect.provideContext(memory))
                : effect.pipe(Effect.provide(Layer.fresh(storage)))

            // Simulate loss of the invocation scope with a committed prefix and a throttled suffix.
            yield* use(
              Effect.scoped(
                Effect.gen(function* () {
                  const { session, gate } = yield* open
                  const writer = yield* ToolProgress.make(gate, {
                    conversationId: root,
                    callId: 'native-call',
                    attempt: 0,
                    retention,
                    policy,
                  })
                  yield* writer.output('prefix\n')
                  yield* writer.details({ phase: 1 }).pipe(Effect.forkChild)
                  yield* turns
                  assert.strictEqual((yield* saved(session)).output.text, 'prefix\n')
                  yield* writer.output('uncommitted suffix')
                }),
              ),
            )
            yield* use(
              Effect.scoped(
                Effect.gen(function* () {
                  const { session, gate } = yield* open
                  const unsafe = yield* ToolProgress.make(gate, {
                    conversationId: root,
                    callId: 'native-call',
                    attempt: 0,
                    retention,
                    policy,
                    recovery: 'interrupt',
                  })
                  const interrupted = yield* unsafe.snapshot
                  assert.strictEqual(interrupted.status, 'interrupted')
                  assert.strictEqual(interrupted.output.text, 'prefix\n')
                  assert.strictEqual(interrupted.output.totalBytes, 7)
                  assert.strictEqual(interrupted.diagnostics.at(-1)?.code, 'interrupted')
                  assert.strictEqual(
                    (yield* unsafe.output('unsafe repeat').pipe(Effect.flip))._tag,
                    'ToolProgressError',
                  )
                  assert.deepStrictEqual(yield* saved(session), interrupted)
                }),
              ),
            )

            let finalBatch: ReadonlyArray<Record.StorageWrite> = []
            yield* use(
              Effect.scoped(
                Effect.gen(function* () {
                  const actual = yield* Storage.Storage
                  const captured: Storage.Storage['Service'] = {
                    ...actual,
                    commit: (input) =>
                      Effect.gen(function* () {
                        const writes = [...input]
                        const seq = yield* actual.commit(writes)
                        if (writes.some((write) => write._tag === 'entry')) finalBatch = writes
                        return seq
                      }),
                  }
                  yield* Effect.scoped(
                    Effect.gen(function* () {
                      const { session, gate } = yield* open
                      const writer = yield* ToolProgress.make(gate, {
                        conversationId: root,
                        callId: 'native-call',
                        attempt: 1,
                        retention,
                        policy,
                        recovery: 'retry',
                      })
                      assert.strictEqual((yield* saved(session)).output.totalBytes, 0)
                      assert.deepStrictEqual((yield* saved(session)).diagnostics, [])
                      assert.isUndefined((yield* saved(session)).details)
                      yield* writer.details({ phase: 'retry' })
                      yield* writer.output('α\nlast 😀\n')
                      const pending = yield* writer
                        .details({ phase: 'final', exact: [1, 2] })
                        .pipe(Effect.forkChild)
                      yield* turns
                      assert.isUndefined(pending.pollUnsafe())
                      const entry = yield* writer.settle('cancelled', (tx, state) =>
                        Effect.gen(function* () {
                          assert.deepStrictEqual(yield* writer.flush(tx), state)
                          assert.isUndefined(pending.pollUnsafe())
                          return yield* Transaction.appendEntry(tx, root, {
                            kind: 'native.progress.receipt',
                            data: {
                              callId: state.callId,
                              status: state.status,
                              text: state.output.text,
                              details: state.details ?? null,
                            },
                          })
                        }),
                      )
                      yield* Fiber.join(pending)
                      const persisted = yield* saved(session)
                      assert.strictEqual(persisted.status, 'cancelled')
                      assert.strictEqual(persisted.output.text, 'last 😀\n')
                      assert.deepStrictEqual(persisted.details, { phase: 'final', exact: [1, 2] })
                      assert.deepStrictEqual(entry.data, {
                        callId: 'native-call',
                        status: 'cancelled',
                        text: 'last 😀\n',
                        details: { phase: 'final', exact: [1, 2] },
                      })
                      assert.isTrue(finalBatch.some((write) => write._tag === 'entry'))
                      assert.isTrue(finalBatch.some((write) => write._tag === 'document.change'))
                    }),
                  ).pipe(Effect.provideService(Storage.Storage, captured))
                }),
              ),
            )
            yield* use(
              Effect.scoped(
                Effect.gen(function* () {
                  const { session, gate } = yield* open
                  assert.strictEqual((yield* saved(session)).status, 'cancelled')
                  assert.strictEqual((yield* saved(session)).output.totalBytes, 13)
                  const entries = yield* Session.scanEntries(session, {
                    conversationId: root,
                  }).pipe(Stream.runCollect)
                  assert.strictEqual(entries.length, 1)
                  assert.strictEqual(entries[0]?.kind, 'native.progress.receipt')
                  assert.strictEqual(
                    (yield* ToolProgress.make(gate, {
                      conversationId: root,
                      callId: 'native-call',
                      attempt: 1,
                      retention,
                      policy,
                    }).pipe(Effect.flip))._tag,
                    'ToolProgressError',
                  )
                  // Replaying a settled call reads its receipt instead of invoking a handler or appending again.
                  const replay = yield* Session.entry(
                    session,
                    entries[0]?.id ?? Record.EntryId.make(0),
                  )
                  assert.deepStrictEqual(
                    Option.isSome(replay) ? replay.value.entry : undefined,
                    entries[0],
                  )
                }),
              ),
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

  it.effect(
    'a failed final callback rolls back the latest snapshot and receipt and fails its pending details',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, gate } = yield* open
          const writer = yield* ToolProgress.make(gate, {
            conversationId: root,
            callId: 'native-call',
            attempt: 0,
            retention,
            policy,
          })
          yield* writer.details({ committed: true })
          const before = yield* saved(session)
          yield* writer.output('latest')
          const waiter = yield* writer.details({ uncommitted: true }).pipe(Effect.forkChild)
          yield* turns
          const result = yield* writer
            .settle('completed', (tx) =>
              Transaction.appendEntry(tx, root, { kind: 'must.rollback', data: null }).pipe(
                Effect.andThen(Effect.fail('receipt rejected')),
              ),
            )
            .pipe(Effect.flip)
          assert.strictEqual(result, 'receipt rejected')
          assert.isTrue(Exit.isFailure(yield* Fiber.await(waiter)))
          assert.deepStrictEqual(yield* saved(session), before)
          assert.deepStrictEqual(
            yield* Session.scanEntries(session, { conversationId: root }).pipe(Stream.runCollect),
            [],
          )
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
  it.effect(
    'cancelled final commit preserves interruption and never acknowledges its unpersisted details',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, gate } = yield* open
          const entered = yield* Deferred.make<void>()
          const never = yield* Deferred.make<void>()
          const writer = yield* ToolProgress.make(gate, {
            conversationId: root,
            callId: 'native-call',
            attempt: 0,
            retention,
            policy,
          })
          yield* writer.details({ before: true })
          const waiter = yield* writer.details({ latest: true }).pipe(Effect.forkChild)
          yield* turns
          const final = yield* writer
            .settle('cancelled', () =>
              Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(never))),
            )
            .pipe(Effect.forkChild)
          yield* Deferred.await(entered)
          yield* Fiber.interrupt(final)
          const interrupted = yield* Fiber.await(final)
          assert.isTrue(Exit.isFailure(interrupted))
          if (Exit.isFailure(interrupted)) assert.isTrue(Exit.hasInterrupts(interrupted))
          assert.isTrue(Exit.isFailure(yield* Fiber.await(waiter)))
          assert.deepStrictEqual((yield* saved(session)).details, { before: true })
          yield* TestClock.adjust(100000)
          assert.strictEqual((yield* saved(session)).status, 'running')
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
})
