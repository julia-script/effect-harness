import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as ConversationInitializer from 'effect-harness/ConversationInitializer'
import * as Document from 'effect-harness/Document'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Hook from 'effect-harness/Hook'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import { StorageError } from 'effect-harness/StorageError'
import * as Transaction from 'effect-harness/Transaction'

const ownerless = { ownership: { _tag: 'ownerless' } } as const
const root = Record.ROOT_CONVERSATION_ID
const document = Document.define({
  kind: 'initialization.state',
  scope: 'conversation',
  version: 1,
  history: 'rewindable',
  fork: 'asOf',
  schema: Schema.Struct({ count: Schema.Natural }),
  initial: () => ({ count: 0 }),
})
const global = Document.define({
  kind: 'initialization.global',
  scope: 'session',
  version: 1,
  schema: Schema.Struct({ count: Schema.Natural }),
  initial: () => ({ count: 0 }),
})
const target = (conversationId: Record.ConversationId): Document.Target => ({
  scope: { _tag: 'conversation', conversationId },
})
const globalTarget = { scope: { _tag: 'session' } } as const
const model = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: () => Effect.die('Initialization must not invoke the model'),
    streamText: () => Stream.empty,
  }),
)
const withMemory = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.scoped(effect).pipe(Effect.provide(Storage.layerMemory))
const initialize = ConversationInitializer.make({
  execute: (tx, record) =>
    Effect.gen(function* () {
      yield* Transaction.ensureDocument(tx, document, target(record.id))
      yield* Transaction.updateDocument(tx, document, target(record.id), ({ count }) => ({
        count: count + 1,
      }))
    }),
})
const fail = ConversationInitializer.make({ execute: () => Effect.fail('initializer rejected') })
const seedParent = (session: Session.Session) =>
  Session.commit(session, (tx) =>
    Effect.gen(function* () {
      yield* Transaction.ensureRoot(tx)
      yield* Transaction.ensureDocument(tx, document, target(root))
      yield* Transaction.setDocument(tx, document, target(root), { count: 7 })
      return yield* Transaction.appendEntry(tx, root, { kind: 'initialization.cutoff' })
    }),
  )

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  for (const boundary of ['root', 'create', 'fork'] as const) {
    it.effect(
      `${backend}: failed Harness ${boundary} persists no partial initialization after reopen`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped({
              prefix: 'conversation-initialization-',
            })
            const disk =
              backend === 'sqlite'
                ? Storage.layerSql.pipe(
                    Layer.provide(SqliteClient.layer({ filename: `${directory}/state.sqlite` })),
                  )
                : Storage.layerJsonl({ filePath: `${directory}/state.jsonl` })
            const run = Effect.gen(function* () {
              const storage = yield* Storage.Storage
              const entry =
                boundary === 'fork'
                  ? yield* Effect.scoped(
                      Effect.gen(function* () {
                        const session = yield* Session.make()
                        return yield* seedParent(session)
                      }),
                    )
                  : undefined
              yield* Effect.scoped(
                Effect.gen(function* () {
                  const runtime = yield* HarnessRuntime.make({
                    initializers: [
                      initialize,
                      ConversationInitializer.make({
                        execute: (tx) =>
                          Effect.gen(function* () {
                            yield* Transaction.ensureDocument(tx, global, globalTarget)
                            yield* Transaction.updateDocument(tx, global, globalTarget, () => ({
                              count: 99,
                            }))
                            return yield* Effect.fail('stop')
                          }),
                      }),
                    ],
                  })
                  let operation = runtime.backend.root
                  if (boundary === 'create') operation = runtime.backend.create()
                  if (boundary === 'fork')
                    operation = runtime.backend.fork({
                      conversationId: root,
                      options: { at: Option.getOrThrow(Option.fromUndefinedOr(entry)).id },
                    })
                  assert.strictEqual((yield* operation.pipe(Effect.flip))._tag, 'HarnessError')
                }).pipe(Effect.provide(model)),
              )
              assert.strictEqual(
                (yield* storage.scanConversations().pipe(Stream.runCollect)).length,
                boundary === 'fork' ? 1 : 0,
              )
              assert.strictEqual(
                (yield* storage
                  .scanDocuments({ scope: globalTarget.scope })
                  .pipe(Stream.runCollect)).length,
                0,
              )
            })
            const read = Effect.scoped(
              Effect.gen(function* () {
                const session = yield* Session.make({ initializers: [fail] })
                assert.strictEqual(
                  (yield* Session.scanConversations(session).pipe(Stream.runCollect)).length,
                  boundary === 'fork' ? 1 : 0,
                )
                assert.isTrue(Option.isNone(yield* Session.snapshot(session, global, globalTarget)))
                if (boundary === 'fork') {
                  assert.strictEqual(
                    Option.getOrThrow(yield* Session.snapshot(session, document, target(root)))
                      .value.count,
                    7,
                  )
                  assert.strictEqual(
                    (yield* Session.scanDocuments(session, { scope: target(root).scope }).pipe(
                      Stream.runCollect,
                    )).length,
                    1,
                  )
                }
              }),
            )
            if (backend === 'memory')
              yield* Effect.gen(function* () {
                yield* run
                yield* read
              }).pipe(Effect.provide(Storage.layerMemory))
            else {
              yield* run.pipe(Effect.provide(disk))
              yield* read.pipe(Effect.provide(disk))
            }
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )
  }
}

describe('transactional conversation initialization', () => {
  it.effect(
    'runs ordered callbacks after agent creation and fork inheritance; root reacquisition skips them',
    () =>
      withMemory(
        Effect.gen(function* () {
          const calls: Array<string> = []
          const runtime = yield* HarnessRuntime.make({
            initializers: [
              initialize,
              ConversationInitializer.make({
                execute: (tx, record) =>
                  Effect.gen(function* () {
                    const records = yield* Transaction.scanDocuments(tx, {
                      scope: target(record.id).scope,
                    }).pipe(Stream.runCollect)
                    assert.isTrue(records.some((record) => record.kind === 'harness.agent'))
                    calls.push(record.parent === undefined ? 'new' : 'fork')
                    const value = Option.getOrThrow(
                      yield* Transaction.snapshot(tx, document, target(record.id)),
                    ).value.count
                    assert.strictEqual(value, record.parent === undefined ? 1 : 8)
                  }),
              }),
            ],
          })
          yield* runtime.backend.root
          yield* runtime.backend.root
          const created = yield* runtime.backend.create({ agent: { instructions: 'new' } })
          assert.strictEqual((yield* runtime.backend.agent(created)).instructions, 'new')
          yield* runtime.backend.configure({
            conversationId: root,
            change: { instructions: 'inherited' },
          })
          const at = yield* Session.commit(runtime.session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.setDocument(tx, document, target(root), { count: 7 })
              return yield* Transaction.appendEntry(tx, root, { kind: 'cutoff' })
            }),
          )
          const fork = yield* runtime.backend.fork({ conversationId: root, options: { at: at.id } })
          assert.strictEqual((yield* runtime.backend.agent(fork)).instructions, 'inherited')
          assert.strictEqual(
            Option.getOrThrow(yield* Session.snapshot(runtime.session, document, target(fork)))
              .value.count,
            8,
          )
          assert.strictEqual(
            Option.getOrThrow(yield* Session.snapshot(runtime.session, document, target(root)))
              .value.count,
            7,
          )
          assert.deepEqual(calls, ['new', 'new', 'fork'])
          yield* runtime.backend.conversation(fork)
          const raw = yield* Session.commit(runtime.session, (tx) =>
            Transaction.createConversation(tx, ownerless),
          )
          assert.strictEqual(
            Option.getOrThrow(yield* Session.snapshot(runtime.session, document, target(raw.id)))
              .value.count,
            1,
          )
          assert.deepEqual(calls, ['new', 'new', 'fork', 'new'])
        }).pipe(Effect.provide(model)),
      ),
  )

  it.effect(
    'caught root/create/fork failures preserve earlier drafts and release fork source restrictions',
    () =>
      withMemory(
        Effect.gen(function* () {
          const at = yield* Effect.scoped(
            Effect.gen(function* () {
              const session = yield* Session.make()
              return yield* seedParent(session)
            }),
          )
          const session = yield* Session.make({
            initializers: [
              ConversationInitializer.make({
                execute: (tx, record) =>
                  Effect.gen(function* () {
                    yield* Transaction.ensureDocument(tx, global, globalTarget)
                    yield* Transaction.updateDocument(tx, global, globalTarget, () => ({
                      count: 99,
                    }))
                    yield* Transaction.ensureDocument(tx, document, target(record.id))
                    yield* Transaction.appendEntry(tx, record.id, { kind: 'should.rollback' })
                    yield* Transaction.createTask(tx, {
                      conversationId: record.id,
                      kind: 'init',
                      version: 1,
                      background: false,
                      abortRequested: false,
                      input: null,
                      state: { status: 'running', checkpoint: { phase: 'work' } },
                    })
                    return yield* Effect.fail('failed')
                  }),
              }),
            ],
          })
          yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.ensureDocument(tx, global, globalTarget)
              yield* Transaction.setDocument(tx, global, globalTarget, { count: 4 })
              for (const operation of [
                Transaction.createConversation(tx, ownerless),
                Transaction.forkConversation(tx, root, at.id, ownerless),
              ]) {
                const failure = yield* operation.pipe(Effect.flip)
                assert.strictEqual(failure._tag, 'SessionError')
                if (failure._tag === 'SessionError')
                  assert.strictEqual(failure.operation, 'conversation.initialize')
                assert.strictEqual(
                  Option.getOrThrow(yield* Transaction.snapshot(tx, global, globalTarget)).value
                    .count,
                  4,
                )
              }
              yield* Transaction.setDocument(tx, document, target(root), { count: 9 })
              assert.deepEqual(
                (yield* Transaction.scanEntries(tx, { conversationId: root }).pipe(
                  Stream.runCollect,
                )).map((entry) => entry.kind),
                ['initialization.cutoff'],
              )
              assert.strictEqual(
                (yield* Transaction.scanTasks(tx).pipe(Stream.runCollect)).length,
                0,
              )
            }),
          )
          assert.strictEqual(
            (yield* Session.scanConversations(session).pipe(Stream.runCollect)).length,
            1,
          )
          assert.strictEqual(
            Option.getOrThrow(yield* Session.snapshot(session, global, globalTarget)).value.count,
            4,
          )
          assert.strictEqual(
            Option.getOrThrow(yield* Session.snapshot(session, document, target(root))).value.count,
            9,
          )
        }),
      ),
  )

  it.effect('caught ensureRoot failure leaves no root and retry initializes it once', () =>
    withMemory(
      Effect.gen(function* () {
        let reject = true
        let count = 0
        const session = yield* Session.make({
          initializers: [
            initialize,
            ConversationInitializer.make({
              execute: () =>
                Effect.suspend(() => {
                  count++
                  return reject ? Effect.fail('stop') : Effect.void
                }),
            }),
          ],
        })
        yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.ensureRoot(tx).pipe(Effect.flip)
            assert.isTrue(Option.isNone(yield* Transaction.conversation(tx, root)))
            reject = false
            yield* Transaction.ensureRoot(tx)
            yield* Transaction.ensureRoot(tx)
          }),
        )
        assert.strictEqual(count, 2)
        assert.strictEqual(
          Option.getOrThrow(yield* Session.snapshot(session, document, target(root))).value.count,
          1,
        )
      }),
    ),
  )

  it.effect('captures initializer services and owns a fresh callback scope', () =>
    withMemory(
      Effect.gen(function* () {
        class Seed extends Context.Service<Seed, { readonly count: number }>()(
          'initialization/Seed',
        ) {}
        let ended = 0
        let saved: Transaction.Transaction | undefined
        const session = yield* Session.make({
          initializers: [
            ConversationInitializer.make({
              execute: (tx, record) =>
                Effect.gen(function* () {
                  saved = tx
                  const seed = yield* Seed
                  yield* Effect.addFinalizer(() =>
                    Effect.sync(() => {
                      ended++
                    }),
                  )
                  yield* Transaction.ensureDocument(tx, document, target(record.id))
                  yield* Transaction.setDocument(tx, document, target(record.id), seed)
                }),
            }),
          ],
        }).pipe(Effect.provideService(Seed, { count: 11 }))
        yield* Session.commit(session, (tx) => Transaction.ensureRoot(tx))
        assert.strictEqual(ended, 1)
        assert.strictEqual(
          Option.getOrThrow(yield* Session.snapshot(session, document, target(root))).value.count,
          11,
        )
        const failure = yield* Transaction.ensureRoot(
          Option.getOrThrow(Option.fromUndefinedOr(saved)),
        ).pipe(Effect.flip)
        assert.strictEqual(failure._tag, 'SessionError')
        if (failure._tag === 'SessionError') assert.strictEqual(failure.reason, 'revoked')
      }),
    ),
  )

  it.effect(
    'nested child creation can use Transaction and rolls back with a failing parent initializer',
    () =>
      withMemory(
        Effect.gen(function* () {
          const session = yield* Session.make({
            initializers: [
              ConversationInitializer.make({
                execute: (tx, record) =>
                  Effect.gen(function* () {
                    yield* Transaction.ensureDocument(tx, document, target(record.id))
                    if (record.id === root) {
                      yield* Transaction.createConversation(tx, ownerless)
                      return yield* Effect.fail('parent failed')
                    }
                  }),
              }),
            ],
          })
          yield* Session.commit(session, (tx) => Transaction.ensureRoot(tx).pipe(Effect.flip))
          assert.strictEqual(
            (yield* Session.scanConversations(session).pipe(Stream.runCollect)).length,
            0,
          )
        }),
      ),
  )

  it.effect('interruption discards drafts and ends initializer resources without publishing', () =>
    withMemory(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>()
        let ended = false
        const session = yield* Session.make({
          initializers: [
            initialize,
            ConversationInitializer.make({
              execute: () =>
                Effect.gen(function* () {
                  yield* Effect.addFinalizer(() =>
                    Effect.sync(() => {
                      ended = true
                    }),
                  )
                  yield* Deferred.succeed(started, undefined)
                  return yield* Effect.never
                }),
            }),
          ],
        })
        const fiber = yield* Session.commit(session, (tx) => Transaction.ensureRoot(tx)).pipe(
          Effect.forkChild,
        )
        yield* Deferred.await(started)
        assert.isTrue(Option.isNone(yield* (yield* Storage.Storage).conversation(root)))
        yield* Fiber.interrupt(fiber)
        assert.isTrue(ended)
        assert.strictEqual(
          (yield* Session.scanConversations(session).pipe(Stream.runCollect)).length,
          0,
        )
      }),
    ),
  )

  it.effect('publishes one complete batch and keeps notification failures post-commit', () =>
    withMemory(
      Effect.gen(function* () {
        const runtime = yield* HarnessRuntime.make({
          initializers: [initialize],
          hooks: [
            Hook.make({
              event: 'conversationCreated',
              execute: () => Effect.fail('notification failed'),
            }),
          ],
        })
        const pull = yield* Session.commits(runtime.session).pipe(Stream.toPull)
        const frame = yield* pull.pipe(Effect.forkChild)
        yield* Effect.yieldNow
        yield* runtime.backend.root.pipe(Effect.flip)
        const changes = (yield* Fiber.join(frame))[0].changes
        assert.strictEqual(changes.filter((write) => write._tag === 'conversation').length, 1)
        assert.deepEqual(
          changes
            .filter((write) => write._tag === 'document.create')
            .map((write) => write.record.kind)
            .sort(),
          ['harness.agent', 'initialization.state'],
        )
        assert.isTrue(Option.isSome(yield* Session.conversation(runtime.session, root)))
        yield* runtime.backend.root
        assert.strictEqual(
          Option.getOrThrow(yield* Session.snapshot(runtime.session, document, target(root))).value
            .count,
          1,
        )
      }).pipe(Effect.provide(model)),
    ),
  )

  it.effect('a caught initializer defect discards drafts and revokes its Transaction', () =>
    withMemory(
      Effect.gen(function* () {
        let saved: Transaction.Transaction | undefined
        const session = yield* Session.make({
          initializers: [
            initialize,
            ConversationInitializer.make({
              execute: (tx) => {
                saved = tx
                return Effect.die('initializer defect')
              },
            }),
          ],
        })
        yield* Session.commit(session, (tx) =>
          Transaction.ensureRoot(tx).pipe(Effect.catchCause(() => Effect.void)),
        )
        assert.isTrue(Option.isNone(yield* Session.conversation(session, root)))
        const failure = yield* Transaction.ensureRoot(
          Option.getOrThrow(Option.fromUndefinedOr(saved)),
        ).pipe(Effect.flip)
        assert.strictEqual(failure._tag, 'SessionError')
        if (failure._tag === 'SessionError') assert.strictEqual(failure.reason, 'revoked')
      }),
    ),
  )

  it.effect('caught initialization rolls back retirement of an earlier task document draft', () =>
    withMemory(
      Effect.gen(function* () {
        const taskDocument = Document.define({
          kind: 'initialization.task',
          scope: 'task',
          version: 1,
          schema: Schema.Struct({ count: Schema.Natural }),
          initial: () => ({ count: 0 }),
        })
        const prior = yield* Effect.scoped(
          Effect.gen(function* () {
            const session = yield* Session.make()
            return yield* Session.commit(session, (tx) =>
              Effect.gen(function* () {
                yield* Transaction.ensureRoot(tx)
                const task = yield* Transaction.createTask(tx, {
                  conversationId: root,
                  kind: 'fixture',
                  version: 1,
                  background: false,
                  abortRequested: false,
                  input: null,
                  state: { status: 'running', checkpoint: { phase: 'work' } },
                })
                yield* Transaction.ensureDocument(tx, taskDocument, {
                  scope: { _tag: 'task', taskId: task.id },
                })
                return task
              }),
            )
          }),
        )
        const taskTarget = { scope: { _tag: 'task', taskId: prior.id } } as const
        const session = yield* Session.make({
          initializers: [
            ConversationInitializer.make({
              execute: (tx) =>
                Effect.gen(function* () {
                  yield* Transaction.putTask(tx, {
                    ...prior,
                    state: { status: 'terminal', outcome: { status: 'completed', result: null } },
                  })
                  assert.isTrue(
                    Option.isNone(yield* Transaction.snapshot(tx, taskDocument, taskTarget)),
                  )
                  return yield* Effect.fail('rollback settlement')
                }),
            }),
          ],
        })
        yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.updateDocument(tx, taskDocument, taskTarget, () => ({ count: 5 }))
            yield* Transaction.createConversation(tx, ownerless).pipe(Effect.flip)
            assert.strictEqual(
              Option.getOrThrow(yield* Transaction.task(tx, prior.id)).state.status,
              'running',
            )
            assert.strictEqual(
              Option.getOrThrow(yield* Transaction.snapshot(tx, taskDocument, taskTarget)).value
                .count,
              5,
            )
          }),
        )
        assert.strictEqual(
          Option.getOrThrow(yield* Session.snapshot(session, taskDocument, taskTarget)).value.count,
          5,
        )
        assert.strictEqual(
          Option.getOrThrow(yield* Session.task(session, prior.id)).state.status,
          'running',
        )
      }),
    ),
  )

  it.effect('reopening a root skips callbacks and retains initialized state', () =>
    withMemory(
      Effect.gen(function* () {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const runtime = yield* HarnessRuntime.make({ initializers: [initialize] })
            yield* runtime.backend.root
          }).pipe(Effect.provide(model)),
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            const runtime = yield* HarnessRuntime.make({ initializers: [fail] })
            yield* runtime.backend.root
            assert.strictEqual(
              Option.getOrThrow(yield* Session.snapshot(runtime.session, document, target(root)))
                .value.count,
              1,
            )
          }).pipe(Effect.provide(model)),
        )
      }),
    ),
  )

  it.effect(
    'initializer Session operations reject promptly while supplied Transaction operations succeed',
    () =>
      withMemory(
        Effect.gen(function* () {
          let captured: Session.Session | undefined
          const session = yield* Session.make({
            initializers: [
              ConversationInitializer.make({
                execute: (tx, record) =>
                  Effect.gen(function* () {
                    const failure = yield* Session.conversation(
                      Option.getOrThrow(Option.fromUndefinedOr(captured)),
                      record.id,
                    ).pipe(Effect.flip)
                    assert.strictEqual(failure._tag, 'SessionError')
                    if (failure._tag === 'SessionError')
                      assert.strictEqual(failure.reason, 'conflict')
                    assert.isTrue(Option.isSome(yield* Transaction.conversation(tx, record.id)))
                  }),
              }),
            ],
          })
          captured = session
          yield* Session.commit(session, (tx) => Transaction.ensureRoot(tx))
        }),
      ),
  )

  it.effect(
    'concurrent callbacks on the supplied Transaction serialize without losing writes',
    () =>
      withMemory(
        Effect.gen(function* () {
          const session = yield* Session.make({
            initializers: [
              initialize,
              ConversationInitializer.make({
                execute: (tx, record) =>
                  Effect.all(
                    Array.from({ length: 8 }, () =>
                      Transaction.updateDocument(tx, document, target(record.id), ({ count }) => ({
                        count: count + 1,
                      })),
                    ),
                    { concurrency: 'unbounded', discard: true },
                  ),
              }),
            ],
          })
          yield* Session.commit(session, (tx) => Transaction.ensureRoot(tx))
          assert.strictEqual(
            Option.getOrThrow(yield* Session.snapshot(session, document, target(root))).value.count,
            9,
          )
        }),
      ),
  )

  it.effect(
    'storage rejection commits no initialization and does not poison a certain failure',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const memory = yield* Storage.Storage
          let reject = true
          const storage = Storage.Storage.of({
            ...memory,
            commit: (writes) =>
              reject
                ? Effect.fail(
                    new StorageError({ reason: 'io', operation: 'commit', message: 'reject' }),
                  )
                : memory.commit(writes),
          })
          yield* Effect.gen(function* () {
            const session = yield* Session.make({ initializers: [initialize] })
            yield* Session.commit(session, (tx) => Transaction.ensureRoot(tx)).pipe(Effect.flip)
            assert.isTrue(Option.isNone(yield* memory.conversation(root)))
            reject = false
            yield* Session.commit(session, (tx) => Transaction.ensureRoot(tx))
            assert.strictEqual(
              Option.getOrThrow(yield* Session.snapshot(session, document, target(root))).value
                .count,
              1,
            )
          }).pipe(Effect.provideService(Storage.Storage, storage))
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
})
