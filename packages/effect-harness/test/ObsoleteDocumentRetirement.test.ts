import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Pull from 'effect/Pull'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Struct from 'effect/Struct'
import * as ConversationInitializer from 'effect-harness/ConversationInitializer'
import * as Document from 'effect-harness/Document'
import * as DocumentRetirement from 'effect-harness/DocumentRetirement'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import { SessionError } from 'effect-harness/SessionError'
import * as Storage from 'effect-harness/Storage'
import { StorageError } from 'effect-harness/StorageError'
import * as Transaction from 'effect-harness/Transaction'

const root = Record.ROOT_CONVERSATION_ID
const ownerless = { ownership: { _tag: 'ownerless' } } satisfies Transaction.ConversationOptions
const scope = { _tag: 'conversation', conversationId: root } satisfies Record.Scope
const target = { scope } satisfies Document.Target
const schema = Schema.Struct({ count: Schema.Natural })
const old = Document.define({
  kind: 'obsolete.counter',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  schema,
  initial: () => ({ count: 2 }),
})
const policy = {
  ...old.definition,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  version: 3,
} satisfies Document.Metadata
const failure = new StorageError({
  reason: 'io',
  operation: 'retirement.test',
  message: 'injected',
})
const some = Option.getOrThrow
const withMemory = <A, E, R>(program: Effect.Effect<A, E, R>) =>
  Effect.scoped(program).pipe(Effect.provide(Storage.layerMemory))
const initialize = (session: Session.Session) =>
  Session.commit(session, (tx) =>
    Effect.gen(function* () {
      yield* Transaction.ensureRoot(tx)
      const snapshot = yield* Transaction.ensureDocument(tx, old, target)
      const entry = yield* Transaction.appendEntry(tx, root, { kind: 'obsolete.before' })
      return { snapshot, entry }
    }),
  )
const conflict = (error: Session.Failure) => {
  assert.strictEqual(error._tag, 'SessionError')
  if (error._tag === 'SessionError') {
    assert.strictEqual(error.reason, 'conflict')
    assert.strictEqual(error.operation, 'document.retireMetadata')
  }
}
class OldDecoder extends Context.Service<OldDecoder, string>()('test/obsolete/Decoder') {}
const oldCodec = schema.pipe(
  Schema.decodeTo(Schema.Struct({ count: Schema.Natural, label: Schema.String }), {
    decode: SchemaGetter.transformEffect(({ count }) =>
      OldDecoder.pipe(Effect.map((label) => ({ count, label }))),
    ),
    encode: SchemaGetter.transform(({ count }) => ({ count })),
  }),
)

// No OldDecoder service is supplied anywhere in this suite.
describe('obsolete document metadata retirement', () => {
  for (const mode of ['missing', 'corrupt', 'io'] as const) {
    it.effect(`retires older ${mode} content without reading it or invoking callbacks`, () =>
      withMemory(
        Effect.gen(function* () {
          const storage = yield* Storage.Storage
          const session = yield* Session.make()
          const { snapshot } = yield* initialize(session)
          const raw = some(yield* storage.document(snapshot.record.id))
          assert.strictEqual(raw.version, 1)
          const selected = DocumentRetirement.target(raw)
          let reads = 0
          let finds = 0
          const wrapped = {
            ...storage,
            findDocument: (address: Record.DocumentAddress, at?: Record.DocumentPoint) =>
              Effect.sync(() => {
                finds++
              }).pipe(Effect.andThen(storage.findDocument(address, at))),
            document: (id: Record.DocumentId, at?: Record.DocumentPoint) => {
              if (id !== selected.id) return storage.document(id, at)
              reads++
              if (mode === 'missing') return Effect.succeedNone
              if (mode === 'io') return Effect.fail(failure)
              return Effect.succeedSome({ ...raw, value: { unavailable: true } })
            },
          } satisfies Storage.Storage['Service']
          // Exact retirement still refuses an older version, without acquiring its body.
          const cleanup = yield* Session.make().pipe(
            Effect.provideService(Storage.Storage, wrapped),
          )
          const obsolete = Document.define({
            ...policy,
            schema: oldCodec,
            initial: () => {
              assert.fail('initializer must not execute')
              return { count: 0, label: '' }
            },
            migrations: {
              1: () => {
                assert.fail('migration must not execute')
                return Effect.succeed({ count: 0 })
              },
            },
          })
          const denied = yield* Session.commit(cleanup, (tx) =>
            Transaction.retireDocument(tx, obsolete, target),
          ).pipe(Effect.flip)
          assert.strictEqual(denied._tag, 'SessionError')
          if (denied._tag === 'SessionError') assert.strictEqual(denied.reason, 'conflict')
          assert.strictEqual(reads, 0)
          reads = 0
          finds = 0
          yield* Session.commit(cleanup, (tx) =>
            Transaction.retireDocumentMetadata(tx, obsolete.definition, selected),
          )
          assert.strictEqual(reads, 0)
          assert.strictEqual(finds, 1)
          assert.isTrue(Option.isNone(yield* storage.findDocument(selected.address)))
          assert.isTrue(Option.isNone(yield* Session.snapshot(cleanup, old, target)))
        }),
      ),
    )
  }

  it.effect(
    'rejects future versions, policies, owner and incarnation mismatches without changing prior drafts',
    () =>
      withMemory(
        Effect.gen(function* () {
          const session = yield* Session.make()
          const { snapshot } = yield* initialize(session)
          const selected = DocumentRetirement.target(snapshot)
          yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              const task = yield* Transaction.createTask(tx, {
                conversationId: root,
                kind: 'obsolete.owner',
                version: 1,
                input: null,
                background: false,
                abortRequested: false,
                state: { status: 'running', checkpoint: { phase: 'work' } },
              })
              yield* Transaction.setDocument(tx, old, target, { count: 8 })
              const entry = yield* Transaction.appendEntry(tx, root, { kind: 'obsolete.prior' })
              for (const invalid of [
                { ...selected, id: Record.DocumentId.make(999) },
                { ...selected, address: { ...selected.address, kind: 'wrong.kind' } },
                {
                  ...selected,
                  address: { ...selected.address, scope: { _tag: 'session' as const } },
                },
              ])
                conflict(
                  yield* Transaction.retireDocumentMetadata(tx, policy, invalid).pipe(Effect.flip),
                )
              conflict(
                yield* Transaction.retireDocumentMetadata(
                  tx,
                  { ...policy, fork: 'current' },
                  selected,
                ).pipe(Effect.flip),
              )
              const invalidVersion = yield* Transaction.retireDocumentMetadata(
                tx,
                { ...policy, version: 0 },
                selected,
              ).pipe(Effect.flip)
              assert.strictEqual(invalidVersion._tag, 'SchemaError')
              assert.strictEqual(some(yield* Transaction.snapshot(tx, old, target)).value.count, 8)
              assert.strictEqual(some(yield* Transaction.task(tx, task.id)).state.status, 'running')
              assert.strictEqual(
                some(yield* Transaction.entry(tx, entry.id)).kind,
                'obsolete.prior',
              )
              conflict(
                yield* Transaction.retireDocumentMetadata(tx, policy, {
                  ...selected,
                  address: {
                    ...selected.address,
                    scope: {
                      _tag: 'conversation',
                      conversationId: Record.ConversationId.make(999),
                    },
                  },
                }).pipe(Effect.flip),
              )
              const other = yield* Transaction.createConversation(tx, ownerless)
              const otherTarget = {
                scope: { _tag: 'conversation' as const, conversationId: other.id },
              }
              yield* Transaction.ensureDocument(tx, old, otherTarget)
              conflict(
                yield* Transaction.retireDocumentMetadata(tx, policy, {
                  ...selected,
                  address: { ...selected.address, ...otherTarget },
                }).pipe(Effect.flip),
              )
            }),
          )
          assert.strictEqual(some(yield* Session.snapshot(session, old, target)).value.count, 8)
        }),
      ),
  )

  for (const loaded of [false, true]) {
    it.effect(
      `retires/recreates with ${loaded ? 'loaded' : 'unloaded'} metadata and ends only the old scoped watch`,
      () =>
        withMemory(
          Effect.gen(function* () {
            const storage = yield* Storage.Storage
            const session = yield* Session.make()
            const { snapshot } = yield* initialize(session)
            const selected = DocumentRetirement.target(snapshot)
            const watch = yield* Session.watch(session, old, target).pipe(Stream.toPull)
            assert.strictEqual((yield* watch)[0].record.id, selected.id)
            const commits = yield* Session.commits(session).pipe(Stream.toPull)
            const next = yield* commits.pipe(Effect.forkChild)
            yield* Effect.yieldNow
            const fresh = yield* Session.commit(session, (tx) =>
              Effect.gen(function* () {
                if (loaded) yield* Transaction.setDocument(tx, old, target, { count: 9 })
                yield* Transaction.retireDocumentMetadata(tx, policy, selected)
                yield* Transaction.retireDocumentMetadata(tx, policy, selected)
                assert.isTrue(Option.isNone(yield* Transaction.snapshot(tx, old, target)))
                assert.strictEqual(
                  (yield* Transaction.scanDocuments(tx, { scope, kind: old.definition.kind }).pipe(
                    Stream.runCollect,
                  )).length,
                  0,
                )
                const fresh = yield* Transaction.ensureDocument(tx, old, target)
                conflict(
                  yield* Transaction.retireDocumentMetadata(tx, policy, selected).pipe(Effect.flip),
                )
                assert.strictEqual(
                  some(yield* Transaction.snapshot(tx, old, target)).record.id,
                  fresh.record.id,
                )
                return fresh
              }),
            )
            assert.notStrictEqual(fresh.record.id, selected.id)
            const batch = (yield* Fiber.join(next))[0]
            assert.strictEqual(batch.changes.filter((w) => w._tag === 'document.retire').length, 1)
            assert.strictEqual(batch.changes.filter((w) => w._tag === 'document.create').length, 1)
            const ended = yield* watch.pipe(Effect.exit)
            assert.isTrue(Exit.isFailure(ended))
            if (Exit.isFailure(ended)) assert.isTrue(Pull.isDoneCause(ended.cause))
            assert.isTrue(Option.isNone(yield* storage.document(selected.id)))
            assert.strictEqual(
              some(yield* Session.snapshot(session, old, target)).record.id,
              fresh.record.id,
            )
            const freshWatch = yield* Session.watch(session, old, target).pipe(Stream.toPull)
            assert.strictEqual((yield* freshWatch)[0].record.id, fresh.record.id)
            yield* Session.commit(session, (tx) =>
              Transaction.setDocument(tx, old, target, { count: 11 }),
            )
            assert.strictEqual((yield* freshWatch)[0].value.count, 11)
          }),
        ),
    )
  }

  it.effect(
    'rejects future versions of loaded migrated drafts and preserves exact retirement',
    () =>
      withMemory(
        Effect.gen(function* () {
          const session = yield* Session.make()
          const { snapshot } = yield* initialize(session)
          const upgraded = Document.define({
            ...policy,
            schema,
            initial: () => ({ count: 0 }),
            migrations: { 1: Effect.succeed },
          })
          yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              const exact = yield* Transaction.retireDocument(tx, upgraded, target).pipe(
                Effect.flip,
              )
              assert.strictEqual(exact._tag, 'SessionError')
              const migrated = yield* Transaction.ensureDocument(tx, upgraded, target)
              conflict(
                yield* Transaction.retireDocumentMetadata(
                  tx,
                  old.definition,
                  DocumentRetirement.target(snapshot),
                ).pipe(Effect.flip),
              )
              assert.strictEqual(some(yield* Transaction.snapshot(tx, upgraded, target)).version, 3)
              yield* Transaction.retireDocumentMetadata(
                tx,
                policy,
                DocumentRetirement.target(migrated),
              )
            }),
          )
          assert.isTrue(Option.isNone(yield* Session.snapshot(session, old, target)))
        }),
      ),
  )

  it.effect('caught metadata lookup failure preserves earlier entry/task/document overlays', () =>
    withMemory(
      Effect.gen(function* () {
        const storage = yield* Storage.Storage
        let deny = false
        const wrapped = {
          ...storage,
          findDocument: (address: Record.DocumentAddress, at?: Record.DocumentPoint) =>
            deny && address.kind === old.definition.kind
              ? Effect.fail(failure)
              : storage.findDocument(address, at),
        } satisfies Storage.Storage['Service']
        const session = yield* Session.make().pipe(Effect.provideService(Storage.Storage, wrapped))
        const { snapshot } = yield* initialize(session)
        const sibling = Document.define({ ...old.definition, kind: 'obsolete.previous' })
        yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            const entry = yield* Transaction.appendEntry(tx, root, { kind: 'obsolete.preserved' })
            const task = yield* Transaction.createTask(tx, {
              conversationId: root,
              kind: 'obsolete.task',
              version: 1,
              input: null,
              background: false,
              abortRequested: false,
              state: { status: 'running', checkpoint: { phase: 'work' } },
            })
            yield* Transaction.ensureDocument(tx, sibling, target)
            yield* Transaction.setDocument(tx, sibling, target, { count: 7 })
            deny = true
            assert.strictEqual(
              yield* Transaction.retireDocumentMetadata(
                tx,
                policy,
                DocumentRetirement.target(snapshot),
              ).pipe(Effect.flip),
              failure,
            )
            deny = false
            assert.strictEqual(
              some(yield* Transaction.entry(tx, entry.id)).kind,
              'obsolete.preserved',
            )
            assert.strictEqual(some(yield* Transaction.task(tx, task.id)).state.status, 'running')
            assert.strictEqual(
              some(yield* Transaction.snapshot(tx, sibling, target)).value.count,
              7,
            )
            assert.strictEqual(
              some(yield* Transaction.snapshot(tx, old, target)).record.id,
              snapshot.record.id,
            )
          }),
        )
        assert.strictEqual(some(yield* Session.snapshot(session, sibling, target)).value.count, 7)
        assert.strictEqual(
          some(yield* storage.findDocument(DocumentRetirement.target(snapshot).address)).id,
          snapshot.record.id,
        )
      }),
    ),
  )

  for (const mode of ['callback', 'commit', 'interruption'] as const) {
    it.effect(
      `rolls back ${mode} failure without cache eviction, notification or premature watch termination`,
      () =>
        withMemory(
          Effect.gen(function* () {
            const storage = yield* Storage.Storage
            let deny = false
            const wrapped = {
              ...storage,
              commit: (writes: Iterable<Record.StorageWrite>) =>
                Effect.suspend(() => (deny ? Effect.fail(failure) : storage.commit(writes))),
            } satisfies Storage.Storage['Service']
            const session = yield* Session.make().pipe(
              Effect.provideService(Storage.Storage, wrapped),
            )
            const { snapshot } = yield* initialize(session)
            const pull = yield* Session.watch(session, old, target).pipe(Stream.toPull)
            yield* pull
            const commits = yield* Session.commits(session).pipe(Stream.toPull)
            const publication = yield* commits.pipe(Effect.forkChild)
            yield* Effect.yieldNow
            const reached = yield* Deferred.make<void>()
            const operation = Session.commit(session, (tx) =>
              Effect.gen(function* () {
                yield* Transaction.retireDocumentMetadata(
                  tx,
                  policy,
                  DocumentRetirement.target(snapshot),
                )
                yield* Transaction.appendEntry(tx, root, { kind: 'obsolete.rollback' })
                if (mode === 'callback') return yield* Effect.fail('rollback')
                if (mode === 'interruption') {
                  yield* Deferred.succeed(reached, undefined)
                  return yield* Effect.never
                }
              }),
            )
            deny = mode === 'commit'
            if (mode === 'interruption') {
              const fiber = yield* operation.pipe(Effect.forkChild)
              yield* Deferred.await(reached)
              yield* Fiber.interrupt(fiber)
            } else yield* operation.pipe(Effect.flip)
            deny = false
            assert.strictEqual(
              some(yield* Session.snapshot(session, old, target)).record.id,
              snapshot.record.id,
            )
            assert.isUndefined(yield* Effect.sync(() => publication.pollUnsafe()))
            yield* Session.commit(session, (tx) =>
              Transaction.setDocument(tx, old, target, { count: 12 }),
            )
            const batch = (yield* Fiber.join(publication))[0]
            assert.isFalse(
              batch.changes.some((w) => w._tag === 'document.retire' || w._tag === 'entry'),
            )
            assert.strictEqual((yield* pull)[0].value.count, 12)
            yield* Session.commit(session, (tx) =>
              Transaction.retireDocumentMetadata(tx, policy, DocumentRetirement.target(snapshot)),
            )
            const ended = yield* pull.pipe(Effect.exit)
            assert.isTrue(Exit.isFailure(ended))
            if (Exit.isFailure(ended)) assert.isTrue(Pull.isDoneCause(ended.cause))
          }),
        ),
    )
  }

  it.effect(
    'savepoint retirement failure preserves parent overlays and successful initialization adopts retirement',
    () =>
      withMemory(
        Effect.gen(function* () {
          const storage = yield* Storage.Storage
          const { snapshot } = yield* Effect.scoped(
            Effect.gen(function* () {
              const seed = yield* Session.make()
              return yield* initialize(seed)
            }),
          )
          let reject = true
          const initializer = ConversationInitializer.make({
            execute: (tx, conversation) =>
              Effect.gen(function* () {
                if (conversation.id === root) return
                yield* Transaction.retireDocumentMetadata(
                  tx,
                  policy,
                  DocumentRetirement.target(snapshot),
                )
                if (reject)
                  return yield* new SessionError({
                    reason: 'invalid',
                    operation: 'test.initializer',
                    message: 'injected',
                  })
              }),
          })
          const session = yield* Session.make({ initializers: [initializer] })
          yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              const before = yield* Transaction.appendEntry(tx, root, {
                kind: 'obsolete.savepoint',
              })
              yield* Transaction.setDocument(tx, old, target, { count: 5 })
              yield* Transaction.createConversation(tx, ownerless).pipe(Effect.flip)
              assert.strictEqual(some(yield* Transaction.snapshot(tx, old, target)).value.count, 5)
              assert.strictEqual(
                some(yield* Transaction.entry(tx, before.id)).kind,
                'obsolete.savepoint',
              )
            }),
          )
          assert.strictEqual(some(yield* storage.document(snapshot.record.id)).value.count, 5)
          reject = false
          yield* Session.commit(session, (tx) => Transaction.createConversation(tx, ownerless))
          assert.isTrue(Option.isNone(yield* Session.snapshot(session, old, target)))
          assert.isTrue(Option.isNone(yield* storage.document(snapshot.record.id)))
        }),
      ),
  )

  it.effect(
    'omits newly retired incarnations and permits metadata cleanup of legacy terminal owners',
    () =>
      withMemory(
        Effect.gen(function* () {
          const storage = yield* Storage.Storage
          const session = yield* Session.make()
          yield* Session.commit(session, (tx) => Transaction.ensureRoot(tx))
          const taskId = yield* storage.mintId<Record.TaskId>()
          const id = yield* storage.mintId<Record.DocumentId>()
          const task: Record.Task = {
            id: taskId,
            conversationId: root,
            kind: 'obsolete.terminal',
            version: 1,
            input: null,
            background: false,
            abortRequested: false,
            state: { status: 'terminal', outcome: { status: 'completed', result: null } },
          }
          const taskScope = { _tag: 'task', taskId } satisfies Record.Scope
          yield* storage.commit([
            { _tag: 'task', value: task },
            {
              _tag: 'document.create',
              record: { id, kind: 'obsolete.task', scope: taskScope },
              content: { _tag: 'base', version: 1, value: { corrupt: 'unavailable old shape' } },
            },
          ])
          const raw = some(yield* storage.document(id))
          const wrapped = {
            ...storage,
            document: (documentId: Record.DocumentId, at?: Record.DocumentPoint) =>
              documentId === id ? Effect.fail(failure) : storage.document(documentId, at),
          } satisfies Storage.Storage['Service']
          const cleanup = yield* Session.make().pipe(
            Effect.provideService(Storage.Storage, wrapped),
          )
          const dropped = yield* Session.commit(cleanup, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.putTask(tx, task) // Terminal receipts remain no-ops, no automatic legacy sweep.
              yield* Transaction.retireDocumentMetadata(
                tx,
                { kind: 'obsolete.task', scope: 'task', version: 3 },
                DocumentRetirement.target(raw),
              )
              const dropped = yield* Transaction.ensureDocument(tx, old, target)
              yield* Transaction.retireDocumentMetadata(
                tx,
                policy,
                DocumentRetirement.target(dropped),
              )
              return dropped
            }),
          )
          assert.isTrue(Option.isNone(yield* storage.document(id)))
          assert.isTrue(Option.isNone(yield* storage.document(dropped.record.id)))
          assert.deepEqual(some(yield* Session.task(cleanup, taskId)), task)
          const taskDoc = Document.define({
            kind: 'obsolete.task',
            version: 3,
            scope: 'task',
            schema,
            initial: () => ({ count: 0 }),
          })
          const rejected = yield* Session.commit(cleanup, (tx) =>
            Transaction.ensureDocument(tx, taskDoc, { scope: taskScope }),
          ).pipe(Effect.flip)
          assert.strictEqual(rejected._tag, 'SessionError')
          if (rejected._tag === 'SessionError') assert.strictEqual(rejected.reason, 'conflict')
        }),
      ),
  )
})

for (const backend of ['memory', 'jsonl', 'sqlite'] as const) {
  for (const fork of ['asOf', 'current', 'initial'] as const) {
    it.effect(`preserves ${fork} history/fork and retirement after ${backend} reopen`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'obsolete-retirement-' })
          const layer =
            backend === 'sqlite'
              ? Storage.layerSql.pipe(
                  Layer.provide(SqliteClient.layer({ filename: `${directory}/store.sqlite` })),
                )
              : Storage.layerJsonl({ filePath: `${directory}/store.jsonl` })
          const definition = Document.define({
            ...old.definition,
            scope: 'conversation',
            history: 'rewindable',
            fork,
          })
          const newer = { ...definition.definition, version: 3 }
          const run = Effect.gen(function* () {
            const storage = yield* Storage.Storage
            const sessionScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(sessionScope, Exit.void))
            const session = yield* Session.make().pipe(
              Effect.provideService(Scope.Scope, sessionScope),
            )
            const { snapshot, cutoff } = yield* Session.commit(session, (tx) =>
              Effect.gen(function* () {
                yield* Transaction.ensureRoot(tx)
                const snapshot = yield* Transaction.ensureDocument(tx, definition, target)
                const cutoff = yield* Transaction.appendEntry(tx, root, {
                  kind: 'obsolete.history',
                })
                return { snapshot, cutoff }
              }),
            )
            const seq = some(yield* storage.entry(cutoff.id)).commitSeq
            const selected = DocumentRetirement.target(snapshot)
            // Fork protections apply in either operation order inside a transaction.
            if (fork !== 'initial')
              yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* Transaction.forkConversation(tx, root, cutoff.id, ownerless)
                  conflict(
                    yield* Transaction.retireDocumentMetadata(tx, newer, selected).pipe(
                      Effect.flip,
                    ),
                  )
                }),
              )
            if (fork === 'asOf')
              yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* Transaction.retireDocumentMetadata(tx, newer, selected)
                  const failure = yield* Transaction.forkConversation(
                    tx,
                    root,
                    cutoff.id,
                    ownerless,
                  ).pipe(Effect.flip)
                  assert.strictEqual(failure._tag, 'SessionError')
                  if (failure._tag === 'SessionError')
                    assert.strictEqual(failure.reason, 'conflict')
                  return yield* Effect.fail('rollback probe')
                }),
              ).pipe(Effect.flip)
            yield* Session.commit(session, (tx) =>
              Transaction.retireDocumentMetadata(tx, newer, selected),
            )
            assert.isTrue(Option.isNone(yield* storage.findDocument(selected.address)))
            assert.strictEqual(some(yield* storage.document(selected.id, seq)).version, 1)
            assert.deepEqual(
              some(yield* Session.snapshotAsOf(session, definition, target, cutoff.id)).value,
              { count: 2 },
            )
            const child = yield* Session.commit(session, (tx) =>
              Transaction.forkConversation(tx, root, cutoff.id, ownerless),
            )
            const childTarget = {
              scope: { _tag: 'conversation' as const, conversationId: child.id },
            }
            const copy = yield* Session.snapshot(session, definition, childTarget)
            if (fork === 'asOf') {
              assert.strictEqual(some(copy).version, 1)
              assert.notStrictEqual(some(copy).record.id, selected.id)
              assert.deepEqual(some(copy).value, { count: 2 })
            } else assert.isTrue(Option.isNone(copy))
            const recreated = yield* Session.commit(session, (tx) =>
              Transaction.ensureDocument(tx, definition, target),
            )
            assert.notStrictEqual(recreated.record.id, selected.id)
            conflict(
              yield* Session.commit(session, (tx) =>
                Transaction.retireDocumentMetadata(tx, newer, selected),
              ).pipe(Effect.flip),
            )
            yield* Scope.close(sessionScope, Exit.void)
            const cold = yield* Session.make()
            assert.strictEqual(
              some(yield* Session.snapshot(cold, definition, target)).record.id,
              recreated.record.id,
            )
            assert.strictEqual(some(yield* storage.document(selected.id, seq)).version, 1)
            yield* Session.commit(cold, (tx) =>
              Transaction.retireDocumentMetadata(tx, newer, DocumentRetirement.target(recreated)),
            )
            // Both old and new targets are idempotent after the current incarnation disappears.
            yield* Session.commit(cold, (tx) =>
              Transaction.retireDocumentMetadata(tx, newer, selected),
            )
            return { selected, recreated, seq }
          })
          if (backend === 'memory') return yield* run.pipe(Effect.provide(Storage.layerMemory))
          const result = yield* run.pipe(Effect.provide(layer))
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            const cold = yield* Session.make()
            assert.isTrue(Option.isNone(yield* Session.snapshot(cold, definition, target)))
            assert.isTrue(Option.isNone(yield* storage.document(result.recreated.record.id)))
            assert.strictEqual(
              some(yield* storage.document(result.selected.id, result.seq)).version,
              1,
            )
            assert.deepEqual(some(yield* storage.document(result.selected.id, result.seq)).value, {
              count: 2,
            })
          }).pipe(Effect.provide(layer))
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    )
  }
}

for (const ordinary of [false, true]) {
  for (const mode of [
    'unavailable',
    'absent',
    'invalid',
    'future',
    'wrong-owner',
    'wrong-id',
    'io',
  ] as const) {
    it.effect(
      `${ordinary ? 'ordinary' : 'metadata'} ${mode} fails without content fallback or partial retirement`,
      () =>
        withMemory(
          Effect.gen(function* () {
            const storage = yield* Storage.Storage
            const seed = yield* Session.make()
            const { snapshot } = yield* initialize(seed)
            const selected = DocumentRetirement.target(snapshot)
            let contentReads = 0
            const wrapped = {
              ...Struct.omit(storage, ['documentMetadata']),
              document: (id: Record.DocumentId, at?: Record.DocumentPoint) => {
                contentReads++
                return storage.document(id, at)
              },
              ...(mode === 'unavailable'
                ? {}
                : {
                    documentMetadata: () => {
                      if (mode === 'io') return Effect.fail(failure)
                      if (mode === 'absent') return Effect.succeedNone
                      let version = 1
                      if (mode === 'future') version = 4
                      if (mode === 'invalid') version = 0
                      let record = snapshot.record
                      if (mode === 'wrong-id')
                        record = { ...record, id: Record.DocumentId.make(999) }
                      return Effect.succeedSome({
                        record:
                          mode === 'wrong-owner'
                            ? { ...snapshot.record, scope: { _tag: 'session' as const } }
                            : record,
                        version,
                      })
                    },
                  }),
            } satisfies Storage.Storage['Service']
            const session = yield* Session.make().pipe(
              Effect.provideService(Storage.Storage, wrapped),
            )
            yield* Session.commit(session, (tx) =>
              Effect.gen(function* () {
                const prior = yield* Transaction.appendEntry(tx, root, {
                  kind: 'obsolete.metadata.failure',
                })
                const error = yield* (
                  ordinary
                    ? Transaction.retireDocument(tx, old, target)
                    : Transaction.retireDocumentMetadata(tx, policy, selected)
                ).pipe(Effect.flip)
                if (mode === 'io') assert.strictEqual(error, failure)
                else if (mode === 'invalid') assert.strictEqual(error._tag, 'SchemaError')
                else if (mode === 'future' || mode === 'wrong-owner' || mode === 'wrong-id') {
                  if (!ordinary) conflict(error)
                  else {
                    assert.strictEqual(error._tag, 'SessionError')
                    if (error._tag === 'SessionError') assert.strictEqual(error.reason, 'conflict')
                  }
                } else {
                  assert.strictEqual(error._tag, 'SessionError')
                  if (error._tag === 'SessionError') assert.strictEqual(error.reason, 'invalid')
                }
                assert.strictEqual(contentReads, 0)
                assert.strictEqual(
                  some(yield* Transaction.entry(tx, prior.id)).kind,
                  'obsolete.metadata.failure',
                )
              }),
            )
            assert.strictEqual(some(yield* storage.findDocument(selected.address)).id, selected.id)
            assert.strictEqual(contentReads, 0)
          }),
        ),
    )
  }
}

it.effect(
  'fresh metadata rejects a future persisted version even when the target came from an older snapshot',
  () =>
    withMemory(
      Effect.gen(function* () {
        const storage = yield* Storage.Storage
        const session = yield* Session.make()
        const { snapshot } = yield* initialize(session)
        const selected = DocumentRetirement.target(snapshot)
        yield* storage.commit([
          {
            _tag: 'document.change',
            id: selected.id,
            content: { _tag: 'base', version: 4, value: { future: 'unknown' } },
          },
        ])
        let reads = 0
        const wrapped = {
          ...storage,
          document: () => {
            reads++
            return Effect.fail(failure)
          },
        } satisfies Storage.Storage['Service']
        const cleanup = yield* Session.make().pipe(Effect.provideService(Storage.Storage, wrapped))
        yield* Session.commit(cleanup, (tx) =>
          Effect.gen(function* () {
            conflict(
              yield* Transaction.retireDocumentMetadata(tx, policy, selected).pipe(Effect.flip),
            )
            yield* Transaction.appendEntry(tx, root, { kind: 'obsolete.future.rejected' })
          }),
        )
        assert.strictEqual(reads, 0)
        assert.strictEqual(some(yield* storage.document(selected.id)).version, 4)
        assert.strictEqual(some(yield* storage.findDocument(selected.address)).id, selected.id)
      }),
    ),
)

it.effect(
  'metadata cleanup and terminal settlement share one batch without reading the retired task content',
  () =>
    withMemory(
      Effect.gen(function* () {
        const storage = yield* Storage.Storage
        const session = yield* Session.make()
        const taskDoc = Document.family({
          ...old.definition,
          scope: 'task',
          kind: 'obsolete.settlement',
        })
        const { task, snapshot } = yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.ensureRoot(tx)
            const task = yield* Transaction.createTask(tx, {
              conversationId: root,
              kind: 'obsolete.settlement',
              version: 1,
              input: null,
              background: false,
              abortRequested: false,
              state: { status: 'running', checkpoint: { phase: 'work' } },
            })
            const snapshot = yield* Transaction.ensureDocument(tx, taskDoc, {
              scope: { _tag: 'task', taskId: task.id },
              key: 'saved',
            })
            return { task, snapshot }
          }),
        )
        const selected = DocumentRetirement.target(snapshot)
        let reads = 0
        let batch: ReadonlyArray<Record.StorageWrite> = []
        const wrapped = {
          ...storage,
          document: () => {
            reads++
            return Effect.fail(failure)
          },
          commit: (writes: Iterable<Record.StorageWrite>) => {
            batch = Array.from(writes)
            return storage.commit(batch)
          },
        } satisfies Storage.Storage['Service']
        const cleanup = yield* Session.make().pipe(Effect.provideService(Storage.Storage, wrapped))
        yield* Session.commit(cleanup, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.retireDocumentMetadata(
              tx,
              { kind: 'obsolete.settlement', scope: 'task', version: 3 },
              selected,
            )
            yield* Transaction.putTask(tx, {
              ...task,
              state: { status: 'terminal', outcome: { status: 'completed', result: null } },
            })
          }),
        )
        assert.strictEqual(reads, 0)
        assert.strictEqual(batch.filter((w) => w._tag === 'document.retire').length, 1)
        assert.strictEqual(batch.filter((w) => w._tag === 'task').length, 1)
        assert.isTrue(Option.isNone(yield* storage.document(selected.id)))
        assert.strictEqual(some(yield* Session.task(cleanup, task.id)).state.status, 'terminal')
      }),
    ),
)
