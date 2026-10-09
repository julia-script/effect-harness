import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Cause from 'effect/Cause'
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
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Identity from 'effect-harness/Identity'
import * as Record from 'effect-harness/Record'
import * as Document from 'effect-harness/Document'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import { StorageError } from 'effect-harness/StorageError'
import * as Transaction from 'effect-harness/Transaction'

const root = Record.ROOT_CONVERSATION_ID
const ownerless = { ownership: { _tag: 'ownerless' } } satisfies Transaction.ConversationOptions
const target = { scope: { _tag: 'conversation', conversationId: root } } satisfies Document.Target
const Counter = Schema.Struct({ count: Schema.Natural })
const counter = Document.define({
  kind: 'counter',
  scope: 'conversation',
  version: 1,
  history: 'rewindable',
  fork: 'asOf',
  schema: Counter,
  initial: () => ({ count: 0 }),
})
const withMemory = <A, E>(program: Effect.Effect<A, E, Storage.Storage | Scope.Scope>) =>
  Effect.scoped(program).pipe(Effect.provide(Storage.layerMemory))
const some = <A>(value: Option.Option<A>): A => {
  assert.isTrue(Option.isSome(value))
  return Option.getOrThrow(value)
}
const initialize = (session: Session.Session) =>
  Session.commit(session, (tx) =>
    Effect.gen(function* () {
      yield* Transaction.ensureRoot(tx)
      yield* Transaction.ensureDocument(tx, counter, target)
    }),
  )
const increment = (session: Session.Session) =>
  session.pipe(
    Session.commit((tx) =>
      tx.pipe(Transaction.updateDocument(counter, target, ({ count }) => ({ count: count + 1 }))),
    ),
  )
const current = (session: Session.Session) =>
  Session.snapshot(session, counter, target).pipe(
    Effect.map((snapshot) => some(snapshot).value.count),
  )

describe('Session', () => {
  it.effect(
    'permits one active coordinator per storage and releases ownership on scope closure',
    () =>
      withMemory(
        Effect.gen(function* () {
          yield* Effect.scoped(
            Effect.gen(function* () {
              const results = yield* Effect.all(
                [Session.make().pipe(Effect.result), Session.make().pipe(Effect.result)],
                { concurrency: 'unbounded' },
              )
              assert.strictEqual(results.filter((result) => result._tag === 'Success').length, 1)
              const failed = results.find((result) => result._tag === 'Failure')
              assert.strictEqual(failed?.failure.reason, 'conflict')
            }),
          )
          const replacement = yield* Session.make()
          yield* initialize(replacement)
          assert.strictEqual(yield* current(replacement), 0)
        }),
      ),
  )

  it.effect('adopts copied documents as bases when their source contains deltas', () =>
    withMemory(
      Effect.gen(function* () {
        const storage = yield* Storage.Storage
        const documentId = yield* storage.mintId<Record.DocumentId>()
        const entryId = yield* storage.mintId<Record.EntryId>()
        yield* storage.commit([
          { _tag: 'conversation', value: { id: root } },
          {
            _tag: 'document.create',
            record: {
              id: documentId,
              kind: 'counter',
              ...target,
              history: 'rewindable',
              fork: 'asOf',
            },
            content: { _tag: 'base', version: 1, value: { count: 0 } },
          },
        ])
        yield* storage.commit([
          {
            _tag: 'document.change',
            id: documentId,
            content: { _tag: 'delta', version: 1, ops: [['set', ['count'], 1]] },
          },
          { _tag: 'entry', value: { id: entryId, conversationId: root, kind: 'input' } },
        ])
        const session = yield* Session.make()
        assert.strictEqual(
          some(yield* Session.snapshot(session, counter, target)).deltasSinceBase,
          1,
        )
        const fork = yield* Session.commit(session, (tx) =>
          Transaction.forkConversation(tx, root, entryId, ownerless),
        )
        const cached = some(
          yield* Session.snapshot(session, counter, {
            scope: { _tag: 'conversation', conversationId: fork.id },
          }),
        )
        assert.strictEqual(cached.value.count, 1)
        assert.strictEqual(cached.deltasSinceBase, 0)
        assert.deepEqual(cached, some(yield* storage.document(cached.record.id)))
      }),
    ),
  )

  it.effect('constructs lazily and leaves backend ownership with its supplying layer', () =>
    withMemory(
      Effect.gen(function* () {
        const storage = yield* Storage.Storage
        let reads = 0
        const tracked: Storage.Storage['Service'] = {
          ...storage,
          conversation: (id) =>
            Effect.sync(() => {
              reads++
            }).pipe(Effect.andThen(storage.conversation(id))),
        }
        const session = yield* Effect.scoped(
          Session.make().pipe(Effect.provideService(Storage.Storage, tracked)),
        )
        assert.strictEqual(reads, 0)
        const error = yield* Session.conversation(session, root).pipe(Effect.flip)
        assert.strictEqual(error._tag, 'SessionError')
        if ('reason' in error) assert.strictEqual(error.reason, 'closed')
        yield* storage.commit([{ _tag: 'conversation', value: { id: root } }])
        assert.isTrue(Option.isSome(yield* storage.conversation(root)))
      }),
    ),
  )

  it.effect('supports dual operations, staged reads and read-only commits', () =>
    withMemory(
      Effect.gen(function* () {
        const session = yield* Session.make()
        const result = yield* session.pipe(
          Session.commit((tx) =>
            Effect.gen(function* () {
              yield* tx.pipe(Transaction.ensureRoot())
              yield* tx.pipe(Transaction.ensureDocument(counter, target))
              yield* tx.pipe(Transaction.setDocument(counter, target, { count: 4 }))
              assert.strictEqual(
                some(yield* tx.pipe(Transaction.snapshot(counter, target))).value.count,
                4,
              )
              const entry = yield* tx.pipe(
                Transaction.appendEntry(root, { kind: 'input', head: 'self' }),
              )
              assert.strictEqual(entry.head, entry.id)
              assert.strictEqual(
                some(yield* tx.pipe(Transaction.entry(entry.id, { conversationId: root }))).id,
                entry.id,
              )
              assert.strictEqual(
                (yield* tx.pipe(Transaction.scanConversations(), Stream.runCollect)).length,
                1,
              )
              assert.strictEqual(
                (yield* tx.pipe(
                  Transaction.scanEntries({ conversationId: root }),
                  Stream.runCollect,
                )).length,
                1,
              )
              return entry
            }),
          ),
        )
        assert.strictEqual(some(yield* session.pipe(Session.entry(result.id))).entry.id, result.id)
        assert.strictEqual(
          (yield* session.pipe(Session.scanConversations(), Stream.runCollect)).length,
          1,
        )
        assert.strictEqual(yield* current(session), 4)
        yield* Session.commit(session, (tx) => Transaction.ensureRoot(tx))
        const storage = yield* Storage.Storage
        // A read-only transaction does not consume another commit sequence.
        assert.strictEqual(yield* storage.commit([]), 2)
      }),
    ),
  )

  for (const failure of ['typed', 'defect', 'interruption', 'schema'] as const)
    it.effect(`discards staged records and documents on ${failure} failure`, () =>
      withMemory(
        Effect.gen(function* () {
          const session = yield* Session.make()
          yield* initialize(session)
          const failed = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.setDocument(tx, counter, target, { count: 9 })
              yield* Transaction.appendEntry(tx, root, { kind: 'input' })
              switch (failure) {
                case 'typed':
                  return yield* Effect.fail('cancelled')
                case 'defect':
                  return yield* Effect.die('injected defect')
                case 'interruption':
                  return yield* Effect.interrupt
                case 'schema':
                  return yield* Transaction.setDocument(tx, counter, target, { count: -1 })
              }
            }),
          ).pipe(Effect.exit)
          assert.isTrue(Exit.isFailure(failed))
          assert.strictEqual(yield* current(session), 0)
          assert.strictEqual(
            (yield* Session.scanEntries(session, { conversationId: root }).pipe(Stream.runCollect))
              .length,
            0,
          )
          yield* increment(session)
          assert.strictEqual(yield* current(session), 1)
        }),
      ),
    )

  it.effect('serializes concurrent transactions and operations within a callback', () =>
    withMemory(
      Effect.gen(function* () {
        const session = yield* Session.make()
        yield* initialize(session)
        yield* Effect.all(
          Array.from({ length: 20 }, () => increment(session)),
          { concurrency: 'unbounded' },
        )
        yield* Session.commit(session, (tx) =>
          Effect.all(
            Array.from({ length: 20 }, () =>
              Transaction.updateDocument(tx, counter, target, ({ count }) => ({
                count: count + 1,
              })),
            ),
            { concurrency: 'unbounded' },
          ),
        )
        assert.strictEqual(yield* current(session), 40)
      }),
    ),
  )

  it.effect(
    'rejects nested Session calls and revokes escaped transaction effects and streams',
    () =>
      withMemory(
        Effect.gen(function* () {
          const session = yield* Session.make()
          yield* initialize(session)
          const nested = yield* Session.commit(session, () =>
            Session.conversation(session, root),
          ).pipe(Effect.flip)
          if ('reason' in nested) assert.strictEqual(nested.reason, 'conflict')
          const tx = yield* Session.commit(session, Effect.succeed)
          const error = yield* Transaction.conversation(tx, root).pipe(Effect.flip)
          if ('reason' in error) assert.strictEqual(error.reason, 'revoked')
          const streamError = yield* Transaction.scanTasks(tx).pipe(Stream.runCollect, Effect.flip)
          if ('reason' in streamError) assert.strictEqual(streamError.reason, 'revoked')
        }),
      ),
  )

  it.effect('keeps encoded storage and decoded mutable values detached', () =>
    withMemory(
      Effect.gen(function* () {
        const session = yield* Session.make()
        const instant = new Date('2026-01-01T00:00:00Z')
        const clock = Document.define({
          kind: 'clock',
          scope: 'session',
          version: 1,
          schema: Schema.Struct({ instant: Schema.DateFromString }),
          initial: () => ({ instant }),
        })
        const clockTarget = { scope: { _tag: 'session' } } satisfies Document.Target
        yield* Session.commit(session, (tx) => Transaction.ensureDocument(tx, clock, clockTarget))
        instant.setUTCFullYear(2000)
        const first = some(yield* Session.snapshot(session, clock, clockTarget))
        first.value.instant.setUTCFullYear(2001)
        const second = some(yield* session.pipe(Session.snapshot(clock, clockTarget)))
        assert.strictEqual(second.value.instant.toISOString(), '2026-01-01T00:00:00.000Z')
        const storage = yield* Storage.Storage
        assert.strictEqual(
          some(yield* storage.document(second.record.id)).value.instant,
          '2026-01-01T00:00:00.000Z',
        )
      }),
    ),
  )

  it.effect('preserves document codec requirements for both encoding and decoding', () =>
    withMemory(
      Effect.gen(function* () {
        class Decoder extends Context.Service<Decoder, { readonly access: Effect.Effect<void> }>()(
          'test/Decoder',
        ) {}
        class Encoder extends Context.Service<Encoder, { readonly access: Effect.Effect<void> }>()(
          'test/Encoder',
        ) {}
        let decodes = 0
        let encodes = 0
        const schema = Counter.pipe(
          Schema.middlewareDecoding((effect) =>
            Effect.gen(function* () {
              yield* (yield* Decoder).access
              return yield* effect
            }),
          ),
          Schema.middlewareEncoding((effect) =>
            Effect.gen(function* () {
              yield* (yield* Encoder).access
              return yield* effect
            }),
          ),
        )
        const secured = Document.define({ ...counter.definition, schema })
        const session = yield* Session.make()
        yield* Effect.gen(function* () {
          yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.ensureRoot(tx)
              yield* Transaction.ensureDocument(tx, secured, target)
              yield* Transaction.setDocument(tx, secured, target, { count: 5 })
            }),
          )
          assert.strictEqual(some(yield* Session.snapshot(session, secured, target)).value.count, 5)
        }).pipe(
          Effect.provideService(Decoder, {
            access: Effect.sync(() => {
              decodes++
            }),
          }),
          Effect.provideService(Encoder, {
            access: Effect.sync(() => {
              encodes++
            }),
          }),
        )
        assert.isAbove(decodes, 0)
        assert.isAbove(encodes, 0)
      }),
    ),
  )

  it.effect(
    'holds subscription acquisition and initial decoding on the same coordination line',
    () =>
      withMemory(
        Effect.gen(function* () {
          const decoding = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          let block = false
          const schema = Counter.pipe(
            Schema.middlewareDecoding((effect) =>
              Effect.gen(function* () {
                if (block) {
                  yield* Deferred.succeed(decoding, undefined)
                  yield* Deferred.await(release)
                }
                return yield* effect
              }),
            ),
          )
          const observed = Document.define({ ...counter.definition, schema })
          const session = yield* Session.make()
          yield* initialize(session)
          const pull = yield* Session.watch(session, observed, target).pipe(
            Stream.rechunk(1),
            Stream.toPull,
          )
          block = true
          const initial = yield* pull.pipe(Effect.forkChild)
          yield* Deferred.await(decoding)
          const writing = yield* increment(session).pipe(Effect.forkChild)
          yield* Deferred.succeed(release, undefined)
          assert.strictEqual((yield* Fiber.join(initial))[0].value.count, 0)
          yield* Fiber.join(writing)
          assert.strictEqual((yield* pull)[0].value.count, 1)
        }),
      ),
  )

  it.effect(
    'seals admission on scope closure, settles the admitted callback and ends observers',
    () =>
      withMemory(
        Effect.gen(function* () {
          const owner = yield* Scope.make()
          const session = yield* Session.make().pipe(Effect.provideService(Scope.Scope, owner))
          yield* initialize(session)
          const pull = yield* Session.watch(session, counter, target).pipe(
            Stream.rechunk(1),
            Stream.toPull,
          )
          yield* pull
          const admitted = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const running = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Deferred.succeed(admitted, undefined)
              yield* Deferred.await(release)
              yield* Transaction.setDocument(tx, counter, target, { count: 8 })
            }),
          ).pipe(Effect.forkChild)
          yield* Deferred.await(admitted)
          const closing = yield* Scope.close(owner, Exit.void).pipe(Effect.forkChild)
          yield* Effect.yieldNow
          const denied = yield* Session.conversation(session, root).pipe(Effect.flip)
          if ('reason' in denied) assert.strictEqual(denied.reason, 'closed')
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(running)
          yield* Fiber.join(closing)
          assert.strictEqual((yield* pull)[0].value.count, 8)
          const ended = yield* pull.pipe(Effect.exit)
          assert.isTrue(Exit.isFailure(ended))
          if (Exit.isFailure(ended)) assert.isTrue(Pull.isDoneCause(ended.cause))
          const storage = yield* Storage.Storage
          const record = some(yield* storage.findDocument({ kind: 'counter', ...target }))
          assert.strictEqual(some(yield* storage.document(record.id)).value.count, 8)
          const scan = yield* Session.scanTasks(session).pipe(Stream.runCollect, Effect.flip)
          if ('reason' in scan) assert.strictEqual(scan.reason, 'closed')
        }),
      ),
  )

  it.effect('validates document scope, family keys and persisted policies', () =>
    withMemory(
      Effect.gen(function* () {
        const session = yield* Session.make()
        assert.isTrue(Option.isNone(yield* Session.snapshot(session, counter, target)))
        yield* initialize(session)
        const wrongTarget = yield* Session.snapshot(session, counter, {
          scope: { _tag: 'session' },
        }).pipe(Effect.flip)
        if ('reason' in wrongTarget) assert.strictEqual(wrongTarget.reason, 'invalid')
        const members = Document.family(counter.definition)
        yield* Session.commit(session, (tx) =>
          Transaction.ensureDocument(tx, members, { ...target, key: 'one' }),
        )
        assert.isTrue(
          Option.isSome(yield* Session.snapshot(session, members, { ...target, key: 'one' })),
        )
        const missingKey = yield* Session.snapshot(session, members, target).pipe(Effect.flip)
        if ('reason' in missingKey) assert.strictEqual(missingKey.reason, 'invalid')
        const changed = Document.define({ ...counter.definition, version: 2 })
        const incompatible = yield* Session.snapshot(session, changed, target).pipe(Effect.flip)
        if ('reason' in incompatible) assert.strictEqual(incompatible.reason, 'conflict')
      }),
    ),
  )

  it.effect(
    'replays document history and forks asOf, current and initial policies independently',
    () =>
      withMemory(
        Effect.gen(function* () {
          const session = yield* Session.make()
          const live = Document.define({
            ...counter.definition,
            kind: 'live',
            scope: 'conversation',
            history: 'latest',
            fork: 'current',
          })
          const fresh = Document.define({
            ...counter.definition,
            kind: 'fresh',
            scope: 'conversation',
            history: 'rewindable',
            fork: 'initial',
          })
          yield* initialize(session)
          const at = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.setDocument(tx, counter, target, { count: 1 })
              yield* Transaction.ensureDocument(tx, live, target)
              yield* Transaction.setDocument(tx, live, target, { count: 1 })
              yield* Transaction.ensureDocument(tx, fresh, target)
              yield* Transaction.setDocument(tx, fresh, target, { count: 1 })
              return yield* Transaction.appendEntry(tx, root, { kind: 'input' })
            }),
          )
          const later = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.setDocument(tx, counter, target, { count: 2 })
              yield* Transaction.setDocument(tx, live, target, { count: 2 })
              return yield* Transaction.appendEntry(tx, root, { kind: 'answer' })
            }),
          )
          const fork = yield* session.pipe(
            Session.commit((tx) => tx.pipe(Transaction.forkConversation(root, at.id, ownerless))),
          )
          const forkTarget = {
            scope: { _tag: 'conversation', conversationId: fork.id },
          } satisfies Document.Target
          const inherited = some(yield* Session.snapshot(session, counter, forkTarget))
          assert.strictEqual(inherited.value.count, 1)
          assert.notStrictEqual(
            inherited.record.id,
            some(yield* Session.snapshot(session, counter, target)).record.id,
          )
          assert.strictEqual(
            some(yield* Session.snapshot(session, live, forkTarget)).value.count,
            2,
          )
          assert.isTrue(Option.isNone(yield* Session.snapshot(session, fresh, forkTarget)))
          yield* Session.commit(session, (tx) => Transaction.ensureDocument(tx, fresh, forkTarget))
          assert.strictEqual(
            some(yield* Session.snapshot(session, fresh, forkTarget)).value.count,
            0,
          )
          assert.strictEqual(
            some(yield* session.pipe(Session.snapshotAsOf(counter, target, at.id))).value.count,
            1,
          )
          assert.strictEqual(
            some(yield* Session.snapshotAsOf(session, counter, forkTarget, at.id)).value.count,
            1,
          )
          assert.isTrue(
            Option.isNone(yield* Session.entry(session, later.id, { conversationId: fork.id })),
          )
          assert.deepEqual(
            (yield* Session.scanEntries(session, { conversationId: fork.id }).pipe(
              Stream.runCollect,
            )).map((entry) => entry.id),
            [at.id],
          )
          yield* Session.commit(session, (tx) =>
            Transaction.setDocument(tx, counter, forkTarget, { count: 7 }),
          )
          assert.strictEqual(yield* current(session), 2)
        }),
      ),
  )

  it.effect('observes exact revisions, ends retired incarnations and admits replacements', () =>
    withMemory(
      Effect.gen(function* () {
        const session = yield* Session.make()
        yield* initialize(session)
        const pull = yield* session.pipe(
          Session.watch(counter, target),
          Stream.rechunk(1),
          Stream.toPull,
        )
        const initial = (yield* pull)[0]
        assert.strictEqual(initial.value.count, 0)
        const committed = yield* session.pipe(Session.commits(), Stream.rechunk(1), Stream.toPull)
        const firstCommit = yield* committed.pipe(Effect.forkChild)
        yield* Effect.yieldNow
        yield* increment(session)
        yield* increment(session)
        assert.strictEqual((yield* pull)[0].value.count, 1)
        assert.strictEqual((yield* pull)[0].value.count, 2)
        assert.strictEqual((yield* Fiber.join(firstCommit))[0].seq, 2)
        assert.strictEqual((yield* committed)[0].seq, 3)
        yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.retireDocument(tx, counter, target)
            yield* Transaction.ensureDocument(tx, counter, target)
          }),
        )
        const end = yield* pull.pipe(Effect.exit)
        assert.isTrue(Exit.isFailure(end))
        if (Exit.isFailure(end)) assert.isTrue(Pull.isDoneCause(end.cause))
        const replacement = some(yield* Session.snapshot(session, counter, target))
        assert.notStrictEqual(replacement.record.id, initial.record.id)
        assert.strictEqual(replacement.value.count, 0)
      }),
    ),
  )

  it.effect('fails a slow observer without blocking commits', () =>
    withMemory(
      Effect.gen(function* () {
        const session = yield* Session.make()
        yield* initialize(session)
        const pull = yield* Session.watch(session, counter, target).pipe(Stream.toPull)
        yield* pull
        for (let index = 0; index < 70; index++) yield* increment(session)
        assert.strictEqual(yield* current(session), 70)
        // Pending frames drain before the overflow error is reported.
        let overflow = false
        for (let index = 0; index < 65; index++) {
          const exit = yield* pull.pipe(Effect.exit)
          if (Exit.isFailure(exit)) {
            const error = Cause.findErrorOption(exit.cause)
            if (Option.isSome(error) && 'reason' in error.value)
              assert.strictEqual(error.value.reason, 'overflow')
            overflow = true
            break
          }
        }
        assert.isTrue(overflow)
      }),
    ),
  )

  it.effect('validates task attribution, staged query filters and submission receipts', () =>
    withMemory(
      Effect.gen(function* () {
        const session = yield* Session.make()
        yield* initialize(session)
        const task = yield* Session.commit(session, (tx) =>
          Transaction.createTask(tx, {
            conversationId: root,
            kind: 'agent',
            version: 1,
            input: null,
            background: false,
            abortRequested: false,
            state: { status: 'pending', checkpoint: { phase: 'start' } },
          }),
        )
        const requestId = Identity.RequestId.make('request-1')
        const receipt = yield* Session.commit(
          session,
          (tx) =>
            Effect.gen(function* () {
              const entry = yield* Transaction.appendEntry(tx, root, { kind: 'input' })
              assert.strictEqual(entry.byTaskId, task.id)
              yield* Transaction.putTask(tx, {
                ...task,
                state: { status: 'running', checkpoint: { phase: 'run' } },
              })
              assert.strictEqual(
                (yield* Transaction.scanTasks(tx, { status: 'pending' }).pipe(Stream.runCollect))
                  .length,
                0,
              )
              assert.strictEqual(
                (yield* Transaction.scanTasks(tx, { status: 'running' }).pipe(Stream.runCollect))
                  .length,
                1,
              )
              return yield* Transaction.createSubmission(tx, {
                _tag: 'InputPlaced',
                type: 'input',
                status: 'placed',
                conversationId: root,
                requestId,
                entry: entry.id,
              })
            }),
          { conversationId: root, taskId: task.id },
        )
        assert.strictEqual(
          some(yield* session.pipe(Session.submissionByRequest(root, requestId))).id,
          receipt.id,
        )
        const duplicate = yield* Session.commit(session, (tx) =>
          Transaction.createSubmission(tx, {
            _tag: 'InputQueued',
            type: 'input',
            status: 'queued',
            conversationId: root,
            requestId,
          }),
        ).pipe(Effect.flip)
        if ('reason' in duplicate) assert.strictEqual(duplicate.reason, 'conflict')
        yield* Session.commit(session, (tx) =>
          Transaction.putSubmission(tx, {
            id: receipt.id,
            conversationId: root,
            requestId,
            ...(receipt.entry === undefined ? {} : { entry: receipt.entry }),
            _tag: 'InputUnanswered',
            type: 'input',
            status: 'unanswered',
            reason: 'aborted',
          }),
        )
        const rewind = yield* Session.commit(session, (tx) =>
          Transaction.putSubmission(tx, receipt),
        ).pipe(Effect.flip)
        if ('reason' in rewind) assert.strictEqual(rewind.reason, 'conflict')
      }),
    ),
  )

  for (const reason of ['io', 'uncertain'] as const)
    it.effect(`handles ${reason} persistence failures without adopting drafts`, () =>
      withMemory(
        Effect.gen(function* () {
          const storage = yield* Storage.Storage
          let fail = false
          const error = new StorageError({ reason, operation: 'test.commit', message: 'injected' })
          const failing: Storage.Storage['Service'] = {
            ...storage,
            commit: (writes) =>
              Effect.suspend(() => (fail ? Effect.fail(error) : storage.commit(writes))),
          }
          const session = yield* Session.make().pipe(
            Effect.provideService(Storage.Storage, failing),
          )
          yield* initialize(session)
          fail = true
          assert.strictEqual(yield* increment(session).pipe(Effect.flip), error)
          fail = false
          if (reason === 'io') {
            assert.strictEqual(yield* current(session), 0)
            yield* increment(session)
            assert.strictEqual(yield* current(session), 1)
          } else assert.strictEqual(yield* current(session).pipe(Effect.flip), error)
          const record = some(yield* storage.findDocument({ kind: 'counter', ...target }))
          assert.strictEqual(
            some(yield* storage.document(record.id)).value.count,
            reason === 'io' ? 1 : 0,
          )
        }),
      ),
    )

  it.effect(
    'settles admitted persistence despite interruption and publishes before releasing coordination',
    () =>
      withMemory(
        Effect.gen(function* () {
          const storage = yield* Storage.Storage
          const admitted = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          let block = false
          const gated: Storage.Storage['Service'] = {
            ...storage,
            commit: (writes) =>
              Effect.gen(function* () {
                if (block) {
                  yield* Deferred.succeed(admitted, undefined)
                  yield* Deferred.await(release)
                }
                return yield* storage.commit(writes)
              }),
          }
          const session = yield* Session.make().pipe(Effect.provideService(Storage.Storage, gated))
          yield* initialize(session)
          const pull = yield* Session.commits(session).pipe(Stream.toPull)
          const nextCommit = yield* pull.pipe(Effect.forkChild)
          yield* Effect.yieldNow
          block = true
          const running = yield* increment(session).pipe(Effect.forkChild)
          yield* Deferred.await(admitted)
          const interrupting = yield* Fiber.interrupt(running).pipe(Effect.forkChild)
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(interrupting)
          assert.strictEqual(yield* current(session), 1)
          assert.strictEqual((yield* Fiber.join(nextCommit))[0].seq, 2)
        }),
      ),
  )
})

for (const backend of ['sqlite', 'jsonl'] as const)
  it.live(`reopens Session state through ${backend} storage`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'harness-session-' })
        const filename = `${directory}/storage`
        const layer =
          backend === 'sqlite'
            ? Storage.layerSql.pipe(Layer.provide(SqliteClient.layer({ filename })))
            : Storage.layerJsonl({ filePath: filename })
        const at = yield* Effect.scoped(
          Effect.gen(function* () {
            const session = yield* Session.make()
            yield* initialize(session)
            return yield* Session.commit(session, (tx) =>
              Effect.gen(function* () {
                yield* Transaction.setDocument(tx, counter, target, { count: 3 })
                return yield* Transaction.appendEntry(tx, root, { kind: 'input' })
              }),
            )
          }),
        ).pipe(Effect.provide(layer))
        yield* Effect.scoped(
          Effect.gen(function* () {
            const session = yield* Session.make()
            assert.strictEqual(yield* current(session), 3)
            const fork = yield* Session.commit(session, (tx) =>
              Transaction.forkConversation(tx, root, at.id, ownerless),
            )
            assert.strictEqual(
              some(
                yield* Session.snapshot(session, counter, {
                  scope: { _tag: 'conversation', conversationId: fork.id },
                }),
              ).value.count,
              3,
            )
          }),
        ).pipe(Effect.provide(layer))
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  )
