import * as Document from 'effect-harness/Document'
import * as Schema from 'effect/Schema'
import { assert, it } from '@effect/vitest'
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
import * as Stream from 'effect/Stream'
import * as Scope from 'effect/Scope'
import * as EventJournal from 'effect/eventlog/EventJournal'
import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import * as Storage from 'effect-harness/Storage'
import * as Format from 'effect-harness/StorageFormat'
import * as Session from 'effect-harness/Session'
import * as Transaction from 'effect-harness/Transaction'
import * as Fixture from './fixtures/Journal.js'

const counter = Document.define({
  kind: 'counter',
  scope: 'conversation',
  version: 1,
  schema: Schema.Struct({ count: Schema.Natural }),
  initial: () => ({ count: 0 }),
  history: 'rewindable',
  fork: 'asOf',
})
const target = {
  scope: { _tag: 'conversation' as const, conversationId: Storage.ROOT_CONVERSATION_ID },
}
const root = Storage.ROOT_CONVERSATION_ID
const rootWrite: Storage.StorageWrite = { _tag: 'conversation', value: { id: root } }
const entryWrite = (id: number): Storage.StorageWrite => ({
  _tag: 'entry',
  value: { id: Storage.EntryId.make(id), conversationId: root, kind: 'note', data: { id } },
})
const open = Layer.build(Storage.layerJournal()).pipe(
  Effect.map((c) => Context.get(c, Storage.Storage)),
)
const metadata = { format: 1, nextId: 2, nextSeq: 1 }
const frame = { version: 1, revision: 1, previous: null, metadata, rows: [] }

it.effect(
  'persists complete batches and reservations, replaying by revision rather than journal order',
  () =>
    Effect.gen(function* () {
      const journal = yield* EventJournal.EventJournal
      yield* Effect.scoped(
        Effect.gen(function* () {
          const storage = yield* open
          assert.strictEqual(yield* storage.commit([rootWrite, entryWrite(2), entryWrite(3)]), 1)
          assert.lengthOf(yield* journal.entries, 1)
          const invalid = yield* storage.commit([entryWrite(4), rootWrite]).pipe(Effect.flip)
          assert.strictEqual(invalid.reason, 'conflict')
          assert.lengthOf(yield* journal.entries, 1)
          assert.strictEqual(yield* storage.mintId(), 4)
          yield* storage.commit([entryWrite(5)])
        }),
      )
      const frames = yield* Fixture.frames
      assert.deepEqual(
        frames.map((f) => f.revision),
        [1, 2, 3],
      )
      assert.deepEqual(
        frames.map((f) => f.previous),
        [null, frames[0]?.id, frames[1]?.id],
      )
      assert.lengthOf(frames[0]?.rows ?? [], 3)
      const unordered = {
        ...journal,
        entries: journal.entries.pipe(Effect.map((entries) => [...entries].reverse())),
      }
      yield* Effect.scoped(
        Effect.gen(function* () {
          const storage = yield* open
          assert.deepEqual(
            (yield* Stream.runCollect(storage.scanEntries({ conversationId: root }))).map(
              (e) => e.id,
            ),
            [5, 3, 2],
          )
          assert.strictEqual(yield* storage.mintId(), 6)
          assert.strictEqual(yield* storage.commit([]), 3)
        }),
      ).pipe(Effect.provideService(EventJournal.EventJournal, unordered))
    }).pipe(Effect.provide(EventJournal.layerMemory)),
)

it.effect(
  'rejects duplicate owners, isolates namespaces and releases ownership on scope close',
  () =>
    Effect.gen(function* () {
      const journal = yield* EventJournal.EventJournal
      yield* journal.write({
        event: 'unrelated',
        primaryKey: 'default',
        payload: new Uint8Array([255]),
        effect: () => Effect.void,
      })
      const escaped = yield* Effect.scoped(
        Effect.gen(function* () {
          const first = yield* open
          assert.strictEqual((yield* open.pipe(Effect.flip)).reason, 'invalid')
          yield* first.commit([rootWrite])
          const other = Context.get(
            yield* Layer.build(Storage.layerJournal({ storeId: 'other' })),
            Storage.Storage,
          )
          assert.isTrue(Option.isNone(yield* other.conversation(root)))
          assert.strictEqual(yield* other.mintId(), 2)
          return first
        }),
      )
      assert.strictEqual((yield* escaped.commit([]).pipe(Effect.flip)).reason, 'closed')
      yield* Effect.scoped(
        Effect.gen(function* () {
          const reopened = yield* open
          assert.isTrue(Option.isSome(yield* reopened.conversation(root)))
          assert.strictEqual(yield* reopened.mintId(), 2)
        }),
      )
    }).pipe(Effect.provide(EventJournal.layerMemory)),
)

for (const mode of ['before append', 'after append', 'defect after append'] as const)
  it.live(
    `SQLite journal ${mode} failure seals Storage and Session; cold reopen reconciles bytes`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped()
          let nextId = 0
          let sequence = 0
          const backend = SqlEventJournal.layer().pipe(
            Layer.provide(SqliteClient.layer({ filename: `${directory}/journal.sqlite` })),
          )
          yield* Effect.gen(function* () {
            const journal = yield* EventJournal.EventJournal
            let fail = false
            const fault = new EventJournal.EventJournalError({
              method: 'write',
              cause: 'lost acknowledgement',
            })
            const injected: EventJournal.EventJournal['Service'] = {
              ...journal,
              write: (options) =>
                Effect.suspend(() => {
                  if (!fail) return journal.write(options)
                  if (mode === 'before append') return Effect.fail(fault)
                  return journal
                    .write(options)
                    .pipe(
                      Effect.andThen(
                        mode === 'after append' ? Effect.fail(fault) : Effect.die(fault),
                      ),
                    )
                }),
            }
            yield* Effect.scoped(
              Effect.gen(function* () {
                const storage = yield* open
                const session = yield* Session.make().pipe(
                  Effect.provideService(Storage.Storage, storage),
                )
                yield* Session.commit(session, (tx) =>
                  Effect.gen(function* () {
                    yield* Transaction.ensureRoot(tx)
                    yield* Transaction.ensureDocument(tx, counter, target)
                  }),
                )
                nextId = yield* storage.mintId()
                sequence = yield* storage.commit([])
                fail = true
                const error = yield* Session.commit(session, (tx) =>
                  Transaction.setDocument(tx, counter, target, { count: 42 }),
                ).pipe(Effect.flip)
                assert.strictEqual(error._tag, 'StorageError')
                if (error._tag === 'StorageError') assert.strictEqual(error.reason, 'uncertain')
                assert.strictEqual(
                  (yield* storage.commit([]).pipe(Effect.flip)).reason,
                  'uncertain',
                )
                assert.strictEqual((yield* storage.mintId().pipe(Effect.flip)).reason, 'uncertain')
                assert.strictEqual(
                  (yield* storage.conversation(root).pipe(Effect.flip)).reason,
                  'uncertain',
                )
                assert.strictEqual(
                  (yield* Stream.runCollect(storage.scanTasks()).pipe(Effect.flip)).reason,
                  'uncertain',
                )
                assert.deepEqual(
                  yield* Session.commit(session, (tx) => Transaction.ensureRoot(tx)).pipe(
                    Effect.flip,
                  ),
                  error,
                )
              }),
            ).pipe(Effect.provideService(EventJournal.EventJournal, injected))
          }).pipe(Effect.provide(backend))
          // A fresh connection and journal, not the previous in-memory projection.
          yield* Effect.scoped(
            Effect.gen(function* () {
              const storage = yield* open
              assert.isTrue(Option.isSome(yield* storage.conversation(root)))
              assert.deepEqual(
                Option.getOrThrow(
                  yield* storage.document(
                    Option.getOrThrow(yield* storage.findDocument({ kind: 'counter', ...target }))
                      .id,
                  ),
                ).value,
                { count: mode === 'before append' ? 0 : 42 },
              )
              assert.strictEqual(yield* storage.mintId(), nextId + 1)
              assert.strictEqual(
                yield* storage.commit([entryWrite(nextId + 2)]),
                sequence + (mode === 'before append' ? 1 : 2),
              )
            }),
          ).pipe(Effect.provide(backend))
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )

for (const persisted of [false, true])
  it.effect(
    `failed format append (persisted=${persisted}) seals the guard and reconciles on reopen`,
    () =>
      Effect.gen(function* () {
        const journal = yield* EventJournal.EventJournal
        const fault = new EventJournal.EventJournalError({
          method: 'write',
          cause: 'lost acknowledgement',
        })
        const failing: EventJournal.EventJournal['Service'] = {
          ...journal,
          write: (options) =>
            (persisted ? journal.write(options) : Effect.void).pipe(
              Effect.andThen(Effect.fail(fault)),
            ),
        }
        yield* Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(Storage.layerJournal())
            const guard = Context.get(context, Format.StorageFormat)
            const error = yield* guard
              .requireSupportedFormat({ formatVersion: 3, minimumReaderVersion: 3 })
              .pipe(Effect.flip)
            assert.strictEqual(error._tag, 'StorageError')
            if (error._tag === 'StorageError') assert.strictEqual(error.reason, 'uncertain')
            yield* guard.inspect.pipe(Effect.flip)
            assert.strictEqual(
              (yield* Context.get(context, Storage.Storage).commit([]).pipe(Effect.flip)).reason,
              'uncertain',
            )
          }),
        ).pipe(Effect.provideService(EventJournal.EventJournal, failing))
        yield* Effect.gen(function* () {
          assert.deepEqual(yield* Format.inspect, {
            formatVersion: persisted ? 3 : 1,
            minimumReaderVersion: persisted ? 3 : 1,
          })
          const storage = yield* Storage.Storage
          assert.strictEqual(yield* storage.mintId(), 2)
          assert.strictEqual(yield* storage.commit([]), 1)
        }).pipe(Effect.provide(Storage.layerJournal()))
      }).pipe(Effect.provide(EventJournal.layerMemory)),
  )

it.effect('does not publish a pending append and completes it despite caller interruption', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const journal = yield* EventJournal.EventJournal
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const delayed: EventJournal.EventJournal['Service'] = {
        ...journal,
        write: (options) =>
          journal.write({
            ...options,
            effect: (entry) =>
              options.effect(entry).pipe(
                Effect.tap(() => Deferred.succeed(started, undefined)),
                Effect.tap(() => Deferred.await(release)),
              ),
          }),
      }
      yield* Effect.scoped(
        Effect.gen(function* () {
          const storage = yield* open
          const fiber = yield* Effect.forkChild(storage.commit([rootWrite, entryWrite(2)]))
          yield* Deferred.await(started)
          const unpublished = Option.isNone(yield* storage.conversation(root))
          const entriesBeforeAppend = yield* journal.entries
          const interrupt = yield* Effect.forkChild(Fiber.interrupt(fiber), {
            startImmediately: true,
          })
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(interrupt)
          assert.isTrue(unpublished)
          assert.lengthOf(entriesBeforeAppend, 0)
          assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(fiber)))
          assert.isTrue(Option.isSome(yield* storage.entry(Storage.EntryId.make(2))))
        }),
      ).pipe(Effect.provideService(EventJournal.EventJournal, delayed))
      yield* Effect.scoped(
        Effect.gen(function* () {
          assert.isTrue(Option.isSome(yield* (yield* open).entry(Storage.EntryId.make(2))))
        }),
      )
    }),
  ).pipe(Effect.provide(EventJournal.layerMemory)),
)

for (const persisted of [false, true])
  it.effect(`interruption cannot bypass uncertain-write sealing (persisted=${persisted})`, () =>
    Effect.gen(function* () {
      const journal = yield* EventJournal.EventJournal
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let fail = true
      const injected: EventJournal.EventJournal['Service'] = {
        ...journal,
        write: (options) =>
          Effect.suspend(() => {
            if (!fail) return journal.write(options)
            fail = false
            return Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(persisted ? journal.write(options) : Effect.void),
              Effect.andThen(
                Effect.fail(
                  new EventJournal.EventJournalError({
                    method: 'write',
                    cause: 'lost acknowledgement',
                  }),
                ),
              ),
            )
          }),
      }
      yield* Effect.scoped(
        Effect.gen(function* () {
          const storage = yield* open
          const fiber = yield* Effect.forkChild(storage.mintId())
          yield* Deferred.await(started)
          const interrupt = yield* Effect.forkChild(Fiber.interrupt(fiber), {
            startImmediately: true,
          })
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(interrupt)
          assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(fiber)))
          assert.strictEqual((yield* storage.mintId().pipe(Effect.flip)).reason, 'uncertain')
          assert.strictEqual((yield* storage.commit([]).pipe(Effect.flip)).reason, 'uncertain')
          assert.strictEqual(
            (yield* storage.conversation(root).pipe(Effect.flip)).reason,
            'uncertain',
          )
          assert.lengthOf(yield* journal.entries, persisted ? 1 : 0)
        }),
      ).pipe(Effect.provideService(EventJournal.EventJournal, injected))
      yield* Effect.scoped(
        Effect.gen(function* () {
          const storage = yield* open
          assert.strictEqual(yield* storage.mintId(), persisted ? 3 : 2)
          assert.strictEqual(yield* storage.commit([rootWrite]), 1)
        }),
      )
    }).pipe(Effect.provide(EventJournal.layerMemory)),
  )

for (const [name, values, reason] of [
  ['future protocol', [{ ...frame, version: 2, rows: [{}] }], 'unsupportedFormat'],
  [
    'future reader',
    [{ ...frame, metadata: { ...metadata, format: 13, minimumReader: 13 }, rows: [{}] }],
    'unsupportedFormat',
  ],
  [
    'mismatched reader',
    [{ ...frame, metadata: { ...metadata, format: 3, minimumReader: 2 } }],
    'corrupt',
  ],
  ['missing first revision', [{ ...frame, revision: 2 }], 'corrupt'],
  ['duplicate revision', [frame, frame], 'corrupt'],
  ['invalid predecessor', [{ ...frame, previous: 'absent' }], 'corrupt'],
  ['invalid row', [{ ...frame, rows: [{ _tag: 'unknown' }] }], 'corrupt'],
] as const)
  it.effect(`rejects ${name} on replay without mutating the journal or retaining ownership`, () =>
    Effect.gen(function* () {
      const journal = yield* EventJournal.EventJournal
      for (const value of values) yield* Fixture.append(value)
      const before = yield* journal.entries
      for (let attempt = 0; attempt < 2; attempt++) {
        const failure = yield* Effect.scoped(open).pipe(Effect.flip)
        assert.strictEqual(failure.reason, reason)
      }
      assert.deepEqual(yield* journal.entries, before)
    }).pipe(Effect.provide(EventJournal.layerMemory)),
  )

it.effect(
  'rejects missing middle revisions and metadata downgrades even with valid row payloads',
  () =>
    Effect.gen(function* () {
      const journal = yield* EventJournal.EventJournal
      const first = yield* Fixture.append(frame)
      const second = yield* Fixture.append({
        ...frame,
        revision: 2,
        previous: first,
        metadata: { ...metadata, format: 3, minimumReader: 3 },
      })
      yield* Fixture.append({ ...frame, revision: 3, previous: second })
      assert.strictEqual((yield* Effect.scoped(open).pipe(Effect.flip)).reason, 'corrupt')
      const missing = {
        ...journal,
        entries: journal.entries.pipe(
          Effect.map((entries) => entries.filter((entry) => entry.idString !== second)),
        ),
      }
      assert.strictEqual(
        (yield* Effect.scoped(open).pipe(
          Effect.provideService(EventJournal.EventJournal, missing),
          Effect.flip,
        )).reason,
        'corrupt',
      )
    }).pipe(Effect.provide(EventJournal.layerMemory)),
)

it.effect(
  'scope closure seals admission and retains ownership until an admitted append settles',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const journal = yield* EventJournal.EventJournal
        const started = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const delayed: EventJournal.EventJournal['Service'] = {
          ...journal,
          write: (options) =>
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(journal.write(options)),
            ),
        }
        yield* Effect.gen(function* () {
          const owner = yield* Scope.make()
          const storage = Context.get(
            yield* Layer.buildWithScope(Storage.layerJournal(), owner),
            Storage.Storage,
          )
          const writing = yield* storage.commit([rootWrite, entryWrite(2)]).pipe(Effect.forkChild)
          yield* Deferred.await(started)
          const closing = yield* Scope.close(owner, Exit.void).pipe(Effect.forkChild)
          yield* Effect.yieldNow
          yield* Effect.gen(function* () {
            assert.strictEqual((yield* storage.commit([]).pipe(Effect.flip)).reason, 'closed')
            assert.strictEqual((yield* Effect.scoped(open).pipe(Effect.flip)).reason, 'invalid')
          }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)))
          yield* Fiber.join(writing)
          yield* Fiber.join(closing)
          yield* Effect.scoped(
            Effect.gen(function* () {
              const reopened = yield* open
              assert.isTrue(Option.isSome(yield* reopened.conversation(root)))
              assert.isTrue(Option.isSome(yield* reopened.entry(Storage.EntryId.make(2))))
            }),
          )
        }).pipe(Effect.provideService(EventJournal.EventJournal, delayed))
      }),
    ).pipe(Effect.provide(EventJournal.layerMemory)),
)
