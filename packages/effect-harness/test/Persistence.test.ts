import { assert, describe, it } from '@effect/vitest'
import { NodeFileSystem } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Persistence from '../src/Persistence.ts'
import * as Record from '../src/Record.ts'
import * as Document from '../src/Document.ts'
import * as Session from '../src/internal/Session.ts'
import * as Memory from '../src/storage/Memory.ts'
import * as SqliteNode from '../src/storage/SqliteNode.ts'
import * as JsonlNode from '../src/storage/JsonlNode.ts'
import * as Identity from '../src/Identity.ts'
import { uncertain, type StorageError } from '../src/StorageError.ts'
import * as Records from '../src/storage/internal/records.ts'

const id = Schema.decodeSync(Record.ConversationId)(1)
const entryId = Schema.decodeSync(Record.EntryId)(2)
const entryFour = Schema.decodeSync(Record.EntryId)(4)
const conversationTwo = Schema.decodeSync(Record.ConversationId)(2)
const taskId = Schema.decodeSync(Record.TaskId)(3)
const Counter = Document.defineUnsafe({
  kind: 'counter',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  schema: Schema.Struct({ count: Schema.Finite }),
  initial: () => ({ count: 0 }),
})
const CurrentCounter = Document.defineUnsafe({
  kind: 'current-counter',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'current',
  schema: Schema.Struct({ count: Schema.Finite }),
  initial: () => ({ count: 0 }),
})
const InitialCounter = Document.defineUnsafe({
  kind: 'initial-counter',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: Schema.Struct({ count: Schema.Finite }),
  initial: () => ({ count: 0 }),
})
const makeTask = (id: Record.TaskId): Record.Task => ({
  id,
  conversationId: Schema.decodeSync(Record.ConversationId)(1),
  kind: 'test',
  version: 1,
  input: null,
  background: false,
  abortRequested: false,
  state: { status: 'pending', checkpoint: { phase: 'initial' } },
})
const layers = Session.layer.pipe(Layer.provideMerge(Memory.layer))

describe('record persistence', () => {
  it.effect(
    'rejects an invalid batch without exposing partial records or advancing allocation',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* Persistence.Persistence
          const result = yield* store
            .commit([
              { _tag: 'conversation', value: { id } },
              { _tag: 'entry', value: { id: entryId, conversationId: id, kind: 'user' } },
              { _tag: 'conversation', value: { id: conversationTwo } },
            ])
            .pipe(Effect.flip)
          assert.strictEqual(result.certainty, 'rejected')
          assert.isTrue(Option.isNone(yield* store.conversation(id)))
          assert.deepStrictEqual(yield* store.metadata, { nextId: 2, revision: 0 })
        }).pipe(Effect.provide(Memory.layer)),
      ),
  )

  it.effect(
    'captures a bounded historical scan cutoff while task discovery sees current records',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* Persistence.Persistence
          yield* store.commit([
            { _tag: 'conversation', value: { id } },
            { _tag: 'entry', value: { id: entryId, conversationId: id, kind: 'user' } },
            { _tag: 'task', value: makeTask(taskId) },
          ])
          const pull = yield* store.scanEntries({ conversationId: id }).pipe(Stream.toPull)
          const first = yield* pull
          yield* store.commit([
            { _tag: 'entry', value: { id: entryFour, conversationId: id, kind: 'assistant' } },
          ])
          assert.strictEqual(first[0]?.id, entryId)
          const current = yield* store.scanEntries({ conversationId: id }).pipe(Stream.runCollect)
          assert.lengthOf(current, 2)
          yield* store.commit([
            {
              _tag: 'task',
              value: {
                ...makeTask(taskId),
                state: { status: 'terminal', outcome: { status: 'completed', result: null } },
              },
            },
          ])
          assert.lengthOf(yield* store.scanTasks({ status: 'pending' }).pipe(Stream.runCollect), 0)
        }).pipe(Effect.provide(Memory.layer)),
      ),
  )

  it.effect('keeps later appends outside a historical scan across backend pages', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const store = yield* Persistence.Persistence
        const entries = yield* Effect.forEach(
          Array.from({ length: 70 }, (_, index) => index + 2),
          Effect.fn(function* (value) {
            return {
              id: yield* Schema.decodeEffect(Record.EntryId)(value),
              conversationId: id,
              kind: 'user',
            }
          }),
        )
        yield* store.commit([
          { _tag: 'conversation', value: { id } },
          ...entries.map((value): Record.Write => ({ _tag: 'entry', value })),
        ])
        const pull = yield* store.scanEntries({ conversationId: id }).pipe(Stream.toPull)
        assert.lengthOf(yield* pull, 64)
        const appended = yield* Schema.decodeEffect(Record.EntryId)(72)
        yield* store.commit([
          { _tag: 'entry', value: { id: appended, conversationId: id, kind: 'assistant' } },
        ])
        const next = yield* pull
        assert.lengthOf(next, 6)
        assert.isFalse(next.some((entry) => entry.id === appended))
        assert.lengthOf(
          yield* store.scanEntries({ conversationId: id }).pipe(Stream.runCollect),
          71,
        )
      }).pipe(Effect.provide(Memory.layer)),
    ),
  )

  it.effect('commits task outcomes and document edits together and revokes drafts', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* Session.Session
        yield* session.root
        let escaped: Document.Document.Draft<{ readonly count: number }> | undefined
        const created = yield* session.transaction(
          Effect.fn(function* (tx) {
            escaped = yield* tx.doc(Counter, { owner: id })
            escaped.count = 2
            const { id: _id, ...input } = makeTask(taskId)
            return yield* tx.createTask(input)
          }),
        )
        yield* session.transaction(
          Effect.fn(function* (tx) {
            const counter = yield* tx.doc(Counter, { owner: id })
            counter.count = 3
            const task = Option.getOrThrow(yield* tx.task(created))
            yield* tx.write({
              _tag: 'task',
              value: {
                ...task,
                state: {
                  status: 'terminal',
                  outcome: { status: 'completed', result: { count: 3 } },
                },
              },
            })
          }),
        )
        assert.strictEqual(
          Option.getOrThrow(yield* session.snapshot(Counter, { owner: id })).value.count,
          3,
        )
        assert.strictEqual(Option.getOrThrow(yield* session.task(created)).state.status, 'terminal')
        assert.throws(() => escaped!.count, /revoked/)
      }).pipe(Effect.provide(layers)),
    ),
  )

  it.effect('burns committed unused identities instead of reusing them', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* Session.Session
        yield* session.root
        const burned = yield* session.transaction((tx) => tx.mint(Record.EntryId))
        const failed = yield* session
          .transaction((tx) =>
            tx.write({ _tag: 'entry', value: { id: burned, conversationId: id, kind: 'user' } }),
          )
          .pipe(Effect.flip)
        assert.strictEqual(failed.reason._tag, 'ConflictError')
        const next = yield* session.transaction((tx) => tx.appendEntry(id, { kind: 'user' }))
        assert.isTrue(next.id > burned)
      }).pipe(Effect.provide(layers)),
    ),
  )

  it.effect('reads staged children before committing the parent wait', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* Session.Session
        yield* session.root
        yield* session.transaction(
          Effect.fn(function* (tx) {
            const { id: _id, ...input } = makeTask(taskId)
            const created = yield* tx.createTask(input)
            assert.isTrue(Option.isSome(yield* tx.task(created)))
            assert.lengthOf(yield* tx.scanTasks({ status: 'pending' }).pipe(Stream.runCollect), 1)
          }),
        )
      }).pipe(Effect.provide(layers)),
    ),
  )

  it.effect('acquires an observer with its coherent snapshot before future commits', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* Session.Session
        yield* session.root
        const observed = yield* session.observe(session.metadata)
        yield* session.transaction((tx) => tx.appendEntry(id, { kind: 'user' }))
        const frame = yield* observed.frames.pipe(Stream.runHead)
        assert.strictEqual(Option.getOrThrow(frame).seq, observed.revision + 1)
        assert.deepStrictEqual(observed.snapshot, { revision: 1, nextId: 2 })
      }).pipe(Effect.provide(layers)),
    ),
  )

  it.effect('reopens SQLite records without requiring the previous Session', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const folder = yield* fs.makeTempDirectoryScoped()
        const filename = `${folder}/harness.sqlite`
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Persistence.Persistence
            yield* store.commit(
              [
                { _tag: 'conversation', value: { id } },
                { _tag: 'entry', value: { id: entryId, conversationId: id, kind: 'user' } },
                { _tag: 'task', value: makeTask(taskId) },
              ],
              8,
            )
          }).pipe(Effect.provide(SqliteNode.layer({ filename }))),
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Persistence.Persistence
            assert.deepStrictEqual(yield* store.metadata, { revision: 1, nextId: 8 })
            assert.strictEqual(Option.getOrThrow(yield* store.task(taskId)).kind, 'test')
            assert.strictEqual(Option.getOrThrow(yield* store.entryRecord(entryId)).commitSeq, 1)
            assert.lengthOf(
              yield* store.scanEntries({ conversationId: id }).pipe(Stream.runCollect),
              1,
            )
          }).pipe(Effect.provide(SqliteNode.layer({ filename }))),
        )
      }).pipe(Effect.provide(NodeFileSystem.layer)),
    ),
  )

  it.effect('forks historical documents and lets creation hooks acquire inherited copies', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* Session.Session
        yield* session.root
        const cutoff = yield* session.transaction(
          Effect.fn(function* (tx) {
            const counter = yield* tx.doc(Counter, { owner: id })
            counter.count = 2
            return yield* tx.appendEntry(id, { kind: 'user' })
          }),
        )
        yield* session.transaction(
          Effect.fn(function* (tx) {
            const counter = yield* tx.doc(Counter, { owner: id })
            counter.count = 9
            yield* tx.appendEntry(id, { kind: 'assistant' })
          }),
        )
        const fork = yield* session.transaction((tx) =>
          tx.forkConversation(id, cutoff.id, { ownership: Session.Ownership.none() }),
        )
        assert.strictEqual(
          Option.getOrThrow(yield* session.snapshot(Counter, { owner: fork.id })).value.count,
          2,
        )
        assert.strictEqual(
          Option.getOrThrow(yield* session.snapshotAsOf(Counter, cutoff.id, { owner: id })).value
            .count,
          2,
        )
        assert.lengthOf(
          yield* session.scanEntries({ conversationId: fork.id }).pipe(Stream.runCollect),
          1,
        )
      }).pipe(
        Effect.provide(
          layers.pipe(
            Layer.provide(
              Layer.succeed(Session.CreationHook, {
                run: Effect.fn(function* (
                  tx: Session.Transaction,
                  conversation: Record.Conversation,
                ) {
                  yield* tx.doc(Counter, { owner: conversation.id })
                }),
              }),
            ),
          ),
        ),
      ),
    ),
  )

  it.effect('retires and recreates a document address with a fresh incarnation', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* Session.Session
        yield* session.root
        yield* session.transaction(
          Effect.fn(function* (tx) {
            const value = yield* tx.doc(Counter, { owner: id })
            value.count = 7
          }),
        )
        const original = Option.getOrThrow(yield* session.snapshot(Counter, { owner: id }))
        yield* session.transaction(
          Effect.fn(function* (tx) {
            yield* tx.doc(Counter, { owner: id })
            yield* tx.retire(Counter, { owner: id })
            const replacement = yield* tx.doc(Counter, { owner: id })
            replacement.count = 1
          }),
        )
        const current = Option.getOrThrow(yield* session.snapshot(Counter, { owner: id }))
        assert.notStrictEqual(current.record.id, original.record.id)
        assert.strictEqual(current.value.count, 1)
        assert.isTrue(Option.isNone(yield* session.document(original.record.id)))
      }).pipe(Effect.provide(layers)),
    ),
  )

  it.effect('rejects duplicate admission identities atomically', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* Session.Session
        yield* session.root
        const requestId = yield* Schema.decodeEffect(Identity.RequestId)('same-request')
        const input = {
          _tag: 'InputQueued' as const,
          conversationId: id,
          requestId,
          type: 'input' as const,
          status: 'queued' as const,
        }
        const error = yield* session
          .transaction(
            Effect.fn(function* (tx) {
              yield* tx.createSubmission(input)
              yield* tx.createSubmission(input)
            }),
          )
          .pipe(Effect.flip)
        assert.strictEqual(error.certainty, 'rejected')
        assert.lengthOf(yield* session.scanSubmissions().pipe(Stream.runCollect), 0)
        assert.deepStrictEqual(yield* session.metadata, { revision: 1, nextId: 2 })
      }).pipe(Effect.provide(layers)),
    ),
  )

  it.effect('rejects a second Session owner over the same persistence service', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const store = yield* Persistence.Persistence
        yield* Session.make
        const error = yield* Session.make.pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, 'ConflictError')
        assert.deepStrictEqual(yield* store.metadata, { revision: 0, nextId: 2 })
      }).pipe(Effect.provide(Memory.layer)),
    ),
  )

  it.effect(
    'repairs only an incomplete final JSONL frame and rejects malformed complete frames',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped()
          const filename = `${directory}/commits.jsonl`
          yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* Persistence.Persistence
              yield* store.commit([
                { _tag: 'conversation', value: { id } },
                { _tag: 'task', value: makeTask(taskId) },
              ])
            }).pipe(Effect.provide(JsonlNode.layer({ directory }))),
          )
          yield* fs.writeFileString(filename, '{"format":2,"metadata":', { flag: 'a' })
          yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* Persistence.Persistence
              assert.strictEqual(Option.getOrThrow(yield* store.task(taskId)).kind, 'test')
              assert.deepStrictEqual(yield* store.metadata, { revision: 1, nextId: 4 })
              yield* store.commit([
                {
                  _tag: 'task',
                  value: {
                    ...makeTask(taskId),
                    state: { status: 'terminal', outcome: { status: 'completed', result: null } },
                  },
                },
              ])
            }).pipe(Effect.provide(JsonlNode.layer({ directory }))),
          )
          const content = yield* fs.readFileString(filename)
          assert.strictEqual(content.split('\n').filter(Boolean).length, 2)
          yield* fs.writeFileString(filename, '{"bad":true}\n', { flag: 'a' })
          const error = yield* Effect.scoped(
            Effect.service(Persistence.Persistence).pipe(
              Effect.provide(JsonlNode.layer({ directory })),
            ),
          ).pipe(Effect.flip)
          assert.strictEqual(error.reason._tag, 'CorruptError')
        }).pipe(Effect.provide(NodeFileSystem.layer)),
      ),
  )

  it.effect('enforces the SQLite file owner while an existing scope is open', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const folder = yield* fs.makeTempDirectoryScoped()
        const filename = `${folder}/owned.sqlite`
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Persistence.Persistence
            yield* store.commit([{ _tag: 'conversation', value: { id } }])
            const second = yield* Effect.scoped(
              Effect.service(Persistence.Persistence).pipe(
                Effect.provide(SqliteNode.layer({ filename, busyTimeout: 0 })),
              ),
            ).pipe(Effect.flip)
            assert.strictEqual(second.certainty, 'rejected')
            assert.deepStrictEqual(yield* store.metadata, { revision: 1, nextId: 2 })
          }).pipe(Effect.provide(SqliteNode.layer({ filename, busyTimeout: 0 }))),
        )
      }).pipe(Effect.provide(NodeFileSystem.layer)),
    ),
  )

  it.effect(
    'poisons an uncertain open adapter and reconstructs the authoritative committed revision',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let metadata: Persistence.Metadata = { revision: 0, nextId: 2 }
          const rows = new Map<number, Records.Row>()
          const driver: Records.Driver = {
            metadata: Effect.sync(() => metadata),
            get: (id) => Effect.sync(() => Option.fromUndefinedOr(rows.get(id))),
            page: () => Effect.succeed([]),
            save: (batch, next) =>
              Effect.sync(() => {
                for (const row of batch) rows.set(Records.idOf(row), row)
                metadata = next
              }).pipe(Effect.andThen(Effect.fail(uncertain('lost commit acknowledgement')))),
          }
          const first = yield* Records.make(driver)
          const failed = yield* first
            .commit([{ _tag: 'conversation', value: { id } }])
            .pipe(Effect.flip)
          assert.strictEqual(failed.certainty, 'uncertain')
          assert.isTrue(yield* first.isClosed)
          assert.strictEqual(
            (yield* first.conversation(id).pipe(Effect.flip)).reason._tag,
            'PoisonedError',
          )
          const reopened = yield* Records.make(driver)
          assert.isTrue(Option.isSome(yield* reopened.conversation(id)))
          assert.deepStrictEqual(yield* reopened.metadata, { revision: 1, nextId: 2 })
        }),
      ),
  )

  it.effect(
    'seals the Session after an uncertain commit instead of admitting later mutations',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const memory = yield* Memory.make
          const faulting: Persistence.Service = {
            ...memory,
            commit: (writes, nextId) =>
              memory
                .commit(writes, nextId)
                .pipe(Effect.andThen(Effect.fail(uncertain('lost reply')))),
          }
          const session = yield* Session.make.pipe(
            Effect.provideService(Persistence.Persistence, faulting),
          )
          assert.strictEqual((yield* session.root.pipe(Effect.flip)).certainty, 'uncertain')
          assert.isTrue(yield* session.isClosed)
          assert.strictEqual(
            (yield* session.transaction((tx) => tx.ensureRoot).pipe(Effect.flip)).reason._tag,
            'ClosedError',
          )
          assert.isTrue(Option.isSome(yield* memory.conversation(id)))
        }),
      ),
  )
})

const adapters: ReadonlyArray<
  readonly [string, (directory: string) => Layer.Layer<Persistence.Persistence, StorageError>]
> = [
  ['Memory', () => Memory.layer],
  ['SQLite', (directory) => SqliteNode.layer({ filename: `${directory}/records.sqlite` })],
  ['JSONL', (directory) => JsonlNode.layer({ directory })],
]
for (const [name, adapter] of adapters)
  describe(`${name} document batch conformance`, () => {
    it.effect('keeps historical fork content and atomic rejection consistent', () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped()
          yield* Effect.scoped(
            Effect.gen(function* () {
              const session = yield* Session.Session
              yield* session.root
              const cutoff = yield* session.transaction(
                Effect.fn(function* (tx) {
                  const counter = yield* tx.doc(Counter, { owner: id })
                  counter.count = 2
                  const current = yield* tx.doc(CurrentCounter, { owner: id })
                  current.count = 2
                  const initial = yield* tx.doc(InitialCounter, { owner: id })
                  initial.count = 2
                  return yield* tx.appendEntry(id, { kind: 'user' })
                }),
              )
              const before = yield* session.metadata
              const failed = yield* session
                .transaction(
                  Effect.fn(function* (tx) {
                    const counter = yield* tx.doc(Counter, { owner: id })
                    counter.count = 99
                    yield* tx.write({ _tag: 'conversation', value: { id } })
                  }),
                )
                .pipe(Effect.flip)
              assert.strictEqual(failed.certainty, 'rejected')
              assert.deepStrictEqual(yield* session.metadata, before)
              assert.strictEqual(
                Option.getOrThrow(yield* session.snapshot(Counter, { owner: id })).value.count,
                2,
              )
              yield* session.transaction(
                Effect.fn(function* (tx) {
                  const counter = yield* tx.doc(Counter, { owner: id })
                  counter.count = 3
                  const current = yield* tx.doc(CurrentCounter, { owner: id })
                  current.count = 9
                  const initial = yield* tx.doc(InitialCounter, { owner: id })
                  initial.count = 9
                }),
              )
              const fork = yield* session.transaction((tx) =>
                tx.forkConversation(id, cutoff.id, { ownership: Session.Ownership.none() }),
              )
              assert.strictEqual(
                Option.getOrThrow(yield* session.snapshot(CurrentCounter, { owner: fork.id })).value
                  .count,
                9,
              )
              assert.isTrue(
                Option.isNone(yield* session.snapshot(InitialCounter, { owner: fork.id })),
              )
              const initialCount = yield* session.transaction(
                Effect.fn(function* (tx) {
                  return (yield* tx.doc(InitialCounter, { owner: fork.id })).count
                }),
              )
              assert.strictEqual(initialCount, 0)

              assert.strictEqual(
                Option.getOrThrow(yield* session.snapshot(Counter, { owner: fork.id })).value.count,
                2,
              )
              assert.strictEqual(
                Option.getOrThrow(yield* session.snapshotAsOf(Counter, cutoff.id, { owner: id }))
                  .value.count,
                2,
              )
            }).pipe(Effect.provide(Session.layer.pipe(Layer.provideMerge(adapter(directory))))),
          )
        }).pipe(Effect.provide(NodeFileSystem.layer)),
      ),
    )
  })
