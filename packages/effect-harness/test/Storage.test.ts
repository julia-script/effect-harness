import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import type * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Domain from 'effect-harness/Record'
import * as Storage from 'effect-harness/Storage'

const root = Domain.ConversationId.make(1)
const entryId = (id: number) => Domain.EntryId.make(id)
const taskId = (id: number) => Domain.TaskId.make(id)
const documentId = (id: number) => Domain.DocumentId.make(id)
const task = (id: number, status: 'pending' | 'terminal' = 'pending'): Domain.Task => ({
  id: taskId(id),
  conversationId: root,
  kind: 'test',
  version: 1,
  input: null,
  background: false,
  abortRequested: false,
  state:
    status === 'pending'
      ? { status, checkpoint: { phase: 'initial' } }
      : { status, outcome: { status: 'completed', result: null } },
})
const document = (id: number, conversationId = root): Domain.DocumentCreate => ({
  id: documentId(id),
  kind: 'counter',
  scope: { _tag: 'conversation', conversationId },
  history: 'rewindable',
  fork: 'asOf',
})
const entry = (id: number, conversationId = root): Domain.Entry => ({
  id: entryId(id),
  conversationId,
  kind: 'user',
  data: { text: `entry ${id}` },
})
type Backend = 'memory' | 'sqlite' | 'jsonl'
const layerFor = (backend: Backend, filename: string) => {
  switch (backend) {
    case 'memory':
      return Storage.layerMemory
    case 'sqlite':
      return Storage.layerJournal().pipe(
        Layer.provide(SqlEventJournal.layer()),
        Layer.provide(SqliteClient.layer({ filename })),
      )
    case 'jsonl':
      return Storage.layerJsonl({ filePath: filename })
  }
}
const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'harness-storage-' })
  return { fs, filename: path.join(directory, 'storage') }
})
const withBackend = <A, E>(
  backend: Backend,
  program: Effect.Effect<A, E, Storage.Storage | Scope.Scope>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const { filename } = yield* fixture
      return yield* program.pipe(Effect.provide(layerFor(backend, filename)))
    }),
  ).pipe(Effect.provide(NodeServices.layer))
const expectSome = <A>(value: Option.Option<A>): A => {
  assert.isTrue(Option.isSome(value))
  return Option.getOrThrow(value)
}

for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  describe(`storage: ${backend}`, () => {
    it.live(
      'commits atomically and rejects identity conflicts without advancing the sequence',
      () =>
        withBackend(
          backend,
          Effect.gen(function* () {
            const storage = yield* Storage.Storage
            const failed = yield* storage
              .commit([
                { _tag: 'conversation', value: { id: root } },
                { _tag: 'entry', value: entry(2) },
                { _tag: 'task', value: task(2) },
              ])
              .pipe(Effect.flip)
            assert.strictEqual(failed.reason, 'conflict')
            assert.isTrue(Option.isNone(yield* storage.conversation(root)))
            const sequence = yield* storage.commit([{ _tag: 'conversation', value: { id: root } }])
            assert.strictEqual(sequence, 1)
            assert.strictEqual(yield* storage.mintId<Domain.EntryId>(), 2)
            const duplicate = yield* storage
              .commit([{ _tag: 'conversation', value: { id: root } }])
              .pipe(Effect.flip)
            assert.strictEqual(duplicate.reason, 'conflict')
            assert.strictEqual(yield* storage.commit([]), 2)
          }),
        ),
    )
    it.live('reads fork-aware history and head markers in either order', () =>
      withBackend(
        backend,
        Effect.gen(function* () {
          const storage = yield* Storage.Storage
          yield* storage.commit([
            { _tag: 'conversation', value: { id: root } },
            { _tag: 'entry', value: entry(2) },
            { _tag: 'entry', value: { ...entry(4), head: entryId(2) } },
            { _tag: 'entry', value: entry(6) },
          ])
          const child = Domain.ConversationId.make(8)
          yield* storage.commit([
            {
              _tag: 'conversation',
              value: { id: child, parent: { conversationId: root, at: entryId(4) } },
            },
            { _tag: 'entry', value: { ...entry(9, child), head: entryId(9) } },
          ])
          const descending = yield* storage
            .scanEntries({ conversationId: child })
            .pipe(Stream.runCollect)
          assert.deepEqual(
            descending.map((entry) => entry.id),
            [9, 4, 2],
          )
          const ascending = yield* storage
            .scanEntries({ conversationId: child, order: 'ascending' })
            .pipe(Stream.runCollect)
          assert.deepEqual(
            ascending.map((entry) => entry.id),
            [2, 4, 9],
          )
          assert.isTrue(Option.isNone(yield* storage.entry(entryId(6), { conversationId: child })))
          assert.strictEqual(
            expectSome(yield* storage.entry(entryId(4), { conversationId: child })).commitSeq,
            1,
          )
          assert.strictEqual(expectSome(yield* storage.findLatestHeadMarker(child)).id, 9)
          assert.strictEqual(
            expectSome(yield* storage.findLatestHeadMarker(child, entryId(4))).head,
            2,
          )
          assert.lengthOf(
            yield* storage.scanConversations({ order: 'descending' }).pipe(Stream.runCollect),
            2,
          )
        }),
      ),
    )
    it.live('keeps an entry scan cutoff across pages while task scans see current state', () =>
      withBackend(
        backend,
        Effect.gen(function* () {
          const storage = yield* Storage.Storage
          yield* storage.commit([
            { _tag: 'conversation', value: { id: root } },
            ...Array.from({ length: 70 }, (_, i): Storage.StorageWrite => ({
              _tag: 'entry',
              value: entry(i + 2),
            })),
            { _tag: 'task', value: task(80) },
          ])
          const pull = yield* storage
            .scanEntries({ conversationId: root, order: 'ascending' })
            .pipe(Stream.rechunk(64), Stream.toPull)
          assert.lengthOf(yield* pull, 64)
          yield* storage.commit([
            { _tag: 'entry', value: entry(81) },
            { _tag: 'task', value: task(80, 'terminal') },
          ])
          const remainder = yield* pull
          assert.lengthOf(remainder, 6)
          assert.isFalse(remainder.some((entry) => entry.id === 81))
          assert.lengthOf(
            yield* storage.scanTasks({ status: 'pending' }).pipe(Stream.runCollect),
            0,
          )
          assert.lengthOf(
            yield* storage.scanTasks({ status: 'terminal' }).pipe(Stream.runCollect),
            1,
          )
          assert.strictEqual(expectSome(yield* storage.task(taskId(80))).state.status, 'terminal')
        }),
      ),
    )
    it.live('rejects malformed document deltas atomically and detaches returned values', () =>
      withBackend(
        backend,
        Effect.gen(function* () {
          const storage = yield* Storage.Storage
          const value = { count: 0 }
          yield* storage.commit([
            { _tag: 'conversation', value: { id: root } },
            {
              _tag: 'document.create',
              record: document(2),
              content: { _tag: 'base', version: 1, value },
            },
          ])
          value.count = 99
          const failed = yield* storage
            .commit([
              { _tag: 'entry', value: entry(3) },
              {
                _tag: 'document.change',
                id: documentId(2),
                content: { _tag: 'delta', version: 1, ops: [['set', ['absent', 'count'], 1]] },
              },
            ])
            .pipe(Effect.flip)
          assert.isTrue(failed.reason === 'corrupt' || failed.reason === 'invalid')
          assert.isTrue(Option.isNone(yield* storage.entry(entryId(3))))
          const stored = expectSome(yield* storage.document(documentId(2)))
          Reflect.set(stored.value, 'count', 100)
          assert.deepEqual(expectSome(yield* storage.document(documentId(2))).value, { count: 0 })
          assert.strictEqual(yield* storage.commit([]), 2)
          yield* storage.commit([{ _tag: 'entry', value: entry(3) }])
          const returned = expectSome(yield* storage.entry(entryId(3)))
          if (
            returned.entry.data !== undefined &&
            typeof returned.entry.data === 'object' &&
            returned.entry.data !== null &&
            !Array.isArray(returned.entry.data)
          )
            Reflect.set(returned.entry.data, 'text', 'mutated')
          assert.deepEqual(expectSome(yield* storage.entry(entryId(3))).entry.data, {
            text: 'entry 3',
          })
        }),
      ),
    )
  })

it.live('recovers an interrupted JSONL tail and rejects malformed completed frames', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { fs, filename } = yield* fixture
      yield* Effect.gen(function* () {
        const storage = yield* Storage.Storage
        yield* storage.commit([{ _tag: 'conversation', value: { id: root } }])
      }).pipe(Effect.provide(Storage.layerJsonl({ filePath: filename })))
      yield* fs.writeFileString(filename, '{"metadata":', { flag: 'a' })
      yield* Effect.gen(function* () {
        const storage = yield* Storage.Storage
        assert.isTrue(Option.isSome(yield* storage.conversation(root)))
        assert.strictEqual(yield* storage.commit([]), 2)
      }).pipe(Effect.provide(Storage.layerJsonl({ filePath: filename })))
      assert.isTrue((yield* fs.readFileString(filename)).endsWith('\n'))
      yield* fs.writeFileString(filename, 'not json\n', { flag: 'a' })
      const error = yield* Effect.service(Storage.Storage).pipe(
        Effect.provide(Storage.layerJsonl({ filePath: filename })),
        Effect.flip,
      )
      assert.strictEqual(error.reason, 'corrupt')
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)

for (const frames of ['single', 'multiple'] as const)
  it.live(
    `discards valid JSON without a newline after ${frames} frames and recovers later appends`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { fs, filename } = yield* fixture
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            yield* storage.commit([{ _tag: 'conversation', value: { id: root } }])
            if (frames === 'multiple') yield* storage.commit([{ _tag: 'entry', value: entry(2) }])
          }).pipe(Effect.provide(Storage.layerJsonl({ filePath: filename })))
          const original = yield* fs.readFile(filename)
          yield* fs.writeFile(filename, original.subarray(0, original.length - 1))
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            const conversation = yield* storage.conversation(root)
            if (frames === 'single') {
              assert.isTrue(Option.isNone(conversation))
              assert.strictEqual((yield* fs.readFile(filename)).length, 0)
              assert.strictEqual(
                yield* storage.commit([{ _tag: 'conversation', value: { id: root } }]),
                1,
              )
            } else assert.isTrue(Option.isSome(conversation))
            assert.isTrue(Option.isNone(yield* storage.entry(entryId(2))))
            const id = yield* storage.mintId<Domain.EntryId>()
            assert.strictEqual(id, 2)
            assert.strictEqual(yield* storage.commit([{ _tag: 'entry', value: entry(id) }]), 2)
          }).pipe(Effect.provide(Storage.layerJsonl({ filePath: filename })))
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            assert.isTrue(Option.isSome(yield* storage.conversation(root)))
            assert.strictEqual(expectSome(yield* storage.entry(entryId(2))).commitSeq, 2)
            assert.strictEqual(yield* storage.mintId<Domain.EntryId>(), 3)
          }).pipe(Effect.provide(Storage.layerJsonl({ filePath: filename })))
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )

it.live(
  'discards unterminated schema-invalid or UTF-8 tails and rejects terminated corruption',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { fs, filename } = yield* fixture
        yield* fs.writeFileString(filename, '{"metadata":{},"rows":[]}')
        yield* Effect.service(Storage.Storage).pipe(
          Effect.flatMap((storage) => storage.conversation(root)),
          Effect.map((conversation) => assert.isTrue(Option.isNone(conversation))),
          Effect.provide(Storage.layerJsonl({ filePath: filename })),
        )
        assert.strictEqual((yield* fs.readFile(filename)).length, 0)
        yield* fs.writeFileString(filename, '{"metadata":{},"rows":[]}\n')
        const invalid = yield* Effect.service(Storage.Storage).pipe(
          Effect.provide(Storage.layerJsonl({ filePath: filename })),
          Effect.flip,
        )
        assert.strictEqual(invalid.reason, 'corrupt')
        yield* fs.writeFileString(filename, '')
        yield* Effect.service(Storage.Storage).pipe(
          Effect.flatMap((storage) =>
            storage.commit([{ _tag: 'conversation', value: { id: root } }]),
          ),
          Effect.provide(Storage.layerJsonl({ filePath: filename })),
        )
        const complete = yield* fs.readFile(filename)
        yield* fs.writeFile(filename, new Uint8Array([0xe2, 0x82]), { flag: 'a' })
        yield* Effect.service(Storage.Storage).pipe(
          Effect.flatMap((storage) => storage.conversation(root)),
          Effect.map((conversation) => assert.isTrue(Option.isSome(conversation))),
          Effect.provide(Storage.layerJsonl({ filePath: filename })),
        )
        assert.deepEqual(yield* fs.readFile(filename), complete)
        // Invalid UTF-8 inside a newline-terminated frame is corruption, not an unfinished tail.
        yield* fs.writeFile(filename, new Uint8Array([0xff, 10]), { flag: 'a' })
        const corrupt = yield* Effect.service(Storage.Storage).pipe(
          Effect.provide(Storage.layerJsonl({ filePath: filename })),
          Effect.flip,
        )
        assert.strictEqual(corrupt.reason, 'corrupt')
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
)

it.live('poisons a JSONL writer after a partial append and recovers on reopen', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { fs, filename } = yield* fixture
      yield* Effect.service(Storage.Storage).pipe(
        Effect.flatMap((storage) =>
          storage.commit([{ _tag: 'conversation', value: { id: root } }]),
        ),
        Effect.provide(Storage.layerJsonl({ filePath: filename })),
      )
      const fault = yield* fs.readFile(filename + '.missing').pipe(Effect.flip)
      const failingFs: FileSystem.FileSystem = {
        ...fs,
        open: (path, options) =>
          fs.open(path, options).pipe(
            Effect.map((file) => ({
              ...file,
              sync: file.sync,
              writeAll: (bytes) =>
                file.writeAll(bytes.subarray(0, 12)).pipe(Effect.andThen(Effect.fail(fault))),
            })),
          ),
      }
      yield* Effect.gen(function* () {
        const storage = yield* Storage.Storage
        const failed = yield* storage.commit([{ _tag: 'entry', value: entry(2) }]).pipe(Effect.flip)
        assert.strictEqual(failed.reason, 'uncertain')
        const length = (yield* fs.readFile(filename)).length
        assert.strictEqual((yield* storage.commit([]).pipe(Effect.flip)).reason, 'uncertain')
        assert.strictEqual((yield* fs.readFile(filename)).length, length)
      }).pipe(
        Effect.provide(Storage.layerJsonl({ filePath: filename })),
        Effect.provideService(FileSystem.FileSystem, failingFs),
      )
      yield* Effect.gen(function* () {
        const storage = yield* Storage.Storage
        assert.isTrue(Option.isSome(yield* storage.conversation(root)))
        assert.isTrue(Option.isNone(yield* storage.entry(entryId(2))))
        assert.strictEqual(yield* storage.commit([{ _tag: 'entry', value: entry(2) }]), 2)
      }).pipe(Effect.provide(Storage.layerJsonl({ filePath: filename })))
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)
