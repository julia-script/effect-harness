import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import { assert, describe, it } from '@effect/vitest'
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as Path from 'effect/Path'
import * as KeyValueStore from 'effect/persistence/KeyValueStore'
import * as EventJournal from 'effect/eventlog/EventJournal'
import * as Record from 'effect-harness/durable/Record'
import * as Store from 'effect-harness/durable/Store'
import * as SnapshotStore from 'effect-harness/durable/storage/SnapshotStore'
import * as Layer from 'effect/Layer'
import { persistence } from './TestStore.ts'

const Database = persistence.pipe(Layer.provide(SqliteClient.layer({ filename: ':memory:' })))
const Memory = Layer.mergeAll(KeyValueStore.layerMemory, EventJournal.layerMemory)
const key = '@effect-harness/durable/session'
const root: Record.Write = {
  _tag: 'conversation',
  type: 'conversation',
  value: { id: Record.ROOT_CONVERSATION_ID },
}

for (const [name, environment] of [
  ['memory', Memory],
  ['SQLite', Database],
] as const)
  describe(`SnapshotStore (${name})`, () => {
    it.effect(
      'reopens coherent state, receipts and retained frames and replays complete results',
      () =>
        Effect.gen(function* () {
          const before = yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* SnapshotStore.make()
              yield* store.commit([root], { key: 'root', fingerprint: 'same-input' })
              yield* store.transact(
                (state) =>
                  Effect.succeed(
                    Store.makeCandidate({
                      state,
                      writes: [],
                      result: { deadline: 1234, handle: 'job' },
                    }),
                  ),
                { key: 'poll' },
              )
              yield* store.transact(
                (state) =>
                  Effect.succeed(Store.makeCandidate({ state, writes: [], result: undefined })),
                { key: 'void' },
              )
              return yield* store.journal(0)
            }),
          )
          const reopened = yield* SnapshotStore.make()
          assert.deepStrictEqual(yield* reopened.journal(0), before)
          assert.strictEqual(
            yield* reopened.commit([root], { key: 'root', fingerprint: 'same-input' }),
            1,
          )
          assert.deepStrictEqual(
            yield* reopened.transact(() => Effect.die('Saved result callback reran'), {
              key: 'poll',
            }),
            { deadline: 1234, handle: 'job' },
          )
          assert.strictEqual(
            yield* reopened.transact(() => Effect.die('Void receipt callback reran'), {
              key: 'void',
            }),
            undefined,
          )
          assert.strictEqual(
            (yield* reopened
              .commit([root], { key: 'root', fingerprint: 'different' })
              .pipe(Effect.flip)).reason._tag,
            'Conflict',
          )
          assert.deepStrictEqual(yield* reopened.journal(0), before)
        }).pipe(Effect.provide(environment)),
    )

    it.effect('serializes independent Store instances without losing concurrent receipts', () =>
      Effect.gen(function* () {
        const first = yield* SnapshotStore.make()
        const second = yield* SnapshotStore.make()
        yield* Effect.forEach(
          Array.from({ length: 20 }, (_, i) => i),
          (index) => (index % 2 === 0 ? first : second).commit([], { key: `receipt-${index}` }),
          { concurrency: 'unbounded', discard: true },
        )
        const state = yield* first.committed
        assert.strictEqual(state.nextSeq, 21)
        assert.strictEqual(new Set(state.receipts.map((receipt) => receipt.key)).size, 20)
        assert.strictEqual((yield* second.journal(0)).frames.length, 20)
      }).pipe(Effect.provide(environment)),
    )

    it.effect('separates sessions by snapshot key', () =>
      Effect.gen(function* () {
        const first = yield* SnapshotStore.make({ key: 'first' })
        const second = yield* SnapshotStore.make({ key: 'second' })
        yield* first.commit([root], { key: 'receipt' })
        assert.deepStrictEqual(yield* second.committed, Record.emptyState())
        assert.strictEqual((yield* first.committed).receipts[0]?.key, 'receipt')
      }).pipe(Effect.provide(environment)),
    )

    it.effect('rejects a future snapshot version without overwriting it', () =>
      Effect.gen(function* () {
        const values = yield* KeyValueStore.KeyValueStore
        const future = '{"version":2,"state":{},"frames":[]}'
        yield* values.set('future', future)
        assert.strictEqual(
          (yield* SnapshotStore.make({ key: 'future' }).pipe(Effect.flip)).reason._tag,
          'Corrupt',
        )
        assert.strictEqual(yield* values.get('future'), future)
      }).pipe(Effect.provide(environment)),
    )

    it.effect('rejects invalid journal sequences and missing saved snapshots', () =>
      Effect.gen(function* () {
        const values = yield* KeyValueStore.KeyValueStore
        const store = yield* SnapshotStore.make()
        yield* KeyValueStore.toSchemaStore(values, SnapshotStore.Snapshot).set(key, {
          version: 1,
          state: Record.emptyState(),
          frames: [{ seq: Record.Seq.make(1), writes: [], documents: [] }],
        })
        assert.strictEqual((yield* store.read.pipe(Effect.flip)).reason._tag, 'Corrupt')
        yield* values.remove(key)
        assert.strictEqual((yield* store.read.pipe(Effect.flip)).reason._tag, 'Corrupt')
      }).pipe(Effect.provide(environment)),
    )

    it.effect('preserves domain failures and defects without publishing a candidate', () =>
      Effect.gen(function* () {
        const store = yield* SnapshotStore.make()
        assert.strictEqual(
          yield* store
            .transact<never, string, never>(() => Effect.fail('domain failure'))
            .pipe(Effect.flip),
          'domain failure',
        )
        const defect = new Error('domain defect')
        const exit = yield* store
          .transact<never, never, never>(() => Effect.die(defect))
          .pipe(Effect.exit)
        assert.isTrue(Exit.isFailure(exit))
        if (Exit.isFailure(exit)) assert.deepStrictEqual(Cause.squash(exit.cause), defect)
        assert.deepStrictEqual(yield* store.committed, Record.emptyState())
      }).pipe(Effect.provide(environment)),
    )

    it.effect('reconciles a key/value write that succeeded before its acknowledgement failed', () =>
      Effect.gen(function* () {
        const values = yield* KeyValueStore.KeyValueStore
        let loseAcknowledgement = false
        const wrapped = KeyValueStore.make({
          ...values,
          set: (key, value) =>
            values.set(key, value).pipe(
              Effect.filterOrElse(
                () => !loseAcknowledgement,
                () =>
                  Effect.fail(
                    new KeyValueStore.KeyValueStoreError({
                      method: 'set',
                      key,
                      message: 'lost acknowledgement',
                    }),
                  ),
              ),
            ),
        })
        const store = yield* SnapshotStore.make().pipe(
          Effect.provideService(KeyValueStore.KeyValueStore, wrapped),
        )
        loseAcknowledgement = true
        assert.strictEqual(
          (yield* store.commit([root], { key: 'saved-before-error' }).pipe(Effect.flip)).certainty,
          'uncertain',
        )
        assert.strictEqual((yield* store.read.pipe(Effect.flip)).reason._tag, 'Poisoned')
        const reopened = yield* SnapshotStore.make()
        assert.strictEqual(yield* reopened.commit([root], { key: 'saved-before-error' }), 1)
        assert.strictEqual((yield* reopened.committed).nextSeq, 2)
      }).pipe(Effect.provide(environment)),
    )

    it.effect(
      'poisons a lost coordinator acknowledgement and reconciles the receipt on reopen',
      () =>
        Effect.gen(function* () {
          const journal = yield* EventJournal.EventJournal
          let loseAcknowledgement = false
          const uncertainJournal = EventJournal.EventJournal.of({
            ...journal,
            withLock: (id) => (effect) =>
              journal
                .withLock(id)(effect)
                .pipe(
                  Effect.filterOrElse(
                    () => !loseAcknowledgement,
                    () => Effect.die('lost acknowledgement'),
                  ),
                ),
          })
          const store = yield* SnapshotStore.make().pipe(
            Effect.provideService(EventJournal.EventJournal, uncertainJournal),
          )
          loseAcknowledgement = true
          const error = yield* store.commit([root], { key: 'uncertain' }).pipe(Effect.flip)
          assert.strictEqual(error.certainty, 'uncertain')
          assert.strictEqual((yield* store.read.pipe(Effect.flip)).reason._tag, 'Poisoned')
          const reopened = yield* SnapshotStore.make()
          assert.strictEqual(yield* reopened.commit([root], { key: 'uncertain' }), 1)
          assert.strictEqual((yield* reopened.committed).receipts[0]?.key, 'uncertain')
        }).pipe(Effect.provide(environment)),
    )
  })

it.live('serializes snapshot commits from independent SQLite clients', () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const directory = yield* fs.makeTempDirectoryScoped()
    const filename = path.join(directory, 'state.sqlite')
    const firstContext = yield* Layer.build(
      persistence.pipe(Layer.provide(SqliteClient.layer({ filename }))),
    )
    const secondContext = yield* Layer.build(
      persistence.pipe(Layer.provide(SqliteClient.layer({ filename }))),
    )
    const first = yield* SnapshotStore.make().pipe(Effect.provideContext(firstContext))
    const second = yield* SnapshotStore.make().pipe(Effect.provideContext(secondContext))
    yield* Effect.forEach(
      Array.from({ length: 12 }, (_, i) => i),
      (i) => (i % 2 === 0 ? first : second).commit([], { key: `client-${i}` }),
      { concurrency: 'unbounded', discard: true },
    )
    assert.strictEqual((yield* first.committed).receipts.length, 12)
    assert.strictEqual((yield* second.committed).nextSeq, 13)
  }).pipe(Effect.provide(NodeServices.layer)),
)
