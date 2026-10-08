import * as DirectoryFixture from '../DirectoryFixture.ts'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as Option from 'effect/Option'

import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Document from 'effect-harness/durable/Document'
import * as Record from 'effect-harness/durable/Record'
import * as Session from 'effect-harness/durable/Session'
import { Store } from 'effect-harness/durable/Store'
import * as JsonlStore from 'effect-harness/durable/storage/JsonlStore'
import * as TestStore from './TestStore.ts'
const token = Document.defineUnsafe({
  kind: 'counter',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ count: Schema.Finite.pipe(Schema.mutableKey) }),
  initial: () => ({ count: 0 }),
})
const env = Layer.merge(NodeFileSystem.layer, Path.layer)
const setup = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const directory = yield* DirectoryFixture.make()
  return { fs, directory, file: path.join(directory, 'commits.jsonl') }
})
const fail = <A, E, R>(effect: Effect.Effect<A, E, R>) => effect.pipe(Effect.flip, Effect.orDie)
const commit = (store: Store['Service']) =>
  Effect.gen(function* () {
    const session = yield* Session.make.pipe(Effect.provideService(Store, store))
    yield* session.root()
    return yield* session.transaction(
      Effect.fnUntraced(function* (tx) {
        const d = yield* tx.doc(token)
        d.count = 3
        return { count: d.count }
      }),
      { key: 'once', fingerprint: 'input' },
    )
  })
describe('JsonlStorePersistence', () => {
  describe('JsonlStorePersistence', () => {
    // P8-live-tests-explain-clock: these cases observe actual directory handles and filesystem settlement.
    it.live(
      'flushes directory entries before acknowledgement and after uncertain replacement',
      () =>
        Effect.gen(function* () {
          const { fs, directory, file } = yield* setup
          const operations: Array<string> = []
          const recording = FileSystem.FileSystem.of({
            ...fs,
            open: (target, options) =>
              Effect.sync(() => operations.push(`open:${target}:${options?.flag}`)).pipe(
                Effect.andThen(fs.open(target, options)),
              ),
            rename: (from, to) =>
              fs.rename(from, to).pipe(
                Effect.tap(() => Effect.sync(() => operations.push('renamed'))),
                Effect.andThen(fs.readFile('/missing-jsonl-post-rename-fault')),
                Effect.asVoid,
              ),
          })
          yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* JsonlStore.make({ directory, fsync: true }).pipe(
                Effect.provideService(FileSystem.FileSystem, recording),
              )
              yield* store.commit([
                {
                  _tag: 'conversation',
                  value: { id: Record.ROOT_CONVERSATION_ID },
                },
              ])
              assert.deepStrictEqual(operations, [
                `open:${file}:r+`,
                `open:${directory}:r`,
                `open:${directory}/commits.reclaim:r+`,
                'renamed',
                `open:${directory}:r`,
              ])
            }),
          )
        }).pipe(Effect.provide(env)),
    )
    // P8-live-tests-explain-clock: real directory-open failure injection observes native filesystem settlement.
    it.live('poisons the store when directory durability cannot be established', () =>
      Effect.gen(function* () {
        const { fs, directory } = yield* setup
        const faulty = FileSystem.FileSystem.of({
          ...fs,
          open: (target, options) =>
            target === directory && options?.flag === 'r'
              ? fs.open('/missing-jsonl-directory-sync-fault', options)
              : fs.open(target, options),
        })
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* JsonlStore.make({ directory, fsync: true }).pipe(
              Effect.provideService(FileSystem.FileSystem, faulty),
            )
            const error = yield* store
              .commit([
                {
                  _tag: 'conversation',
                  value: { id: Record.ROOT_CONVERSATION_ID },
                },
              ])
              .pipe(Effect.flip)
            assert.strictEqual(error.certainty, 'uncertain')
            assert.strictEqual((yield* store.read.pipe(Effect.flip)).reason._tag, 'PoisonedError')
          }),
        )
      }).pipe(Effect.provide(env)),
    )
    it.effect('reopens full state and durable receipt after compaction', () =>
      Effect.gen(function* () {
        const { directory } = yield* setup
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* JsonlStore.make({ directory, fsync: true })
            return yield* commit(store)
          }),
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* JsonlStore.make({ directory })
            const session = yield* Session.make.pipe(Effect.provideService(Store, store))
            assert.strictEqual(
              (yield* session.snapshot(token).pipe(Effect.map(Option.getOrUndefined)))?.value.count,
              3,
            )
            assert.deepStrictEqual(
              yield* session.transaction(() => Effect.die('receipt callback ran'), {
                key: 'once',
                fingerprint: 'input',
              }),
              result,
            )
          }),
        )
      }).pipe(Effect.provide(env)),
    )
    it.effect('reopens explicit void receipt results without collapsing them to null', () =>
      Effect.gen(function* () {
        const { directory } = yield* setup
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* JsonlStore.make({ directory })
            const session = yield* Session.make.pipe(Effect.provideService(Store, store))
            assert.strictEqual(
              yield* session.transaction((tx) => tx.ensureRoot.pipe(Effect.asVoid), {
                key: 'void',
              }),
              undefined,
            )
          }),
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* JsonlStore.make({ directory })
            const session = yield* Session.make.pipe(Effect.provideService(Store, store))
            assert.strictEqual(
              yield* session.transaction(() => Effect.die('receipt callback ran'), { key: 'void' }),
              undefined,
            )
            assert.strictEqual((yield* store.read).receipts[0]?.resultIsVoid, true)
          }),
        )
      }).pipe(Effect.provide(env)),
    )
    it.effect('truncates torn utf8 tails but rejects malformed newline-terminated commits', () =>
      Effect.gen(function* () {
        const { fs, directory, file } = yield* setup
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* JsonlStore.make({ directory })
            yield* commit(store)
          }),
        )
        const valid = yield* fs.readFile(file)
        yield* fs.writeFile(file, new Uint8Array([0xe2, 0x82]), { flag: 'a' })
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* JsonlStore.make({ directory })
            assert.strictEqual((yield* store.read).receipts.length, 1)
          }),
        )
        assert.deepStrictEqual(yield* fs.readFile(file), valid)
        yield* fs.writeFileString(file, '{"garbage":true}\n', { flag: 'a' })
        const error = yield* fail(Effect.scoped(JsonlStore.make({ directory })))
        assert.strictEqual(error.reason._tag, 'CorruptError')
      }).pipe(Effect.provide(env)),
    )
    it.effect(
      'poisons after an append with uncertain settlement and recovers durable effects on reopen',
      () =>
        Effect.gen(function* () {
          const { fs, directory } = yield* setup
          let inject = true
          const faulty = FileSystem.FileSystem.of({
            ...fs,
            writeFileString: (file, data, options) => {
              if (inject && options?.flag === 'a') {
                inject = false
                return fs
                  .writeFileString(file, data, options)
                  .pipe(Effect.andThen(fs.readFile('/missing-jsonl-injected-fault')), Effect.asVoid)
              }
              return fs.writeFileString(file, data, options)
            },
          })
          yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* JsonlStore.make({ directory }).pipe(
                Effect.provideService(FileSystem.FileSystem, faulty),
              )
              const error = yield* fail(
                store.commit([
                  {
                    _tag: 'conversation' as const,
                    value: { id: Record.ROOT_CONVERSATION_ID },
                  },
                ]),
              )
              assert.strictEqual(error.certainty, 'uncertain')
              assert.strictEqual((yield* fail(store.read)).reason._tag, 'PoisonedError')
              assert.strictEqual((yield* fail(store.commit([]))).reason._tag, 'PoisonedError')
            }),
          )
          yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* JsonlStore.make({ directory })
              assert.deepStrictEqual((yield* store.read).conversations, [
                { id: Record.ROOT_CONVERSATION_ID },
              ])
            }),
          )
        }).pipe(Effect.provide(env)),
    )
    it.effect('best-effort reclaim errors do not reverse an admitted append', () =>
      Effect.gen(function* () {
        const { fs, directory } = yield* setup
        const faulty = FileSystem.FileSystem.of({
          ...fs,
          rename: () => fs.readFile('/missing-reclaim-injected-fault').pipe(Effect.asVoid),
        })
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* JsonlStore.make({ directory }).pipe(
              Effect.provideService(FileSystem.FileSystem, faulty),
            )
            yield* commit(store)
            assert.strictEqual((yield* store.read).receipts.length, 1)
          }),
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* JsonlStore.make({ directory })
            assert.strictEqual((yield* store.read).receipts.length, 1)
          }),
        )
      }).pipe(Effect.provide(env)),
    )
  })
  describe('SQLite schema and reopen', () => {
    const client = SqliteClient.layer({ filename: ':memory:' })
    it.effect('reopens state, seq and stable receipts without Session cache', () =>
      Effect.gen(function* () {
        const first = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* TestStore.make
            return yield* commit(store)
          }),
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* TestStore.make
            const session = yield* Session.make.pipe(Effect.provideService(Store, store))
            assert.strictEqual(
              (yield* session.snapshot(token).pipe(Effect.map(Option.getOrUndefined)))?.value.count,
              3,
            )
            assert.deepStrictEqual(
              yield* session.transaction(() => Effect.die('receipt callback ran'), {
                key: 'once',
                fingerprint: 'input',
              }),
              first,
            )
          }),
        )
      }).pipe(Effect.provide(client)),
    )
  })
})
