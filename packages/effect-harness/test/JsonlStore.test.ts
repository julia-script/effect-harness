import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Storage from 'effect-harness/Storage'
import * as Store from '../src/internal/Store.js'
import * as Jsonl from '../src/internal/Jsonl.js'

const root: Storage.StorageWrite = {
  _tag: 'conversation',
  value: { id: Storage.ROOT_CONVERSATION_ID },
}
const note: Storage.StorageWrite = {
  _tag: 'entry',
  value: {
    id: Storage.EntryId.make(2),
    conversationId: Storage.ROOT_CONVERSATION_ID,
    kind: 'note',
    data: { text: 'readable inline message' },
  },
}
const fault = new Storage.StorageError({
  reason: 'uncertain',
  operation: 'append',
  message: 'Lost acknowledgement',
})
const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const directory = yield* fs.makeTempDirectoryScoped()
  return { fs, filePath: `${yield* fs.realPath(directory)}/main.jsonl` }
})
const open = (filePath: string) => Jsonl.make({ filePath }).pipe(Effect.flatMap(Store.make))
const Frame = Schema.fromJsonString(
  Schema.Struct({
    format: Schema.Finite,
    type: Schema.String,
    seq: Schema.Finite,
    writes: Schema.Array(Schema.Struct({ type: Schema.String, value: Schema.Unknown })),
  }),
)

it.live(
  'a readable commit stores entries inline; unused IDs cause no writes and may be reused after reopen',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { fs, filePath } = yield* fixture
        yield* Effect.scoped(
          Effect.gen(function* () {
            const storage = yield* open(filePath)
            assert.strictEqual(yield* storage.mintId(), 2)
            assert.strictEqual(yield* storage.mintId(), 3)
            assert.strictEqual(yield* fs.readFileString(filePath), '')
            yield* storage.commit([root, note])
            assert.strictEqual(yield* storage.mintId(), 4)
            const lines = (yield* fs.readFileString(filePath)).trim().split('\n')
            assert.lengthOf(lines, 1)
            const frame = yield* Schema.decodeEffect(Frame)(lines[0]!)
            assert.deepEqual(
              frame.writes.map((write) => write.type),
              ['conversation', 'entry'],
            )
            assert.deepEqual(frame.writes[1]?.value, note.value)
            assert.deepEqual(yield* fs.readDirectory(`${filePath}.parts`), [])
          }),
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            const storage = yield* open(filePath)
            assert.strictEqual(yield* storage.mintId(), 4)
            assert.isTrue(Option.isSome(yield* storage.entry(Storage.EntryId.make(2), undefined)))
          }),
        )
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
)

for (const persisted of [false, true])
  it.live(
    `an uncertain append seals the state engine and reopening reconciles the actual log (persisted=${persisted})`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { filePath } = yield* fixture
          yield* Effect.scoped(
            Effect.gen(function* () {
              const persistence = yield* Jsonl.make({ filePath })
              const storage = yield* Store.make({
                ...persistence,
                append: (batch) =>
                  (persisted ? persistence.append(batch) : Effect.void).pipe(
                    Effect.andThen(Effect.fail(fault)),
                  ),
              })
              assert.strictEqual(
                (yield* storage.commit([root, note]).pipe(Effect.flip)).reason,
                'uncertain',
              )
              assert.strictEqual((yield* storage.mintId().pipe(Effect.flip)).reason, 'uncertain')
              assert.strictEqual(
                (yield* storage.entry(Storage.EntryId.make(2), undefined).pipe(Effect.flip)).reason,
                'uncertain',
              )
            }),
          )
          yield* Effect.scoped(
            Effect.gen(function* () {
              const storage = yield* open(filePath)
              assert.strictEqual(
                Option.isSome(yield* storage.entry(Storage.EntryId.make(2), undefined)),
                persisted,
              )
            }),
          )
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )

it.live(
  'a pending append remains invisible, caller interruption cannot split a commit, and closing drains the writer',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { filePath } = yield* fixture
        const started = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const owner = yield* Scope.make()
        const storage = yield* Effect.gen(function* () {
          const persistence = yield* Jsonl.make({ filePath })
          return yield* Store.make({
            ...persistence,
            append: (batch) =>
              Deferred.succeed(started, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(persistence.append(batch)),
              ),
          })
        }).pipe(Effect.provideService(Scope.Scope, owner))
        const writing = yield* storage.commit([root, note]).pipe(Effect.forkChild)
        yield* Deferred.await(started)
        assert.isTrue(Option.isNone(yield* storage.entry(Storage.EntryId.make(2), undefined)))
        const interrupting = yield* Fiber.interrupt(writing).pipe(Effect.forkChild)
        const closing = yield* Scope.close(owner, Exit.void).pipe(Effect.forkChild)
        yield* Effect.yieldNow
        yield* Effect.sync(() => {
          assert.isUndefined(closing.pollUnsafe())
        }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)))
        yield* Fiber.join(interrupting)
        yield* Fiber.join(closing)
        yield* Effect.scoped(
          Effect.gen(function* () {
            const reopened = yield* open(filePath)
            assert.isTrue(Option.isSome(yield* reopened.entry(Storage.EntryId.make(2), undefined)))
          }),
        )
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
)

for (const boundary of [
  'payload-write',
  'payload-sync',
  'directory-sync',
  'marker-partial',
  'marker-sync',
  'reclaim',
  'reclaim-write',
  'reclaim-sync',
] as const)
  it.live(`JSONL ${boundary} failure recovers a complete old or new document`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { fs, filePath } = yield* fixture
        const id = Storage.DocumentId.make(2)
        yield* Effect.scoped(
          Effect.gen(function* () {
            const storage = yield* open(filePath)
            yield* storage.commit([
              {
                _tag: 'document.create',
                record: { id, kind: 'counter', scope: { _tag: 'session' } },
                content: { _tag: 'base', version: 1, value: { count: 0 } },
              },
            ])
          }),
        )
        const error = yield* fs.readFile(`${filePath}.absent`).pipe(Effect.flip)
        let armed = false
        let directorySyncs = 0
        const committed = boundary === 'marker-sync' || boundary.startsWith('reclaim')
        const injected: FileSystem.FileSystem = {
          ...fs,
          rename: (from, to) =>
            armed && boundary === 'reclaim' ? Effect.fail(error) : fs.rename(from, to),
          open: (path, options) =>
            fs.open(path, options).pipe(
              Effect.map((file) => ({
                ...file,
                sync: Effect.suspend(() => {
                  if (armed && path === `${filePath}.parts`) {
                    directorySyncs++
                    if (
                      (boundary === 'directory-sync' && directorySyncs === 1) ||
                      (boundary === 'reclaim-sync' && directorySyncs === 2)
                    )
                      return Effect.fail(error)
                  }
                  if (
                    armed &&
                    ((boundary === 'marker-sync' && path === filePath) ||
                      (boundary === 'payload-sync' && path.endsWith('/doc-2.jsonl')))
                  )
                    return file.sync.pipe(Effect.andThen(Effect.fail(error)))
                  return file.sync
                }),
                writeAll: (bytes) =>
                  Effect.suspend(() => {
                    if (
                      armed &&
                      ((boundary === 'marker-partial' && path === filePath) ||
                        (boundary === 'payload-write' && path.endsWith('/doc-2.jsonl')) ||
                        (boundary === 'reclaim-write' && path.endsWith('.tmp')))
                    )
                      return file
                        .writeAll(bytes.subarray(0, 10))
                        .pipe(Effect.andThen(Effect.fail(error)))
                    return file.writeAll(bytes)
                  }),
              })),
            ),
        }
        yield* Effect.scoped(
          Effect.gen(function* () {
            const storage = yield* open(filePath)
            armed = true
            const result = yield* storage
              .commit([
                {
                  _tag: 'document.change',
                  id,
                  content: { _tag: 'base', version: 1, value: { count: 1 } },
                },
              ])
              .pipe(Effect.exit)
            if (boundary.startsWith('reclaim')) assert.isTrue(Exit.isSuccess(result))
            else assert.isTrue(Exit.isFailure(result))
          }),
        ).pipe(Effect.provideService(FileSystem.FileSystem, injected))
        yield* Effect.scoped(
          Effect.gen(function* () {
            const storage = yield* open(filePath)
            const value = Option.getOrThrow(yield* storage.document(id)).value
            assert.deepEqual(value, {
              count: committed ? 1 : 0,
            })
            yield* storage.commit([
              {
                _tag: 'document.change',
                id,
                content: { _tag: 'base', version: 1, value: { count: 2 } },
              },
            ])
          }),
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            const storage = yield* open(filePath)
            assert.deepEqual(Option.getOrThrow(yield* storage.document(id)).value, { count: 2 })
          }),
        )
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  )

it.live('recovery cannot delete an older base when a retained replacement is a delta', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { fs, filePath } = yield* fixture
      const id = Storage.DocumentId.make(2)
      yield* Effect.scoped(
        Effect.gen(function* () {
          const persistence = yield* Jsonl.make({ filePath })
          // Leave reclamation for the next process, as after a post-commit crash.
          const storage = yield* Store.make({ ...persistence, replace: () => Effect.void })
          yield* storage.commit([
            {
              _tag: 'document.create',
              record: { id, kind: 'counter', scope: { _tag: 'session' } },
              content: { _tag: 'base', version: 1, value: { count: 0 } },
            },
          ])
          yield* storage.commit([
            {
              _tag: 'document.change',
              id,
              content: { _tag: 'base', version: 1, value: { count: 1 } },
            },
          ])
        }),
      )
      const path = `${filePath}.parts/doc-2.jsonl`
      const lines = (yield* fs.readFileString(path)).trim().split('\n')
      assert.lengthOf(lines, 2)
      const corrupted = `${lines[0]}\n${lines[1]!.replace('"_tag":"base"', '"_tag":"delta"')}\n`
      yield* fs.writeFileString(path, corrupted)
      yield* fs.writeFileString(filePath, '{torn', { flag: 'a' })
      const journal = yield* fs.readFileString(filePath)
      const error = yield* Effect.scoped(open(filePath)).pipe(Effect.flip)
      assert.strictEqual(error.reason, 'corrupt')
      assert.strictEqual(yield* fs.readFileString(path), corrupted)
      assert.strictEqual(yield* fs.readFileString(filePath), journal)
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)

for (const recoveryFails of [false, true])
  it.live(
    `recovery syncs a complete marker before pruning its predecessor (sync fails=${recoveryFails})`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { fs, filePath } = yield* fixture
          const id = Storage.DocumentId.make(2)
          const replace = (storage: Storage.Storage['Service'], count: number) =>
            storage.commit([
              {
                _tag: 'document.change',
                id,
                content: { _tag: 'base', version: 1, value: { count } },
              },
            ])
          yield* Effect.scoped(
            Effect.gen(function* () {
              const storage = yield* open(filePath)
              yield* storage.commit([
                {
                  _tag: 'document.create',
                  record: { id, kind: 'counter', scope: { _tag: 'session' } },
                  content: { _tag: 'base', version: 1, value: { count: 0 } },
                },
              ])
            }),
          )
          let durable = yield* fs.readFile(filePath)
          const fault = yield* fs.readFile(`${filePath}.absent`).pipe(Effect.flip)
          let phase: 'opening' | 'commit' | 'recovery' = 'opening'
          const injected: FileSystem.FileSystem = {
            ...fs,
            open: (path, options) =>
              fs.open(path, options).pipe(
                Effect.map((file) => ({
                  ...file,
                  writeAll: (bytes) => file.writeAll(bytes),
                  truncate: (size) => file.truncate(size),
                  sync: Effect.suspend(() => {
                    if (path !== filePath) return file.sync
                    if (phase === 'commit' || (phase === 'recovery' && recoveryFails))
                      return Effect.fail(fault)
                    return file.sync.pipe(
                      Effect.andThen(fs.readFile(filePath)),
                      Effect.map((bytes) => {
                        durable = bytes
                      }),
                    )
                  }),
                })),
              ),
          }
          yield* Effect.scoped(
            Effect.gen(function* () {
              const storage = yield* open(filePath)
              phase = 'commit'
              assert.strictEqual((yield* replace(storage, 1).pipe(Effect.flip)).reason, 'uncertain')
            }),
          ).pipe(Effect.provideService(FileSystem.FileSystem, injected))
          const before = yield* fs.readFileString(`${filePath}.parts/doc-2.jsonl`)
          assert.lengthOf(before.trim().split('\n'), 2)
          phase = 'recovery'
          const recovered = Effect.scoped(open(filePath)).pipe(
            Effect.provideService(FileSystem.FileSystem, injected),
          )
          if (recoveryFails) {
            assert.strictEqual((yield* recovered.pipe(Effect.flip)).reason, 'io')
            assert.strictEqual(yield* fs.readFileString(`${filePath}.parts/doc-2.jsonl`), before)
          } else yield* recovered
          // Simulate a second power loss discarding every main-log byte not successfully synced.
          yield* fs.writeFile(filePath, durable)
          yield* Effect.scoped(
            Effect.gen(function* () {
              const storage = yield* open(filePath)
              assert.deepEqual(Option.getOrThrow(yield* storage.document(id)).value, {
                count: recoveryFails ? 0 : 1,
              })
              yield* replace(storage, 2)
            }),
          )
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )
