import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import * as Document from 'effect-harness/Document'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import { SessionError } from 'effect-harness/SessionError'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'

const root = Record.ROOT_CONVERSATION_ID
const target = { scope: { _tag: 'conversation', conversationId: root } } satisfies Document.Target
const ownerless = { ownership: { _tag: 'ownerless' } } satisfies Transaction.ConversationOptions
const Legacy = Schema.Struct({ legacy: Schema.Natural })
const Current = Schema.Struct({ count: Schema.Natural, label: Schema.String })
const old = Document.define({
  kind: 'migration.counter',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  schema: Legacy,
  initial: () => ({ legacy: 4 }),
})
const migrations = {
  1: (value) =>
    Schema.decodeUnknownEffect(Legacy)(value).pipe(
      Effect.map(({ legacy }) => ({ count: legacy, label: 'v1' })),
    ),
  2: (value) =>
    Schema.decodeUnknownEffect(Schema.Struct({ count: Schema.Natural }))(value).pipe(
      Effect.map(({ count }) => ({ count, label: 'v2' })),
    ),
} satisfies NonNullable<Document.Definition<typeof Current>['migrations']>
const current = Document.define({
  ...old.definition,
  version: 3,
  schema: Current,
  initial: () => ({ count: 0, label: 'new' }),
  migrations,
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
      yield* Transaction.ensureDocument(tx, old, target)
      return yield* Transaction.appendEntry(tx, root, { kind: 'migration.before' })
    }),
  )

describe('Document migrations', () => {
  it.effect('persists the upgrade and original historical revision across JSONL reopen', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'document-migration-' })
        const layer = Storage.layerJsonl({ filePath: `${directory}/storage.jsonl` })
        const before = yield* Effect.gen(function* () {
          const session = yield* Session.make()
          return yield* initialize(session)
        }).pipe(Effect.provide(layer))
        yield* Effect.gen(function* () {
          const session = yield* Session.make()
          yield* Session.commit(session, (tx) => Transaction.ensureDocument(tx, current, target))
        }).pipe(Effect.provide(layer))
        yield* Effect.gen(function* () {
          const session = yield* Session.make()
          assert.deepEqual(some(yield* Session.snapshot(session, current, target)).value, {
            count: 4,
            label: 'v1',
          })
          assert.deepEqual(
            some(yield* Session.snapshotAsOf(session, old, target, before.id)).value,
            {
              legacy: 4,
            },
          )
        }).pipe(Effect.provide(layer))
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  )

  for (const version of [1, 2]) {
    it.effect(`directly migrates persisted version ${version} on acquisition after reopen`, () =>
      withMemory(
        Effect.gen(function* () {
          const storage = yield* Storage.Storage
          const id = yield* storage.mintId<Record.DocumentId>()
          yield* storage.commit([
            { _tag: 'conversation', value: { id: root } },
            {
              _tag: 'document.create',
              record: {
                id,
                kind: old.definition.kind,
                ...target,
                history: 'rewindable',
                fork: 'asOf',
              },
              content: {
                _tag: 'base',
                version,
                value: version === 1 ? { legacy: 7 } : { count: 7 },
              },
            },
          ])
          yield* Effect.scoped(
            Effect.gen(function* () {
              const session = yield* Session.make()
              const acquired = yield* Session.commit(session, (tx) =>
                Transaction.ensureDocument(tx, current, target, { seed: { ignored: true } }),
              )
              assert.deepEqual(acquired.value, { count: 7, label: `v${version}` })
              assert.strictEqual(acquired.record.id, id)
              assert.strictEqual(acquired.version, 3)
              assert.deepEqual(some(yield* storage.document(id)).value, acquired.value)
            }),
          )
          const reopened = yield* Session.make()
          assert.strictEqual(some(yield* Session.snapshot(reopened, current, target)).version, 3)
        }),
      ),
    )
  }

  it.effect('serializes concurrent acquisition into one migration and one storage commit', () =>
    withMemory(
      Effect.gen(function* () {
        const storage = yield* Storage.Storage
        let commits = 0
        let calls = 0
        const tracked = {
          ...storage,
          commit: (writes: Iterable<Record.StorageWrite>) =>
            Effect.sync(() => {
              commits++
            }).pipe(Effect.andThen(storage.commit(writes))),
        } satisfies Storage.Storage['Service']
        const session = yield* Session.make().pipe(Effect.provideService(Storage.Storage, tracked))
        yield* initialize(session)
        const upgraded = Document.define({
          ...current.definition,
          migrations: {
            1: (value) =>
              Effect.sync(() => {
                calls++
              }).pipe(Effect.andThen(migrations[1](value))),
          },
        })
        commits = 0
        const values = yield* Effect.all(
          Array.from({ length: 8 }, () =>
            Session.commit(session, (tx) => Transaction.ensureDocument(tx, upgraded, target)),
          ),
          { concurrency: 'unbounded' },
        )
        assert.strictEqual(calls, 1)
        assert.strictEqual(commits, 1)
        assert.isTrue(values.every((value) => value.version === 3 && value.value.count === 4))
      }),
    ),
  )

  it.effect('migrates and updates in the same base write while retaining old history', () =>
    withMemory(
      Effect.gen(function* () {
        const session = yield* Session.make()
        const before = yield* initialize(session)
        const after = yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.updateDocument(tx, current, target, (value) => ({
              ...value,
              count: value.count + 1,
            }))
            assert.strictEqual(
              some(yield* Transaction.snapshot(tx, current, target)).value.count,
              5,
            )
            return yield* Transaction.appendEntry(tx, root, { kind: 'migration.after' })
          }),
        )
        assert.deepEqual(some(yield* Session.snapshotAsOf(session, old, target, before.id)).value, {
          legacy: 4,
        })
        assert.strictEqual(
          some(yield* Session.snapshotAsOf(session, current, target, after.id)).value.count,
          5,
        )
        const wrongHistory = yield* Session.snapshotAsOf(session, current, target, before.id).pipe(
          Effect.flip,
        )
        assert.strictEqual(wrongHistory._tag, 'SessionError')
        const storage = yield* Storage.Storage
        assert.strictEqual(yield* storage.commit([]), 3)
      }),
    ),
  )

  for (const failure of ['callback', 'schema', 'update'] as const) {
    it.effect(`caught ${failure} failure stages no migration but permits unrelated writes`, () =>
      withMemory(
        Effect.gen(function* () {
          const session = yield* Session.make()
          yield* initialize(session)
          const failing = Document.define({
            ...current.definition,
            migrations: {
              1: (value) => {
                Object.assign(value, { legacy: 999 })
                return failure === 'callback'
                  ? Effect.fail(
                      new SessionError({
                        reason: 'invalid',
                        operation: 'test.migrate',
                        message: 'unsupported',
                      }),
                    )
                  : Effect.succeed({ count: -1, label: 'invalid' })
              },
            },
          })
          const entry = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              const operation =
                failure === 'update'
                  ? Transaction.updateDocument(tx, current, target, (value) => ({
                      ...value,
                      count: -1,
                    }))
                  : Transaction.ensureDocument(tx, failing, target)
              const error = yield* operation.pipe(Effect.flip)
              assert.strictEqual(
                error._tag,
                failure === 'callback' ? 'SessionError' : 'SchemaError',
              )
              assert.deepEqual(some(yield* Transaction.snapshot(tx, old, target)).value, {
                legacy: 4,
              })
              return yield* Transaction.appendEntry(tx, root, { kind: 'migration.caught' })
            }),
          )
          assert.deepEqual(some(yield* Session.snapshot(session, old, target)).value, { legacy: 4 })
          assert.isTrue(Option.isSome(yield* Session.entry(session, entry.id)))
        }),
      ),
    )
  }

  it.effect('uncaught failure rolls back acquisition and other staged writes', () =>
    withMemory(
      Effect.gen(function* () {
        const session = yield* Session.make()
        yield* initialize(session)
        yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.ensureDocument(tx, current, target)
            yield* Transaction.appendEntry(tx, root, { kind: 'migration.rollback' })
            return yield* Effect.fail('rollback')
          }),
        ).pipe(Effect.flip)
        assert.strictEqual(some(yield* Session.snapshot(session, old, target)).version, 1)
        const storage = yield* Storage.Storage
        assert.strictEqual(yield* storage.commit([]), 2)
      }),
    ),
  )

  it.effect('rejects missing migration, downgrade, policy changes and implicit read upgrades', () =>
    withMemory(
      Effect.gen(function* () {
        const session = yield* Session.make()
        yield* initialize(session)
        const absent = Document.define({ ...current.definition, migrations: {} })
        const forkPolicy = Document.define({
          ...current.definition,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'current',
        })
        const historyPolicy = Document.define({
          ...current.definition,
          scope: 'conversation',
          history: 'latest',
          fork: 'current',
        })
        for (const definition of [absent, forkPolicy, historyPolicy]) {
          const error = yield* Session.commit(session, (tx) =>
            Transaction.ensureDocument(tx, definition, target),
          ).pipe(Effect.flip)
          assert.strictEqual(error._tag, 'SessionError')
          if (error._tag === 'SessionError') assert.strictEqual(error.reason, 'conflict')
        }
        assert.strictEqual(
          (yield* Session.snapshot(session, current, target).pipe(Effect.flip))._tag,
          'SessionError',
        )
        yield* Session.commit(session, (tx) => Transaction.ensureDocument(tx, current, target))
        const downgrade = Document.define({
          ...current.definition,
          version: 2,
          migrations: { 3: Effect.succeed },
        })
        assert.strictEqual(
          (yield* Session.commit(session, (tx) =>
            Transaction.ensureDocument(tx, downgrade, target),
          ).pipe(Effect.flip))._tag,
          'SessionError',
        )
      }),
    ),
  )

  for (const fork of ['asOf', 'current', 'initial'] as const) {
    it.effect(`preserves ${fork} fork selection and upgrades copied values on acquisition`, () =>
      withMemory(
        Effect.gen(function* () {
          const session = yield* Session.make()
          const source = Document.define({
            ...old.definition,
            scope: 'conversation',
            history: 'rewindable',
            fork,
          })
          const upgraded = Document.define({
            ...current.definition,
            scope: 'conversation',
            history: 'rewindable',
            fork,
          })
          const cutoff = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.ensureRoot(tx)
              yield* Transaction.ensureDocument(tx, source, target)
              return yield* Transaction.appendEntry(tx, root, { kind: 'fork.cutoff' })
            }),
          )
          yield* Session.commit(session, (tx) =>
            Transaction.updateDocument(tx, upgraded, target, (value) => ({ ...value, count: 9 })),
          )
          const child = yield* Session.commit(session, (tx) =>
            Transaction.forkConversation(tx, root, cutoff.id, ownerless),
          )
          const childTarget = {
            scope: { _tag: 'conversation', conversationId: child.id },
          } satisfies Document.Target
          if (fork === 'asOf')
            assert.strictEqual(
              some(yield* Session.snapshot(session, source, childTarget)).version,
              1,
            )
          if (fork === 'current')
            assert.strictEqual(
              some(yield* Session.snapshot(session, upgraded, childTarget)).value.count,
              9,
            )
          if (fork === 'initial')
            assert.isTrue(Option.isNone(yield* Session.snapshot(session, upgraded, childTarget)))
          const acquired = yield* Session.commit(session, (tx) =>
            Transaction.ensureDocument(tx, upgraded, childTarget),
          )
          const expected = { initial: 0, current: 9, asOf: 4 }
          assert.strictEqual(acquired.value.count, expected[fork])
          assert.strictEqual(acquired.version, 3)
        }),
      ),
    )
  }

  it.effect('rejects migration of a source already copied in the same transaction', () =>
    withMemory(
      Effect.gen(function* () {
        const session = yield* Session.make()
        const cutoff = yield* initialize(session)
        yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            const child = yield* Transaction.forkConversation(tx, root, cutoff.id, ownerless)
            const error = yield* Transaction.ensureDocument(tx, current, target).pipe(Effect.flip)
            assert.strictEqual(error._tag, 'SessionError')
            const childTarget = {
              scope: { _tag: 'conversation', conversationId: child.id },
            } satisfies Document.Target
            yield* Transaction.ensureDocument(tx, current, childTarget)
          }),
        )
        assert.strictEqual(some(yield* Session.snapshot(session, old, target)).version, 1)
      }),
    ),
  )
})
