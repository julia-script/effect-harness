/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
import { describe, expect, expectTypeOf, test } from 'bun:test'
import { Data, Effect, Exit, Scope, Stream } from 'effect'
import { cast } from 'effect/Function'
import type { Ids, Json, Write } from '../../storage/schema/index.ts'
import { Document, SessionError } from '../index.ts'
import type { Session } from '../index.ts'
import { createConversation, openSession, published, runSession } from './support.ts'

/** The document writes of one committed batch. */
const documentWrites = (writes: ReadonlyArray<Write.StorageWrite> | undefined) =>
  (writes ?? []).filter(
    (write) =>
      write.type === 'document.create' ||
      write.type === 'document.change' ||
      write.type === 'document.retire',
  )

/** A document write without its record: its type, and the content it stores. */
const contentOf = (write: Write.StorageWrite) =>
  write.type === 'document.change' || write.type === 'document.create'
    ? { type: write.type, content: write.content }
    : { type: write.type }

/** The document changes of one publication. */
const documentChanges = (publication: Session.CommitPublication) =>
  publication.changes.filter(
    (change): change is Extract<Session.CommitChange, { readonly type: 'document' }> =>
      change.type === 'document',
  )

/** A commit callback's own failure. */
class Rollback extends Data.TaggedError('Rollback')<{ readonly message: string }> {}

/** A migration's own typed failure. */
class MigrationFailed extends Data.TaggedError('MigrationFailed')<{ readonly from: number }> {}

/** The message of an expected failure. */
const failureMessage = <A, E extends { readonly message: string }, R>(
  effect: Effect.Effect<A, E, R>,
) => Effect.map(Effect.flip(effect), (error) => error.message)

describe('Session document migrations', () => {
  test('migrates read-only once per cold load, copies the callback result, and writes nothing', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        interface Current {
          count: number
          labels: Array<string>
        }
        const Old = Document.make<{ count: number }>({
          kind: 'migration.read-only',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 2 }),
        })
        let calls = 0
        let retained: Current | undefined
        const fromVersions: Array<number> = []
        const CurrentDoc = Document.make<{ count: number; labels: Array<string> }>({
          kind: 'migration.read-only',
          version: 3,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0, labels: [] }),
          migrate: (value, fromVersion) =>
            Effect.sync(() => {
              fromVersions.push(fromVersion)
              calls++
              retained = { count: Number(value.count), labels: ['migrated'] }
              return retained
            }),
        })
        yield* session.commit((tx) => tx.doc(Old))
        yield* session.unloadDocuments
        const commits = recorder.commits.length

        const first = yield* session.snapshot(CurrentDoc)
        expect(yield* session.snapshot(CurrentDoc)).toBe(first)
        expect(calls).toBe(1)
        expect(fromVersions).toEqual([1])
        expect(recorder.commits).toHaveLength(commits)
        if (retained !== undefined) {
          retained.count = 99
          retained.labels.push('mutated')
        }
        expect(first).toEqual({ count: 2, labels: ['migrated'] })

        yield* session.unloadDocuments
        const second = yield* session.snapshot(CurrentDoc)
        expect(second).not.toBe(first)
        expect(second).toEqual(first)
        expect(calls).toBe(2)
        expect(recorder.commits).toHaveLength(commits)
      }),
    ))

  test('writes the required base on the first successful transaction, then writes deltas', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        const Old = Document.make<{ count: number }>({
          kind: 'migration.transition',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 4 }),
        })
        let checkpoints = 0
        const Current = Document.make<{ count: number }>({
          kind: 'migration.transition',
          version: 3,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0 }),
          migrate: (value, fromVersion) =>
            Effect.succeed({ count: Number(value.count) + fromVersion - 1 }),
          checkpointWhen: () =>
            Effect.sync(() => {
              checkpoints++
              return false
            }),
        })
        yield* session.commit((tx) => tx.doc(Old))
        yield* session.unloadDocuments
        // An observer of the older shape.
        const watch = yield* Effect.flatMap(session.watchDoc(Old), Effect.fromNullishOr)
        expect(yield* session.snapshot(Current)).toEqual({ count: 4 })
        const snapshot = yield* session.snapshot(Current)
        // An observer of the new shape: the migration changes nothing it sees.
        const current = yield* Effect.flatMap(session.watchDoc(Current), Effect.fromNullishOr)

        const { publication } = yield* published(session, (tx) => tx.doc(Current))
        expect(recorder.commits.at(-1)?.[0]).toMatchObject({
          type: 'document.change',
          content: { kind: 'base', version: 3, value: { count: 4 } },
        })
        expect(checkpoints).toBe(0)
        // The migration-only base is published, so the older-shape watch receives the new value as
        // a root replacement.
        expect(documentChanges(publication)).toMatchObject([
          { version: 3, value: { count: 4 }, ops: [] },
        ])
        expect(yield* session.snapshot(Current)).toBe(snapshot)

        yield* session.commit((tx) =>
          Effect.map(tx.doc(Current), (draft) => {
            draft.count = 7
          }),
        )
        expect(recorder.commits.at(-1)?.[0]).toMatchObject({
          type: 'document.change',
          content: { kind: 'delta', version: 3 },
        })
        expect(checkpoints).toBe(1)
        // pi stops the watches before this edit; here it shows what each one received: the
        // new-shape watch's first change is the edit, not the base.
        const frames = yield* Stream.runCollect(Stream.take(watch.changes, 2))
        expect(frames).toEqual([
          { value: { count: 4 }, ops: [['r', { count: 4 }]] },
          { value: { count: 7 }, ops: [['s', ['count'], 7]] },
        ])
        const currentFrames = yield* Stream.runCollect(Stream.take(current.changes, 1))
        expect(currentFrames.map(({ value }) => value)).toEqual([{ count: 7 }])
        yield* session.unloadDocuments
        expect(yield* session.snapshot(Current)).toEqual({ count: 7 })
      }),
    ))

  test('rolls migration and edits back with the callback, then coalesces later edits into one base', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        const Old = Document.make<{ count: number }>({
          kind: 'migration.rollback',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 1 }),
        })
        let migrations = 0
        const Current = Document.make<{ count: number; migrated: boolean }>({
          kind: 'migration.rollback',
          version: 2,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0, migrated: false }),
          migrate: (value) =>
            Effect.sync(() => {
              migrations++
              return { count: Number(value.count), migrated: true }
            }),
        })
        yield* session.commit((tx) => tx.doc(Old))
        yield* session.unloadDocuments
        const commits = recorder.commits.length

        const message = yield* failureMessage(
          session.commit((tx) =>
            Effect.gen(function* () {
              ;(yield* tx.doc(Current)).count = 8
              return yield* new Rollback({ message: 'rollback' })
            }),
          ),
        )
        expect(message).toBe('rollback')
        expect(recorder.commits).toHaveLength(commits)
        expect(yield* session.snapshot(Current)).toEqual({ count: 1, migrated: true })
        expect(migrations).toBe(1)

        const { publication } = yield* published(session, (tx) =>
          Effect.map(tx.doc(Current), (value) => {
            value.count = 9
            value.migrated = false
          }),
        )
        expect(documentWrites(recorder.commits.at(-1)).map((write) => contentOf(write))).toEqual([
          {
            type: 'document.change',
            content: { kind: 'base', version: 2, value: { count: 9, migrated: false } },
          },
        ])
        const change = documentChanges(publication)[0]
        const admitted = recorder.commits.at(-1)?.[0]
        expect(change?.value).toBe(yield* session.snapshot(Current))
        expect(change?.value).toBe(
          admitted?.type === 'document.change' && admitted.content.kind === 'base'
            ? admitted.content.value
            : undefined,
        )
        expect(change?.ops.length).toBeGreaterThan(0)
        expect(migrations).toBe(1)
      }),
    ))

  test('strict-checks migration results before tracker ownership and remains usable after rejection', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        const Old = Document.make<{ count: number }>({
          kind: 'migration.invalid',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 1 }),
        })
        const Invalid = Document.make<Json.JsonObject>({
          kind: 'migration.invalid',
          version: 2,
          scope: 'session',
          initial: () => Effect.succeed({}),
          // A callback that breaks its contract, as untyped extension code can.
          migrate: cast(() => Effect.succeed({ invalid: new Date() })),
        })
        const Other = Document.make<{ ok: boolean }>({
          kind: 'migration.invalid.other',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ ok: true }),
        })
        yield* session.commit((tx) => tx.doc(Old))
        yield* session.unloadDocuments
        const commits = recorder.commits.length
        const snapshotError = yield* Effect.flip(session.snapshot(Invalid))
        expect(snapshotError).toBeInstanceOf(SessionError.DocumentError)
        expect(snapshotError.message).toContain('strict JSON')
        const commitError = yield* Effect.flip(session.commit((tx) => tx.doc(Invalid)))
        expect(commitError).toBeInstanceOf(SessionError.DocumentError)
        expect(commitError.message).toContain('strict JSON')
        expect(recorder.commits).toHaveLength(commits)
        yield* session.commit((tx) => tx.doc(Other))
        expect(yield* session.snapshot(Other)).toEqual({ ok: true })
      }),
    ))

  test('rejects newer stored versions and older versions without migration for snapshots and transactions', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        const V2 = Document.make<{ count: number }>({
          kind: 'migration.compatibility',
          version: 2,
          scope: 'session',
          initial: () => Effect.succeed({ count: 2 }),
        })
        const V1 = Document.make<{ count: number }>({
          kind: 'migration.compatibility',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 1 }),
        })
        const V3WithoutMigration = Document.make<{ count: number }>({
          kind: 'migration.compatibility',
          version: 3,
          scope: 'session',
          initial: () => Effect.succeed({ count: 3 }),
        })
        yield* session.commit((tx) => tx.doc(V2))
        yield* session.unloadDocuments
        const commits = recorder.commits.length

        const refusals = [
          Effect.flip(session.snapshot(V1)),
          Effect.flip(session.commit((tx) => tx.doc(V1))),
          Effect.flip(session.snapshot(V3WithoutMigration)),
          Effect.flip(session.commit((tx) => tx.doc(V3WithoutMigration))),
        ]
        const errors = yield* Effect.all(refusals)
        for (const error of errors) expect(error).toBeInstanceOf(SessionError.DocumentError)
        const expected = [
          'newer version 2 than 1',
          'newer version 2 than 1',
          'requires migration from version 2',
          'requires migration from version 2',
        ]
        expect(errors).toHaveLength(expected.length)
        for (const [index, error] of errors.entries()) {
          expect(error.message).toContain(expected[index] ?? '')
        }
        expect(recorder.commits).toHaveLength(commits)
      }),
    ))

  test('fails snapshots and transactions with the typed error of a failed migration and writes nothing', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        const Old = Document.make<{ count: number }>({
          kind: 'migration.failed',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 1 }),
        })
        const Current = Document.make<{ count: number }, MigrationFailed>({
          kind: 'migration.failed',
          version: 2,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0 }),
          migrate: (_value, from) => Effect.fail(new MigrationFailed({ from })),
        })
        yield* session.commit((tx) => tx.doc(Old))
        const commits = recorder.commits.length
        const read = session.snapshot(Current)
        expectTypeOf<MigrationFailed>().toExtend<Effect.Error<typeof read>>()
        expect(yield* Effect.flip(read)).toEqual(new MigrationFailed({ from: 1 }))
        const commit = session.commit((tx) => tx.doc(Current))
        expectTypeOf<MigrationFailed>().toExtend<Effect.Error<typeof commit>>()
        expect(yield* Effect.flip(commit)).toEqual(new MigrationFailed({ from: 1 }))
        expect(recorder.commits).toHaveLength(commits)
        expect(yield* session.snapshot(Old)).toEqual({ count: 1 })
      }),
    ))

  test('persists a required migration base before retirement without consulting the checkpoint predicate', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        const Old = Document.make<{ count: number }>({
          kind: 'migration.retire',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 1 }),
        })
        const Current = Document.make<{ count: number }>({
          kind: 'migration.retire',
          version: 2,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0 }),
          migrate: (value) => Effect.succeed({ count: Number(value.count) }),
          checkpointWhen: () =>
            Effect.sync(() => {
              throw new Error('must not run')
            }),
        })
        yield* session.commit((tx) => tx.doc(Old))
        yield* session.unloadDocuments
        yield* session.commit((tx) => Effect.andThen(tx.doc(Current), tx.retireDoc(Current)))
        expect(documentWrites(recorder.commits.at(-1)).map((write) => contentOf(write))).toEqual([
          { type: 'document.change', content: { kind: 'base', version: 2, value: { count: 1 } } },
          { type: 'document.retire' },
        ])
      }),
    ))

  test('leaves unaccessed older documents and unavailable definitions untouched', () =>
    runSession(({ session, storage, recorder }) =>
      Effect.gen(function* () {
        const FirstV1 = Document.make<{ count: number }>({
          kind: 'migration.lazy.first',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 1 }),
        })
        const SecondV1 = Document.make<{ count: number }>({
          kind: 'migration.lazy.second',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 2 }),
        })
        let secondMigrations = 0
        const FirstV2 = Document.make<{ count: number }>({
          kind: 'migration.lazy.first',
          version: 2,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0 }),
          migrate: (value) => Effect.succeed({ count: Number(value.count) }),
        })
        // Defined but never used: defining a newer version migrates nothing.
        Document.make<{ count: number }>({
          kind: 'migration.lazy.second',
          version: 2,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0 }),
          migrate: (value) =>
            Effect.sync(() => {
              secondMigrations++
              return { count: Number(value.count) }
            }),
        })
        yield* session.commit((tx) => Effect.all([tx.doc(FirstV1), tx.doc(SecondV1)]))
        yield* session.unloadDocuments
        const commits = recorder.commits.length
        expect(yield* session.snapshot(FirstV2)).toEqual({ count: 1 })
        expect(recorder.commits).toHaveLength(commits)
        expect(secondMigrations).toBe(0)
        const secondRecord = yield* storage.findDocument(
          { kind: 'migration.lazy.second', scope: { kind: 'session' } },
          'current',
        )
        const stored =
          secondRecord === undefined
            ? undefined
            : yield* storage.document(secondRecord.id, 'current')
        expect(stored?.version).toBe(1)
      }),
    ))
})

describe('Session historical document snapshots', () => {
  test('migrates current and historical rewindable values independently and follows fork ancestry', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        const V1 = Document.make<{ count: number }>({
          kind: 'history.migration',
          version: 1,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: () => Effect.succeed({ count: 0 }),
        })
        const Family = Document.makeFamily<{ seed: string; count: number }, string>({
          kind: 'history.family',
          version: 1,
          family: true,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: (seed) => Effect.succeed({ seed, count: 0 }),
        })
        const migrations: Array<number> = []
        const V3 = Document.make<{ count: number; version: number }>({
          kind: 'history.migration',
          version: 3,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: () => Effect.succeed({ count: 0, version: 3 }),
          migrate: (value, fromVersion) =>
            Effect.sync(() => {
              migrations.push(fromVersion)
              return { count: Number(value.count), version: 3 }
            }),
        })
        const conversationId = yield* createConversation(session)
        const firstEntry = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const { id } = yield* tx.appendEntry(conversationId, { kind: 'first' })
            ;(yield* tx.doc(V1, conversationId)).count = 1
            ;(yield* tx.doc(Family, conversationId, 'member', 'seed')).count = 1
            return id
          }),
        )
        const secondEntry = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const { id } = yield* tx.appendEntry(conversationId, { kind: 'second' })
            ;(yield* tx.doc(V1, conversationId)).count = 2
            return id
          }),
        )
        yield* session.unloadDocuments
        const commits = recorder.commits.length
        expect(yield* session.snapshot(V3, conversationId)).toEqual({ count: 2, version: 3 })
        expect(migrations).toEqual([1])
        expect(recorder.commits).toHaveLength(commits)

        const thirdEntry = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const { id } = yield* tx.appendEntry(conversationId, { kind: 'third' })
            yield* tx.doc(V3, conversationId)
            return id
          }),
        )
        const migrated = (recorder.commits.at(-1) ?? []).flatMap((write) =>
          write.type === 'document.change' ? [write.content] : [],
        )
        expect(migrated).toMatchObject([{ kind: 'base', version: 3 }])

        expect(yield* session.snapshotAsOf(V3, conversationId, firstEntry)).toEqual({
          count: 1,
          version: 3,
        })
        expect(yield* session.snapshotAsOf(V3, conversationId, secondEntry)).toEqual({
          count: 2,
          version: 3,
        })
        expect(yield* session.snapshotAsOf(V3, conversationId, thirdEntry)).toEqual({
          count: 2,
          version: 3,
        })
        expect(migrations).toEqual([1, 1, 1])
        expect(yield* session.snapshotAsOf(Family, conversationId, 'member', firstEntry)).toEqual({
          seed: 'seed',
          count: 1,
        })

        const child = yield* session.commit((tx) =>
          tx.forkConversation(conversationId, secondEntry, { ownership: { kind: 'ownerless' } }),
        )
        const childId: Ids.ConversationId = child.id
        expect(yield* session.snapshotAsOf(V3, childId, firstEntry)).toEqual({
          count: 1,
          version: 3,
        })
        expect(yield* session.snapshotAsOf(V3, childId, secondEntry)).toEqual({
          count: 2,
          version: 3,
        })
        expect(yield* session.snapshotAsOf(Family, childId, 'member', firstEntry)).toEqual({
          seed: 'seed',
          count: 1,
        })
        const error = yield* Effect.flip(session.snapshotAsOf(V3, childId, thirdEntry))
        expect(error).toBeInstanceOf(SessionError.DocumentError)
        expect(error.message).toContain(`Entry ${thirdEntry} is not visible`)
      }),
    ))

  test('selects the incarnation alive at the entry commit across retirement and recreation', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const Doc = Document.make<{ value: string }>({
          kind: 'history.incarnation',
          version: 1,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: () => Effect.succeed({ value: 'initial' }),
        })
        const scope = yield* Scope.make()
        const { session } = yield* Scope.provide(openSession, scope)
        const conversationId = yield* createConversation(session)
        const beforeCreation = yield* session.commit((tx) =>
          Effect.map(tx.appendEntry(conversationId, { kind: 'before' }), ({ id }) => id),
        )
        const createdAt = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const { id } = yield* tx.appendEntry(conversationId, { kind: 'create' })
            ;(yield* tx.doc(Doc, conversationId)).value = 'old'
            return id
          }),
        )
        const retiredAt = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const { id } = yield* tx.appendEntry(conversationId, { kind: 'retire' })
            yield* tx.retireDoc(Doc, conversationId)
            return id
          }),
        )
        const recreatedAt = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const { id } = yield* tx.appendEntry(conversationId, { kind: 'recreate' })
            ;(yield* tx.doc(Doc, conversationId)).value = 'new'
            return id
          }),
        )

        expect(yield* session.snapshotAsOf(Doc, conversationId, beforeCreation)).toBeUndefined()
        expect(yield* session.snapshotAsOf(Doc, conversationId, createdAt)).toEqual({
          value: 'old',
        })
        expect(yield* session.snapshotAsOf(Doc, conversationId, retiredAt)).toBeUndefined()
        expect(yield* session.snapshotAsOf(Doc, conversationId, recreatedAt)).toEqual({
          value: 'new',
        })
        yield* Scope.close(scope, Exit.void)
        const error = yield* Effect.flip(session.snapshotAsOf(Doc, conversationId, recreatedAt))
        expect(error).toBeInstanceOf(SessionError.SessionClosed)
        expect(error.message).toContain('closed')
      }).pipe(Effect.orDie),
    ))
})

describe('Session tracker cache across definition versions', () => {
  const V1Doc = Document.make<{ name: string }>({
    kind: 'cache.versioned',
    version: 1,
    scope: 'session',
    initial: () => Effect.succeed({ name: 'first' }),
  })
  const V2Doc = Document.make<{ names: Array<string> }>({
    kind: 'cache.versioned',
    version: 2,
    scope: 'session',
    initial: () => Effect.succeed({ names: [] }),
    migrate: (value) =>
      Effect.succeed({ names: [typeof value.name === 'string' ? value.name : ''] }),
  })

  test('migrates a document cached by an older token without unloading', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        yield* session.commit((tx) => tx.doc(V1Doc))
        expect(yield* session.snapshot(V1Doc)).toEqual({ name: 'first' })

        // Reloaded extension code accesses the still-cached document with a newer token.
        expect(yield* session.snapshot(V2Doc)).toEqual({ names: ['first'] })
        yield* session.commit((tx) =>
          Effect.map(tx.doc(V2Doc), (draft) => {
            draft.names.push('second')
          }),
        )
        expect(documentWrites(recorder.commits.at(-1))[0]).toMatchObject({
          type: 'document.change',
          content: { version: 2, kind: 'base', value: { names: ['first', 'second'] } },
        })
        const error = yield* Effect.flip(session.snapshot(V1Doc))
        expect(error).toBeInstanceOf(SessionError.DocumentError)
        expect(error.message).toContain('newer version 2')
      }),
    ))

  test('sends observers of an older shape a root replacement after a newer token writes', () =>
    runSession(({ session }) =>
      Effect.gen(function* () {
        yield* session.commit((tx) => tx.doc(V1Doc))
        const state = yield* Effect.flatMap(session.documentState(V1Doc), Effect.fromNullishOr)
        const watch = yield* Effect.flatMap(session.watchDoc(V1Doc), Effect.fromNullishOr)
        yield* session.commit((tx) =>
          Effect.map(tx.doc(V2Doc), (draft) => {
            draft.names.push('second')
          }),
        )
        // Typed by the older token, the observers hold the newer shape.
        const migrated: unknown = yield* state.get
        expect(migrated).toEqual({ names: ['first', 'second'] })
        // pi's watch value follows the changes; here `value` is the baseline, and the change
        // carries the new value.
        expect(watch.value).toEqual({ name: 'first' })
        const frames: unknown = yield* Stream.runCollect(Stream.take(watch.changes, 1))
        expect(frames).toEqual([
          { value: { names: ['first', 'second'] }, ops: [['r', { names: ['first', 'second'] }]] },
        ])
      }),
    ))

  test('serves an older token from Storage after a newer token migrated only in memory', () =>
    runSession(({ session }) =>
      Effect.gen(function* () {
        yield* session.commit((tx) => tx.doc(V1Doc))
        yield* session.unloadDocuments
        expect(yield* session.snapshot(V2Doc)).toEqual({ names: ['first'] })
        expect(yield* session.snapshot(V1Doc)).toEqual({ name: 'first' })
        yield* session.commit((tx) =>
          Effect.map(tx.doc(V1Doc), (draft) => {
            draft.name = 'renamed'
          }),
        )
        expect(yield* session.snapshot(V2Doc)).toEqual({ names: ['renamed'] })
      }),
    ))
})
