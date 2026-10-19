/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
// Ported from pi's `packages/durable/test/session-forks.test.ts`.
import { describe, expect, test } from 'bun:test'
import { Effect, PubSub } from 'effect'
import type { Scope } from 'effect'
import { Storage, StorageError } from '../../storage/index.ts'
import type { Ids } from '../../storage/schema/index.ts'
import type { Write } from '../../storage/schema/index.ts'
import { Document, Session, SessionError } from '../index.ts'
import { createConversation, published, Work } from './support.ts'

/** A Storage that counts document reads, records admitted batches, and can fail its next commit. */
interface Controlled {
  readonly session: Session.Service
  /** The memory store behind the Session, for reads outside it. */
  readonly storage: Storage.Service
  /** Every batch the Session handed to Storage, committed or not. */
  readonly commits: Array<ReadonlyArray<Write.StorageWrite>>
  readonly reads: { document: number }
  readonly failNextCommit: (error: StorageError.StorageError) => void
}

const openControlled = Effect.gen(function* () {
  const storage = yield* Storage.makeMemory
  const commits: Array<ReadonlyArray<Write.StorageWrite>> = []
  const reads = { document: 0 }
  let failure: StorageError.StorageError | undefined
  const controlled: Storage.Service = {
    ...storage,
    commit: (writes) =>
      Effect.suspend(() => {
        commits.push(writes)
        const error = failure
        failure = undefined
        return error === undefined ? storage.commit(writes) : Effect.fail(error)
      }),
    document: (id, at) =>
      Effect.suspend(() => {
        reads.document++
        return storage.document(id, at)
      }),
  }
  const session = yield* Session.make.pipe(Effect.provideService(Storage.Storage, controlled))
  const harness: Controlled = {
    session,
    storage,
    commits,
    reads,
    failNextCommit: (error) => {
      failure = error
    },
  }
  return harness
})

const run = <A, E>(body: (harness: Controlled) => Effect.Effect<A, E, Scope.Scope>): Promise<A> =>
  Effect.runPromise(Effect.scoped(Effect.orDie(Effect.flatMap(openControlled, body))))

/** Acquire a draft and assign fields to it. */
const set = <T extends object, E>(draft: Effect.Effect<T, E>, fields: Partial<T>) =>
  Effect.map(draft, (value) => Object.assign(value, fields))

const ownerless = { ownership: { kind: 'ownerless' as const } }

const documentCreates = (writes: ReadonlyArray<Write.StorageWrite>) =>
  writes.filter((write) => write.type === 'document.create')

const documentCopies = (writes: ReadonlyArray<Write.StorageWrite>) =>
  writes.filter((write) => write.type === 'document.copy')

const documentChanges = (publication: Session.CommitPublication) =>
  publication.changes.filter((change) => change.type === 'document')

const documentCopyChanges = (publication: Session.CommitPublication) =>
  publication.changes.filter((change) => change.type === 'document.copy')

const inConversation = (conversationId: Ids.ConversationId) => ({
  kind: 'conversation' as const,
  conversationId,
})

/** Commit one entry and return its ID. */
const appendEntry = (session: Session.Service, conversationId: Ids.ConversationId, kind: string) =>
  session.commit((tx) => Effect.map(tx.appendEntry(conversationId, { kind }), ({ id }) => id))

describe('Session conversation document forks', () => {
  test('copies as-of and current singleton and family bases while leaving initial documents absent', () =>
    run(({ session, storage, commits, reads }) =>
      Effect.gen(function* () {
        const AsOf = Document.make<{ value: string }>({
          kind: 'fork.policies.as-of',
          version: 1,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: () => Effect.succeed({ value: 'as-initial' }),
        })
        const Current = Document.make<{ value: string }>({
          kind: 'fork.policies.current',
          version: 1,
          scope: 'conversation',
          history: 'latest',
          fork: 'current',
          initial: () => Effect.succeed({ value: 'current-initial' }),
        })
        const Initial = Document.make<{ value: string }>({
          kind: 'fork.policies.initial',
          version: 1,
          scope: 'conversation',
          history: 'latest',
          fork: 'initial',
          initial: () => Effect.succeed({ value: 'fresh' }),
        })
        const AsOfFamily = Document.makeFamily<{ value: string }, string>({
          kind: 'fork.policies.as-of-family',
          version: 1,
          family: true,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: (seed) => Effect.succeed({ value: seed }),
        })
        const CurrentFamily = Document.makeFamily<{ value: string }, string>({
          kind: 'fork.policies.current-family',
          version: 1,
          family: true,
          scope: 'conversation',
          history: 'latest',
          fork: 'current',
          initial: (seed) => Effect.succeed({ value: seed }),
        })
        const parentId = yield* createConversation(session)
        const forkAt = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const entry = yield* tx.appendEntry(parentId, { kind: 'fork-point' })
            yield* set(tx.doc(AsOf, parentId), { value: 'as-at-fork' })
            yield* set(tx.doc(Current, parentId), { value: 'current-at-fork' })
            yield* set(tx.doc(Initial, parentId), { value: 'parent-only' })
            yield* set(tx.doc(AsOfFamily, parentId, 'a', 'unused'), { value: 'family-as-a' })
            yield* set(tx.doc(AsOfFamily, parentId, 'b', 'unused'), { value: 'family-as-b' })
            yield* set(tx.doc(CurrentFamily, parentId, 'a', 'unused'), {
              value: 'family-current-a',
            })
            return entry.id
          }),
        )
        yield* session.commit((tx) =>
          Effect.gen(function* () {
            yield* set(tx.doc(AsOf, parentId), { value: 'as-after-fork' })
            yield* set(tx.doc(Current, parentId), { value: 'current-when-copied' })
            yield* set(tx.doc(AsOfFamily, parentId, 'a', 'unused'), { value: 'family-as-after' })
            yield* set(tx.doc(CurrentFamily, parentId, 'a', 'unused'), {
              value: 'family-current-when-copied',
            })
          }),
        )
        const documentReads = reads.document

        const { result: child, publication } = yield* published(session, (tx) =>
          tx.forkConversation(parentId, forkAt, ownerless),
        )
        expect(reads.document).toBe(documentReads)

        expect(yield* session.snapshot(AsOf, child.id)).toEqual({ value: 'as-at-fork' })
        expect(yield* session.snapshot(Current, child.id)).toEqual({ value: 'current-when-copied' })
        expect(yield* session.snapshot(Initial, child.id)).toBeUndefined()
        expect(yield* session.snapshot(AsOfFamily, child.id, 'a')).toEqual({
          value: 'family-as-a',
        })
        expect(yield* session.snapshot(AsOfFamily, child.id, 'b')).toEqual({
          value: 'family-as-b',
        })
        expect(yield* session.snapshot(CurrentFamily, child.id, 'a')).toEqual({
          value: 'family-current-when-copied',
        })

        const admitted = commits.at(-1) ?? []
        const copies = documentCopies(admitted)
        expect(copies).toHaveLength(5)
        expect(
          copies.every(
            (write) =>
              write.record.scope.kind === 'conversation' &&
              write.record.scope.conversationId === child.id,
          ),
        ).toBe(true)
        const copied = documentCopyChanges(publication)
        expect(copied).toHaveLength(5)
        for (const change of copied) {
          expect(change.record.createdAt).toBe(publication.seq)
          expect(change.conversationId).toBe(child.id)
          const write = copies.find((candidate) => candidate.record.id === change.record.id)
          expect(write).toBeDefined()
          if (write !== undefined) expect(change.source).toEqual(write.source)
        }

        const parentRecord = yield* storage.findDocument(
          { kind: AsOf.definition.kind, scope: inConversation(parentId) },
          'current',
        )
        const childRecord = yield* storage.findDocument(
          { kind: AsOf.definition.kind, scope: inConversation(child.id) },
          'current',
        )
        expect(childRecord?.id).toBeNumber()
        expect(childRecord?.id).not.toBe(parentRecord?.id)

        yield* session.commit((tx) => set(tx.doc(AsOf, child.id), { value: 'child-independent' }))
        expect(yield* session.snapshot(AsOf, parentId)).toEqual({ value: 'as-after-fork' })
        expect(yield* session.snapshot(AsOf, child.id)).toEqual({ value: 'child-independent' })
        yield* session.commit((tx) => set(tx.doc(Initial, child.id), { value: 'child-created' }))
        expect(yield* session.snapshot(Initial, child.id)).toEqual({ value: 'child-created' })
      }),
    ))

  test('uses final document state from the fork entry commit while excluding later same-commit entries', () =>
    run(({ session }) =>
      Effect.gen(function* () {
        const Doc = Document.make<{ value: string }>({
          kind: 'fork.same-commit',
          version: 1,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: () => Effect.succeed({ value: 'initial' }),
        })
        const parentId = yield* createConversation(session)
        const { forkAt, excluded } = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const included = yield* tx.appendEntry(parentId, { kind: 'included' })
            yield* set(tx.doc(Doc, parentId), { value: 'final-state-of-commit' })
            const later = yield* tx.appendEntry(parentId, { kind: 'excluded' })
            return { forkAt: included.id, excluded: later.id }
          }),
        )
        const child = yield* session.commit((tx) =>
          tx.forkConversation(parentId, forkAt, ownerless),
        )
        expect(yield* session.snapshot(Doc, child.id)).toEqual({ value: 'final-state-of-commit' })
        const visible = yield* session.commit((tx) =>
          tx.scanEntries({ conversationId: child.id }, 10),
        )
        expect(visible.items.map(({ id }) => id)).toContain(forkAt)
        expect(visible.items.map(({ id }) => id)).not.toContain(excluded)
      }),
    ))

  test('selects the entry-owning ancestor for as-of copies and the immediate parent for current copies', () =>
    run(({ session }) =>
      Effect.gen(function* () {
        const AsOf = Document.make<{ value: string }>({
          kind: 'fork.ancestry.as-of',
          version: 1,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: () => Effect.succeed({ value: 'initial' }),
        })
        const Current = Document.make<{ value: string }>({
          kind: 'fork.ancestry.current',
          version: 1,
          scope: 'conversation',
          history: 'latest',
          fork: 'current',
          initial: () => Effect.succeed({ value: 'initial' }),
        })
        const rootId = yield* createConversation(session)
        const inherited = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const entry = yield* tx.appendEntry(rootId, { kind: 'root' })
            yield* set(tx.doc(AsOf, rootId), { value: 'root-at-entry' })
            yield* set(tx.doc(Current, rootId), { value: 'root-current' })
            return entry.id
          }),
        )
        const parent = yield* session.commit((tx) =>
          tx.forkConversation(rootId, inherited, ownerless),
        )
        const parentEntry = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const entry = yield* tx.appendEntry(parent.id, { kind: 'parent' })
            yield* set(tx.doc(AsOf, parent.id), { value: 'parent-at-own-entry' })
            yield* set(tx.doc(Current, parent.id), { value: 'parent-current' })
            return entry.id
          }),
        )

        const inheritedFork = yield* session.commit((tx) =>
          tx.forkConversation(parent.id, inherited, ownerless),
        )
        expect(yield* session.snapshot(AsOf, inheritedFork.id)).toEqual({ value: 'root-at-entry' })
        expect(yield* session.snapshot(Current, inheritedFork.id)).toEqual({
          value: 'parent-current',
        })

        const ownEntryFork = yield* session.commit((tx) =>
          tx.forkConversation(parent.id, parentEntry, ownerless),
        )
        expect(yield* session.snapshot(AsOf, ownEntryFork.id)).toEqual({
          value: 'parent-at-own-entry',
        })
        expect(yield* session.snapshot(Current, ownEntryFork.id)).toEqual({
          value: 'parent-current',
        })
      }),
    ))

  test('copies the stored value and version without consulting migration definitions or migrated caches', () =>
    run(({ session, storage }) =>
      Effect.gen(function* () {
        const V1 = Document.make<{ count: number }>({
          kind: 'fork.stored-version',
          version: 1,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: () => Effect.succeed({ count: 1 }),
        })
        let migrations = 0
        const V3 = Document.make<{ count: number; migrated: boolean }>({
          kind: 'fork.stored-version',
          version: 3,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: () => Effect.succeed({ count: 0, migrated: false }),
          migrate: (value, fromVersion) =>
            Effect.sync(() => {
              expect(fromVersion).toBe(1)
              migrations++
              return {
                count: typeof value['count'] === 'number' ? value['count'] : 0,
                migrated: true,
              }
            }),
        })
        const parentId = yield* createConversation(session)
        const forkAt = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const entry = yield* tx.appendEntry(parentId, { kind: 'point' })
            yield* tx.doc(V1, parentId)
            return entry.id
          }),
        )
        yield* session.unloadDocuments
        expect(yield* session.snapshot(V3, parentId)).toEqual({ count: 1, migrated: true })
        expect(migrations).toBe(1)

        const child = yield* session.commit((tx) =>
          tx.forkConversation(parentId, forkAt, ownerless),
        )
        expect(migrations).toBe(1)
        const childRecord = yield* storage.findDocument(
          { kind: V1.definition.kind, scope: inConversation(child.id) },
          'current',
        )
        expect(childRecord).toBeDefined()
        const childStored =
          childRecord === undefined ? undefined : yield* storage.document(childRecord.id, 'current')
        expect(childStored).toMatchObject({ version: 1, value: { count: 1 } })
        expect(yield* session.snapshot(V3, child.id)).toEqual({ count: 1, migrated: true })
        expect(migrations).toBe(2)
      }),
    ))

  test('coalesces a typed migration and override into the copied creation base', () =>
    run(({ session, storage, commits, reads }) =>
      Effect.gen(function* () {
        const V1 = Document.make<{ count: number }>({
          kind: 'fork.override',
          version: 1,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: () => Effect.succeed({ count: 2 }),
        })
        let checkpoints = 0
        const V2 = Document.make<{ count: number; migrated: boolean }>({
          kind: 'fork.override',
          version: 2,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: () => Effect.succeed({ count: 0, migrated: false }),
          migrate: (value) =>
            Effect.succeed({
              count: typeof value['count'] === 'number' ? value['count'] : 0,
              migrated: true,
            }),
          checkpointWhen: () =>
            Effect.sync(() => {
              checkpoints++
              return false
            }),
        })
        const parentId = yield* createConversation(session)
        const forkAt = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const entry = yield* tx.appendEntry(parentId, { kind: 'point' })
            yield* tx.doc(V1, parentId)
            return entry.id
          }),
        )

        const documentReads = reads.document
        const { result: child, publication } = yield* published(session, (tx) =>
          Effect.gen(function* () {
            const created = yield* tx.forkConversation(parentId, forkAt, ownerless)
            yield* set(tx.doc(V2, created.id), { count: 9 })
            return created
          }),
        )
        expect(reads.document).toBe(documentReads + 1)
        const creates = documentCreates(commits.at(-1) ?? [])
        expect(creates).toHaveLength(1)
        expect(creates[0]?.content).toMatchObject({
          kind: 'base',
          version: 2,
          value: { count: 9, migrated: true },
        })
        expect(checkpoints).toBe(0)
        expect(documentChanges(publication)).toMatchObject([{ version: 2 }])
        expect(yield* session.snapshot(V2, child.id)).toEqual({ count: 9, migrated: true })
        const parentRecord = yield* storage.findDocument(
          { kind: V1.definition.kind, scope: inConversation(parentId) },
          'current',
        )
        expect(parentRecord).toBeDefined()
        const parentStored =
          parentRecord === undefined
            ? undefined
            : yield* storage.document(parentRecord.id, 'current')
        expect(parentStored?.version).toBe(1)
      }),
    ))

  test('copies the incarnation alive at the fork point across retirement and recreation', () =>
    run(({ session }) =>
      Effect.gen(function* () {
        const Doc = Document.make<{ value: string }>({
          kind: 'fork.incarnations',
          version: 1,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: () => Effect.succeed({ value: 'initial' }),
        })
        const parentId = yield* createConversation(session)
        const oldAt = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const entry = yield* tx.appendEntry(parentId, { kind: 'old' })
            yield* set(tx.doc(Doc, parentId), { value: 'old' })
            return entry.id
          }),
        )
        const retiredAt = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const entry = yield* tx.appendEntry(parentId, { kind: 'retired' })
            yield* tx.retireDoc(Doc, parentId)
            return entry.id
          }),
        )
        const newAt = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const entry = yield* tx.appendEntry(parentId, { kind: 'new' })
            yield* set(tx.doc(Doc, parentId), { value: 'new' })
            return entry.id
          }),
        )

        const oldChild = yield* session.commit((tx) =>
          tx.forkConversation(parentId, oldAt, ownerless),
        )
        const emptyChild = yield* session.commit((tx) =>
          tx.forkConversation(parentId, retiredAt, ownerless),
        )
        const newChild = yield* session.commit((tx) =>
          tx.forkConversation(parentId, newAt, ownerless),
        )
        expect(yield* session.snapshot(Doc, oldChild.id)).toEqual({ value: 'old' })
        expect(yield* session.snapshot(Doc, emptyChild.id)).toBeUndefined()
        expect(yield* session.snapshot(Doc, newChild.id)).toEqual({ value: 'new' })
      }),
    ))

  test('rejects invisible fork points before admission and remains usable', () =>
    run(({ session, storage, commits }) =>
      Effect.gen(function* () {
        const rootId = yield* createConversation(session)
        const { visible, hidden } = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const first = yield* tx.appendEntry(rootId, { kind: 'visible' })
            const second = yield* tx.appendEntry(rootId, { kind: 'hidden' })
            return { visible: first.id, hidden: second.id }
          }),
        )
        const parent = yield* session.commit((tx) =>
          tx.forkConversation(rootId, visible, ownerless),
        )
        const admitted = commits.length
        const failure = yield* Effect.scoped(
          Effect.gen(function* () {
            const subscription = yield* session.subscribe
            const error = yield* Effect.flip(
              session.commit((tx) => tx.forkConversation(parent.id, hidden, ownerless)),
            )
            const independent = yield* session.commit((tx) => tx.createConversation(ownerless))
            // The rejected fork published nothing: the next publication is the independent one.
            const next = yield* PubSub.take(subscription)
            return { error, independent, next }
          }),
        )
        expect(failure.error).toBeInstanceOf(SessionError.TransactionError)
        expect(failure.error.message).toContain(`Entry ${hidden} is not visible`)
        expect(commits).toHaveLength(admitted + 1)
        expect(failure.next.changes).toEqual([{ type: 'conversation', value: failure.independent }])
        expect(yield* storage.conversation(failure.independent.id)).toEqual(failure.independent)
      }),
    ))

  test('rejects duplicate as-of and current selections for one child address before admission', () =>
    run(({ session, storage, commits }) =>
      Effect.gen(function* () {
        const AsOf = Document.make<{ value: string }>({
          kind: 'fork.duplicate-policy',
          version: 1,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: () => Effect.succeed({ value: 'as-of' }),
        })
        const Current = Document.make<{ value: string }>({
          kind: 'fork.duplicate-policy',
          version: 1,
          scope: 'conversation',
          history: 'latest',
          fork: 'current',
          initial: () => Effect.succeed({ value: 'current' }),
        })
        const parentId = yield* createConversation(session)
        const oldAt = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const entry = yield* tx.appendEntry(parentId, { kind: 'old' })
            yield* tx.doc(AsOf, parentId)
            return entry.id
          }),
        )
        yield* session.commit((tx) => tx.retireDoc(AsOf, parentId))
        yield* session.commit((tx) => Effect.asVoid(tx.doc(Current, parentId)))
        const admitted = commits.length
        const error = yield* Effect.flip(
          session.commit((tx) => tx.forkConversation(parentId, oldAt, ownerless)),
        )
        expect(error).toBeInstanceOf(SessionError.TransactionError)
        expect(error.message).toContain('Fork selects multiple source documents')
        expect(commits).toHaveLength(admitted)
        const independent = yield* session.commit((tx) => tx.createConversation(ownerless))
        expect(yield* storage.conversation(independent.id)).toEqual(independent)
      }),
    ))

  test('rejects current and as-of source writes in the fork transaction', () =>
    run(({ session, commits }) =>
      Effect.gen(function* () {
        const Current = Document.make<{ value: string }>({
          kind: 'fork.same-transaction-current',
          version: 1,
          scope: 'conversation',
          history: 'latest',
          fork: 'current',
          initial: () => Effect.succeed({ value: 'committed' }),
        })
        const AsOf = Document.make<{ value: string }>({
          kind: 'fork.same-transaction-as-of',
          version: 1,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: () => Effect.succeed({ value: 'committed' }),
        })
        const parentId = yield* createConversation(session)
        const forkAt = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const entry = yield* tx.appendEntry(parentId, { kind: 'point' })
            yield* tx.doc(Current, parentId)
            yield* tx.doc(AsOf, parentId)
            return entry.id
          }),
        )
        const admitted = commits.length
        const attempts = [
          (tx: Session.Tx) =>
            Effect.gen(function* () {
              yield* set(tx.doc(Current, parentId), { value: 'before-fork' })
              yield* tx.forkConversation(parentId, forkAt, ownerless)
            }),
          (tx: Session.Tx) =>
            Effect.gen(function* () {
              yield* tx.forkConversation(parentId, forkAt, ownerless)
              yield* set(tx.doc(Current, parentId), { value: 'after-fork' })
            }),
          (tx: Session.Tx) =>
            Effect.gen(function* () {
              yield* set(tx.doc(AsOf, parentId), { value: 'as-of-write' })
              yield* tx.forkConversation(parentId, forkAt, ownerless)
            }),
        ]
        for (const attempt of attempts) {
          const error = yield* Effect.flip(session.commit(attempt))
          expect(error).toBeInstanceOf(SessionError.TransactionError)
          expect(error.message).toContain('Cannot change fork source document')
        }
        expect(commits).toHaveLength(admitted)
        expect(yield* session.snapshot(Current, parentId)).toEqual({ value: 'committed' })
        const child = yield* session.commit((tx) =>
          tx.forkConversation(parentId, forkAt, ownerless),
        )
        expect(yield* session.snapshot(Current, child.id)).toEqual({ value: 'committed' })
      }),
    ))

  test('rolls every copied base back when later pre-admission assembly fails', () =>
    run(({ session, storage, commits }) =>
      Effect.gen(function* () {
        const Copied = Document.make<{ value: string }>({
          kind: 'fork.rollback.copied',
          version: 1,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: () => Effect.succeed({ value: 'copied' }),
        })
        const Failure = Document.make<{ count: number }>({
          kind: 'fork.rollback.failure',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0 }),
        })
        const parentId = yield* createConversation(session)
        const forkAt = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const entry = yield* tx.appendEntry(parentId, { kind: 'point' })
            yield* tx.doc(Copied, parentId)
            yield* tx.doc(Failure)
            return entry.id
          }),
        )
        const admitted = commits.length
        let childId: Ids.ConversationId | undefined
        const outcome = yield* Effect.scoped(
          Effect.gen(function* () {
            const subscription = yield* session.subscribe
            // pi throws from a checkpoint predicate; here the session document's change fails to
            // prepare (a non-JSON number), which also fails assembly after the fork was staged.
            const error = yield* Effect.flip(
              session.commit((tx) =>
                Effect.gen(function* () {
                  childId = (yield* tx.forkConversation(parentId, forkAt, ownerless)).id
                  yield* set(tx.doc(Failure), { count: Number.NaN })
                }),
              ),
            )
            yield* session.commit((tx) => set(tx.doc(Failure), { count: 2 }))
            const next = yield* PubSub.take(subscription)
            return { error, next }
          }),
        )
        expect(outcome.error).toBeInstanceOf(SessionError.DocumentError)
        // Only the later commit reached Storage and published.
        expect(commits).toHaveLength(admitted + 1)
        expect(documentCopyChanges(outcome.next)).toEqual([])
        expect(childId).toBeDefined()
        if (childId !== undefined) expect(yield* storage.conversation(childId)).toBeUndefined()
        expect(yield* session.snapshot(Failure)).toEqual({ count: 2 })
        expect(yield* session.snapshot(Copied, parentId)).toEqual({ value: 'copied' })
      }),
    ))

  test('fails the Session on a rejected fork commit, which leaves no effect', () =>
    run(({ session, storage, failNextCommit }) =>
      Effect.gen(function* () {
        const Doc = Document.make<{ value: string }>({
          kind: 'fork.storage-rejected',
          version: 1,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: () => Effect.succeed({ value: 'source' }),
        })
        const parentId = yield* createConversation(session)
        const forkAt = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const entry = yield* tx.appendEntry(parentId, { kind: 'point' })
            yield* tx.doc(Doc, parentId)
            return entry.id
          }),
        )
        let rejectedChildId: Ids.ConversationId | undefined
        const rejection = new StorageError.StorageError({ message: 'copy rejected' })
        failNextCommit(rejection)
        const error = yield* Effect.flip(
          session.commit((tx) =>
            Effect.map(tx.forkConversation(parentId, forkAt, ownerless), ({ id }) => {
              rejectedChildId = id
            }),
          ),
        )
        expect(error).toBeInstanceOf(SessionError.SessionFailed)
        expect(error).toMatchObject({ cause: rejection })
        const later = yield* Effect.flip(session.commit((tx) => tx.createConversation(ownerless)))
        expect(later).toBeInstanceOf(SessionError.SessionFailed)
        expect(later).toMatchObject({ cause: rejection })
        expect(yield* session.ended).toEqual({ reason: 'failed', error: rejection })
        expect(rejectedChildId).toBeDefined()
        if (rejectedChildId !== undefined) {
          expect(yield* storage.conversation(rejectedChildId)).toBeUndefined()
        }
      }),
    ))

  test('retires a copied document and can recreate the address in the fork transaction', () =>
    run(({ session, commits }) =>
      Effect.gen(function* () {
        const Doc = Document.make<{ value: string }>({
          kind: 'fork.retire-recreate',
          version: 1,
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
          initial: () => Effect.succeed({ value: 'fresh' }),
        })
        const parentId = yield* createConversation(session)
        const forkAt = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const entry = yield* tx.appendEntry(parentId, { kind: 'point' })
            yield* set(tx.doc(Doc, parentId), { value: 'copied' })
            return entry.id
          }),
        )
        const child = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const created = yield* tx.forkConversation(parentId, forkAt, ownerless)
            yield* tx.retireDoc(Doc, created.id)
            yield* set(tx.doc(Doc, created.id), { value: 'replacement' })
            return created
          }),
        )
        const writes = commits.at(-1) ?? []
        expect(writes.filter((write) => write.type === 'document.copy')).toHaveLength(1)
        expect(writes.filter((write) => write.type === 'document.create')).toHaveLength(1)
        expect(writes.filter((write) => write.type === 'document.retire')).toHaveLength(1)
        expect(yield* session.snapshot(Doc, child.id)).toEqual({ value: 'replacement' })
      }),
    ))

  test('copies only conversation documents, leaving Session and task documents in their original scopes', () =>
    run(({ session, commits }) =>
      Effect.gen(function* () {
        const Copied = Document.make<{ value: string }>({
          kind: 'fork.scope.conversation',
          version: 1,
          scope: 'conversation',
          history: 'latest',
          fork: 'current',
          initial: () => Effect.succeed({ value: 'conversation' }),
        })
        const SessionOnly = Document.make<{ value: string }>({
          kind: 'fork.scope.session',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ value: 'session' }),
        })
        const TaskOnly = Document.make<{ value: string }>({
          kind: 'fork.scope.task',
          version: 1,
          scope: 'task',
          initial: () => Effect.succeed({ value: 'task' }),
        })
        const parentId = yield* createConversation(session)
        const { forkAt, taskId } = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const entry = yield* tx.appendEntry(parentId, { kind: 'point' })
            const task = yield* tx.createTask(
              Work,
              { path: 'a' },
              { ownership: { kind: 'conversation' }, conversationId: parentId },
            )
            yield* tx.doc(Copied, parentId)
            yield* tx.doc(SessionOnly)
            yield* tx.doc(TaskOnly, task)
            return { forkAt: entry.id, taskId: task }
          }),
        )
        const { result: child, publication } = yield* published(session, (tx) =>
          tx.forkConversation(parentId, forkAt, ownerless),
        )
        const copies = documentCopies(commits.at(-1) ?? [])
        expect(copies.map((write) => write.record.kind)).toEqual([Copied.definition.kind])
        expect(documentCopyChanges(publication).map((change) => change.record.kind)).toEqual([
          Copied.definition.kind,
        ])
        expect(yield* session.snapshot(Copied, child.id)).toEqual({ value: 'conversation' })
        expect(yield* session.snapshot(SessionOnly)).toEqual({ value: 'session' })
        expect(yield* session.snapshot(TaskOnly, taskId)).toEqual({ value: 'task' })
      }),
    ))

  test('copies every family member across storage scan pages', () =>
    run(({ session, commits }) =>
      Effect.gen(function* () {
        const Family = Document.makeFamily<{ value: number }, number>({
          kind: 'fork.pagination',
          version: 1,
          family: true,
          scope: 'conversation',
          history: 'latest',
          fork: 'current',
          initial: (seed) => Effect.succeed({ value: seed }),
        })
        const parentId = yield* createConversation(session)
        const forkAt = yield* appendEntry(session, parentId, 'point')
        yield* session.commit((tx) =>
          Effect.forEach(
            Array.from({ length: 260 }, (_, index) => index),
            (index) => tx.doc(Family, parentId, `member-${index}`, index),
            { concurrency: 'unbounded', discard: true },
          ),
        )
        const child = yield* session.commit((tx) =>
          tx.forkConversation(parentId, forkAt, ownerless),
        )
        expect(documentCopies(commits.at(-1) ?? [])).toHaveLength(260)
        expect(yield* session.snapshot(Family, child.id, 'member-0')).toEqual({ value: 0 })
        expect(yield* session.snapshot(Family, child.id, 'member-259')).toEqual({ value: 259 })
      }),
    ))
})
