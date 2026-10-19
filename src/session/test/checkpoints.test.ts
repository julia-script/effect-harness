/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
import { describe, expect, expectTypeOf, test } from 'bun:test'
import { Data, Effect, PubSub } from 'effect'
import type { Document as Stored } from '../../storage/schema/index.ts'
import type { Json, Write } from '../../storage/schema/index.ts'
import { Document } from '../index.ts'
import type { Session, SessionError } from '../index.ts'
import { published, runSession } from './support.ts'

type StorageWrite = Write.StorageWrite

/** The document writes of one committed batch. */
const documentWrites = (writes: ReadonlyArray<StorageWrite> | undefined) =>
  (writes ?? []).filter(
    (write) =>
      write.type === 'document.create' ||
      write.type === 'document.change' ||
      write.type === 'document.retire',
  )

/** The document changes of one publication. */
const documentChanges = (publication: Session.CommitPublication) =>
  publication.changes.filter(
    (change): change is Extract<Session.CommitChange, { readonly type: 'document' }> =>
      change.type === 'document',
  )

/** The content kind of every document change of every batch. */
const changeKinds = (commits: ReadonlyArray<ReadonlyArray<StorageWrite>>) =>
  commits.flatMap((writes) =>
    writes.flatMap((write) => (write.type === 'document.change' ? [write.content.kind] : [])),
  )

/** A checkpoint predicate's own typed failure. */
class CheckpointFailed extends Data.TaggedError('CheckpointFailed')<{ readonly message: string }> {}

interface Call {
  readonly value: Readonly<Json.JsonObject>
  readonly ops: ReadonlyArray<Stored.Op>
}

describe('Session document checkpoints', () => {
  test('calls checkpointWhen with its definition as the receiver', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        const Doc = Document.make<{ count: number }>({
          kind: 'checkpoint.receiver',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0 }),
          checkpointWhen(value) {
            return Effect.map(this.initial(), (initial) => value.count === initial.count + 2)
          },
        })
        for (let count = 0; count <= 2; count++) {
          yield* session.commit((tx) =>
            Effect.map(tx.doc(Doc), (draft) => {
              draft.count = count
            }),
          )
        }
        expect(changeKinds(recorder.commits)).toEqual(['delta', 'base'])
      }),
    ))

  test('selects bases only for nonempty ordinary batches and passes the exact prepared revision and ops', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        const calls: Array<Call> = []
        const falseCalls: Array<Call> = []
        const BaseDoc = Document.make<{ items: Array<string> }>({
          kind: 'checkpoint.base',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ items: [] }),
          checkpointWhen: (value, ops) =>
            Effect.sync(() => {
              calls.push({ value, ops })
              return true
            }),
        })
        const DeltaDoc = Document.make<{ count: number }>({
          kind: 'checkpoint.delta',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0 }),
          checkpointWhen: (value, ops) =>
            Effect.sync(() => {
              falseCalls.push({ value, ops })
              return false
            }),
        })
        const DefaultDoc = Document.make<{ count: number }>({
          kind: 'checkpoint.default',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0 }),
        })

        yield* session.commit((tx) =>
          Effect.all([tx.doc(BaseDoc), tx.doc(DeltaDoc), tx.doc(DefaultDoc)]),
        )
        expect(calls).toHaveLength(0)
        expect(falseCalls).toHaveLength(0)
        const created = documentWrites(recorder.commits.at(-1))
        expect(created).toHaveLength(3)
        expect(
          created.every(
            (write) => write.type === 'document.create' && write.content.kind === 'base',
          ),
        ).toBe(true)

        const { publication } = yield* published(session, (tx) =>
          Effect.gen(function* () {
            ;(yield* tx.doc(BaseDoc)).items.push('x')
            ;(yield* tx.doc(DeltaDoc)).count++
            ;(yield* tx.doc(DefaultDoc)).count++
          }),
        )
        const writes = (recorder.commits.at(-1) ?? []).filter(
          (write) => write.type === 'document.change',
        )
        expect(writes.map((write) => write.content.kind)).toEqual(['base', 'delta', 'delta'])
        expect(calls).toHaveLength(1)
        expect(falseCalls).toHaveLength(1)
        const snapshot = yield* session.snapshot(BaseDoc)
        const deltaSnapshot = yield* session.snapshot(DeltaDoc)
        const [publishedBase, publishedDelta] = documentChanges(publication)
        expect(calls[0]?.value).toBe(snapshot)
        expect(calls[0]?.ops).toBe(publishedBase?.ops)
        expect(falseCalls[0]?.value).toBe(deltaSnapshot)
        expect(falseCalls[0]?.ops).toBe(publishedDelta?.ops)
        const [base, delta] = writes
        expect(delta?.content.kind === 'delta' ? delta.content.ops : undefined).toBe(
          falseCalls[0]?.ops,
        )
        expect(base?.content.kind === 'base' ? base.content.value : undefined).toBe(snapshot)
      }),
    ))

  test('passes the stored delta count since the newest base, including after unload and version bases', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        const seen: Array<number> = []
        const V1 = Document.make<{ count: number }>({
          kind: 'checkpoint.deltas-since-base',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0 }),
          checkpointWhen: (_value, _ops, info) =>
            Effect.sync(() => {
              seen.push(info.deltasSinceBase)
              return info.deltasSinceBase >= 2
            }),
        })
        const V2 = Document.make<{ count: number }>({
          kind: 'checkpoint.deltas-since-base',
          version: 2,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0 }),
          migrate: (value) => Effect.succeed({ count: Number(value.count) }),
          checkpointWhen: (_value, _ops, info) =>
            Effect.sync(() => {
              seen.push(info.deltasSinceBase)
              return false
            }),
        })
        const increment = (token: typeof V1) =>
          session.commit((tx) =>
            Effect.map(tx.doc(token), (draft) => {
              draft.count++
            }),
          )
        yield* session.commit((tx) => tx.doc(V1))
        yield* increment(V1)
        yield* increment(V1)
        yield* increment(V1)
        yield* session.unloadDocuments
        yield* increment(V1)
        expect(seen).toEqual([0, 1, 2, 0])
        const kinds = recorder.commits.slice(-4).map((writes) => {
          const write = writes[0]
          return write?.type === 'document.change' ? write.content.kind : write?.type
        })
        expect(kinds).toEqual(['delta', 'delta', 'base', 'delta'])

        // A required version base resets the count without calling the predicate.
        yield* increment(V2)
        yield* increment(V2)
        expect(seen).toEqual([0, 1, 2, 0, 0])
        expect(recorder.commits.at(-2)?.[0]).toMatchObject({
          content: { kind: 'base', version: 2 },
        })
      }),
    ))

  test('skips the predicate for empty batches but calls it for nonempty structural no-ops', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        let calls = 0
        const Doc = Document.make<{ items: Array<string> }>({
          kind: 'checkpoint.no-op',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ items: ['a', 'b'] }),
          checkpointWhen: () =>
            Effect.sync(() => {
              calls++
              return false
            }),
        })
        yield* session.commit((tx) => tx.doc(Doc))
        const commits = recorder.commits.length

        yield* session.commit((tx) =>
          Effect.map(tx.doc(Doc), (value) => {
            value.items.push('x')
            value.items.pop()
          }),
        )
        expect(calls).toBe(0)
        expect(recorder.commits).toHaveLength(commits)

        yield* session.commit((tx) =>
          Effect.map(tx.doc(Doc), (value) => {
            const first = value.items.shift()
            if (first !== undefined) value.items.unshift(first)
          }),
        )
        expect(calls).toBe(1)
        expect(recorder.commits.at(-1)?.[0]).toMatchObject({
          type: 'document.change',
          content: { kind: 'delta' },
        })
      }),
    ))

  test('rolls back every prepared document when a checkpoint predicate fails', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        let failCheckpoint = true
        let firstCalls = 0
        const First = Document.make<{ count: number }>({
          kind: 'checkpoint.rollback.first',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0 }),
          checkpointWhen: () =>
            Effect.sync(() => {
              firstCalls++
              return false
            }),
        })
        const Second = Document.make<{ count: number }, CheckpointFailed>({
          kind: 'checkpoint.rollback.second',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0 }),
          checkpointWhen: () =>
            failCheckpoint
              ? Effect.fail(new CheckpointFailed({ message: 'checkpoint failed' }))
              : Effect.succeed(false),
        })
        yield* session.commit((tx) => Effect.all([tx.doc(First), tx.doc(Second)]))
        const first = yield* session.snapshot(First)
        const second = yield* session.snapshot(Second)
        const commits = recorder.commits.length
        const subscription = yield* session.subscribe

        const commit = session.commit((tx) =>
          Effect.gen(function* () {
            ;(yield* tx.doc(First)).count = 1
            ;(yield* tx.doc(Second)).count = 2
          }),
        )
        // The definition's error type reaches the commit's error channel.
        expectTypeOf<Effect.Error<typeof commit>>().toExtend<
          SessionError.CommitError | CheckpointFailed
        >()
        expectTypeOf<CheckpointFailed>().toExtend<Effect.Error<typeof commit>>()
        const error = yield* Effect.flip(commit)
        expect(error).toBeInstanceOf(CheckpointFailed)
        expect(error.message).toBe('checkpoint failed')
        expect(firstCalls).toBe(1)
        expect(recorder.commits).toHaveLength(commits)
        expect(yield* PubSub.remaining(subscription)).toBe(0)
        expect(yield* session.snapshot(First)).toBe(first)
        expect(yield* session.snapshot(Second)).toBe(second)

        failCheckpoint = false
        yield* session.commit((tx) =>
          Effect.gen(function* () {
            ;(yield* tx.doc(First)).count = 3
            ;(yield* tx.doc(Second)).count = 4
          }),
        )
        expect(yield* session.snapshot(First)).toEqual({ count: 3 })
        expect(yield* session.snapshot(Second)).toEqual({ count: 4 })
      }),
    ))

  test('persists repeated false decisions as deltas and replays the complete tail', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        let calls = 0
        const Doc = Document.make<{ values: Array<number> }>({
          kind: 'checkpoint.tail',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ values: [] }),
          checkpointWhen: () =>
            Effect.sync(() => {
              calls++
              return false
            }),
        })
        yield* session.commit((tx) => tx.doc(Doc))
        for (let value = 1; value <= 8; value++) {
          yield* session.commit((tx) =>
            Effect.map(tx.doc(Doc), (draft) => {
              draft.values.push(value)
            }),
          )
        }
        const kinds = changeKinds(recorder.commits)
        expect(kinds).toHaveLength(8)
        expect(kinds.every((kind) => kind === 'delta')).toBe(true)
        expect(calls).toBe(8)
        yield* session.unloadDocuments
        expect(yield* session.snapshot(Doc)).toEqual({ values: [1, 2, 3, 4, 5, 6, 7, 8] })
      }),
    ))

  test('keeps a prepared root replacement as a delta when the predicate is false', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        const initial: Record<string, number> = {}
        for (let index = 0; index < 4100; index++) initial[`field${index}`] = 0
        const Doc = Document.make<Record<string, number>>({
          kind: 'checkpoint.root-replacement',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed(initial),
          checkpointWhen: () => Effect.succeed(false),
        })
        yield* session.commit((tx) => tx.doc(Doc))
        yield* session.commit((tx) =>
          Effect.map(tx.doc(Doc), (value) => {
            for (let index = 0; index < 4100; index++) value[`field${index}`] = 1
          }),
        )
        const write = recorder.commits.at(-1)?.[0]
        expect(write?.type).toBe('document.change')
        const ops =
          write?.type === 'document.change' && write.content.kind === 'delta'
            ? write.content.ops
            : undefined
        expect(ops).toHaveLength(1)
        expect(ops?.[0]?.[0]).toBe('r')
        yield* session.unloadDocuments
        expect((yield* session.snapshot(Doc))?.field4099).toBe(1)
      }),
    ))

  test('uses ordinary checkpoint selection before retirement', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        let calls = 0
        const Doc = Document.make<{ count: number }>({
          kind: 'checkpoint.retire',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0 }),
          checkpointWhen: () =>
            Effect.sync(() => {
              calls++
              return true
            }),
        })
        yield* session.commit((tx) => tx.doc(Doc))
        yield* session.commit((tx) =>
          Effect.gen(function* () {
            ;(yield* tx.doc(Doc)).count = 1
            yield* tx.retireDoc(Doc)
          }),
        )
        expect(calls).toBe(1)
        const writes = documentWrites(recorder.commits.at(-1)).map((write) =>
          write.type === 'document.retire' ? [write.type] : [write.type, write.content.kind],
        )
        expect(writes).toEqual([['document.change', 'base'], ['document.retire']])
      }),
    ))
})
