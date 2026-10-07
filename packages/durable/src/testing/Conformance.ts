import * as Effect from 'effect/Effect'
import * as Scope from 'effect/Scope'
import * as Exit from 'effect/Exit'
import { ResourceScope } from './Storage.ts'
import * as Schema from 'effect/Schema'
import * as Document from '../Document.ts'
import * as Record from '../Record.ts'
import * as Session from '../Session.ts'
import { Store, mintId } from '../Store.ts'
import { rejected, StorageError } from '../StorageError.ts'
import type { Assertions, Case } from './Storage.ts'

const root = Record.ROOT_CONVERSATION_ID
const cid = Schema.decodeSync(Record.ConversationId)
const eid = Schema.decodeSync(Record.EntryId)
const sid = Schema.decodeSync(Record.SubmissionId)
const tid = Schema.decodeSync(Record.TaskId)
const did = Schema.decodeSync(Record.DocumentId)
const seq = Schema.decodeSync(Record.Seq)
const pending = (id: Record.TaskId, conversationId = root): Record.Task => ({
  id,
  conversationId,
  kind: 'task',
  version: 1,
  input: {},
  background: false,
  abortRequested: false,
  state: { status: 'pending' },
})
const document = (
  id: number,
  extra: Partial<Record.DocumentCreate> = {},
): Record.DocumentCreate => ({
  id: did(id),
  kind: 'doc',
  scope: { kind: 'conversation', conversationId: root },
  history: 'rewindable',
  fork: 'asOf',
  ...extra,
})
const counter = Document.defineUnsafe({
  kind: 'counter',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ count: Schema.Finite.pipe(Schema.mutableKey) }),
  initial: () => ({ count: 0 }),
})
const historical = Document.defineUnsafe({
  ...counter.definition,
  kind: 'history',
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
})
const current = Document.defineUnsafe({
  ...historical.definition,
  kind: 'current',
  history: 'latest',
  fork: 'current',
})
const initial = Document.defineUnsafe({
  ...historical.definition,
  kind: 'initial',
  fork: 'initial',
})
const failure = <A, E, R>(effect: Effect.Effect<A, E, R>) => effect.pipe(Effect.flip, Effect.orDie)

/** Storage semantics shared by every backend, independent from the selected test runner. */
export const createStorageConformance = (assert: Assertions): ReadonlyArray<Case> => {
  const cases: Array<Case> = []
  const test = (
    name: string,
    run: Effect.Effect<void, StorageError, Store | Session.Session | ResourceScope>,
  ) => {
    cases.push({ name, run })
  }
  test(
    'reserves root 1, creates it lazily, initializes once atomically',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      assert.deepStrictEqual((yield* store.read).conversations, [])
      assert.strictEqual(yield* mintId(Record.EntryId), 2)
      let calls = 0
      const initialize = (tx: Session.Transaction) =>
        Effect.gen(function* () {
          calls++
          const draft = yield* tx.doc(counter)
          draft.count = 7
        })
      assert.deepStrictEqual(yield* session.root(initialize), { id: root })
      yield* session.root(initialize)
      assert.strictEqual(calls, 1)
      assert.strictEqual((yield* session.snapshot(counter))?.value.count, 7)
      assert.strictEqual(
        (yield* failure(
          store.commit([
            { type: 'entry', value: { id: eid(1), conversationId: root, kind: 'bad' } },
          ]),
        )).certainty,
        'rejected',
      )
    }),
  )
  test(
    'rolls mixed writes back, including seq and indexes',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      yield* store.commit([
        { type: 'task', value: pending(tid(2)) },
        {
          type: 'submission',
          value: {
            id: sid(3),
            conversationId: root,
            type: 'input',
            status: 'queued',
            requestId: 'r',
          },
        },
      ])
      const before = yield* store.read
      yield* failure(
        store.commit([
          { type: 'task', value: { ...pending(tid(2)), state: { status: 'running' } } },
          { type: 'entry', value: { id: eid(4), conversationId: root, kind: 'transient' } },
          { type: 'conversation', value: { id: root } },
        ]),
      )
      assert.deepStrictEqual(yield* store.read, before)
      assert.strictEqual(yield* session.entry(eid(4)), undefined)
      assert.strictEqual((yield* session.scanTasks({ status: 'pending' }, 10)).items.length, 1)
    }),
  )
  test(
    'detaches writes, state reads and returned records, preserving prototype keys',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      const fixture = '{"__proto__":{"bad":1},"constructor":[1,2],"prototype":"ok"}'
      const parse = Effect.try({
        try: () => JSON.parse(fixture),
        catch: (cause) => rejected('Invalid conformance JSON fixture', undefined, cause),
      })
      const data = yield* parse
      yield* store.commit([
        { type: 'entry', value: { id: eid(2), conversationId: root, kind: 'data', data } },
      ])
      data.constructor.push(3)
      const first = yield* session.entry(eid(2))
      assert.ok(first)
      assert.deepStrictEqual(first.entry.data, yield* parse)
      assert.strictEqual(Object.getPrototypeOf(first.entry.data), Object.prototype)
      const state = yield* store.read
      Reflect.set(state.entries[0]?.entry ?? {}, 'kind', 'mutated')
      assert.strictEqual((yield* session.entry(eid(2)))?.entry.kind, 'data')
    }),
  )
  test(
    'indexes entries out of id order, head markers and stable descending cursors',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      yield* store.commit(
        [8, 3, 6, 2].map((id) => ({
          type: 'entry' as const,
          value: {
            id: eid(id),
            conversationId: root,
            kind: 'e',
            ...(id === 3 ? { head: eid(3) } : {}),
          },
        })),
      )
      const first = yield* session.scanEntries({ conversationId: root }, 2)
      assert.deepStrictEqual(
        first.items.map((e) => e.id),
        [8, 6],
      )
      yield* store.commit([
        { type: 'entry', value: { id: eid(9), conversationId: root, kind: 'new' } },
      ])
      assert.deepStrictEqual(
        (yield* session.scanEntries({ conversationId: root }, 10, first.next)).items.map(
          (e) => e.id,
        ),
        [3, 2],
      )
      assert.deepStrictEqual(
        (yield* session.scanEntries(
          { conversationId: root, minEntryId: eid(3), maxEntryId: eid(6) },
          10,
        )).items.map((e) => e.id),
        [6, 3],
      )
      assert.strictEqual(
        yield* session.transaction((tx) =>
          tx.latestHeadMarker(root).pipe(Effect.map((entry) => entry?.id ?? 0)),
        ),
        3,
      )
      assert.strictEqual((yield* session.latestHeadMarker(root, eid(8)))?.id, 3)
      assert.strictEqual(yield* session.latestHeadMarker(root, eid(2)), undefined)
    }),
  )
  test(
    'pages conversations and owner edges in ascending id order',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      yield* store.commit([
        { type: 'task', value: pending(tid(2)) },
        ...[8, 5, 3].map((id) => ({
          type: 'conversation' as const,
          value: {
            id: cid(id),
            ...(id === 5 ? {} : { owner: { conversationId: root, taskId: tid(2) } }),
          },
        })),
      ])
      const first = yield* session.scanConversations({}, 2)
      assert.deepStrictEqual(
        first.items.map((c) => c.id),
        [1, 3],
      )
      assert.deepStrictEqual(
        (yield* session.scanConversations({}, 10, first.next)).items.map((c) => c.id),
        [5, 8],
      )
      assert.deepStrictEqual(
        (yield* session.scanConversations(
          { ownerTaskId: tid(2), ownerConversationId: root },
          10,
        )).items.map((c) => c.id),
        [3, 8],
      )
    }),
  )
  test(
    'scans deep fork ancestry with every ancestor cap and raw commit seq',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      const first = yield* store.commit([
        { type: 'entry', value: { id: eid(2), conversationId: root, kind: 'a' } },
        { type: 'entry', value: { id: eid(3), conversationId: root, kind: 'hidden' } },
      ])
      yield* store.commit([
        {
          type: 'conversation',
          value: { id: cid(4), parent: { conversationId: root, at: eid(2) } },
        },
        { type: 'entry', value: { id: eid(5), conversationId: cid(4), kind: 'b' } },
        { type: 'entry', value: { id: eid(6), conversationId: cid(4), kind: 'hidden' } },
        {
          type: 'conversation',
          value: { id: cid(7), parent: { conversationId: cid(4), at: eid(5) } },
        },
        { type: 'entry', value: { id: eid(8), conversationId: cid(7), kind: 'c' } },
      ])
      assert.deepStrictEqual(
        (yield* session.scanEntries({ conversationId: cid(7) }, 10)).items.map((e) => e.id),
        [8, 5, 2],
      )
      assert.strictEqual(yield* session.entry(eid(3), cid(7)), undefined)
      assert.strictEqual((yield* session.entry(eid(2), cid(7)))?.commitSeq, first)
    }),
  )
  test(
    'replaces complete task records and filters waiting/completing/background/abort flags',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      yield* store.commit(
        [2, 3, 4].map((id) => ({
          type: 'task' as const,
          value: {
            ...pending(tid(id)),
            background: id === 3,
            abortRequested: id === 4,
            state: { status: id === 2 ? ('waiting' as const) : ('completing' as const) },
            ...(id === 2 ? { owner: tid(3), memos: { old: 1 } } : {}),
          },
        })),
      )
      assert.strictEqual(
        (yield* session.scanTasks({ status: 'waiting', conversationId: root }, 10)).items[0]?.id,
        2,
      )
      assert.strictEqual((yield* session.scanTasks({ background: true }, 10)).items[0]?.id, 3)
      assert.strictEqual(
        (yield* session.scanTasks({ abortRequested: true, status: 'completing' }, 10)).items[0]?.id,
        4,
      )
      yield* store.commit([{ type: 'task', value: pending(tid(2)) }])
      assert.strictEqual((yield* session.task(tid(2)))?.owner, undefined)
      assert.strictEqual((yield* session.task(tid(2)))?.memos, undefined)
    }),
  )
  test(
    'preserves input and write submission lifecycles and stable string request identities',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      const ids = yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          const input = yield* tx.createSubmission({
            conversationId: root,
            type: 'input',
            status: 'queued',
            requestId: '\ud800',
          })
          const write = yield* tx.createSubmission({
            conversationId: root,
            type: 'write',
            status: 'queued',
            requestId: '\ud801',
          })
          yield* tx.placeSubmission(input.id, eid(100))
          yield* tx.placeSubmission(write.id, eid(101))
          yield* tx.settleSubmission(input.id, { status: 'done', answer: eid(102) })
          yield* tx.placeSubmission(write.id, eid(999))
          return [input.id, write.id]
        }),
      )
      assert.strictEqual((yield* session.scanSubmissions({ status: 'done' }, 10)).items.length, 2)
      assert.strictEqual((yield* store.read).submissions.find((s) => s.id === ids[1])?.entry, 101)
      assert.strictEqual(
        yield* session.transaction((tx) =>
          tx.submissionByRequest(root, '\ud800').pipe(Effect.map((s) => s?.answer ?? 0)),
        ),
        102,
      )
      assert.strictEqual(
        yield* session.transaction((tx) =>
          tx.submissionByRequest(root, '\ud801').pipe(Effect.map((s) => s?.answer ?? 0)),
        ),
        0,
      )
    }),
  )
  test(
    'rejects invalid submission variants and indexes complete replacements by scoped request identity',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      yield* store.commit([
        { type: 'conversation', value: { id: cid(2) } },
        {
          type: 'submission',
          value: {
            id: sid(3),
            conversationId: root,
            type: 'input',
            status: 'queued',
            requestId: 'same',
          },
        },
        {
          type: 'submission',
          value: {
            id: sid(4),
            conversationId: cid(2),
            type: 'write',
            status: 'queued',
            requestId: 'same',
          },
        },
      ])
      assert.strictEqual((yield* session.submissionByRequest(root, 'same'))?.id, 3)
      assert.strictEqual((yield* session.submissionByRequest(cid(2), 'same'))?.id, 4)
      const before = yield* store.read
      for (const invalid of [
        { type: 'write', status: 'placed', entry: eid(9) },
        { type: 'write', status: 'done', entry: eid(9), answer: eid(10) },
        { type: 'input', status: 'done', entry: eid(9) },
        { type: 'input', status: 'queued', entry: eid(9) },
      ]) {
        // Deliberate JavaScript boundary misuse must still be rejected by the public Store.
        const write: unknown = {
          type: 'submission',
          value: { id: sid(5), conversationId: root, ...invalid },
        }
        yield* failure(store.commit([write as Record.Write]))
      }
      assert.deepStrictEqual(yield* store.read, before)
      yield* store.commit([
        {
          type: 'submission',
          value: {
            id: sid(3),
            conversationId: root,
            type: 'input',
            status: 'placed',
            entry: eid(9),
            requestId: 'other',
          },
        },
      ])
      assert.strictEqual(yield* session.submissionByRequest(root, 'same'), undefined)
      assert.strictEqual((yield* session.submissionByRequest(root, 'other'))?.entry, 9)
      const page = yield* session.scanSubmissions({}, 1)
      assert.strictEqual(page.items[0]?.id, 3)
      assert.strictEqual((yield* session.scanSubmissions({}, 1, page.next)).items[0]?.id, 4)
      assert.strictEqual(
        (yield* session.scanSubmissions({ conversationId: cid(2), status: 'placed' }, 10)).items
          .length,
        0,
      )
    }),
  )
  test(
    'rewinds documents with bases/deltas/version boundaries and half-open incarnations',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      const creation = yield* store.commit([
        {
          type: 'document.create',
          record: document(2),
          content: { kind: 'base', version: 1, value: { n: 0 } },
        },
      ])
      const update = yield* store.commit([
        {
          type: 'document.change',
          id: did(2),
          content: { kind: 'delta', version: 1, ops: [['set', ['n'], 1]] },
        },
      ])
      const replacement = yield* store.commit([
        {
          type: 'document.change',
          id: did(2),
          content: { kind: 'base', version: 2, value: { n: 2 } },
        },
      ])
      assert.strictEqual((yield* session.document(did(2), creation))?.value.n, 0)
      assert.strictEqual((yield* session.document(did(2), update))?.value.n, 1)
      assert.strictEqual((yield* session.document(did(2), replacement))?.version, 2)
      const retired = yield* store.commit([
        { type: 'document.retire', id: did(2) },
        {
          type: 'document.create',
          record: document(3),
          content: { kind: 'base', version: 2, value: { n: 3 } },
        },
      ])
      assert.strictEqual(yield* session.document(did(2), retired), undefined)
      assert.strictEqual((yield* session.findDocument(document(2), update))?.id, 2)
      assert.strictEqual((yield* session.findDocument(document(2)))?.id, 3)
    }),
  )
  test(
    'streams long delta tails including root replacement and prototype keys',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      yield* store.commit([
        {
          type: 'document.create',
          record: document(2),
          content: { kind: 'base', version: 1, value: { n: 0 } },
        },
      ])
      for (let n = 1; n <= 110; n++)
        yield* store.commit([
          {
            type: 'document.change',
            id: did(2),
            content: {
              kind: 'delta',
              version: 1,
              ops: n === 50 ? [['replace', { n }]] : [['set', ['n'], n]],
            },
          },
        ])
      assert.strictEqual((yield* session.document(did(2)))?.value.n, 110)
      assert.strictEqual((yield* session.document(did(2), seq(2)))?.value.n, 0)
    }),
  )
  test(
    'latest/task/session documents reject rewind and reclaim old bases and retired payloads',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      for (const record of [
        document(2, { history: 'latest', fork: 'current' }),
        { id: did(3), kind: 'sessiondoc', scope: { kind: 'session' as const } },
      ]) {
        yield* store.commit([
          {
            type: 'document.create',
            record,
            content: { kind: 'base', version: 1, value: { n: 1 } },
          },
        ])
        yield* failure(session.document(record.id, seq(2)))
        yield* store.commit([
          {
            type: 'document.change',
            id: record.id,
            content: { kind: 'base', version: 1, value: { n: 2 } },
          },
        ])
        assert.strictEqual(
          (yield* store.read).documents.find((d) => d.record.id === record.id)?.revisions.length,
          1,
        )
        yield* store.commit([{ type: 'document.retire', id: record.id }])
        assert.strictEqual(
          (yield* store.read).documents.find((d) => d.record.id === record.id)?.revisions.length,
          0,
        )
      }
    }),
  )
  test(
    'validates document metadata, content commands and copy sources atomically',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      yield* failure(
        store.commit([
          {
            type: 'document.create',
            record: document(2, { history: 'latest', fork: 'asOf' }),
            content: { kind: 'base', version: 1, value: {} },
          },
        ]),
      )
      yield* failure(
        store.commit([
          {
            type: 'document.create',
            record: document(2),
            content: { kind: 'delta', version: 1, ops: [] },
          },
        ]),
      )
      yield* store.commit([
        {
          type: 'document.create',
          record: document(2),
          content: { kind: 'base', version: 1, value: { n: 0 } },
        },
      ])
      const before = yield* store.read
      yield* failure(
        store.commit([
          { type: 'document.change', id: did(2), content: { kind: 'delta', version: 2, ops: [] } },
        ]),
      )
      yield* failure(
        store.commit([
          { type: 'document.copy', record: document(3), source: { id: did(2), at: 'current' } },
          { type: 'document.retire', id: did(2) },
        ]),
      )
      yield* failure(
        store.commit([
          { type: 'document.change', id: did(2), content: { kind: 'base', version: 1, value: {} } },
          { type: 'document.change', id: did(2), content: { kind: 'base', version: 1, value: {} } },
        ]),
      )
      yield* failure(
        store.commit([
          {
            type: 'document.create',
            record: document(3),
            content: { kind: 'base', version: 1, value: {} },
          },
        ]),
      )
      assert.deepStrictEqual(yield* store.read, before)
    }),
  )
  test(
    'indexes exact document scopes and family/singleton addresses losslessly',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      const records = [
        document(2, { kind: '\ud800' }),
        document(3, { kind: '\ud801' }),
        document(4, { key: '' }),
        document(5, { key: '\ud800' }),
        document(6, { key: '\ud801' }),
        document(7, { scope: { kind: 'conversation', conversationId: cid(100) } }),
      ]
      yield* store.commit(
        records.map((record) => ({
          type: 'document.create' as const,
          record,
          content: { kind: 'base' as const, version: 1, value: { id: record.id } },
        })),
      )
      assert.deepStrictEqual(
        (yield* session.scanDocuments(
          { scope: { kind: 'conversation', conversationId: root }, at: 'current' },
          10,
        )).items.map((d) => d.id),
        [2, 3, 4, 5, 6],
      )
      for (const record of records)
        assert.strictEqual((yield* session.findDocument(record))?.id, record.id)
      assert.strictEqual(
        yield* session.findDocument({
          kind: 'doc',
          scope: { kind: 'conversation', conversationId: root },
        }),
        undefined,
      )
    }),
  )
  test(
    'copies detached historical/current snapshots preserving metadata',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      const created = yield* store.commit([
        {
          type: 'document.create',
          record: document(2),
          content: { kind: 'base', version: 1, value: { n: 1 } },
        },
      ])
      yield* store.commit([
        {
          type: 'document.change',
          id: did(2),
          content: { kind: 'delta', version: 1, ops: [['set', ['n'], 2]] },
        },
      ])
      yield* store.commit([
        {
          type: 'document.copy',
          record: document(3, { scope: { kind: 'conversation', conversationId: cid(100) } }),
          source: { id: did(2), at: created },
        },
      ])
      assert.strictEqual((yield* session.document(did(3)))?.value.n, 1)
      assert.strictEqual((yield* session.document(did(3)))?.record.fork, 'asOf')
    }),
  )
  test(
    'keeps global IDs and allows the last safe ID before exhaustion',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      yield* store.commit([
        { type: 'entry', value: { id: eid(100), conversationId: root, kind: 'explicit' } },
      ])
      assert.strictEqual(yield* mintId(Record.TaskId), 101)
      yield* failure(store.commit([{ type: 'task', value: pending(tid(100)) }]))
      yield* store.commit([
        {
          type: 'entry',
          value: { id: eid(Number.MAX_SAFE_INTEGER), conversationId: root, kind: 'last' },
        },
      ])
      yield* failure(mintId(Record.EntryId))
      yield* failure(mintId(Record.EntryId))
    }),
  )
  test(
    'rolls back callback failures, revokes drafts and rejects table reads after writes',
    Effect.gen(function* () {
      const session = yield* Session.Session
      yield* session.root()
      let draft: { count: number } | undefined
      yield* failure(
        session.transaction(
          Effect.fnUntraced(function* (tx) {
            draft = yield* tx.doc(counter)
            draft.count = 99
            return yield* rejected('no commit')
          }),
        ),
      )
      assert.strictEqual(yield* session.snapshot(counter), undefined)
      assert.throws(() => {
        if (draft !== undefined) draft.count = 1
      }, /revoked/)
      const error = yield* failure(
        session.transaction(
          Effect.fnUntraced(function* (tx) {
            yield* tx.appendEntry(root, { kind: 'e' })
            yield* tx.scanEntries({ conversationId: root }, 10)
            return null
          }),
        ),
      )
      assert.strictEqual(error.reason._tag, 'ReadAfterWrite')
      assert.strictEqual((yield* session.scanEntries({ conversationId: root }, 10)).items.length, 0)
    }),
  )
  test(
    'persisted idempotency receipts replay results without rerunning callbacks',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      let calls = 0
      const execute = () =>
        session.transaction(
          Effect.fnUntraced(function* (tx) {
            calls++
            const entry = yield* tx.appendEntry(root, { kind: 'receipt' })
            const value = yield* tx.doc(counter)
            value.count++
            return { id: entry.id, count: value.count }
          }),
          { key: 'stable\ud800', fingerprint: 'input' },
        )
      const result = yield* execute()
      assert.deepStrictEqual(yield* execute(), result)
      assert.strictEqual(calls, 1)
      assert.strictEqual((yield* store.read).receipts.length, 1)
      assert.strictEqual(
        (yield* failure(
          session.transaction(() => Effect.succeed(null), {
            key: 'stable\ud800',
            fingerprint: 'other',
          }),
        )).reason._tag,
        'Conflict',
      )
      assert.strictEqual((yield* session.snapshot(counter))?.value.count, 1)
    }),
  )
  test(
    'migrates read-only in memory and persists a base on the next commit',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.transaction((tx) => tx.doc(counter).pipe(Effect.as(null)))
      const newer = Document.defineUnsafe({
        ...counter.definition,
        version: 2,
        migrate: (value) => ({ count: Number(value.count) + 1 }),
      })
      assert.strictEqual((yield* session.snapshot(newer))?.value.count, 1)
      assert.strictEqual((yield* store.read).documents[0]?.revisions[0]?.content.version, 1)
      yield* session.transaction((tx) => tx.doc(newer).pipe(Effect.as(null)))
      assert.strictEqual((yield* store.read).documents[0]?.revisions.at(-1)?.content.kind, 'base')
      assert.strictEqual((yield* store.read).documents[0]?.revisions.at(-1)?.content.version, 2)
      yield* failure(session.snapshot(counter))
    }),
  )
  test(
    'retires and recreates fresh document incarnations within one transaction',
    Effect.gen(function* () {
      const session = yield* Session.Session
      yield* session.transaction((tx) => tx.doc(counter).pipe(Effect.as(null)))
      const before = yield* session.snapshot(counter)
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          const old = yield* tx.doc(counter)
          old.count = 1
          yield* tx.retire(counter)
          const replacement = yield* tx.doc(counter)
          replacement.count = 2
          return null
        }),
      )
      const after = yield* session.snapshot(counter)
      assert.ok(before)
      assert.ok(after)
      assert.notStrictEqual(after.record.id, before.record.id)
      assert.strictEqual(after.value.count, 2)
    }),
  )
  test(
    'forks asOf/current/initial documents with entry commit cutoffs',
    Effect.gen(function* () {
      const session = yield* Session.Session
      yield* session.root()
      const entry = yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          for (const token of [historical, current, initial]) {
            const d = yield* tx.doc(token, { owner: root })
            d.count = 1
          }
          return yield* tx.appendEntry(root, { kind: 'cutoff' })
        }),
      )
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          for (const token of [historical, current, initial]) {
            const d = yield* tx.doc(token, { owner: root })
            d.count = 2
          }
          return null
        }),
      )
      const fork = yield* session.transaction((tx) =>
        tx.forkConversation(root, entry.id, { ownership: { kind: 'ownerless' } }),
      )
      assert.strictEqual((yield* session.snapshot(historical, { owner: fork.id }))?.value.count, 1)
      assert.strictEqual((yield* session.snapshot(current, { owner: fork.id }))?.value.count, 2)
      assert.strictEqual(yield* session.snapshot(initial, { owner: fork.id }), undefined)
      assert.strictEqual(
        (yield* session.snapshotAsOf(historical, fork.id, entry.id))?.value.count,
        1,
      )
    }),
  )
  test(
    'validates final owned-work candidates and retires only terminal task documents',
    Effect.gen(function* () {
      const session = yield* Session.Session
      yield* session.root()
      const { id: _id, ...taskInput } = pending(tid(100))
      const task = yield* session.transaction((tx) => tx.createTask(taskInput))
      const token = Document.defineUnsafe({ ...counter.definition, kind: 'taskdoc', scope: 'task' })
      yield* session.transaction((tx) => tx.doc(token, { owner: task }).pipe(Effect.as(null)))
      yield* failure(
        session.transaction(
          Effect.fnUntraced(function* (tx) {
            yield* tx.createConversation({ ownership: { kind: 'task', taskId: task } })
            yield* tx.write({
              type: 'task',
              value: { ...pending(task), state: { status: 'terminal' } },
            })
            return null
          }),
        ),
      )
      assert.strictEqual((yield* session.task(task))?.state.status, 'pending')
      yield* session.transaction((tx) =>
        tx
          .write({ type: 'task', value: { ...pending(task), state: { status: 'completing' } } })
          .pipe(Effect.as(null)),
      )
      assert.ok(yield* session.snapshot(token, { owner: task }))
      yield* session.transaction((tx) =>
        tx
          .write({ type: 'task', value: { ...pending(task), state: { status: 'terminal' } } })
          .pipe(Effect.as(null)),
      )
      assert.strictEqual(yield* session.snapshot(token, { owner: task }), undefined)
      yield* failure(
        session.transaction((tx) => tx.doc(token, { owner: task }).pipe(Effect.as(null))),
      )
    }),
  )
  test(
    'closes idempotently and rejects all operations after close',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      const scope = yield* ResourceScope
      yield* Scope.close(scope, Exit.succeed(undefined))
      yield* Scope.close(scope, Exit.succeed(undefined))
      yield* session.awaitClosed
      for (const operation of [
        store.read.pipe(Effect.as(null)),
        store.committed.pipe(Effect.as(null)),
        store.commit([]).pipe(Effect.as(null)),
        mintId(Record.EntryId).pipe(Effect.as(null)),
        session.conversation(root).pipe(Effect.as(null)),
        store.journal(0).pipe(Effect.as(null)),
      ])
        assert.strictEqual((yield* failure(operation)).reason._tag, 'Closed')
    }),
  )
  return cases
}
