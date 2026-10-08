/**
 * Cross-backend durable storage conformance cases.
 */
import * as MutableHashMap from 'effect/MutableHashMap'
import * as Option from 'effect/Option'
import * as Identity from '../Identity.ts'
import * as Effect from 'effect/Effect'
import * as Scope from 'effect/Scope'
import * as Exit from 'effect/Exit'
import { makeCase, ResourceScope } from './Storage.ts'
import * as Schema from 'effect/Schema'
import * as Arr from 'effect/Array'
import * as Document from '../Document.ts'
import * as Record from '../Record.ts'
import * as Session from '../Session.ts'
import { Store, mintId } from '../Store.ts'
import { rejected, type StorageError } from '../StorageError.ts'
import type { Assertions, Case } from './Storage.ts'

const root = Record.ROOT_CONVERSATION_ID
// effect-nit-allow P4-decode-effect-at-boundary: all fixture IDs are decoded once
// at module initialization. Cases use the fixed 1..1000 literals and the last-safe
// sentinel; deliberately malformed IDs still enter the actual Store boundary.
const fixtureIdsUnsafe = <A extends number>(schema: Schema.Codec<A, number>) => {
  const decodeUnsafe = Schema.decodeSync(schema)
  const values = MutableHashMap.empty<number, A>()
  for (const value of [...Arr.range(1, 1000), Number.MAX_SAFE_INTEGER])
    MutableHashMap.set(values, value, decodeUnsafe(value))
  return (value: number): A => {
    const decoded = Option.getOrUndefined(MutableHashMap.get(values, value))
    if (decoded === undefined) throw new TypeError('Unknown literal fixture ID')
    return decoded
  }
}
const cidUnsafe = fixtureIdsUnsafe(Record.ConversationId)
const eidUnsafe = fixtureIdsUnsafe(Record.EntryId)
const sidUnsafe = fixtureIdsUnsafe(Record.SubmissionId)
const tidUnsafe = fixtureIdsUnsafe(Record.TaskId)
const didUnsafe = fixtureIdsUnsafe(Record.DocumentId)
const seqUnsafe = fixtureIdsUnsafe(Record.Seq)
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
const documentUnsafe = (
  id: number,
  extra: Partial<Record.DocumentCreate> = {},
): Record.DocumentCreate => ({
  id: didUnsafe(id),
  kind: 'doc',
  scope: { _tag: 'conversation', conversationId: root },
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

/**
 * Creates storage conformance cases shared by every backend and test runner.
 *
 * @category constructors
 */
export const makeStorageConformance = (assert: Assertions): Array<Case> => {
  const cases: Array<Case> = []
  const test = (
    name: string,
    run: Effect.Effect<void, StorageError, Store | Session.Session | ResourceScope>,
  ) => {
    cases.push(makeCase({ name, run }))
  }
  test(
    'reserves root 1, creates it lazily, initializes once atomically',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      assert.deepStrictEqual((yield* store.read).conversations, [])
      assert.strictEqual(yield* mintId(Record.EntryId), 2)
      let calls = 0
      const initialize = Effect.fnUntraced(function* (tx: Session.Transaction) {
        calls++
        const draft = yield* tx.doc(counter)
        draft.count = 7
      })
      assert.deepStrictEqual(yield* session.root(initialize), { id: root })
      yield* session.root(initialize)
      assert.strictEqual(calls, 1)
      assert.strictEqual(
        (yield* session.snapshot(counter).pipe(Effect.map(Option.getOrThrow)))?.value.count,
        7,
      )
      assert.strictEqual(
        (yield* failure(
          store.commit([
            {
              _tag: 'entry',
              value: { id: eidUnsafe(1), conversationId: root, kind: 'bad' },
            },
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
        { _tag: 'task', value: pending(tidUnsafe(2)) },
        {
          _tag: 'submission',
          value: {
            _tag: 'InputQueued' as const,
            id: sidUnsafe(3),
            conversationId: root,
            type: 'input',
            status: 'queued',
            requestId: Identity.RequestId.make('r'),
          },
        },
      ])
      const before = yield* store.read
      yield* failure(
        store.commit([
          {
            _tag: 'task',
            value: { ...pending(tidUnsafe(2)), state: { status: 'running' } },
          },
          {
            _tag: 'entry',
            value: { id: eidUnsafe(4), conversationId: root, kind: 'transient' },
          },
          { _tag: 'conversation', value: { id: root } },
        ]),
      )
      assert.deepStrictEqual(yield* store.read, before)
      assert.strictEqual(yield* session.entry(eidUnsafe(4)).pipe(Effect.map(Option.isNone)), true)
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
      const fixtureSchema = Schema.fromJsonString(
        Schema.Struct({
          ['__proto__']: Schema.Struct({ bad: Schema.Finite }),
          constructor: Schema.mutable(Schema.Array(Schema.Finite)),
          prototype: Schema.String,
        }),
      )
      const parse = Schema.decodeEffect(fixtureSchema)(fixture).pipe(
        Effect.mapError((cause) => rejected('Invalid conformance JSON fixture', undefined, cause)),
      )
      const data = yield* parse
      yield* store.commit([
        {
          _tag: 'entry',
          value: { id: eidUnsafe(2), conversationId: root, kind: 'data', data },
        },
      ])
      data.constructor.push(3)
      const first = yield* session.entry(eidUnsafe(2)).pipe(Effect.map(Option.getOrThrow))
      assert.ok(first)
      assert.deepStrictEqual(first.entry.data, yield* parse)
      assert.strictEqual(Object.getPrototypeOf(first.entry.data), Object.prototype)
      const state = yield* store.read
      Reflect.set(state.entries[0]?.entry ?? {}, 'kind', 'mutated')
      assert.strictEqual(
        (yield* session.entry(eidUnsafe(2)).pipe(Effect.map(Option.getOrThrow)))?.entry.kind,
        'data',
      )
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
          _tag: 'entry' as const,
          value: {
            id: eidUnsafe(id),
            conversationId: root,
            kind: 'e',
            ...(id === 3 ? { head: eidUnsafe(3) } : {}),
          },
        })),
      )
      const first = yield* session.scanEntries({ conversationId: root }, 2)
      assert.deepStrictEqual(
        first.items.map((e) => e.id),
        [8, 6],
      )
      yield* store.commit([
        {
          _tag: 'entry',
          value: { id: eidUnsafe(9), conversationId: root, kind: 'new' },
        },
      ])
      assert.deepStrictEqual(
        (yield* session.scanEntries({ conversationId: root }, 10, first.next)).items.map(
          (e) => e.id,
        ),
        [3, 2],
      )
      assert.deepStrictEqual(
        (yield* session.scanEntries(
          { conversationId: root, minEntryId: eidUnsafe(3), maxEntryId: eidUnsafe(6) },
          10,
        )).items.map((e) => e.id),
        [6, 3],
      )
      assert.strictEqual(
        yield* session.transaction((tx) =>
          tx.latestHeadMarker(root).pipe(
            Effect.map(Option.getOrThrow),
            Effect.map((entry) => entry?.id ?? 0),
          ),
        ),
        3,
      )
      assert.strictEqual(
        (yield* session.latestHeadMarker(root, eidUnsafe(8)).pipe(Effect.map(Option.getOrThrow)))
          ?.id,
        3,
      )
      assert.strictEqual(
        yield* session.latestHeadMarker(root, eidUnsafe(2)).pipe(Effect.map(Option.isNone)),
        true,
      )
    }),
  )
  test(
    'pages conversations and owner edges in ascending id order',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      yield* store.commit([
        { _tag: 'task', value: pending(tidUnsafe(2)) },
        ...[8, 5, 3].map((id) => ({
          _tag: 'conversation' as const,
          value: {
            id: cidUnsafe(id),
            ...(id === 5 ? {} : { owner: { conversationId: root, taskId: tidUnsafe(2) } }),
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
          { ownerTaskId: tidUnsafe(2), ownerConversationId: root },
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
        {
          _tag: 'entry',
          value: { id: eidUnsafe(2), conversationId: root, kind: 'a' },
        },
        {
          _tag: 'entry',
          value: { id: eidUnsafe(3), conversationId: root, kind: 'hidden' },
        },
      ])
      yield* store.commit([
        {
          _tag: 'conversation',
          value: { id: cidUnsafe(4), parent: { conversationId: root, at: eidUnsafe(2) } },
        },
        {
          _tag: 'entry',
          value: { id: eidUnsafe(5), conversationId: cidUnsafe(4), kind: 'b' },
        },
        {
          _tag: 'entry',
          value: { id: eidUnsafe(6), conversationId: cidUnsafe(4), kind: 'hidden' },
        },
        {
          _tag: 'conversation',
          value: { id: cidUnsafe(7), parent: { conversationId: cidUnsafe(4), at: eidUnsafe(5) } },
        },
        {
          _tag: 'entry',
          value: { id: eidUnsafe(8), conversationId: cidUnsafe(7), kind: 'c' },
        },
      ])
      assert.deepStrictEqual(
        (yield* session.scanEntries({ conversationId: cidUnsafe(7) }, 10)).items.map((e) => e.id),
        [8, 5, 2],
      )
      assert.strictEqual(
        yield* session.entry(eidUnsafe(3), cidUnsafe(7)).pipe(Effect.map(Option.isNone)),
        true,
      )
      assert.strictEqual(
        (yield* session.entry(eidUnsafe(2), cidUnsafe(7)).pipe(Effect.map(Option.getOrThrow)))
          ?.commitSeq,
        first,
      )
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
          _tag: 'task' as const,
          value: {
            ...pending(tidUnsafe(id)),
            background: id === 3,
            abortRequested: id === 4,
            state: { status: id === 2 ? ('waiting' as const) : ('completing' as const) },
            ...(id === 2 ? { owner: tidUnsafe(3), memos: { old: 1 } } : {}),
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
      yield* store.commit([{ _tag: 'task', value: pending(tidUnsafe(2)) }])
      assert.strictEqual(
        (yield* session.task(tidUnsafe(2)).pipe(Effect.map(Option.getOrThrow)))?.owner,
        undefined,
      )
      assert.strictEqual(
        (yield* session.task(tidUnsafe(2)).pipe(Effect.map(Option.getOrThrow)))?.memos,
        undefined,
      )
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
            _tag: 'InputQueued' as const,
            conversationId: root,
            type: 'input',
            status: 'queued',
            requestId: Identity.RequestId.make('\ud800'),
          })
          const write = yield* tx.createSubmission({
            _tag: 'WriteQueued' as const,
            conversationId: root,
            type: 'write',
            status: 'queued',
            requestId: Identity.RequestId.make('\ud801'),
          })
          yield* tx.placeSubmission(input.id, eidUnsafe(100))
          yield* tx.placeSubmission(write.id, eidUnsafe(101))
          yield* tx.settleSubmission(input.id, { status: 'done', answer: eidUnsafe(102) })
          yield* tx.placeSubmission(write.id, eidUnsafe(999))
          return [input.id, write.id]
        }),
      )
      const settled = (yield* session.scanSubmissions({ status: 'done' }, 10)).items
      assert.strictEqual(settled.length, 2)
      assert.deepStrictEqual(
        settled.map((submission) => submission._tag),
        ['InputDone', 'WriteDone'],
      )
      assert.strictEqual(
        Option.getOrUndefined(
          Option.map(
            Arr.findFirst(
              (yield* store.read).submissions,
              (submission) => submission.id === ids[1],
            ),
            (submission) => submission.entry,
          ),
        ),
        101,
      )
      assert.strictEqual(
        yield* session.transaction((tx) =>
          tx.submissionByRequest(root, Identity.RequestId.make('\ud800')).pipe(
            Effect.map(Option.getOrThrow),
            Effect.map((s) => s?.answer ?? 0),
          ),
        ),
        102,
      )
      assert.strictEqual(
        yield* session.transaction((tx) =>
          tx.submissionByRequest(root, Identity.RequestId.make('\ud801')).pipe(
            Effect.map(Option.getOrThrow),
            Effect.map((s) => s?.answer ?? 0),
          ),
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
        { _tag: 'conversation', value: { id: cidUnsafe(2) } },
        {
          _tag: 'submission',
          value: {
            _tag: 'InputQueued' as const,
            id: sidUnsafe(3),
            conversationId: root,
            type: 'input',
            status: 'queued',
            requestId: Identity.RequestId.make('same'),
          },
        },
        {
          _tag: 'submission',
          value: {
            _tag: 'WriteQueued' as const,
            id: sidUnsafe(4),
            conversationId: cidUnsafe(2),
            type: 'write',
            status: 'queued',
            requestId: Identity.RequestId.make('same'),
          },
        },
      ])
      assert.strictEqual(
        (yield* session
          .submissionByRequest(root, Identity.RequestId.make('same'))
          .pipe(Effect.map(Option.getOrThrow)))?.id,
        3,
      )
      assert.strictEqual(
        (yield* session
          .submissionByRequest(cidUnsafe(2), Identity.RequestId.make('same'))
          .pipe(Effect.map(Option.getOrThrow)))?.id,
        4,
      )
      const before = yield* store.read
      for (const invalid of [
        { _tag: 'WritePlaced', type: 'write', status: 'placed', entry: eidUnsafe(9) },
        {
          _tag: 'WriteDone',
          type: 'write',
          status: 'done',
          entry: eidUnsafe(9),
          answer: eidUnsafe(10),
        },
        { _tag: 'InputDone', type: 'input', status: 'done', entry: eidUnsafe(9) },
        { _tag: 'InputQueued', type: 'input', status: 'queued', entry: eidUnsafe(9) },
      ]) {
        // Deliberate JavaScript boundary misuse must still be rejected by the public Store.
        const write: unknown = {
          _tag: 'submission',
          value: { id: sidUnsafe(5), conversationId: root, ...invalid },
        }
        yield* failure(store.commit([write as Record.Write]))
      }
      assert.deepStrictEqual(yield* store.read, before)
      yield* store.commit([
        {
          _tag: 'submission',
          value: {
            _tag: 'InputPlaced' as const,
            id: sidUnsafe(3),
            conversationId: root,
            type: 'input',
            status: 'placed',
            entry: eidUnsafe(9),
            requestId: Identity.RequestId.make('other'),
          },
        },
      ])
      assert.strictEqual(
        yield* session
          .submissionByRequest(root, Identity.RequestId.make('same'))
          .pipe(Effect.map(Option.isNone)),
        true,
      )
      assert.strictEqual(
        (yield* session
          .submissionByRequest(root, Identity.RequestId.make('other'))
          .pipe(Effect.map(Option.getOrThrow)))?.entry,
        9,
      )
      const page = yield* session.scanSubmissions({}, 1)
      assert.strictEqual(page.items[0]?.id, 3)
      assert.strictEqual((yield* session.scanSubmissions({}, 1, page.next)).items[0]?.id, 4)
      assert.strictEqual(
        (yield* session.scanSubmissions({ conversationId: cidUnsafe(2), status: 'placed' }, 10))
          .items.length,
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
          _tag: 'document.create',
          record: documentUnsafe(2),
          content: { _tag: 'base', version: 1, value: { n: 0 } },
        },
      ])
      const update = yield* store.commit([
        {
          _tag: 'document.change',
          id: didUnsafe(2),
          content: { _tag: 'delta', version: 1, ops: [['set', ['n'], 1]] },
        },
      ])
      const replacement = yield* store.commit([
        {
          _tag: 'document.change',
          id: didUnsafe(2),
          content: { _tag: 'base', version: 2, value: { n: 2 } },
        },
      ])
      assert.strictEqual(
        (yield* session.document(didUnsafe(2), creation).pipe(Effect.map(Option.getOrThrow)))
          ?.value['n'],
        0,
      )
      assert.strictEqual(
        (yield* session.document(didUnsafe(2), update).pipe(Effect.map(Option.getOrThrow)))?.value[
          'n'
        ],
        1,
      )
      assert.strictEqual(
        (yield* session.document(didUnsafe(2), replacement).pipe(Effect.map(Option.getOrThrow)))
          ?.version,
        2,
      )
      const retired = yield* store.commit([
        { _tag: 'document.retire', id: didUnsafe(2) },
        {
          _tag: 'document.create',
          record: documentUnsafe(3),
          content: { _tag: 'base', version: 2, value: { n: 3 } },
        },
      ])
      assert.strictEqual(
        yield* session.document(didUnsafe(2), retired).pipe(Effect.map(Option.isNone)),
        true,
      )
      assert.strictEqual(
        (yield* session.findDocument(documentUnsafe(2), update).pipe(Effect.map(Option.getOrThrow)))
          ?.id,
        2,
      )
      assert.strictEqual(
        (yield* session.findDocument(documentUnsafe(2)).pipe(Effect.map(Option.getOrThrow)))?.id,
        3,
      )
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
          _tag: 'document.create',
          record: documentUnsafe(2),
          content: { _tag: 'base', version: 1, value: { n: 0 } },
        },
      ])
      for (let n = 1; n <= 110; n++)
        yield* store.commit([
          {
            _tag: 'document.change',
            id: didUnsafe(2),
            content: {
              _tag: 'delta',
              version: 1,
              ops: n === 50 ? [['replace', { n }]] : [['set', ['n'], n]],
            },
          },
        ])
      assert.strictEqual(
        (yield* session.document(didUnsafe(2)).pipe(Effect.map(Option.getOrThrow)))?.value['n'],
        110,
      )
      assert.strictEqual(
        (yield* session.document(didUnsafe(2), seqUnsafe(2)).pipe(Effect.map(Option.getOrThrow)))
          ?.value['n'],
        0,
      )
    }),
  )
  test(
    'latest/task/session documents reject rewind and reclaim old bases and retired payloads',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      for (const record of [
        documentUnsafe(2, { history: 'latest', fork: 'current' }),
        {
          id: didUnsafe(3),
          kind: 'sessiondoc',
          scope: { _tag: 'session' as const },
        },
      ]) {
        yield* store.commit([
          {
            _tag: 'document.create',
            record,
            content: { _tag: 'base', version: 1, value: { n: 1 } },
          },
        ])
        yield* failure(session.document(record.id, seqUnsafe(2)))
        yield* store.commit([
          {
            _tag: 'document.change',
            id: record.id,
            content: { _tag: 'base', version: 1, value: { n: 2 } },
          },
        ])
        assert.strictEqual(
          Option.getOrUndefined(
            Option.map(
              Arr.findFirst(
                (yield* store.read).documents,
                (document) => document.record.id === record.id,
              ),
              (document) => document.revisions.length,
            ),
          ),
          1,
        )
        yield* store.commit([{ _tag: 'document.retire', id: record.id }])
        assert.strictEqual(
          Option.getOrUndefined(
            Option.map(
              Arr.findFirst(
                (yield* store.read).documents,
                (document) => document.record.id === record.id,
              ),
              (document) => document.revisions.length,
            ),
          ),
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
            _tag: 'document.create',
            record: documentUnsafe(2, { history: 'latest', fork: 'asOf' }),
            content: { _tag: 'base', version: 1, value: {} },
          },
        ]),
      )
      yield* failure(
        store.commit([
          {
            _tag: 'document.create',
            record: documentUnsafe(2),
            content: { _tag: 'delta', version: 1, ops: [] },
          },
        ]),
      )
      yield* store.commit([
        {
          _tag: 'document.create',
          record: documentUnsafe(2),
          content: { _tag: 'base', version: 1, value: { n: 0 } },
        },
      ])
      const before = yield* store.read
      yield* failure(
        store.commit([
          {
            _tag: 'document.change',
            id: didUnsafe(2),
            content: { _tag: 'delta', version: 2, ops: [] },
          },
        ]),
      )
      yield* failure(
        store.commit([
          {
            _tag: 'document.copy',
            record: documentUnsafe(3),
            source: { id: didUnsafe(2), at: 'current' },
          },
          { _tag: 'document.retire', id: didUnsafe(2) },
        ]),
      )
      yield* failure(
        store.commit([
          {
            _tag: 'document.change',
            id: didUnsafe(2),
            content: { _tag: 'base', version: 1, value: {} },
          },
          {
            _tag: 'document.change',
            id: didUnsafe(2),
            content: { _tag: 'base', version: 1, value: {} },
          },
        ]),
      )
      yield* failure(
        store.commit([
          {
            _tag: 'document.create',
            record: documentUnsafe(3),
            content: { _tag: 'base', version: 1, value: {} },
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
        documentUnsafe(2, { kind: '\ud800' }),
        documentUnsafe(3, { kind: '\ud801' }),
        documentUnsafe(4, { key: '' }),
        documentUnsafe(5, { key: '\ud800' }),
        documentUnsafe(6, { key: '\ud801' }),
        documentUnsafe(7, {
          scope: { _tag: 'conversation', conversationId: cidUnsafe(100) },
        }),
      ]
      yield* store.commit(
        records.map((record) => ({
          _tag: 'document.create' as const,
          record,
          content: {
            _tag: 'base' as const,
            version: 1,
            value: { id: record.id },
          },
        })),
      )
      assert.deepStrictEqual(
        (yield* session.scanDocuments(
          {
            scope: { _tag: 'conversation', conversationId: root },
            at: 'current',
          },
          10,
        )).items.map((d) => d.id),
        [2, 3, 4, 5, 6],
      )
      for (const record of records)
        assert.strictEqual(
          (yield* session.findDocument(record).pipe(Effect.map(Option.getOrThrow)))?.id,
          record.id,
        )
      assert.strictEqual(
        yield* session
          .findDocument({
            kind: 'doc',
            scope: { _tag: 'conversation', conversationId: root },
          })
          .pipe(Effect.map(Option.isNone)),
        true,
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
          _tag: 'document.create',
          record: documentUnsafe(2),
          content: { _tag: 'base', version: 1, value: { n: 1 } },
        },
      ])
      yield* store.commit([
        {
          _tag: 'document.change',
          id: didUnsafe(2),
          content: { _tag: 'delta', version: 1, ops: [['set', ['n'], 2]] },
        },
      ])
      yield* store.commit([
        {
          _tag: 'document.copy',
          record: documentUnsafe(3, {
            scope: { _tag: 'conversation', conversationId: cidUnsafe(100) },
          }),
          source: { id: didUnsafe(2), at: created },
        },
      ])
      assert.strictEqual(
        (yield* session.document(didUnsafe(3)).pipe(Effect.map(Option.getOrThrow)))?.value['n'],
        1,
      )
      assert.strictEqual(
        (yield* session.document(didUnsafe(3)).pipe(Effect.map(Option.getOrThrow)))?.record.fork,
        'asOf',
      )
    }),
  )
  test(
    'keeps global IDs and allows the last safe ID before exhaustion',
    Effect.gen(function* () {
      const store = yield* Store
      const session = yield* Session.Session
      yield* session.root()
      yield* store.commit([
        {
          _tag: 'entry',
          value: { id: eidUnsafe(100), conversationId: root, kind: 'explicit' },
        },
      ])
      assert.strictEqual(yield* mintId(Record.TaskId), 101)
      yield* failure(store.commit([{ _tag: 'task', value: pending(tidUnsafe(100)) }]))
      yield* store.commit([
        {
          _tag: 'entry',
          value: { id: eidUnsafe(Number.MAX_SAFE_INTEGER), conversationId: root, kind: 'last' },
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
      assert.strictEqual(yield* session.snapshot(counter).pipe(Effect.map(Option.isNone)), true)
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
      assert.strictEqual(error.reason._tag, 'ReadAfterWriteError')
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
      const execute = session.transaction(
        Effect.fnUntraced(function* (tx) {
          calls++
          const entry = yield* tx.appendEntry(root, { kind: 'receipt' })
          const value = yield* tx.doc(counter)
          value.count++
          return { id: entry.id, count: value.count }
        }),
        { key: 'stable\ud800', fingerprint: 'input' },
      )
      const result = yield* execute
      assert.deepStrictEqual(yield* execute, result)
      assert.strictEqual(calls, 1)
      assert.strictEqual((yield* store.read).receipts.length, 1)
      assert.strictEqual(
        (yield* failure(
          session.transaction(() => Effect.succeed(null), {
            key: 'stable\ud800',
            fingerprint: 'other',
          }),
        )).reason._tag,
        'ConflictError',
      )
      assert.strictEqual(
        (yield* session.snapshot(counter).pipe(Effect.map(Option.getOrThrow)))?.value.count,
        1,
      )
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
        migrate: (value) => ({ count: Number(value['count']) + 1 }),
      })
      assert.strictEqual(
        (yield* session.snapshot(newer).pipe(Effect.map(Option.getOrThrow)))?.value.count,
        1,
      )
      assert.strictEqual((yield* store.read).documents[0]?.revisions[0]?.content.version, 1)
      yield* session.transaction((tx) => tx.doc(newer).pipe(Effect.as(null)))
      assert.strictEqual((yield* store.read).documents[0]?.revisions.at(-1)?.content._tag, 'base')
      assert.strictEqual((yield* store.read).documents[0]?.revisions.at(-1)?.content.version, 2)
      yield* failure(session.snapshot(counter))
    }),
  )
  test(
    'retires and recreates fresh document incarnations within one transaction',
    Effect.gen(function* () {
      const session = yield* Session.Session
      yield* session.transaction((tx) => tx.doc(counter).pipe(Effect.as(null)))
      const before = yield* session.snapshot(counter).pipe(Effect.map(Option.getOrThrow))
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
      const after = yield* session.snapshot(counter).pipe(Effect.map(Option.getOrThrow))
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
        tx.forkConversation(root, entry.id, {
          ownership: { _tag: 'ownerless' },
        }),
      )
      assert.strictEqual(
        (yield* session
          .snapshot(historical, { owner: fork.id })
          .pipe(Effect.map(Option.getOrThrow)))?.value.count,
        1,
      )
      assert.strictEqual(
        (yield* session.snapshot(current, { owner: fork.id }).pipe(Effect.map(Option.getOrThrow)))
          ?.value.count,
        2,
      )
      assert.strictEqual(
        yield* session.snapshot(initial, { owner: fork.id }).pipe(Effect.map(Option.isNone)),
        true,
      )
      assert.strictEqual(
        (yield* session
          .snapshotAsOf(historical, fork.id, entry.id)
          .pipe(Effect.map(Option.getOrThrow)))?.value.count,
        1,
      )
    }),
  )
  test(
    'validates final owned-work candidates and retires only terminal task documents',
    Effect.gen(function* () {
      const session = yield* Session.Session
      yield* session.root()
      const { id: _id, ...taskInput } = pending(tidUnsafe(100))
      const task = yield* session.transaction((tx) => tx.createTask(taskInput))
      const token = Document.defineUnsafe({ ...counter.definition, kind: 'taskdoc', scope: 'task' })
      yield* session.transaction((tx) => tx.doc(token, { owner: task }).pipe(Effect.as(null)))
      yield* failure(
        session.transaction(
          Effect.fnUntraced(function* (tx) {
            yield* tx.createConversation({
              ownership: { _tag: 'task', taskId: task },
            })
            yield* tx.write({
              _tag: 'task',
              value: { ...pending(task), state: { status: 'terminal' } },
            })
            return null
          }),
        ),
      )
      assert.strictEqual(
        (yield* session.task(task).pipe(Effect.map(Option.getOrThrow)))?.state.status,
        'pending',
      )
      yield* session.transaction((tx) =>
        tx
          .write({
            _tag: 'task',
            value: { ...pending(task), state: { status: 'completing' } },
          })
          .pipe(Effect.as(null)),
      )
      assert.ok(yield* session.snapshot(token, { owner: task }).pipe(Effect.map(Option.getOrThrow)))
      yield* session.transaction((tx) =>
        tx
          .write({
            _tag: 'task',
            value: { ...pending(task), state: { status: 'terminal' } },
          })
          .pipe(Effect.as(null)),
      )
      assert.strictEqual(
        yield* session.snapshot(token, { owner: task }).pipe(Effect.map(Option.isNone)),
        true,
      )
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
        session.conversation(root).pipe(Effect.map(Option.getOrThrow), Effect.as(null)),
        store.journal(0).pipe(Effect.as(null)),
      ])
        assert.strictEqual((yield* failure(operation)).reason._tag, 'ClosedError')
    }),
  )
  return cases
}
