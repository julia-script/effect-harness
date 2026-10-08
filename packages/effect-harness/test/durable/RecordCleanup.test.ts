import * as Serialization from 'effect-harness/durable/Serialization'
import { assertSome } from '@effect/vitest/utils'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as TestSchema from 'effect/testing/TestSchema'
import * as Layer from 'effect/Layer'
import * as Document from 'effect-harness/durable/Document'
import * as Record from 'effect-harness/durable/Record'
import * as Session from 'effect-harness/durable/Session'
import * as Store from 'effect-harness/durable/Store'
import * as Inbox from 'effect-harness/durable/Inbox'
import * as Observation from 'effect-harness/durable/Observation'
import * as Submission from 'effect-harness/durable/workflow/Submission'
import * as Ownership from 'effect-harness/durable/Ownership'
import * as View from 'effect-harness/durable/View'
import * as Stream from 'effect/Stream'

describe('RecordCleanup', () => {
  it.effect('encodes and decodes canonical tagged domain values', () =>
    Effect.gen(function* () {
      const examples = [
        [Record.Scope, { _tag: 'session' }, { _tag: 'session' }],
        [
          Record.Scope,
          { _tag: 'conversation', conversationId: 1 },
          {
            _tag: 'conversation',
            conversationId: Record.ConversationId.make(1),
          },
        ],
        [
          Record.Content,
          { _tag: 'delta', version: 1, ops: [] },
          { _tag: 'delta', version: 1, ops: [] },
        ],
        [
          Record.Write,
          { _tag: 'conversation', value: { id: 1 } },
          {
            _tag: 'conversation',
            value: { id: Record.ConversationId.make(1) },
          },
        ],
        [
          Record.Submission,
          { _tag: 'WriteDone', id: 2, conversationId: 1, type: 'write', status: 'done', entry: 3 },
          {
            id: Record.SubmissionId.make(2),
            conversationId: Record.ConversationId.make(1),
            _tag: 'WriteDone',
            type: 'write',
            status: 'done',
            entry: Record.EntryId.make(3),
          },
        ],
        [
          Record.ContextEdit,
          { _tag: 'omit', target: 1 },
          { target: Record.EntryId.make(1), _tag: 'omit' },
        ],
      ] as const
      const [scope, conversation, content, write, submission, edit] = examples
      yield* new TestSchema.Asserts(scope[0]).decoding().succeedEffect(scope[1], scope[2])
      yield* new TestSchema.Asserts(scope[0]).encoding().succeedEffect(scope[2], scope[1])
      yield* new TestSchema.Asserts(conversation[0])
        .decoding()
        .succeedEffect(conversation[1], conversation[2])
      yield* new TestSchema.Asserts(conversation[0])
        .encoding()
        .succeedEffect(conversation[2], conversation[1])
      yield* new TestSchema.Asserts(content[0]).decoding().succeedEffect(content[1], content[2])
      yield* new TestSchema.Asserts(content[0]).encoding().succeedEffect(content[2], content[1])
      yield* new TestSchema.Asserts(write[0]).decoding().succeedEffect(write[1], write[2])
      yield* new TestSchema.Asserts(write[0]).encoding().succeedEffect(write[2], write[1])
      yield* new TestSchema.Asserts(submission[0])
        .decoding()
        .succeedEffect(submission[1], submission[2])
      yield* new TestSchema.Asserts(submission[0])
        .encoding()
        .succeedEffect(submission[2], submission[1])
      yield* new TestSchema.Asserts(edit[0]).decoding().succeedEffect(edit[1], edit[2])
      yield* new TestSchema.Asserts(edit[0]).encoding().succeedEffect(edit[2], edit[1])
      const payload = {
        sessionId: 'cleanup',
        conversationId: 1,
        requestId: 'same',
        submission: { _tag: 'write', entry: { kind: 'note' } },
      }
      const decoded = yield* Schema.decodeUnknownEffect(Submission.Submission.payloadSchema)(
        payload,
      )
      assert.strictEqual(decoded.submission._tag, 'write')
      if (decoded.submission._tag !== 'write') return yield* Effect.die('expected write variant')
      assert.strictEqual(
        yield* Submission.Submission.executionId(decoded),
        yield* Submission.Submission.executionId({
          ...decoded,
          submission: { ...decoded.submission, entry: { kind: 'different' } },
        }),
      )
      assert.strictEqual(Submission.Submission._tag, '@effect-harness/durable/Submission/v1')
    }),
  )
  it.effect('keeps draft deltas and persisted codecs distinct', () =>
    Effect.gen(function* () {
      const session = yield* Session.Session
      const token = Document.defineUnsafe({
        kind: 'cleanup.note',
        version: 1,
        scope: 'session',
        schema: Schema.Struct({ note: Schema.String }),
        initial: () => ({ note: 'old' }),
      })
      yield* session.transaction((tx) =>
        tx.doc(token).pipe(
          Effect.map((draft) => {
            draft.note = 'held'
          }),
        ),
      )
      assertSome(
        Option.map(yield* session.snapshot(token), (snapshot) => snapshot.value.note),
        'held',
      )
      yield* session.transaction((tx) =>
        tx.doc(token).pipe(
          Effect.map((draft) => {
            draft.note = 'held'
          }),
        ),
      )
      const snapshot = yield* session.snapshot(token)
      assertSome(
        Option.map(snapshot, (snapshot) => snapshot.value.note),
        'held',
      )
      assert.strictEqual(
        yield* Document.address(token).pipe(Effect.map(Record.addressKey)),
        yield* Document.address()(token).pipe(Effect.map(Record.addressKey)),
      )
    }).pipe(Effect.provide(Session.layer.pipe(Layer.provideMerge(Store.layerMemory)))),
  )
  it.effect('encodes transformed document operations before persistence', () =>
    Effect.gen(function* () {
      const session = yield* Session.Session
      const store = yield* Store.Store
      const token = Document.defineUnsafe({
        kind: 'cleanup.tagged',
        version: 1,
        scope: 'session',
        schema: Serialization.object(Inbox.State),
        initial: () => ({ items: [] }),
      })
      const item = {
        _tag: 'write',
        id: Record.SubmissionId.make(2),
        entry: { kind: 'before' },
      } satisfies Inbox.Item
      yield* session.transaction((tx) =>
        tx.doc(token).pipe(
          Effect.map((draft) => {
            draft.items.push(item)
          }),
        ),
      )
      yield* session.transaction((tx) =>
        tx.doc(token).pipe(
          Effect.map((draft) => {
            draft.items[0] = { ...item, entry: { kind: 'after' } }
          }),
        ),
      )
      const state = yield* store.committed
      const persisted = state.documents[0]!.revisions.at(-1)!.content
      assert.strictEqual(persisted._tag, 'delta')
      if (persisted._tag === 'delta')
        assert.strictEqual(JSON.stringify(persisted.ops).includes('after'), true)
      assertSome(
        Option.map(yield* session.snapshot(token), (snapshot) => snapshot.value.items),
        [{ ...item, entry: { kind: 'after' } }],
      )
    }).pipe(Effect.provide(Session.layer.pipe(Layer.provideMerge(Store.layerMemory)))),
  )
  it.effect('allocates independent memory state for every acquisition', () =>
    Effect.gen(function* () {
      const first = yield* Store.makeMemory
      const second = yield* Store.makeMemory
      const initial = (yield* second.committed).nextId
      const allocated = yield* Store.mintId(Record.EntryId).pipe(
        Effect.provideService(Store.Store, first),
      )
      assert.strictEqual(allocated, initial)
      assert.strictEqual((yield* first.committed).nextId, initial + 1)
      assert.strictEqual((yield* second.committed).nextId, initial)
      assert.notStrictEqual(first, second)
    }),
  )
  it('preserves frozen handle inputs and live getters without reading during construction', () => {
    let reads = 0
    let current: Readonly<{ count: number }> = { count: 0 }
    const record = Schema.decodeSync(Record.Document)({
      id: 1,
      kind: 'cleanup',
      scope: { _tag: 'session' },
      createdAt: 1,
    })
    const input = Object.freeze({
      get value() {
        reads++
        return current
      },
      record,
      changes: Stream.empty,
      closed: Effect.succeed('stopped' as const),
      stop: Effect.void,
      listen: () => Effect.void,
    })
    const watch = Observation.makeWatch<{ count: number }>(input)
    assert.strictEqual(reads, 0)
    assert.notStrictEqual<object>(watch, input)
    assert.strictEqual(
      watch.pipe((self) => self),
      watch,
    )
    assert.deepStrictEqual(watch.toJSON(), {
      _id: '@effect-harness/durable/Observation/Watch',
      value: '[Getter]',
      record: '[Opaque]',
    })
    assert.strictEqual(reads, 0)
    current = { count: 1 }
    assert.strictEqual(watch.value, current)
    assert.strictEqual(reads, 1)
  })
  it('constructs frozen entry tokens without sampling input getters', () => {
    let reads = 0
    const input = Object.freeze({
      get kind() {
        reads++
        return 'cleanup'
      },
      schema: Record.Entry,
      is: (entry: Record.Entry | undefined): entry is Record.Entry & { readonly kind: string } =>
        entry?.kind === 'cleanup',
      decode: Schema.decodeUnknownEffect(Record.Entry),
    })
    const token = Record.makeEntryToken(input)
    assert.strictEqual(reads, 0)
    assert.strictEqual(token.kind, 'cleanup')
    assert.strictEqual(reads, 1)
    assert.strictEqual(Record.isEntryToken(token), true)
    assert.strictEqual(
      token.pipe((self) => self),
      token,
    )
  })
  it('retains data-last ownership traversal and immutable view replay', () => {
    const graph: Ownership.Graph = {
      tasks: [],
      conversations: [{ id: Record.ROOT_CONVERSATION_ID }],
    }
    const target = Ownership.Target.conversation({
      id: Record.ROOT_CONVERSATION_ID,
    })
    assert.deepStrictEqual(Ownership.reach(graph, target), Ownership.reach(target)(graph))
    const value = {
      conversation: graph.conversations[0]!,
      entries: [],
      docs: {},
    } satisfies View.Value
    assert.strictEqual(View.applyUnsafe(value, []), View.applyUnsafe([])(value))
    assert.strictEqual(
      Record.isAlive('current')({ ...recordForAliveUnsafe(), createdAt: Record.Seq.make(1) }),
      true,
    )
  })
})
const recordForAliveUnsafe = () =>
  Schema.decodeSync(Record.Document)({
    id: 1,
    kind: 'cleanup',
    scope: { _tag: 'session' },
    createdAt: 1,
  })
