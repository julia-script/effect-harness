import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Layer from 'effect/Layer'
import * as Document from '@effect-harness/durable/Document'
import * as Record from '@effect-harness/durable/Record'
import * as Session from '@effect-harness/durable/Session'
import * as Store from '@effect-harness/durable/Store'
import * as Inbox from '@effect-harness/durable/Inbox'
import * as Observation from '@effect-harness/durable/Observation'
import * as Submission from '@effect-harness/durable/workflow/Submission'
import * as Ownership from '@effect-harness/durable/Ownership'
import * as View from '@effect-harness/durable/View'
import * as Stream from 'effect/Stream'

describe('RecordCleanup', () => {
  it.effect('decodes legacy owned variants and reencodes the same wire without tags', () =>
    Effect.gen(function* () {
      const examples = [
        [Record.Scope, { kind: 'session' }],
        [Record.Scope, { kind: 'conversation', conversationId: 1 }],
        [Record.Content, { kind: 'delta', version: 1, ops: [] }],
        [Record.Write, { type: 'conversation', value: { id: 1 } }],
        [Record.Submission, { id: 2, conversationId: 1, type: 'write', status: 'done', entry: 3 }],
        [Record.ContextEdit, { target: 1, action: 'omit' }],
      ] as const
      for (const [codec, wire] of examples) {
        const decoded = yield* Schema.decodeEffect(codec)(wire)
        assert.isString(decoded._tag)
        const encoded = yield* Schema.encodeUnknownEffect(codec)(decoded)
        assert.deepStrictEqual(encoded, wire)
        assert.strictEqual(JSON.stringify(encoded), JSON.stringify(wire))
      }
      const payload = {
        sessionId: 'cleanup',
        conversationId: 1,
        requestId: 'same',
        submission: { type: 'write', entry: { kind: 'note' } },
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
      assert.strictEqual(Option.getOrThrow(yield* session.snapshot(token)).value.note, 'held')
      yield* session.transaction((tx) =>
        tx.doc(token).pipe(
          Effect.map((draft) => {
            draft.note = 'held'
          }),
        ),
      )
      const snapshot = yield* session.snapshot(token)
      assert.strictEqual(Option.getOrThrow(snapshot).value.note, 'held')
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
        schema: Document.jsonObjectCodec(Inbox.State),
        initial: () => ({ items: [] }),
      })
      const item = {
        _tag: 'write',
        mode: 'write',
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
      assert.strictEqual(persisted.kind, 'delta')
      if (persisted.kind === 'delta')
        assert.strictEqual(JSON.stringify(persisted.ops).includes('_tag'), false)
      const reopened = Option.getOrThrow(yield* session.snapshot(token))
      assert.deepStrictEqual(reopened.value.items, [{ ...item, entry: { kind: 'after' } }])
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
      scope: { kind: 'session' },
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
    assert.deepStrictEqual(watch.toJSON(), { _id: '@effect-harness/durable/Observation/Watch' })
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
      kind: 'conversation',
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
      Record.isAlive('current')({ ...recordForAlive(), createdAt: Record.Seq.make(1) }),
      true,
    )
  })
})
const recordForAlive = () =>
  Schema.decodeSync(Record.Document)({
    id: 1,
    kind: 'cleanup',
    scope: { kind: 'session' },
    createdAt: 1,
  })
