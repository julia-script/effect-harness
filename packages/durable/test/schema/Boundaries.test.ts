// effect-review-allow P8-tests-import-public-specifiers: these adversarial tests exercise private storage validation seams that intentionally have no public package export; all public behavior uses package specifiers.
import * as Option from 'effect/Option'
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as FileSystem from 'effect/FileSystem'
import * as Path from 'effect/Path'
import * as Jsonl from '@effect-harness/durable/storage/JsonlStore'
import * as SchemaIssue from 'effect/SchemaIssue'
import * as Ownership from '@effect-harness/durable/Ownership'
import { ToolCall } from '@effect-harness/durable/workflow/ToolCall'
import { assert, describe, it } from '@effect/vitest'
import * as SharedIdentity from '@effect-harness/harness/Identity'
import * as SharedPatch from '@effect-harness/harness/SystemPatch'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Context from 'effect/Context'
import * as Ref from 'effect/Ref'
import * as HashSet from 'effect/HashSet'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Result from 'effect/Result'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Prompt from 'effect/ai/Prompt'
import * as Conversation from '@effect-harness/durable/Conversation'
import * as Document from '@effect-harness/durable/Document'
import * as Entry from '@effect-harness/durable/Entry'
import * as Event from '@effect-harness/durable/Event'
import * as Identity from '@effect-harness/durable/Identity'
import * as Inspection from '@effect-harness/durable/Inspection'
import * as Record from '@effect-harness/durable/Record'
import * as Session from '@effect-harness/durable/Session'
import * as Store from '@effect-harness/durable/Store'
import * as View from '@effect-harness/durable/View'
import * as Memory from '@effect-harness/durable/storage/Memory'
import * as Sqlite from '@effect-harness/durable/storage/SqliteStore'
import { StrictReceiptJson } from '@effect-harness/durable/storage/StrictReceiptJson'
import { cursor } from '../../src/storage/internal/state.ts'
import * as Outcome from '@effect-harness/durable/workflow/Outcome'
import * as Submission from '@effect-harness/durable/workflow/Submission'

class DecoderValue extends Context.Service<DecoderValue, { readonly prefix: string }>()(
  'test/DecoderValue',
) {}
const data = Schema.String.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transformEffect((input) =>
      DecoderValue.pipe(Effect.map(({ prefix }) => prefix + input)),
    ),
    encode: SchemaGetter.transform((value) => value),
  }),
)

describe('Boundaries', () => {
  describe('Boundaries', () => {
    it('shares exact canonical numeric identities and required system patches', () => {
      assert.strictEqual(Record.ConversationId, SharedIdentity.ConversationId)
      assert.strictEqual(Record.EntryId, SharedIdentity.EntryId)
      assert.strictEqual(Conversation.SystemPatch, SharedPatch.SystemPatch)
      assert.strictEqual(Entry.SystemData.fields.harness.fields.system, SharedPatch.SystemPatch)
      assert.isFalse(Schema.is(Entry.SystemData)({ harness: {} }))
      for (const schema of [
        Record.ConversationId,
        Record.EntryId,
        Record.TaskId,
        Record.SubmissionId,
        Record.DocumentId,
        Record.Seq,
      ]) {
        assert.isTrue(Schema.is(schema)(Number.MAX_SAFE_INTEGER))
        assert.isFalse(Schema.is(schema)(0))
        assert.isFalse(Schema.is(schema)(Number.MAX_SAFE_INTEGER + 1))
      }
      const session = Identity.SessionId.make('')
      assert.strictEqual(session, '')
      assert.strictEqual(Identity.RequestId.make('\ud800'), '\ud800')
    })

    it.effect(
      'kind identity never validates data; decoder preserves schema services and failure',
      () =>
        Effect.gen(function* () {
          const schema = Schema.Struct({
            ...Record.Entry.fields,
            kind: Schema.Literal('custom'),
            data,
          })
          const token = Result.getOrThrow(Record.defineEntry('custom', schema))
          const invalid: Record.Entry = {
            id: Record.EntryId.make(2),
            conversationId: Record.ROOT_CONVERSATION_ID,
            kind: 'custom',
            data: 42,
          }
          assert.isTrue(token.is(invalid))
          const decoding = token.decode(invalid)
          assert.strictEqual(
            (yield* decoding.pipe(
              Effect.provideService(DecoderValue, { prefix: 'decoded:' }),
              Effect.flip,
            ))._tag,
            'SchemaError',
          )
          const valid = yield* token
            .decode({ ...invalid, data: 'value' })
            .pipe(Effect.provideService(DecoderValue, { prefix: 'decoded:' }))
          assert.strictEqual(valid.data, 'decoded:value')
        }),
    )

    it('derives exact document and entry variants without accepting explicit undefined', () => {
      assert.deepStrictEqual(Object.keys(Record.DocumentCreate.fields), [
        'id',
        'kind',
        'scope',
        'key',
        'history',
        'fork',
      ])
      assert.strictEqual(Submission.EntryDraft.fields.data, Record.Entry.fields.data)
      assert.isTrue(Schema.is(Submission.EntryDraft)({ kind: 'custom', head: 'self' }))
      assert.isFalse(Schema.is(Submission.EntryDraft)({ kind: 'custom', head: undefined }))
      assert.isFalse(Schema.is(Submission.EntryDraft)({ kind: 'custom', model: undefined }))
      assert.isFalse(
        Schema.is(Record.DocumentCreate)({
          id: 2,
          kind: 'x',
          scope: { _tag: 'session' as const, kind: 'session' },
          history: undefined,
        }),
      )
    })

    it.effect('round-trips native message events and validates structural/graph frames', () =>
      Effect.gen(function* () {
        const batch: Event.Batch = [
          {
            _tag: 'message_start' as const,
            type: 'message_start',
            message: Prompt.assistantMessage({ content: [Prompt.textPart({ text: 'hello' })] }),
          },
        ]
        const wire = yield* Schema.encodeEffect(Schema.fromJsonString(Event.BatchJson))(batch)
        const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(Event.BatchJson))(wire)
        assert.isTrue(Schema.toEquivalence(Event.Batch)(batch, decoded))
        assert.isFalse(
          Schema.is(Event.AgentEvent)({
            _tag: 'tool_execution_start' as const,
            type: 'tool_execution_start',
            toolCallId: 'x',
            toolName: 'x',
            args: undefined,
          }),
        )
        assert.isFalse(Schema.is(View.Op)(['splice', [], 1.5, 0, []]))
        assert.isFalse(
          Schema.is(Inspection.GraphChange)({
            seq: -1,
            before: { tasks: {} },
            value: { tasks: {} },
            ops: [],
            reset: false,
          }),
        )
      }),
    )

    it('classifies explicit extension envelopes without interpreting arbitrary custom results', () => {
      assert.isTrue(
        Outcome.failed({ receipt: { status: 'failed' }, execution: { arbitrary: true } }),
      )
      assert.isFalse(Outcome.failed({ status: 'completed', receipt: { status: 'failed' } }))
      assert.isFalse(Outcome.failed({ status: 1, receipt: { status: 'failed' } }))
      assert.isTrue(Outcome.failed({ status: null, receipt: { status: 'aborted' } }))
      assert.isFalse(Outcome.failed({ arbitrary: { status: 'failed' } }))
      assert.strictEqual(
        Outcome.classify({ status: 'faulted', detail: 42, error: { message: 'ignored' } })?.message,
        undefined,
      )
    })

    it.effect('validates zero/last-safe journal cursors and the exhausted allocator sentinel', () =>
      Effect.gen(function* () {
        assert.strictEqual(yield* cursor(1), 0)
        assert.strictEqual(yield* cursor(Number.MAX_SAFE_INTEGER + 1), Number.MAX_SAFE_INTEGER)
        assert.strictEqual((yield* cursor(0).pipe(Effect.flip)).reason._tag, 'Corrupt')
        const state = {
          ...Record.emptyState(),
          nextId: Number.MAX_SAFE_INTEGER + 1,
          nextSeq: Number.MAX_SAFE_INTEGER + 1,
        }
        assert.isTrue(Schema.is(Record.State)(state))
      }),
    )
  })

  describe('Unknown-before-Json receipt policy', () => {
    it('rejects descriptors without invoking getters and accepts own __proto__ and shared acyclic values', () => {
      let reads = 0
      const getter = {
        get value() {
          reads++
          throw new Error('getter ran')
        },
      }
      assert.isTrue(Result.isFailure(Schema.decodeResult(StrictReceiptJson)(getter)))
      assert.strictEqual(reads, 0)
      const trapped = new Proxy(
        { value: 1 },
        {
          get: () => {
            reads++
            throw new Error('get trap ran')
          },
        },
      )
      assert.isTrue(Result.isSuccess(Schema.decodeResult(StrictReceiptJson)(trapped)))
      assert.strictEqual(reads, 0)
      const own = Object.create(null)
      Object.defineProperty(own, '__proto__', { value: { accepted: true }, enumerable: true })
      const decoded = Schema.decodeResult(StrictReceiptJson)(own)
      assert.isTrue(Result.isSuccess(decoded))
      const shared = { value: 1 }
      assert.isTrue(
        Result.isSuccess(Schema.decodeResult(StrictReceiptJson)({ a: shared, b: shared })),
      )
      const extra = [1]
      Object.defineProperty(extra, 'extra', { value: 'accepted', enumerable: true })
      assert.isTrue(Result.isSuccess(Schema.decodeResult(StrictReceiptJson)(extra)))
    })

    it('rejects holes, symbols, nonenumerable fields, classes, cycles and throwing reflection as typed schema issues', () => {
      const cycle: { self?: unknown } = {}
      cycle.self = cycle
      const symbol = { [Symbol('hidden')]: 1 }
      const hidden = Object.defineProperty({}, 'hidden', { value: 1 })
      const broken = [
        new Proxy(
          {},
          {
            getPrototypeOf: () => {
              throw new Error('prototype')
            },
          },
        ),
        new Proxy(
          {},
          {
            ownKeys: () => {
              throw new Error('keys')
            },
          },
        ),
        new Proxy(
          { value: 1 },
          {
            getOwnPropertyDescriptor: () => {
              throw new Error('descriptor')
            },
          },
        ),
        Array(2),
        symbol,
        hidden,
        new Date(),
        cycle,
      ]
      for (const input of broken) {
        const decoded = Schema.decodeResult(StrictReceiptJson)(input)
        assert.isTrue(Result.isFailure(decoded))
        if (Result.isFailure(decoded)) assert.strictEqual(decoded.failure._tag, 'SchemaError')
      }
    })

    it.effect('rejects a reflected receipt before saving and leaves admission state intact', () =>
      Effect.gen(function* () {
        const store = yield* Memory.make
        const before = yield* store.read
        const result = new Proxy(
          { value: 1 },
          {
            ownKeys: () => {
              throw new Error('reflect failure')
            },
          },
        )
        const failure = yield* store
          .transact((state) => Effect.succeed(Store.makeCandidate({ state, writes: [], result })), {
            key: 'reflection',
          })
          .pipe(Effect.flip)
        assert.strictEqual(failure.reason._tag, 'Invalid')
        assert.strictEqual(failure.certainty, 'rejected')
        assert.deepStrictEqual(yield* store.read, before)
      }),
    )
  })

  it.effect(
    'decodes malformed native SQL driver rows as Corrupt and preserves text exhaustion counters',
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        let malformed = false
        const wrapped = new Proxy(sql, {
          apply(target, self, args) {
            const statement = Reflect.apply(target, self, args)
            if (!malformed || !String(args[0]).includes('SELECT state,')) return statement
            return statement.pipe(
              Effect.map(() => [
                { state: JSON.stringify(Record.emptyState()), next_seq: 1, format: 1 },
              ]),
            )
          },
        })
        const store = yield* Sqlite.make.pipe(Effect.provideService(SqlClient.SqlClient, wrapped))
        const exhausted = {
          ...Record.emptyState(),
          nextId: Number.MAX_SAFE_INTEGER + 1,
          nextSeq: Number.MAX_SAFE_INTEGER + 1,
        }
        const text = yield* Schema.encodeEffect(Schema.fromJsonString(Record.State))(exhausted)
        yield* sql`UPDATE durable_state SET state=${text},next_seq=${exhausted.nextSeq} WHERE singleton=1`
        assert.strictEqual((yield* store.read).nextSeq, exhausted.nextSeq)
        const rows = yield* sql`SELECT CAST(next_seq AS TEXT) AS next_seq FROM durable_state`
        assert.deepStrictEqual(rows, [{ next_seq: String(exhausted.nextSeq) }])
        malformed = true
        const error = yield* store.read.pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, 'Corrupt')
        assert.strictEqual(error.certainty, 'rejected')
      }).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
  )

  it.effect(
    'omits optional undefined at the schema storage boundary and reopens the decoded document',
    () =>
      Effect.gen(function* () {
        const store = yield* Sqlite.make
        const schema = Schema.Struct({ value: Schema.String, note: Schema.optional(Schema.String) })
        const token = Document.defineUnsafe({
          kind: 'undefined-friendly',
          version: 1,
          scope: 'session',
          schema: Document.jsonObjectCodec(schema),
          initial: () => ({ value: 'kept', note: undefined }),
        })
        const session = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
        yield* session.transaction((tx) => tx.doc(token).pipe(Effect.asVoid))
        const encoded = yield* Document.encode(token, { value: 'kept', note: undefined })
        assert.deepStrictEqual(encoded, { value: 'kept' })
        const sql = yield* SqlClient.SqlClient
        const row = yield* sql`SELECT state FROM durable_state`
        assert.isFalse(JSON.stringify(row).includes('null'))
        const reopened = yield* Sqlite.make
        const next = yield* Session.make.pipe(Effect.provideService(Store.Store, reopened))
        const snapshot = yield* next.snapshot(token).pipe(Effect.map(Option.getOrUndefined))
        assert.deepStrictEqual(snapshot?.value, { value: 'kept' })
        const copied: { value: string; note?: string | undefined } = Document.copyUnsafe({
          value: 'kept',
          note: undefined,
        })
        assert.isTrue(Object.hasOwn(copied, 'note'))
      }).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
  )

  it('uses native part equivalence for content replacements with reordered option keys', () => {
    const before = Prompt.assistantMessage({
      content: [Prompt.textPart({ text: 'same', options: { a: 1, b: 2 } })],
    })
    const next = Prompt.assistantMessage({
      content: [Prompt.textPart({ text: 'same', options: { b: 2, a: 1 } })],
    })
    assert.deepStrictEqual(
      Event.messageChanges(
        [['set', ['docs', 'harness.live', 'generation', 'message', 'content'], next.content]],
        before,
        next,
      ),
      [],
    )
  })

  for (const [select, bad] of [
    ['SELECT name FROM sqlite_master', [{ name: 1 }]],
    ['SELECT version FROM durable_schema', [{ version: '2' }]],
    ['SELECT seq,frame FROM durable_journal', [{ seq: 1, frame: 42 }]],
    [
      'SELECT key,fingerprint,result,seq,is_void FROM durable_receipt',
      [{ key: '"key"', fingerprint: '""', result: '{}', seq: 1, is_void: true }],
    ],
  ] as const)
    it.effect(`validates driver row array for ${select}`, () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        let active = select.includes('sqlite_master') || select.includes('durable_schema')
        const wrapped = new Proxy(sql, {
          apply(target, self, args) {
            const statement = Reflect.apply(target, self, args)
            return active && String(args[0]).includes(select)
              ? statement.pipe(Effect.as(bad))
              : statement
          },
        })
        const failure = active
          ? yield* Sqlite.make.pipe(
              Effect.provideService(SqlClient.SqlClient, wrapped),
              Effect.flip,
            )
          : yield* Effect.gen(function* () {
              const store = yield* Sqlite.make.pipe(
                Effect.provideService(SqlClient.SqlClient, wrapped),
              )
              active = true
              return yield* store.read.pipe(Effect.flip)
            })
        assert.strictEqual(failure.reason._tag, 'Corrupt')
        assert.ok(failure.cause instanceof Schema.SchemaError)
      }).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
    )

  it.effect(
    'uses declared tool payload and pinned checkpoint schemas for tool-start arguments',
    () =>
      Effect.gen(function* () {
        const conversation = { id: Record.ROOT_CONVERSATION_ID }
        const taskId = Record.TaskId.make(2)
        const payload = {
          sessionId: Identity.SessionId.make('schema'),
          conversationId: conversation.id,
          taskId,
          generationTaskId: taskId,
          assistantId: Record.EntryId.make(3),
          callId: 'call',
          name: 'tool',
          arguments: { original: true },
        }
        const binding = Ownership.Binding.make({
          workflow: ToolCall._tag,
          executionId: 'external-native-execution',
          payload,
        })
        const task: Record.Task = {
          id: taskId,
          conversationId: conversation.id,
          kind: 'harness.tool',
          version: 1,
          input: binding,
          background: false,
          abortRequested: false,
          state: {
            status: 'running',
            checkpoint: Outcome.ToolCheckpoint.make({ arguments: { pinned: true } }),
          },
        }
        const before: View.Value = { conversation, entries: [], docs: {} }
        const value: View.Value = {
          ...before,
          docs: {
            'harness.live': {
              tools: [{ taskId, callId: 'call', name: 'tool', status: 'running' }],
            },
          },
        }
        const change: View.Change = {
          seq: Record.Seq.make(1),
          before,
          value,
          ops: [],
          reset: false,
          publication: {
            seq: Record.Seq.make(1),
            writes: [{ _tag: 'task' as const, type: 'task', value: task }],
            documents: [],
          },
        }
        const events = yield* Event.translate(
          conversation.id,
          change,
          yield* Ref.make(HashSet.empty<Record.TaskId>()),
        )
        assert.deepStrictEqual(events[0], {
          _tag: 'tool_execution_start' as const,
          type: 'tool_execution_start',
          toolCallId: 'call',
          toolName: 'tool',
          args: { pinned: true },
        })
        const invalid = {
          ...change,
          publication: {
            ...change.publication!,
            writes: [
              {
                _tag: 'task' as const,
                type: 'task' as const,
                value: { ...task, input: { ...binding, payload: { arguments: { wrong: true } } } },
              },
            ],
          },
        }
        assert.deepStrictEqual(
          (yield* Event.translate(
            conversation.id,
            invalid,
            yield* Ref.make(HashSet.empty<Record.TaskId>()),
          ))[0],
          {
            _tag: 'tool_execution_start' as const,
            type: 'tool_execution_start',
            toolCallId: 'call',
            toolName: 'tool',
            args: {},
          },
        )
      }),
  )

  it.effect('rejects a schema encoding failure before any SQL write and retains its cause', () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      let mutations = 0
      const wrapped = new Proxy(sql, {
        apply(target, self, args) {
          if (String(args[0]).includes('UPDATE durable_state')) mutations++
          return Reflect.apply(target, self, args)
        },
      })
      const store = yield* Sqlite.make.pipe(Effect.provideService(SqlClient.SqlClient, wrapped))
      const shape = Schema.Struct({ value: Schema.String })
      const rejectedCodec = shape.pipe(
        Schema.decodeTo(shape, {
          decode: SchemaGetter.passthrough(),
          encode: SchemaGetter.transformEffect(() =>
            Effect.fail(new SchemaIssue.InvalidValue({ message: 'encoder rejected' })),
          ),
        }),
      )
      const token = Document.defineUnsafe({
        kind: 'encoding-failure',
        version: 1,
        scope: 'session',
        schema: rejectedCodec,
        initial: () => ({ value: 'decode succeeds' }),
      })
      const session = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
      const before = yield* store.read
      const error = yield* session
        .transaction((tx) => tx.doc(token).pipe(Effect.asVoid))
        .pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, 'Invalid')
      assert.strictEqual(error.certainty, 'rejected')
      assert.ok(error.cause instanceof Schema.SchemaError)
      assert.strictEqual(mutations, 0)
      assert.deepStrictEqual(yield* store.read, before)
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
  )

  it.effect(
    'preserves scalar receipt columns and state/frame bytes through schema serialization',
    () =>
      Effect.gen(function* () {
        const store = yield* Sqlite.make
        const result: Record.JsonObject = { z: ['first', 1], a: { text: 'unicode \ud800' } }
        Object.defineProperty(result, '__proto__', { value: 'own', enumerable: true })
        const key = 'key:\ud800'
        const fingerprint = 'fingerprint:"\\'
        yield* store.transact(
          (state) => Effect.succeed(Store.makeCandidate({ state, writes: [], result })),
          {
            key,
            fingerprint,
          },
        )
        const state = yield* store.read
        const frame = (yield* store.journal(0)).frames[0]
        const sql = yield* SqlClient.SqlClient
        assert.deepStrictEqual(yield* sql`SELECT state FROM durable_state`, [
          { state: JSON.stringify(state) },
        ])
        assert.deepStrictEqual(yield* sql`SELECT frame FROM durable_journal`, [
          { frame: JSON.stringify(frame) },
        ])
        assert.deepStrictEqual(yield* sql`SELECT key,fingerprint,result FROM durable_receipt`, [
          {
            key: JSON.stringify(key),
            fingerprint: JSON.stringify(fingerprint),
            result: JSON.stringify(result),
          },
        ])
        const reopened = yield* Sqlite.make
        assert.deepStrictEqual(
          yield* reopened.transact(() => Effect.die('receipt replay callback'), {
            key,
            fingerprint,
          }),
          result,
        )
      }).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
  )

  it.effect('preserves exact JSONL snapshot bytes and schema-owned values on reopen', () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const directory = yield* fs.makeTempDirectoryScoped()
      const store = yield* Jsonl.make({ directory })
      yield* store.commit(
        [
          {
            _tag: 'conversation' as const,
            type: 'conversation',
            value: { id: Record.ROOT_CONVERSATION_ID },
          },
        ],
        {
          key: 'jsonl-bytes',
          fingerprint: 'unicode \ud800',
        },
      )
      const state = yield* store.read
      const frames = (yield* store.journal(0)).frames
      assert.strictEqual(
        yield* fs.readFileString(path.join(directory, 'commits.jsonl')),
        `${JSON.stringify({ state, frames }, (key, value: unknown) => (key === '_tag' ? undefined : value))}\n`,
      )
      const reopened = yield* Jsonl.make({ directory })
      assert.deepStrictEqual(yield* reopened.read, state)
    }).pipe(Effect.provide(NodeServices.layer)),
  )
})
