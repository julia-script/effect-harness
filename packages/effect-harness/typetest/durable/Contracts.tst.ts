import { expect, test } from 'tstyche'
import * as Event from 'effect-harness/durable/Event'
import type * as Prompt from 'effect/ai/Prompt'
import * as Document from 'effect-harness/durable/Document'
import * as Identity from 'effect-harness/durable/Identity'
import type * as Runner from 'effect-harness/durable/testing/Runner'
import type * as Storage from 'effect-harness/durable/testing/Storage'
import * as Entry from 'effect-harness/durable/Entry'
import * as Record from 'effect-harness/durable/Record'
import * as Session from 'effect-harness/durable/Session'
import * as Store from 'effect-harness/durable/Store'
import * as StorageError from 'effect-harness/durable/StorageError'
import * as Ownership from 'effect-harness/durable/Ownership'
import * as Observation from 'effect-harness/durable/Observation'
import * as View from 'effect-harness/durable/View'
import * as Executor from 'effect-harness/durable/Executor'
import * as Conversation from 'effect-harness/durable/Conversation'
import * as Directory from 'effect-harness/durable/SessionDirectory'
import * as Memory from 'effect-harness/durable/storage/Memory'
import * as SnapshotStore from 'effect-harness/durable/storage/SnapshotStore'
import type * as EventJournal from 'effect/eventlog/EventJournal'
import type * as KeyValueStore from 'effect/persistence/KeyValueStore'
import * as Jsonl from 'effect-harness/durable/storage/JsonlStore'
import * as Cancellation from 'effect-harness/durable/workflow/Cancellation'
import { Generation } from 'effect-harness/durable/workflow/Generation'
import { ToolCall } from 'effect-harness/durable/workflow/ToolCall'
import { Compaction } from 'effect-harness/durable/workflow/Compaction'
import * as Structured from 'effect-harness/durable/workflow/Structured'
import * as Execution from 'effect-harness/durable/workflow/ExecutionError'
import type * as Harness from 'effect-harness/Executor'
import type * as Model from 'effect-harness/Model'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import type * as Scope from 'effect/Scope'
import type * as FileSystem from 'effect/FileSystem'
import type * as Path from 'effect/Path'
import type * as Option from 'effect/Option'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import type * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import type * as Crypto from 'effect/Crypto'

class Caller extends Context.Service<Caller, { readonly value: string }>()(
  'typetest/durable/Caller',
) {}
class Failure extends Schema.TaggedError<Failure>()('Failure', { message: Schema.String }) {}
declare const session: Session.Service
declare const tx: Session.Transaction
declare const store: Store.Service
declare const schema: Schema.Codec<string, string, Caller, Caller>
declare const document: Document.Document<{ readonly count: number }>
declare const unknownValue: unknown
declare const rawEntry: Record.Entry
const id = Record.ROOT_CONVERSATION_ID

test('session and transaction lookups expose explicit Option with exact channels', () => {
  expect(session.conversation(id)).type.toBe<
    Effect.Effect<Option.Option<Record.Conversation>, StorageError.StorageError>
  >()
  expect(tx.conversation(id)).type.toBe<
    Effect.Effect<Option.Option<Record.Conversation>, StorageError.StorageError>
  >()
  expect(session.snapshot(document)).type.toBe<
    Effect.Effect<
      Option.Option<Document.Snapshot<{ readonly count: number }>>,
      StorageError.StorageError
    >
  >()
  expect(session.watchDoc(document)).type.toBe<
    Effect.Effect<
      Option.Option<Observation.Watch<{ readonly count: number }>>,
      StorageError.StorageError,
      Scope.Scope
    >
  >()
  expect(session.state(document)).type.toBe<
    Effect.Effect<
      Option.Option<Observation.State<{ readonly count: number }>>,
      StorageError.StorageError,
      Scope.Scope
    >
  >()
  expect(tx.doc(document)).type.toBe<
    Effect.Effect<Document.Draft<{ readonly count: number }>, StorageError.StorageError>
  >()
  expect(tx.mint(schema)).type.toBe<Effect.Effect<string, StorageError.StorageError, Caller>>()
  expect(Store.mintId(schema)).type.toBe<
    Effect.Effect<string, StorageError.StorageError, Store.Store | Caller>
  >()
  expect(session.transaction(() => Caller.pipe(Effect.as(1)))).type.toBe<
    Effect.Effect<number, StorageError.StorageError, Caller>
  >()
  expect(session.transaction(() => Caller.pipe(Effect.as(1)), { key: 'key' })).type.toBe<
    Effect.Effect<number, StorageError.StorageError, Caller>
  >()
  expect(session.transaction).type.not.toBeCallableWith(() => Effect.succeed(new Date()), {
    key: 'invalid',
  })
  expect(session.transaction).type.not.toBeCallableWith(() => Effect.succeed(() => true), {
    key: 'invalid',
  })
  expect(store.transact(() => Effect.fail(new Failure({ message: 'typed' })))).type.toBe<
    Effect.Effect<unknown, StorageError.StorageError | Failure>
  >()
})

test('native ownership helpers retain supplied schema and caller requirements', () => {
  expect(Ownership.memo('key', schema, Caller.pipe(Effect.as('value')))).type.toBe<
    Effect.Effect<
      string,
      Schema.SchemaError | StorageError.StorageError | Execution.ExecutionError,
      Caller | Ownership.Current
    >
  >()
  expect(
    Ownership.execute({ workflow: 'native', executionId: 'external', payload: null }),
  ).type.toBe<
    Effect.Effect<
      unknown,
      Execution.ExecutionError,
      Ownership.Declarations | WorkflowEngine.WorkflowEngine
    >
  >()
  expect(
    Structured.evaluate(
      {
        sessionId: Identity.SessionId.make('session'),
        conversationId: id,
        taskId: Record.TaskId.make(2),
      },
      session,
      Caller.pipe(Effect.as(null)),
    ),
  ).type.toBe<
    Effect.Effect<
      Record.Json,
      StorageError.StorageError | Execution.ExecutionError,
      Caller | Ownership.Declarations | Cancellation.Cancellation | WorkflowEngine.WorkflowEngine
    >
  >()
  expect(
    Ownership.reach(
      { tasks: [], conversations: [] },
      Ownership.Target.conversation({ kind: 'conversation', id }),
    ),
  ).type.toBe<Option.Option<Ownership.Reached>>()
})

test('all native storage and executor layers expose full inputs and acquisition errors', () => {
  expect(Conversation.layer()).type.toBe<
    Layer.Layer<
      Conversation.Configuration | Session.CreationHook,
      Schema.SchemaError,
      Crypto.Crypto
    >
  >()
  expect(Conversation.layerFromSession).type.toBe<
    Layer.Layer<Conversation.Conversation, never, Session.Session>
  >()
  expect(Memory.layer).type.toBe<Layer.Layer<Store.Store>>()
  expect(Session.layer).type.toBe<Layer.Layer<Session.Session, never, Store.Store>>()
  expect(View.layer).type.toBe<Layer.Layer<View.View, never, Store.Store>>()
  expect(SnapshotStore.layer).type.toBe<
    Layer.Layer<
      Store.Store,
      StorageError.StorageError,
      EventJournal.EventJournal | KeyValueStore.KeyValueStore
    >
  >()
  expect(Jsonl.layer({ directory: 'data' })).type.toBe<
    Layer.Layer<Store.Store, StorageError.StorageError, FileSystem.FileSystem | Path.Path>
  >()
  expect(Executor.layerExecutors).type.toBe<
    Layer.Layer<
      Structured.DrainConversations | Cancellation.Cancellation,
      never,
      | Model.Catalog
      | Conversation.Configuration
      | Harness.Executor
      | Directory.SessionDirectory
      | WorkflowEngine.WorkflowEngine
      | Ownership.Declarations
    >
  >()
  expect(Executor.layer).type.toBe<
    Layer.Layer<
      Structured.DrainConversations | Cancellation.Cancellation | Ownership.Declarations,
      never,
      | Model.Catalog
      | Conversation.Configuration
      | Harness.Executor
      | Directory.SessionDirectory
      | WorkflowEngine.WorkflowEngine
    >
  >()
})

test('private runtime carriers cannot be fabricated and nonempty operations reject empty paths', () => {
  expect(
    Document.define({
      kind: 'counter',
      version: 1,
      scope: 'session',
      schema: Schema.Struct({ count: Schema.Finite }),
      initial: () => ({ count: 0 }),
    }),
  ).type.toBe<
    Result.Result<Document.Document<{ readonly count: number }>, Document.DocumentDefinitionError>
  >()
  expect<Document.Document<{ readonly count: number }>>().type.not.toBeAssignableFrom({
    definition: {
      kind: 'counter',
      version: 1,
      scope: 'session',
      schema: Schema.Struct({ count: Schema.Finite }),
      initial: () => ({ count: 0 }),
    },
    family: false,
  })
  expect(View.apply).type.not.toBeCallableWith({ conversation: { id }, entries: [], docs: {} }, [
    ['delete', []],
  ])
  expect(
    View.apply({ conversation: { id }, entries: [], docs: {} }, [
      ['set', [], { conversation: { id }, entries: [], docs: {} }],
    ]),
  ).type.toBe<Result.Result<View.Value, View.ViewOperationError>>()
  if (Record.isEntryToken(unknownValue)) expect(unknownValue).type.toBe<Record.EntryToken>()
  if (Document.isMigrationCache(unknownValue))
    expect(unknownValue).type.toBe<Document.MigrationCache>()
  if (Observation.isWatch(unknownValue)) expect(unknownValue).type.toBe<Observation.Watch<object>>()
  if (View.isProjectionWatch(unknownValue))
    expect(unknownValue).type.toBe<View.ProjectionWatch<unknown>>()
  if (Entry.ToolResultEntry.is(rawEntry)) {
    expect(rawEntry).type.toBe<Record.Entry & { readonly kind: 'harness.tool' }>()
    expect(rawEntry.data).type.toBe<Record.Json | undefined>()
  }
})

type Narrow = { readonly value: 'specific' }
type Wide = { readonly value: string }
test('constructed carrier variance follows each public ownership contract', () => {
  expect<Document.Definition<Narrow>>().type.not.toBeAssignableTo<Document.Definition<Wide>>()
  expect<Document.Definition<Wide>>().type.not.toBeAssignableTo<Document.Definition<Narrow>>()
  expect<Document.Document<Narrow>>().type.not.toBeAssignableTo<Document.Document<Wide>>()
  expect<Document.Document<Wide>>().type.not.toBeAssignableTo<Document.Document<Narrow>>()
  expect<Document.Snapshot<Narrow>>().type.toBeAssignableTo<Document.Snapshot<Wide>>()
  expect<Document.Snapshot<Wide>>().type.not.toBeAssignableTo<Document.Snapshot<Narrow>>()
  expect<Record.Page<Narrow>>().type.toBeAssignableTo<Record.Page<Wide>>()
  expect<Record.Page<Wide>>().type.not.toBeAssignableTo<Record.Page<Narrow>>()
  expect<Store.Candidate<Narrow>>().type.toBeAssignableTo<Store.Candidate<Wide>>()
  expect<Store.Candidate<Wide>>().type.not.toBeAssignableTo<Store.Candidate<Narrow>>()
  expect<View.Projection<Narrow>>().type.toBeAssignableTo<View.Projection<Wide>>()
  expect<View.Projection<Wide>>().type.not.toBeAssignableTo<View.Projection<Narrow>>()
  expect<View.ProjectionWatch<Narrow>>().type.toBeAssignableTo<View.ProjectionWatch<Wide>>()
  expect<View.ProjectionWatch<Wide>>().type.not.toBeAssignableTo<View.ProjectionWatch<Narrow>>()
  expect<Observation.Watch<Narrow>>().type.toBeAssignableTo<Observation.Watch<Wide>>()
  expect<Observation.Watch<Wide>>().type.not.toBeAssignableTo<Observation.Watch<Narrow>>()
  expect<Observation.State<Narrow>>().type.toBeAssignableTo<Observation.State<Wide>>()
  expect<Observation.State<Wide>>().type.not.toBeAssignableTo<Observation.State<Narrow>>()
})

test('secondary value carriers and runner input channels have deliberate variance', () => {
  expect<Observation.Change<Narrow>>().type.toBeAssignableTo<Observation.Change<Wide>>()
  expect<Observation.Change<Wide>>().type.not.toBeAssignableTo<Observation.Change<Narrow>>()
  expect<Record.EntryToken<'narrow'>>().type.toBeAssignableTo<Record.EntryToken<string>>()
  expect<Record.EntryToken<string>>().type.not.toBeAssignableTo<Record.EntryToken<'narrow'>>()
  expect<Runner.Runner<Failure, Caller>>().type.toBeAssignableTo<Runner.Runner<never, never>>()
  expect<Runner.Runner<never, never>>().type.not.toBeAssignableTo<Runner.Runner<Failure, Caller>>()
  expect<Storage.Case<Caller>>().type.not.toBeAssignableTo<Storage.Case<never>>()
  expect<Storage.Case<never>>().type.toBeAssignableTo<Storage.Case<Caller>>()
})

test('lazy scoped factories and deliberate never channels are exact', () => {
  expect(Session.make).type.toBe<Effect.Effect<Session.Service, never, Store.Store | Scope.Scope>>()
  expect(View.make).type.toBe<Effect.Effect<View.Service, never, Store.Store | Scope.Scope>>()
  expect(Memory.make).type.toBe<Effect.Effect<Store.Service, never, Scope.Scope>>()
  expect<{ readonly error: Effect.Error<typeof Session.make> }>().type.toBe<{
    readonly error: never
  }>()
  expect<{ readonly services: Layer.Services<typeof Memory.layer> }>().type.toBe<{
    readonly services: never
  }>()
  expect(Document.copy({ count: 1 })).type.toBe<
    Result.Result<Document.Draft<{ count: number }>, Document.CloneError>
  >()
  expect<Record.Op>().type.not.toBeAssignableFrom<readonly ['delete', readonly []]>()
  expect<Record.Op>().type.not.toBeAssignableFrom<readonly ['set', readonly [], null]>()
})

declare const contextualEntrySchema: Schema.Codec<
  { readonly kind: 'custom'; readonly data: string },
  Record.Entry,
  Caller
>
declare const brandedDraft: Document.Draft<{
  readonly id: Record.TaskId
  readonly items: ReadonlyArray<Record.SubmissionId>
}>
test('migrated runtime compiler proofs retain native metadata, schema services and primitive brands', () => {
  expect<Identity.SessionId>().type.not.toBeAssignableTo<Identity.RequestId>()
  expect<Identity.RequestId>().type.not.toBeAssignableTo<Identity.SessionId>()
  expect(Ownership.layerDeclarations([Generation, ToolCall, Compaction])).type.toBe<
    Layer.Layer<Ownership.Declarations>
  >()
  const token = Result.getOrThrow(Record.defineEntry('custom', contextualEntrySchema))
  expect(token.decode(rawEntry)).type.toBe<
    Effect.Effect<{ readonly kind: 'custom'; readonly data: string }, Schema.SchemaError, Caller>
  >()
  expect(brandedDraft.id).type.toBe<Record.TaskId>()
  expect(brandedDraft.items[0]).type.toBe<Record.SubmissionId | undefined>()
})

declare const snapshot: Document.Snapshot
declare const graph: Ownership.Graph
declare const target: Ownership.Target
declare const view: View.Value
declare const ops: ReadonlyArray<View.Op>
declare const beforeMessage: Prompt.AssistantMessage
declare const afterMessage: Prompt.AssistantMessage

test('dual public operations preserve exact results with optional tails in both forms', () => {
  expect(Document.address(document)).type.toBe<
    Effect.Effect<Record.Address, StorageError.StorageError>
  >()
  expect(Document.address()(document)).type.toBe<
    Effect.Effect<Record.Address, StorageError.StorageError>
  >()
  expect(Document.address({ owner: id })(document)).type.toBe<
    Effect.Effect<Record.Address, StorageError.StorageError>
  >()
  expect(Document.typed(document, snapshot)).type.toBe<
    Effect.Effect<Document.Snapshot<{ readonly count: number }>, StorageError.StorageError>
  >()
  expect(Document.typed(snapshot)(document)).type.toBe<
    Effect.Effect<Document.Snapshot<{ readonly count: number }>, StorageError.StorageError>
  >()
  expect(Record.isAlive('current')).type.toBe<(self: Record.Document) => boolean>()
  expect(Ownership.reach(graph, target, true)).type.toBe<Option.Option<Ownership.Reached>>()
  expect(Ownership.reach(target, true)(graph)).type.toBe<Option.Option<Ownership.Reached>>()
  expect(View.apply(view, ops)).type.toBe<Result.Result<View.Value, View.ViewOperationError>>()
  expect(View.apply(ops)(view)).type.toBe<Result.Result<View.Value, View.ViewOperationError>>()
  expect(View.applyUnsafe(ops)(view)).type.toBe<View.Value>()
  expect(Event.messageChanges([], beforeMessage, afterMessage)).type.toBe<
    Array<Event.MessageChange>
  >()
  expect(Event.messageChanges(beforeMessage, afterMessage)([])).type.toBe<
    Array<Event.MessageChange>
  >()
  expect(Document.address).type.not.toBeCallableWith({
    kind: 'markerless',
    family: false,
    definition: document.definition,
  })
  expect(View.apply).type.not.toBeCallableWith(view, [['delete', []]])
})

test('owner companions preserve compatibility alias equality and scoped memory channels', () => {
  expect<Document.Definition<{ count: number }>>().type.toBe<
    Document.Document.Definition<{ count: number }>
  >()
  expect<Document.DefinitionInput<{ count: number }>>().type.toBe<
    Document.Document.DefinitionInput<{ count: number }>
  >()
  expect<Document.Snapshot<{ count: number }>>().type.toBe<
    Document.Document.Snapshot<{ count: number }>
  >()
  expect<Record.EntryToken<'custom'>>().type.toBe<Record.Entry.Token<'custom'>>()
  expect<Record.EntryDraft>().type.toBe<Record.Entry.Draft>()
  expect<Record.SubmissionCreate>().type.toBe<Record.Submission.Create>()
  expect<Conversation.Options>().type.toBe<Conversation.Conversation.Options>()
  expect<Store.Candidate<number>>().type.toBe<Store.Store.Candidate<number>>()
  expect<Session.ConversationQuery>().type.toBe<Session.Session.ConversationQuery>()
  expect<View.ProjectionWatch<number>>().type.toBe<View.View.ProjectionWatch<number>>()
  expect(Store.makeMemory).type.toBe<Effect.Effect<Store.Service, never, Scope.Scope>>()
  expect(Store.layerMemory).type.toBe<Layer.Layer<Store.Store>>()
  expect(Observation.makeWatch).type.not.toBeCallableWith({ value: {}, stop: Effect.void })
})
