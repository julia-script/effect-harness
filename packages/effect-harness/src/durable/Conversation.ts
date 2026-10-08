import * as Serialization from './Serialization.ts'
/**
 * Agent and provider documents, conversation projections and configuration.
 */
import * as MutableHashMap from 'effect/MutableHashMap'
import { constTrue } from 'effect/Function'
import * as Array from 'effect/Array'
import * as Record from 'effect/Record'
import * as Predicate from 'effect/Predicate'
import * as SystemPatch from 'effect-harness/SystemPatch'
import * as Agent from 'effect-harness/Agent'
import * as Transcript from 'effect-harness/Transcript'
import * as Registry from 'effect-harness/Registry'
import * as Invocation from 'effect-harness/Invocation'
import * as Usage from 'effect-harness/Usage'
import * as Context from 'effect/Context'
import * as Crypto from 'effect/Crypto'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Ref from 'effect/Ref'
import * as HashMap from 'effect/HashMap'
import * as HashSet from 'effect/HashSet'
import * as Option from 'effect/Option'
import * as Prompt from 'effect/ai/Prompt'
import * as AiError from 'effect/ai/AiError'
import * as Schema from 'effect/Schema'
import * as Document from './Document.ts'
import * as Inbox from './Inbox.ts'
import type {
  Conversation as ConversationRecord,
  Entry as EntryRecord,
  ConversationId as ConversationIdRecord,
  EntryId as EntryIdRecord,
  TaskId as TaskIdRecord,
  Task as TaskRecord,
} from './Record.ts'
import * as Session from './Session.ts'
import { rejected, type StorageError, NotFoundError } from './StorageError.ts'
import { UsageDoc } from './Usage.ts'
import * as Ownership from './Ownership.ts'
import type * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import {
  ExecutionError,
  InvalidStateError,
  InvalidArgumentsError,
} from './workflow/ExecutionError.ts'

/**
 * Agent settings document definition.
 *
 * @category models
 */
export const AgentDoc = Document.defineUnsafe({
  kind: 'harness.agent',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  schema: Serialization.object(Agent.State),
  initial: (): Agent.State => ({}),
  checkpointWhen: constTrue,
})
/**
 * Schema for persisted provider session UUID used across retries and reopening.
 *
 * @category schemas
 */
export const ProviderState = Schema.Struct({ sessionId: Schema.NonEmptyString })
/**
 * Persisted provider session UUID used across retries and reopening.
 *
 * @category models
 */
export type ProviderState = typeof ProviderState.Type

/**
 * Provider session document definition.
 *
 * @category models
 */
export const ProviderDoc = Document.defineUnsafe({
  kind: 'harness.provider',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: ProviderState,
  initial: (seed) => ({ sessionId: Predicate.isString(seed) ? seed : '' }),
  checkpointWhen: constTrue,
})

/**
 * Type-level contracts for `Conversation`.
 */
export declare namespace Conversation {
  /**
   * Host settings and callbacks used to build conversation Configuration.
   *
   * @category models
   */
  interface Options {
    /**
     * Positive safe integer, default sixteen; sequential tool rounds remain one.
     */
    readonly toolConcurrency?: number | undefined

    readonly settings?: Agent.Settings.Input | undefined
    readonly cwd?: string | undefined
    readonly report?: ((error: unknown) => Effect.Effect<void>) | undefined
    readonly created?:
      | ((
          tx: Session.Transaction,
          conversation: ConversationRecord,
        ) => Effect.Effect<void, StorageError>)
      | undefined
  }
}

/**
 * Configuration service.
 *
 * @category services
 */
export class Configuration extends Context.Service<
  Configuration,
  {
    /**
     * Lazily returns a fresh validated defensive copy of the current policy.
     */
    readonly settings: Effect.Effect<Agent.Settings, Schema.SchemaError>
    readonly toolConcurrency: number
    /**
     * Replaces host defaults for subsequent preparation and current retry/compaction policy decisions.
     */
    readonly updateSettings: (
      options: Agent.Settings.Input,
    ) => Effect.Effect<void, Schema.SchemaError>
    readonly cwd: string
    readonly report: (error: unknown) => Effect.Effect<void>
    readonly created: (
      tx: Session.Transaction,
      conversation: ConversationRecord,
    ) => Effect.Effect<void, StorageError>
  }
>()('effect-harness/durable/Conversation/Configuration') {}
/**
 * Builds host policy and callbacks for conversation execution.
 *
 * **When to use**
 *
 * Use when supplying Configuration independently, such as when sharing one live policy
 * across Sessions. For ordinary Session construction, use {@link layer}.
 *
 * **Details**
 *
 * Validates initial settings and supplies validated live updates. toolConcurrency defaults
 * to 16 and must be a positive safe integer; sequential mode uses one permit.
 *
 * **Gotchas**
 *
 * Invalid settings fail with SchemaError. Agent overrides are committed separately through
 * Conversation.configure.
 *
 * @category constructors
 */
export const makeConfiguration = Effect.fnUntraced(function* (
  options: Conversation.Options = {},
): Effect.fn.Return<Configuration['Service'], Schema.SchemaError> {
  const settings = yield* Ref.make(yield* Agent.settings(options.settings))
  return Configuration.of({
    settings: Ref.get(settings).pipe(
      Effect.flatMap(Schema.encodeEffect(Agent.Settings)),
      Effect.flatMap(Schema.decodeEffect(Agent.Settings)),
    ),
    updateSettings: (input) =>
      Agent.settings(input).pipe(
        Effect.tap((next) => Ref.set(settings, next)),
        Effect.asVoid,
      ),
    toolConcurrency: yield* Schema.decodeEffect(
      Schema.Int.check(
        Schema.isGreaterThan(0),
        Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
      ),
    )(options.toolConcurrency ?? 16),
    cwd: options.cwd ?? '.',
    report: options.report ?? (() => Effect.void),
    created: options.created ?? (() => Effect.void),
  })
})

/**
 * Provides validated live host settings and conversation callbacks.
 *
 * @category layers
 */
export const layerConfiguration = (
  options: Conversation.Options = {},
): Layer.Layer<Configuration, Schema.SchemaError> =>
  Layer.effect(Configuration, makeConfiguration(options))

/**
 * Installs atomic built-in document creation and recovery initialization.
 *
 * **When to use**
 *
 * Use with an independently supplied Configuration. {@link layer} combines both Layers
 * for ordinary Session construction.
 *
 * **Details**
 *
 * Captures native Crypto for provider session UUIDs and an optional Registry for creation
 * callbacks. Provide this Layer while constructing Session.
 *
 * **Gotchas**
 *
 * Recovery repairs missing provider affinity without rerunning conversation-created
 * callbacks. UUIDs are distinct from authorization account and Workflow execution
 * identities.
 *
 * @category layers
 */
export const layerCreation: Layer.Layer<
  Session.CreationHook,
  never,
  Configuration | Crypto.Crypto
> = Layer.effect(Session.CreationHook)(
  Effect.gen(function* () {
    const config = yield* Configuration
    const crypto = yield* Crypto.Crypto
    const registry = yield* Effect.serviceOption(Registry.Registry)
    const recover = Effect.fnUntraced(function* (
      tx: Session.Transaction,
      conversation: ConversationRecord,
    ) {
      const sessionId = yield* crypto.randomUUIDv7.pipe(
        Effect.mapError((cause) =>
          rejected('Provider identity generation failed', undefined, cause),
        ),
      )
      yield* tx.doc(ProviderDoc, { owner: conversation.id, seed: sessionId })
    })
    return Session.CreationHook.of({
      recover,
      run: Effect.fnUntraced(function* (tx, conversation) {
        // Fork copies have already selected the stored agent at the historical cutoff.
        if (conversation.parent === undefined) {
          const agent = yield* tx.doc(AgentDoc, { owner: conversation.id })
          if (conversation.owner !== undefined) {
            const owner = yield* tx.doc(AgentDoc, { owner: conversation.owner.conversationId })
            Object.assign(agent, owner)
          }
        }
        yield* tx.doc(Inbox.InboxDoc, { owner: conversation.id })
        yield* tx.doc(Inbox.LiveDoc, { owner: conversation.id })
        yield* tx.doc(UsageDoc, { owner: conversation.id })
        yield* recover(tx, conversation)
        if (Option.isSome(registry)) {
          const invocation = Invocation.Invocation.of({
            cwd: config.cwd,
            report: config.report,
            progress: () => Effect.void,
          })
          const agent = yield* Registry.resolve(
            yield* registry.value.snapshot,
            yield* tx.doc(AgentDoc, { owner: conversation.id }),
            yield* config.settings.pipe(
              Effect.mapError((cause) => rejected('Invalid host settings', undefined, cause)),
            ),
          ).pipe(Effect.provideService(Invocation.Invocation, invocation))
          for (const handlers of Registry.handlers(agent, 'conversation')) {
            const callback = handlers.conversationCreated
            if (callback !== undefined)
              yield* Effect.suspend(() => callback.call(handlers, conversation.id)).pipe(
                Effect.provideService(Invocation.Invocation, invocation),
                Effect.mapError((error) => rejected(error.message, undefined, error)),
              )
          }
        }
        yield* config.created(tx, conversation)
      }),
    })
  }),
)

/**
 * Provides host configuration and built-in conversation creation and recovery hooks.
 *
 * **When to use**
 *
 * Use when constructing a Session for durable AI conversations.
 * Use Layer.provideMerge to retain Configuration for the executor registration Layer
 * and live policy updates.
 *
 * **Details**
 *
 * The creation hooks and executor share one validated Configuration instance. Captures
 * Crypto for provider session UUIDs and an optional Registry for creation callbacks.
 * Invalid settings fail with SchemaError during Layer construction.
 *
 * **Example** (Composing creation and Session Layers)
 *
 * ```ts
 * const Creation = Conversation.layer({
 *   settings: {
 *     retry: { enabled: false },
 *     compaction: { enabled: false },
 *   },
 * })
 * const Sessions = Session.layer.pipe(Layer.provideMerge(Creation))
 * ```
 *
 * @category layers
 */
export const layer = (
  options: Conversation.Options = {},
): Layer.Layer<Configuration | Session.CreationHook, Schema.SchemaError, Crypto.Crypto> =>
  layerCreation.pipe(Layer.provideMerge(layerConfiguration(options)))

/**
 * Schema for entry metadata for conversation context projection.
 *
 * Failed generations retain their native AI error. Invalid output contributes
 * generic corrective feedback when projected into the next model request;
 * the rejected response itself is unavailable.
 *
 * @category schemas
 */
export const Metadata = Schema.Struct({
  status: Schema.optionalKey(
    Schema.Literals(['stop', 'length', 'tool-calls', 'aborted', 'error', 'deferred']),
  ),
  usage: Schema.optionalKey(Usage.Usage),
  error: Schema.optionalKey(AiError.AiError),
  system: Schema.optionalKey(SystemPatch.SystemPatch),
})
/**
 * Entry metadata for conversation context projection.
 *
 * @category models
 */
export type Metadata = typeof Metadata.Type

/**
 * Converts committed encoded AI messages and context edits to pure harness context inputs.
 *
 * @category combinators
 */
export const projectEntry = Effect.fnUntraced(function* (
  entry: EntryRecord,
): Effect.fn.Return<Transcript.Entry, ExecutionError> {
  const invalid = (cause?: unknown) =>
    new ExecutionError({
      reason: new InvalidStateError({
        message: `Entry ${entry.id} has invalid model context`,
        ...(cause === undefined ? {} : { cause }),
      }),
    })
  const messages = yield* Schema.decodeEffect(Schema.toCodecJson(Schema.Array(Prompt.Message)))(
    entry.model ?? [],
  ).pipe(Effect.mapError(invalid))
  const edits: Array<Transcript.Edit> = []
  for (const edit of entry.edits ?? []) {
    if (edit._tag === 'omit') edits.push(edit)
    else
      edits.push({
        ...edit,
        messages: yield* Schema.decodeEffect(Schema.toCodecJson(Schema.Array(Prompt.Message)))(
          edit.messages,
        ).pipe(Effect.mapError(invalid)),
      })
  }
  const data = entry.data
  const metadata =
    data !== null &&
    Predicate.isObject(data) &&
    !Array.isArray(data) &&
    Object.hasOwn(data, 'harness')
      ? yield* Schema.decodeUnknownEffect(Schema.Struct({ harness: Metadata }))(data).pipe(
          Effect.map((value) => value.harness),
          Effect.mapError(invalid),
        )
      : {}
  return {
    id: entry.id,
    kind: entry.kind,
    ...(entry.head === undefined ? {} : { head: entry.head }),
    messages,
    edits,
    ...metadata,
  }
})

/**
 * Reads the complete inherited transcript in ascending entry order; scans themselves remain newest first.
 *
 * @category combinators
 */
// effect-nit-allow B-no-service-arguments: context is a public combinator over the supplied Session self capability; its facts, journal and owning lifetime must remain those of the selected instance even when ambient services differ.
export const context = Effect.fnUntraced(function* (
  session: Session.Session.Service,
  conversationId: ConversationIdRecord,
  at?: EntryIdRecord,
): Effect.fn.Return<Transcript.View, StorageError | ExecutionError> {
  if (at !== undefined && Option.isNone(yield* session.entry(at, conversationId)))
    return yield* new ExecutionError({
      reason: new InvalidArgumentsError({
        message: 'Context cutoff is not visible in this conversation',
      }),
    })
  const entries: Array<EntryRecord> = []
  // Freeze the visible cutoff before paginating so concurrent appends cannot extend this read.
  const first = yield* session.scanEntries(
    { conversationId, ...(at === undefined ? {} : { maxEntryId: at }) },
    100,
  )
  entries.push(...first.items)
  const cutoff = at ?? first.items[0]?.id
  let cursor = first.next
  while (cursor !== undefined) {
    const page = yield* session.scanEntries(
      { conversationId, ...(cutoff === undefined ? {} : { maxEntryId: cutoff }) },
      100,
      cursor,
    )
    entries.push(...page.items)
    cursor = page.next
  }
  const head = Option.getOrUndefined(
    Option.flatMap(
      Array.findFirst(entries, (entry) => entry.head !== undefined),
      (entry) => Option.fromUndefinedOr(entry.head),
    ),
  )
  const active = Array.filter(entries, (entry) => head === undefined || entry.id >= head)
  const projected = yield* Effect.forEach(active.reverse(), projectEntry)
  return Transcript.derive(projected, at)
})

/**
 * Creates a reset entry draft for submission admission.
 *
 * **Details**
 *
 * Reset preserves the durable transcript, agent, usage and provider identity.
 *
 * @category combinators
 */
export const resetDraft = Effect.fnUntraced(function* (
  note?: string,
): Effect.fn.Return<EntryRecord.Draft, ExecutionError> {
  const message =
    note === undefined ? [] : [Prompt.userMessage({ content: [Prompt.textPart({ text: note })] })]
  const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(Schema.Array(Prompt.Message)))(
    message,
  ).pipe(
    Effect.mapError(
      (cause) =>
        new ExecutionError({
          reason: new InvalidArgumentsError({ message: 'Invalid reset note', cause }),
        }),
    ),
  )
  const model = yield* Schema.decodeEffect(Schema.toCodecJson(Schema.Array(Schema.Json)))(
    encoded,
  ).pipe(
    Effect.mapError(
      (cause) =>
        new ExecutionError({
          reason: new InvalidArgumentsError({ message: 'Invalid reset messages', cause }),
        }),
    ),
  )
  return { kind: 'harness.reset', head: 'self' as const, model } satisfies EntryRecord.Draft
})

/**
 * Domain reads and atomic configuration only. Execute durable work with the exported native Workflow services.
 *
 * @category services
 */
export class Conversation extends Context.Service<
  Conversation,
  {
    readonly root: Session.Session.Service['root']
    readonly create: (
      ownership?: Session.Session.Ownership,
    ) => Effect.Effect<ConversationRecord, StorageError>
    readonly fork: (
      parent: ConversationIdRecord,
      at: EntryIdRecord,
      ownership?: Session.Session.Ownership,
    ) => Effect.Effect<ConversationRecord, StorageError>
    readonly configure: (
      id: ConversationIdRecord,
      change: Agent.State.Change,
    ) => Effect.Effect<Agent.State, StorageError>
    readonly context: (
      id: ConversationIdRecord,
      at?: EntryIdRecord,
    ) => Effect.Effect<Transcript.View, StorageError | ExecutionError>
  }
>()('effect-harness/durable/Conversation') {}

/**
 * Provides conversation creation, fork, configuration and context operations.
 *
 * **Details**
 *
 * Consumes the Session and its creation/configuration dependencies. Mutation operations
 * publish through the Session transaction boundary.
 *
 * @category layers
 */
export const layerFromSession: Layer.Layer<Conversation, never, Session.Session> = Layer.effect(
  Conversation,
)(
  Effect.gen(function* () {
    const session = yield* Session.Session
    return Conversation.of({
      root: session.root,
      create: (ownership = { _tag: 'ownerless' }) =>
        session.transaction((tx) => tx.createConversation({ ownership })),
      fork: (parent, at, ownership = { _tag: 'ownerless' }) =>
        session.transaction((tx) => tx.forkConversation(parent, at, { ownership })),
      configure: (id, change) =>
        session.transaction(
          Effect.fnUntraced(function* (tx) {
            if (Option.isNone(yield* tx.conversation(id)))
              return yield* rejected('Conversation is absent', NotFoundError)
            const draft = yield* tx.doc(AgentDoc, { owner: id })
            const next = Agent.configure(draft, change)
            for (const key of Record.keys<string, unknown>(draft))
              Reflect.deleteProperty(draft, key)
            Object.assign(draft, next)
            return yield* Document.copyEffect(next)
          }),
        ),
      context: (id, at) => context(session, id, at),
    })
  }),
)

/**
 * Waits for non-background owned work and drives its declared native executions.
 *
 * **When to use**
 *
 * Use when committed submission settlement must be followed by completion of ordinary owned
 * work.
 *
 * **Details**
 *
 * A conversation ID selects one ownership root; omitting it selects the Session’s ownerless
 * conversation roots. Requires ownership declarations and the native WorkflowEngine.
 *
 * **Gotchas**
 *
 * Missing declarations leave work blocked until compatible code is restored. Background
 * tasks do not hold this wait.
 *
 * @category combinators
 */
// effect-nit-allow B-no-service-arguments: awaitIdle is a public combinator over the supplied Session self capability; its facts, journal and owning lifetime must remain those of the selected instance even when ambient services differ.
export const awaitIdle = Effect.fnUntraced(function* (
  session: Session.Session.Service,
  id?: ConversationIdRecord,
): Effect.fn.Return<
  void,
  StorageError | ExecutionError,
  Ownership.Declarations | WorkflowEngine.WorkflowEngine
> {
  const declarations = yield* Ownership.Declarations
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const started = yield* Ref.make(HashSet.empty<TaskIdRecord>())
      const failures = yield* Ref.make(HashMap.empty<TaskIdRecord, ExecutionError>())
      while (true) {
        const state = yield* session.committed
        const roots =
          id === undefined
            ? Array.filter(
                state.conversations,
                (conversation) => conversation.owner === undefined,
              ).map((conversation) => conversation.id)
            : [id]
        const tasks = MutableHashMap.empty<TaskIdRecord, TaskRecord>()
        for (const root of roots) {
          const reachedOption = Ownership.reach(state, {
            _tag: 'conversation',
            id: root,
          })
          if (Option.isNone(reachedOption))
            return yield* rejected('Conversation is absent', NotFoundError)
          const reached = reachedOption.value
          for (const task of reached.tasks) MutableHashMap.set(tasks, task.id, task)
        }
        if (MutableHashMap.size(tasks) === 0) return
        for (const task of MutableHashMap.values(tasks)) {
          const failure = HashMap.get(yield* Ref.get(failures), task.id)
          if (Option.isSome(failure)) return yield* failure.value
          if (HashSet.has(yield* Ref.get(started), task.id)) continue
          const binding = yield* Schema.decodeUnknownEffect(Ownership.Binding)(task.input).pipe(
            Effect.mapError(
              (cause) =>
                new ExecutionError({
                  reason: new InvalidStateError({
                    message: `Task ${task.id} has no native binding`,
                    cause,
                  }),
                }),
            ),
          )
          if (Option.isNone(declarations.get(binding.workflow))) continue
          yield* Ref.update(started, HashSet.add(task.id))
          yield* Ownership.execute(binding).pipe(
            Effect.catch((error) => Ref.update(failures, HashMap.set(task.id, error))),
            Effect.forkScoped,
          )
        }
        yield* Effect.sleep('20 millis')
      }
    }),
  )
})

/** Checks the decoded ProviderState contract without decoding or coercing input.
 * @category guards
 */
export const isProviderState: (u: unknown) => u is ProviderState = Schema.is(
  Schema.toType(ProviderState),
)

/** Checks the decoded Metadata contract without decoding or coercing input.
 * @category guards
 */
export const isMetadata: (u: unknown) => u is Metadata = Schema.is(Schema.toType(Metadata))
