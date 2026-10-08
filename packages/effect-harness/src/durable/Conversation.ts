/**
 * Agent and provider documents, conversation projections and configuration.
 */
import { constTrue } from 'effect/Function'
import * as Arr from 'effect/Array'
import * as records from 'effect/Record'
// effect-review-allow P9-namespace-alias-equals-module: effect/Record and durable/Record both introduce Record; the records alias distinguishes dictionary operations from the imported domain schema namespace.
import * as Predicate from 'effect/Predicate'
import * as systemPatch from 'effect-harness/SystemPatch'
// effect-review-allow P9-namespace-alias-equals-module: the exported SystemPatch schema binding collides with its canonical source namespace.
import * as Agent from 'effect-harness/Agent'
import * as Context from 'effect-harness/Context'
import * as Registry from 'effect-harness/Registry'
import * as Invocation from 'effect-harness/Invocation'
import * as Usage from 'effect-harness/Usage'
import { Service } from 'effect/Context'
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
import type * as Record from './Record.ts'
import * as Session from './Session.ts'
import { rejected, type StorageError, NotFound } from './StorageError.ts'
import { UsageDoc } from './Usage.ts'
import * as Ownership from './Ownership.ts'
import type * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import { ExecutionError, InvalidState, InvalidArguments } from './workflow/ExecutionError.ts'

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
  schema: Document.jsonObjectCodec(Agent.State),
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
 *
 * @category utility types
 */
export declare namespace Conversation {
  /**
   * Host settings and callbacks used to build conversation Configuration.
   *
   * @category models
   */
  interface Options {
    /** Positive safe integer, default sixteen; sequential tool rounds remain one. */
    readonly toolConcurrency?: number | undefined

    readonly settings?: Agent.SettingsInput | undefined
    readonly cwd?: string | undefined
    readonly report?: ((error: unknown) => Effect.Effect<void>) | undefined
    readonly created?:
      | ((
          tx: Session.Transaction,
          conversation: Record.Conversation,
        ) => Effect.Effect<void, StorageError>)
      | undefined
  }
}
/**
 * Host settings and callbacks used to build conversation Configuration.
 *
 * @category models
 */
export type Options = Conversation.Options

/**
 * Configuration service.
 *
 * @category services
 */
export class Configuration extends Service<
  Configuration,
  {
    readonly settings: Agent.Settings
    readonly toolConcurrency: number
    /** Replace host defaults for subsequent preparation and current retry/compaction policy decisions. */
    readonly updateSettings: (
      options: Agent.SettingsInput,
    ) => Effect.Effect<void, Schema.SchemaError>
    readonly cwd: string
    readonly report: (error: unknown) => Effect.Effect<void>
    readonly created: (
      tx: Session.Transaction,
      conversation: Record.Conversation,
    ) => Effect.Effect<void, StorageError>
  }
>()('@effect-harness/durable/Conversation/Configuration') {}
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
 * @category layers
 */
export const layerConfiguration = (
  options: Options = {},
): Layer.Layer<Configuration, Schema.SchemaError> =>
  Layer.effect(
    Configuration,
    Effect.gen(function* () {
      const settings = yield* Ref.make(yield* Agent.settings(options.settings))
      return Configuration.of({
        get settings() {
          // effect-review-allow P1-throw-only-in-unsafe-orthrow: this synchronous
          // service getter copies settings already validated by Agent.Settings.
          return Schema.decodeSync(Agent.Settings)(
            Schema.encodeSync(Agent.Settings)(Ref.getUnsafe(settings)),
          )
        },
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
    }),
  )

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
      conversation: Record.Conversation,
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
          const invocation = { cwd: config.cwd, report: config.report, progress: () => Effect.void }
          const agent = yield* Registry.resolve(
            yield* registry.value.snapshot,
            yield* tx.doc(AgentDoc, { owner: conversation.id }),
            config.settings,
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
 * Supply this Layer when constructing a Session for durable AI conversations.
 * Use Layer.provideMerge to retain Configuration for the executor registration Layer
 * and live policy updates.
 *
 * **Details**
 *
 * The creation hooks and executor share one validated Configuration instance. Captures
 * Crypto for provider session UUIDs and an optional Registry for creation callbacks.
 * Invalid settings fail with SchemaError during Layer construction.
 *
 * @example
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
  options: Options = {},
): Layer.Layer<Configuration | Session.CreationHook, Schema.SchemaError, Crypto.Crypto> =>
  layerCreation.pipe(Layer.provideMerge(layerConfiguration(options)))

/**
 * Managed prompt sections and tool declarations recorded in conversation history.
 *
 * @category combinators
 */
export const SystemPatch = systemPatch.SystemPatch
/**
 * Managed prompt sections and tool declarations recorded in conversation history.
 *
 * @category models
 */
export type SystemPatch = typeof SystemPatch.Type
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
  system: Schema.optionalKey(SystemPatch),
})
/**
 * Entry metadata for conversation context projection.
 *
 * @category models
 */
export type Metadata = typeof Metadata.Type

/**
 * Convert committed encoded AI messages and context edits into the generic harness's pure context inputs.
 *
 * @category combinators
 */
export const projectEntry = Effect.fnUntraced(function* (
  entry: Record.Entry,
): Effect.fn.Return<Context.Entry, ExecutionError> {
  const invalid = (cause?: unknown) =>
    new ExecutionError({
      reason: new InvalidState({
        message: `Entry ${entry.id} has invalid model context`,
        ...(cause === undefined ? {} : { cause }),
      }),
    })
  const messages = yield* Schema.decodeEffect(Schema.toCodecJson(Schema.Array(Prompt.Message)))(
    entry.model ?? [],
  ).pipe(Effect.mapError(invalid))
  const edits: Array<Context.Edit> = []
  for (const edit of entry.edits ?? []) {
    if (edit.action === 'omit') edits.push(edit)
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
    !Arr.isArray(data) &&
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
export const context = Effect.fnUntraced(function* (
  session: Session.Service,
  conversationId: Record.ConversationId,
  at?: Record.EntryId,
): Effect.fn.Return<Context.View, StorageError | ExecutionError> {
  if (at !== undefined && Option.isNone(yield* session.entry(at, conversationId)))
    return yield* new ExecutionError({
      reason: new InvalidArguments({
        message: 'Context cutoff is not visible in this conversation',
      }),
    })
  const entries: Array<Record.Entry> = []
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
      Arr.findFirst(entries, (entry) => entry.head !== undefined),
      (entry) => Option.fromUndefinedOr(entry.head),
    ),
  )
  const active = Arr.filter(entries, (entry) => head === undefined || entry.id >= head)
  const projected = yield* Effect.forEach(active.reverse(), projectEntry)
  return Context.derive(projected, at)
})

/**
 * Reset is an ordinary entry draft admitted by Submission; transcript, agent, usage and provider identity remain durable.
 *
 * @category combinators
 */
export const resetDraft = Effect.fnUntraced(function* (
  note?: string,
): Effect.fn.Return<Record.EntryDraft, ExecutionError> {
  const message =
    note === undefined ? [] : [Prompt.userMessage({ content: [Prompt.textPart({ text: note })] })]
  const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(Schema.Array(Prompt.Message)))(
    message,
  ).pipe(
    Effect.mapError(
      (cause) =>
        new ExecutionError({
          reason: new InvalidArguments({ message: 'Invalid reset note', cause }),
        }),
    ),
  )
  const model = yield* Schema.decodeEffect(Schema.toCodecJson(Schema.Array(Schema.Json)))(
    encoded,
  ).pipe(
    Effect.mapError(
      (cause) =>
        new ExecutionError({
          reason: new InvalidArguments({ message: 'Invalid reset messages', cause }),
        }),
    ),
  )
  return { kind: 'harness.reset', head: 'self' as const, model } satisfies Record.EntryDraft
})

/**
 * Domain reads and atomic configuration only. Execute durable work with the exported native Workflow services.
 *
 * @category services
 */
export class Conversation extends Service<
  Conversation,
  {
    readonly root: Session.Service['root']
    readonly create: (
      ownership?: Session.Ownership,
    ) => Effect.Effect<Record.Conversation, StorageError>
    readonly fork: (
      parent: Record.ConversationId,
      at: Record.EntryId,
      ownership?: Session.Ownership,
    ) => Effect.Effect<Record.Conversation, StorageError>
    readonly configure: (
      id: Record.ConversationId,
      change: Agent.Change,
    ) => Effect.Effect<Agent.State, StorageError>
    readonly context: (
      id: Record.ConversationId,
      at?: Record.EntryId,
    ) => Effect.Effect<Context.View, StorageError | ExecutionError>
  }
>()('@effect-harness/durable/Conversation') {}

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
      create: (ownership = { _tag: 'ownerless', kind: 'ownerless' }) =>
        session.transaction((tx) => tx.createConversation({ ownership })),
      fork: (parent, at, ownership = { _tag: 'ownerless', kind: 'ownerless' }) =>
        session.transaction((tx) => tx.forkConversation(parent, at, { ownership })),
      configure: (id, change) =>
        session.transaction(
          Effect.fnUntraced(function* (tx) {
            if (Option.isNone(yield* tx.conversation(id)))
              return yield* rejected('Conversation is absent', NotFound)
            const draft = yield* tx.doc(AgentDoc, { owner: id })
            const next = Agent.configure(draft, change)
            for (const key of records.keys<string, unknown>(draft))
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
export const awaitIdle = Effect.fnUntraced(function* (
  session: Session.Service,
  id?: Record.ConversationId,
): Effect.fn.Return<
  void,
  StorageError | ExecutionError,
  Ownership.Declarations | WorkflowEngine.WorkflowEngine
> {
  const declarations = yield* Ownership.Declarations
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const started = yield* Ref.make(HashSet.empty<Record.TaskId>())
      const failures = yield* Ref.make(HashMap.empty<Record.TaskId, ExecutionError>())
      while (true) {
        const state = yield* session.committed
        const roots =
          id === undefined
            ? Arr.filter(
                state.conversations,
                (conversation) => conversation.owner === undefined,
              ).map((conversation) => conversation.id)
            : [id]
        const tasks = new Map<Record.TaskId, Record.Task>()
        for (const root of roots) {
          const reachedOption = Ownership.reach(state, {
            _tag: 'conversation',
            kind: 'conversation',
            id: root,
          })
          if (Option.isNone(reachedOption))
            return yield* rejected('Conversation is absent', NotFound)
          const reached = reachedOption.value
          for (const task of reached.tasks) tasks.set(task.id, task)
        }
        if (tasks.size === 0) return
        for (const task of tasks.values()) {
          const failure = HashMap.get(yield* Ref.get(failures), task.id)
          if (Option.isSome(failure)) return yield* failure.value
          if (HashSet.has(yield* Ref.get(started), task.id)) continue
          const binding = yield* Schema.decodeUnknownEffect(Ownership.Binding)(task.input).pipe(
            Effect.mapError(
              (cause) =>
                new ExecutionError({
                  reason: new InvalidState({
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
