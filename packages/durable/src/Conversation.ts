import * as Agent from '@effect-harness/harness/Agent'
import * as ConversationContext from '@effect-harness/harness/Context'
import * as ModelExecutor from '@effect-harness/harness/Executor'
import * as Registry from '@effect-harness/harness/Registry'
import * as Invocation from '@effect-harness/harness/Invocation'
import * as Totals from '@effect-harness/harness/Usage'
import * as Context from 'effect/Context'
import * as Crypto from 'effect/Crypto'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Document from './Document.ts'
import * as Inbox from './Inbox.ts'
import * as Record from './Record.ts'
import * as Session from './Session.ts'
import { rejected, type StorageError, NotFound } from './StorageError.ts'
import * as Usage from './Usage.ts'
import * as Ownership from './Ownership.ts'
import type * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import { ExecutionError, InvalidState, InvalidArguments } from './workflow/ExecutionError.ts'

export const AgentDoc = Document.defineUnsafe({
  kind: 'harness.agent',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  schema: Agent.State,
  initial: (): Agent.State => ({}),
  checkpointWhen: () => true,
})
export const ProviderState = Schema.Struct({ sessionId: Schema.NonEmptyString })
export const ProviderDoc = Document.defineUnsafe({
  kind: 'harness.provider',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: ProviderState,
  initial: (seed) => ({ sessionId: typeof seed === 'string' ? seed : '' }),
  checkpointWhen: () => true,
})

export interface Options {
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
export class Configuration extends Context.Service<
  Configuration,
  {
    readonly settings: Agent.Settings
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
export const layerConfiguration = (
  options: Options = {},
): Layer.Layer<Configuration, Schema.SchemaError> =>
  Layer.effect(
    Configuration,
    Effect.gen(function* () {
      let settings = yield* Schema.decodeUnknownEffect(Agent.Settings)(
        Agent.settings(options.settings),
      )
      return Configuration.of({
        get settings() {
          // effect-review-allow P1-throw-only-in-unsafe-orthrow: this synchronous
          // service getter copies settings already validated by Agent.Settings.
          return Document.copyUnsafe(settings)
        },
        updateSettings: (input) =>
          Schema.decodeUnknownEffect(Agent.Settings)(Agent.settings(input)).pipe(
            Effect.tap((next) =>
              Effect.sync(() => {
                settings = next
              }),
            ),
            Effect.asVoid,
          ),
        cwd: options.cwd ?? '.',
        report: options.report ?? (() => Effect.void),
        created: options.created ?? (() => Effect.void),
      })
    }),
  )

/** Provide this to Session.layer so raw transaction creation and native Workflow creation share atomic initialization. */
export const layerCreation = Layer.effect(Session.CreationHook)(
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
        yield* tx.doc(Usage.UsageDoc, { owner: conversation.id })
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

const SystemPatch = Schema.Struct({
  sections: Schema.optionalKey(
    Schema.Record(Schema.String, Schema.Union([Schema.String, Schema.Null])),
  ),
  toolsRemoved: Schema.optionalKey(Schema.Array(Schema.String)),
  toolsAdded: Schema.optionalKey(ModelExecutor.Request.fields.tools),
})
export const Metadata = Schema.Struct({
  status: Schema.optionalKey(
    Schema.Literals(['stop', 'length', 'tool-calls', 'aborted', 'error', 'deferred']),
  ),
  usage: Schema.optionalKey(Totals.Usage),
  system: Schema.optionalKey(SystemPatch),
})
export type Metadata = typeof Metadata.Type

/** Convert committed encoded AI messages and context edits into the generic harness's pure context inputs. */
export const projectEntry = Effect.fnUntraced(function* (
  entry: Record.Entry,
): Effect.fn.Return<ConversationContext.Entry, ExecutionError> {
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
  const edits: ConversationContext.Edit[] = []
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
    typeof data === 'object' &&
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

/** Read the complete inherited transcript in ascending entry order; scans themselves remain newest first. */
export const context = Effect.fnUntraced(function* (
  session: Session.Service,
  conversationId: Record.ConversationId,
  at?: Record.EntryId,
) {
  if (at !== undefined && (yield* session.entry(at, conversationId)) === undefined)
    return yield* new ExecutionError({
      reason: new InvalidArguments({
        message: 'Context cutoff is not visible in this conversation',
      }),
    })
  const entries: Record.Entry[] = []
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
  const head = entries.find((entry) => entry.head !== undefined)?.head
  const active = entries.filter((entry) => head === undefined || entry.id >= head)
  const projected = yield* Effect.forEach(active.reverse(), projectEntry)
  return ConversationContext.derive(projected, at)
})

/** Reset is an ordinary entry draft admitted by Submission; transcript, agent, usage and provider identity remain durable. */
export const resetDraft = Effect.fnUntraced(function* (note?: string) {
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

/** Domain reads and atomic configuration only. Execute durable work with the exported native Workflow services. */
export class Conversation extends Context.Service<
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
    ) => Effect.Effect<ConversationContext.View, StorageError | ExecutionError>
  }
>()('@effect-harness/durable/Conversation') {}

export const layer = Layer.effect(Conversation)(
  Effect.gen(function* () {
    const session = yield* Session.Session
    return Conversation.of({
      root: session.root,
      create: (ownership = { kind: 'ownerless' }) =>
        session.transaction((tx) => tx.createConversation({ ownership })),
      fork: (parent, at, ownership = { kind: 'ownerless' }) =>
        session.transaction((tx) => tx.forkConversation(parent, at, { ownership })),
      configure: (id, change) =>
        session.transaction(
          Effect.fnUntraced(function* (tx) {
            if ((yield* tx.conversation(id)) === undefined)
              return yield* rejected('Conversation is absent', NotFound)
            const draft = yield* tx.doc(AgentDoc, { owner: id })
            const next = Agent.configure(draft, change)
            for (const key of Object.keys(draft)) Reflect.deleteProperty(draft, key)
            Object.assign(draft, next)
            return yield* Document.copyEffect(next)
          }),
        ),
      context: (id, at) => context(session, id, at),
    })
  }),
)

/** Wait for ordinary owned work to finish, driving its declared native executions. Omit id to wait across the Session's ownerless conversation roots. Background subtrees are excluded; missing declarations remain blocked until restored. */
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
      const started = new Set<Record.TaskId>()
      const failures = new Map<Record.TaskId, ExecutionError>()
      while (true) {
        const state = yield* session.committed
        const roots =
          id === undefined
            ? state.conversations
                .filter((conversation) => conversation.owner === undefined)
                .map((conversation) => conversation.id)
            : [id]
        const tasks = new Map<Record.TaskId, Record.Task>()
        for (const root of roots) {
          const reached = Ownership.reach(state, { kind: 'conversation', id: root })
          if (reached === undefined) return yield* rejected('Conversation is absent', NotFound)
          for (const task of reached.tasks) tasks.set(task.id, task)
        }
        if (tasks.size === 0) return
        for (const task of tasks.values()) {
          const failure = failures.get(task.id)
          if (failure !== undefined) return yield* failure
          if (started.has(task.id)) continue
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
          if (declarations.get(binding.workflow) === undefined) continue
          started.add(task.id)
          yield* declarations.execute(binding).pipe(
            Effect.catch((error) =>
              Effect.sync(() => {
                failures.set(task.id, error)
              }),
            ),
            Effect.forkScoped,
          )
        }
        yield* Effect.sleep('20 millis')
      }
    }),
  )
})
