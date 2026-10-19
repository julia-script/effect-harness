/** Built-in durable compaction uses the existing scheduler and Session transaction line. */
import * as PromptWire from '../PromptWire.js'
import * as Cause from 'effect/Cause'
import * as Clock from 'effect/Clock'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as AiError from 'effect/ai/AiError'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import type * as Agent from '../Agent.js'
import * as Compaction from '../Compaction.js'
import { CompactionError } from '../CompactionError.js'
import * as Contract from '../CompactionTask.js'
import * as Status from '../CompactionStatus.js'
import { CompactionRequest } from '../CompactionRequest.js'
import * as Identity from '../Identity.js'
import * as Model from '../Model.js'
import * as Prepared from '../PreparedRequest.js'
import { ProviderAffinity } from '../ProviderAffinity.js'
import type * as Record from '../Record.js'
import * as Settings from '../Settings.js'
import { SessionError } from '../SessionError.js'
import { StorageError } from '../StorageError.js'
import * as StorageFormat from '../StorageFormat.js'
import { Storage } from '../Storage.js'
import * as Task from '../Task.js'
import { TaskExecution } from '../TaskExecution.js'
import * as Transaction from '../Transaction.js'
import * as Usage from '../Usage.js'
import * as Value from './DocumentValue.js'
import * as Plan from './CompactionPlan.js'
import * as Protocol from './ContextProtocol.js'
import * as Inbox from './Inbox.js'
import * as ProviderState from './ProviderState.js'
import * as Preparation from './RequestPreparation.js'
import * as Restore from './PreparationRestoreFailure.js'
import * as ModelStream from './ModelStream.js'
import { document as agentDocument, target as agentTarget } from './AgentState.js'
import type * as Hook from '../Hook.js'
import type { HarnessError } from '../HarnessError.js'
import type { RegistryError } from '../RegistryError.js'

export interface Services {
  readonly context: Context.Context<never>
  readonly models: ReadonlyArray<Model.Any>
  readonly nativeDefault: Option.Option<typeof LanguageModel.LanguageModel.Service>
  readonly nativeModel: Prepared.Binding
  readonly nativeCompaction?: Model.CompactionCapability
  readonly defaultAgent: Agent.State
  readonly runHooks: (
    id: Record.ConversationId,
    agent: Agent.State,
    event: Hook.Event,
    taskId?: Record.TaskId,
  ) => Effect.Effect<Hook.Output<Hook.Name>, HarnessError | RegistryError>
}
export const requireFormat = StorageFormat.requireSupportedFormat({
  formatVersion: 8,
  minimumReaderVersion: 8,
})
export const activateFormat = (services: Pick<Services, 'context'>) =>
  requireFormat.pipe(
    Effect.provideContext(services.context),
    Effect.provideService(Storage, Context.getUnsafe(services.context, Storage)),
  )
const invalid = (message: string) => new CompactionError({ reason: 'invalid', message })
const identity = (
  envelope: Contract.Envelope,
  attempt: Usage.AttemptId,
): Usage.ReceiptIdentity => ({
  _tag: 'requestAttempt',
  requestId: envelope.preparation.requestId,
  attempt,
  purpose: 'compaction',
})
const updateStatus = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  id: Record.ConversationId,
  taskId: Record.TaskId,
  change?: Omit<typeof Status.ItemSchema.Type, 'taskId' | 'reason' | 'blocking'>,
) {
  const saved = yield* Transaction.ensureDocument(tx, Status.document, Status.target(id))
  const items = saved.value.items.flatMap((item) => {
    if (item.taskId !== taskId) return [item]
    if (change === undefined) return []
    const { retryAt: _retryAt, ...previous } = item
    return [{ ...previous, ...change }]
  })
  yield* Transaction.setDocument(tx, Status.document, Status.target(id), { items })
})
export const canonical = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  id: Record.ConversationId,
) {
  const saved = yield* Transaction.snapshot(tx, importContextProtocol, Inbox.target(id))
  const entries = yield* Transaction.scanEntries(tx, {
    conversationId: id,
    order: 'ascending',
  }).pipe(Stream.runCollect)
  const protocol = Option.isSome(saved)
    ? saved.value.value
    : yield* Protocol.activate({
        conversationId: id,
        ...(entries.at(-1) === undefined ? {} : { tail: entries.at(-1)!.id }),
      })
  const view = yield* Protocol.project(id, entries, protocol)
  return { protocol, entries, view }
})
/** Atomic background uniqueness includes completing records, regardless of status cleanup. */
export const admit = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  id: Record.ConversationId,
  input: typeof Contract.InputSchema.Type,
  owner?: Record.TaskId,
  ready?: Ready,
) {
  const background = owner === undefined && input.reason !== 'manual'
  if (background) {
    const tasks = yield* Transaction.scanTasks(tx, {
      conversationId: id,
      kind: Contract.kind,
    }).pipe(Stream.runCollect)
    const existing = tasks.find((task) => task.background && task.state.status !== 'terminal')
    if (existing !== undefined) return existing
  }
  if (ready !== undefined) yield* stageReady(tx, id, ready)
  const record = yield* Transaction.createTask(tx, {
    conversationId: id,
    kind: Contract.kind,
    version: 1,
    background,
    abortRequested: false,
    ...(owner === undefined ? {} : { owner }),
    input: yield* Task.encode(Contract.InputSchema, input),
    state: {
      status: 'pending',
      checkpoint: yield* Task.encode(
        Contract.CheckpointSchema,
        ready === undefined ? { phase: 'select' } : checkpoint(ready),
      ),
    },
  })
  const saved = yield* Transaction.ensureDocument(tx, Status.document, Status.target(id))
  yield* Transaction.setDocument(tx, Status.document, Status.target(id), {
    items: [
      ...saved.value.items,
      {
        taskId: record.id,
        reason: input.reason,
        blocking: input.blocking,
        phase: ready === undefined ? 'select' : 'summarize',
        attempt: 0,
      },
    ],
  })
  return record
})

export type Ready =
  | { readonly _tag: 'Supplied'; readonly fingerprint: Plan.Fingerprint; readonly summary: string }
  | { readonly _tag: 'Prepared'; readonly envelope: Contract.Envelope }
/** Validate hooks, summary capability and encoded configuration before incompatible progress. */
export const prepare = Effect.fn('Compaction.prepare')(function* (
  services: Services,
  id: Record.ConversationId,
  input: typeof Contract.InputSchema.Type,
  read: <A, E, R>(
    change: (tx: Transaction.Transaction) => Effect.Effect<A, E, R>,
  ) => Effect.Effect<
    A,
    | E
    | import('../TaskError.js').Failure
    | import('./NativeWriteFailure.js').NativeWriteFailure
    | import('./StreamPublicationFailure.js').StreamPublicationFailure,
    R
  >,
  hookTask: Record.TaskId,
  requestId: Identity.RequestId,
  suppliedSelection?: Compaction.Selected,
) {
  const settings = yield* Settings.getOrDefault.pipe(Effect.provideContext(services.context))
  const selected = yield* read((tx) =>
    Effect.gen(function* () {
      const saved = yield* Transaction.snapshot(tx, agentDocument, agentTarget(id))
      const current = {
        agent: { ...services.defaultAgent, ...(Option.isSome(saved) ? saved.value.value : {}) },
        ...(yield* canonical(tx, id)),
      }
      const selection =
        suppliedSelection ??
        (yield* Compaction.select(current.view, settings.compaction, {
          reason: input.reason,
          ...(input.instructions === undefined ? {} : { instructions: input.instructions }),
        }).pipe(Effect.provideContext(services.context)))
      if (selection._tag === 'Noop') return { _tag: 'Noop' as const, reason: selection.reason }
      const fingerprint = yield* Plan.capture(selection, current)
      // Capture the compatible interpretation before hooks can append context. Noop or
      // invalid selection leaves no activation; later summary failure leaves only this fact.
      yield* Inbox.ensureContext(tx, id)
      return { _tag: 'Selected' as const, current, selection, fingerprint }
    }),
  )
  if (selected._tag === 'Noop') return selected
  const { current, selection, fingerprint } = selected
  const descriptor =
    current.agent.model === undefined
      ? services.models[0]
      : services.models.find((model) =>
          Schema.toEquivalence(Model.ReferenceSchema)(
            model.definition.ref,
            current.agent.model!.ref,
          ),
        )
  const hook = yield* services.runHooks(
    id,
    current.agent,
    {
      _tag: 'beforeCompact',
      reason: input.reason,
      entries: fingerprint.selected.map((item) => item.entry),
      prompt: selection.source,
      firstKept: selection.firstKept,
      ...(input.instructions === undefined ? {} : { instructions: input.instructions }),
    },
    hookTask,
  )
  if (hook !== undefined && '_tag' in hook && hook._tag === 'decline')
    return { _tag: 'Noop' as const, reason: 'declined' }
  if (hook !== undefined && '_tag' in hook && hook._tag === 'summary') {
    const summary = hook.summary.trim()
    if (summary.length === 0)
      return yield* new CompactionError({
        reason: 'invalidSummary',
        message: 'Hook summary contains no text',
      })
    return { _tag: 'Supplied' as const, fingerprint, summary }
  }
  const capability =
    descriptor?.compaction ??
    (current.agent.model === undefined ? services.nativeCompaction : undefined)
  if (capability === undefined)
    return yield* new CompactionError({
      reason: 'noModel',
      message: 'Selected model has no explicit summary capability',
    })
  yield* Schema.decodeEffect(Prepared.BindingSchema)(capability.binding)
  yield* Schema.decodeEffect(Prepared.BindingSchema)(capability.configuration.binding)
  yield* Schema.decodeEffect(Prepared.BindingSchema)(services.nativeModel)
  if (descriptor !== undefined)
    yield* Schema.decodeEffect(Model.DefinitionSchema)(descriptor.definition)
  const budget = Math.min(
    Math.floor(settings.compaction.reserveTokens * 0.8),
    descriptor?.definition.maxOutputTokens ?? Number.MAX_SAFE_INTEGER,
  )
  const policy = yield* Schema.decodeEffect(importPolicy)({
    requestId,
    attempt: 0,
    maxOutputTokens: budget,
    cacheRetention: 'none',
    deferred: false,
  })
  const affinity = yield* read((tx) =>
    Effect.gen(function* () {
      const found = yield* Transaction.snapshot(
        tx,
        ProviderState.document,
        ProviderState.target(id),
      )
      return Option.isSome(found) && found.value.value.id.length > 0
        ? found.value.value.id
        : yield* ProviderState.fresh
    }),
  )
  const scope = yield* Scope.Scope
  const context = Context.add(
    Context.add(services.context, ProviderAffinity, { id: affinity }),
    Scope.Scope,
    scope,
  )
  const preparationPolicy: Model.PreparationPolicy & typeof importPolicy.Type = {
    ...policy,
    ordinal: 0,
    settings: { ...settings, stream: { ...settings.stream, deferred: false } },
  }
  let configured = Context.empty()
  let options: Schema.JsonObject = {}
  if (descriptor !== undefined) {
    const codec = Schema.toCodecJson(
      descriptor.options as Schema.ConstraintCodec<unknown, unknown, never, never>,
    )
    const decoded = yield* Schema.decodeEffect(codec)(current.agent.model?.options ?? {}).pipe(
      Effect.provideContext(context),
    )
    options = yield* Schema.encodeEffect(codec)(decoded).pipe(
      Effect.provideContext(context),
      Effect.flatMap(Schema.decodeUnknownEffect(Schema.JsonObject)),
    )
    const configure = descriptor.configure as (
      options: unknown,
      policy: Model.PreparationPolicy,
    ) => Effect.Effect<Context.Context<never>, import('../ModelError.js').ModelError>
    configured = yield* configure(decoded, preparationPolicy).pipe(Effect.provideContext(context))
  }
  configured = yield* capability
    .configure(configured, preparationPolicy)
    .pipe(Effect.provideService(Scope.Scope, scope))
  const configuration = yield* capability.configuration.encode(configured)
  const prepared = yield* Plan.preparePrompt(selection.source, input.instructions)
  const envelope = yield* Value.copy(Schema.toCodecJson(Contract.EnvelopeSchema), {
    capability: capability.binding,
    fingerprint,
    omittedMedia: prepared.transcript.omittedMedia,
    policy,
    preparation: {
      requestId: policy.requestId,
      attempt: policy.attempt,
      ordinal: 0,
      prompt: prepared.prompt,
      agent: current.agent,
      settings: preparationPolicy.settings,
      affinity,
      model:
        descriptor === undefined
          ? { _tag: 'native', binding: services.nativeModel }
          : {
              _tag: 'descriptor',
              binding: {
                id: JSON.stringify([
                  descriptor.definition.ref.instanceId ?? descriptor.definition.ref.provider,
                  descriptor.definition.ref.modelId,
                ]),
                version: descriptor.version ?? 1,
              },
              definition: descriptor.definition,
            },
      options,
      configuration,
      configurationBinding: capability.configuration.binding,
      execution: 'streaming',
      registryGeneration: 0,
      requiredBindings: [],
      tools: [],
      allTools: [],
      translation: {
        prompt: prepared.prompt,
        tools: [],
        frames: [],
        toolPositions: [],
        positioning: { sections: 'leading', tools: 'latest' },
      },
    },
  })
  return { _tag: 'Prepared' as const, envelope }
})

const checkpoint = (ready: Ready): Contract.Checkpoint =>
  ready._tag === 'Supplied'
    ? { phase: 'supplied', fingerprint: ready.fingerprint, summary: ready.summary }
    : {
        phase: 'summarize',
        envelope: ready.envelope,
        attempt: ready.envelope.policy.attempt,
        retries: 0,
        started: false,
      }
export const stageReady = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  id: Record.ConversationId,
  ready: Ready,
) {
  yield* Inbox.validateCompaction(
    tx,
    id,
    ready._tag === 'Supplied' ? ready.fingerprint : ready.envelope.fingerprint,
  )
  if (ready._tag === 'Prepared') {
    const found = yield* Transaction.snapshot(tx, ProviderState.document, ProviderState.target(id))
    if (
      Option.isSome(found) &&
      found.value.value.id.length > 0 &&
      found.value.value.id !== ready.envelope.preparation.affinity
    )
      return yield* new CompactionError({
        reason: 'stale',
        message: 'Provider affinity changed during summary preparation',
      })
    yield* Transaction.ensureDocument(tx, ProviderState.document, ProviderState.target(id))
    yield* Transaction.setDocument(tx, ProviderState.document, ProviderState.target(id), {
      id: ready.envelope.preparation.affinity,
    })
  }
})

export const make = (services: Services) => {
  const pinned = Effect.fnUntraced(function* (envelope: Contract.Envelope) {
    const model = Preparation.selectedModel(envelope.preparation, services.models)
    const capability =
      model?.compaction ??
      (envelope.preparation.model._tag === 'native' ? services.nativeCompaction : undefined)
    const definition = envelope.preparation.model
    if (
      definition._tag === 'descriptor' &&
      (model === undefined ||
        !Schema.toEquivalence(Model.DefinitionSchema)(model.definition, definition.definition) ||
        (model.version ?? 1) !== definition.binding.version)
    )
      return yield* invalid('Pinned summary model binding is unavailable or changed')
    if (
      definition._tag === 'native' &&
      (!Schema.toEquivalence(Prepared.BindingSchema)(definition.binding, services.nativeModel) ||
        Option.isNone(services.nativeDefault))
    )
      return yield* invalid('Pinned native summary model is unavailable or changed')
    if (
      capability === undefined ||
      !Schema.toEquivalence(Prepared.BindingSchema)(capability.binding, envelope.capability) ||
      !Schema.toEquivalence(Prepared.BindingSchema)(
        capability.configuration.binding,
        envelope.preparation.configurationBinding,
      )
    )
      return yield* invalid('Pinned summary capability or configuration codec changed')
    return {
      model: model?.languageModel ?? Option.getOrUndefined(services.nativeDefault)!,
      capability,
    }
  })
  const finish = Effect.fnUntraced(function* (result: Contract.Result) {
    const execution = yield* TaskExecution
    yield* execution.commit((tx) =>
      Effect.gen(function* () {
        yield* updateStatus(tx, execution.conversationId, execution.taskId)
        yield* execution.progress(tx, Contract.contract, {
          status: 'completing',
          outcome: { status: 'completed', result },
        })
      }),
    )
  })
  const fail = Effect.fnUntraced(function* <E>(
    cause: Cause.Cause<E>,
    observation?: {
      readonly envelope: Contract.Envelope
      readonly attempt: Usage.AttemptId
      readonly usage?: Usage.Record
    },
    publicationRejected = false,
  ) {
    if (Cause.hasInterruptsOnly(cause)) return yield* Effect.failCause(cause)
    const reason = cause.reasons.length === 1 ? cause.reasons[0] : undefined
    if (
      Cause.hasInterrupts(cause) ||
      reason === undefined ||
      !Cause.isFailReason(reason) ||
      reason.error instanceof Restore.PreparationRestoreFailure ||
      Schema.is(SessionError)(reason.error) ||
      (Schema.is(StorageError)(reason.error) && !publicationRejected)
    )
      return yield* new SessionError({
        reason: 'invalid',
        operation: 'compaction.invoke',
        message: Cause.pretty(cause),
        cause,
      })
    const execution = yield* TaskExecution
    yield* execution.commit((tx) =>
      Effect.gen(function* () {
        if (observation !== undefined)
          yield* Usage.record(tx, {
            conversationId: execution.conversationId,
            identity: identity(observation.envelope, observation.attempt),
            outcome: { _tag: 'failed', error: { message: Cause.pretty(cause) } },
            ...(observation.usage === undefined ? {} : { usage: observation.usage }),
          })
        yield* updateStatus(tx, execution.conversationId, execution.taskId)
        yield* execution.progress(tx, Contract.contract, {
          status: 'completing',
          outcome: {
            status: 'failed',
            error: { message: Cause.pretty(cause) },
            result: {
              reason: Schema.is(CompactionError)(reason.error) ? reason.error.reason : 'failed',
              message: Cause.pretty(cause),
            },
          },
        })
      }),
    )
  })
  const place = Effect.fn('Compaction.place')(function* (
    fingerprint: Plan.Fingerprint,
    text: string,
    envelope?: Contract.Envelope,
    attempt?: Usage.AttemptId,
    usage?: Usage.Record,
  ) {
    const execution = yield* TaskExecution
    const summary = text.trim()
    if (summary.length === 0)
      return yield* new CompactionError({
        reason: 'invalidSummary',
        message: 'Summary contains no text',
      })
    let staged = false
    yield* execution
      .commit((tx) =>
        Effect.gen(function* () {
          yield* Inbox.validateCompaction(tx, execution.conversationId, fingerprint)
          const current = yield* Transaction.task(tx, execution.taskId)
          if (Option.isNone(current)) return yield* invalid('Compaction task is missing')
          const input = yield* Task.decode(Contract.InputSchema, current.value.input)
          const draft = {
            kind: 'compaction',
            head: fingerprint.firstKept,
            model: yield* Schema.encodeEffect(Schema.Array(Schema.toCodecJson(PromptWire.Message)))(
              [
                Prompt.userMessage({
                  content: [
                    Prompt.textPart({
                      text: `The conversation history before this point was compacted into the following summary:\n\n<summary>\n${summary}\n</summary>`,
                    }),
                  ],
                }),
              ],
            ),
          }
          let result: Contract.Result
          if (input.blocking) {
            if (current.value.owner === undefined)
              return yield* invalid('Blocking compaction has no generation owner')
            const owner = yield* Transaction.task(tx, current.value.owner)
            if (
              Option.isNone(owner) ||
              owner.value.abortRequested ||
              owner.value.state.status !== 'waiting' ||
              !owner.value.state.on?.includes(current.value.id)
            )
              return yield* new CompactionError({
                reason: 'stale',
                message: 'Blocking compaction owner is no longer waiting',
              })
            const entry = yield* Transaction.appendEntry(tx, execution.conversationId, draft)
            result = { _tag: 'Placed', entryId: entry.id }
          } else {
            const submission = yield* Inbox.admit(tx, execution.conversationId, {
              mode: 'write',
              payload: draft,
              compaction: fingerprint,
              requestId: yield* Schema.decodeEffect(Identity.RequestId)(
                `compaction:${execution.taskId}`,
              ),
            })
            result = { _tag: 'Queued', submissionId: submission.id }
          }
          if (envelope !== undefined && attempt !== undefined)
            yield* Usage.record(tx, {
              conversationId: execution.conversationId,
              identity: identity(envelope, attempt),
              outcome: { _tag: 'completed', value: result },
              ...(usage === undefined ? {} : { usage }),
              ...(result._tag === 'Placed' && usage !== undefined
                ? { linkedEntryId: result.entryId }
                : {}),
            })
          yield* updateStatus(tx, execution.conversationId, execution.taskId)
          yield* execution.progress(tx, Contract.contract, {
            status: 'completing',
            outcome: { status: 'completed', result },
          })
          yield* activateFormat(services)
          staged = true
        }),
      )
      .pipe(
        Effect.catchCause((cause) => {
          const reason = cause.reasons.length === 1 ? cause.reasons[0] : undefined
          const rejected =
            staged &&
            reason !== undefined &&
            Cause.isFailReason(reason) &&
            Schema.is(StorageError)(reason.error) &&
            ['io', 'invalid', 'conflict', 'notFound'].includes(reason.error.reason)
          return fail(
            cause,
            envelope === undefined || attempt === undefined
              ? undefined
              : { envelope, attempt, ...(usage === undefined ? {} : { usage }) },
            rejected,
          )
        }),
      )
  })
  const select = Effect.fnUntraced(function* ({
    input,
  }: Task.PhaseInput<typeof Contract.InputSchema, typeof Contract.CheckpointSchema, 'select'>) {
    const execution = yield* TaskExecution
    const ready = yield* prepare(
      services,
      execution.conversationId,
      input,
      execution.commit,
      execution.taskId,
      yield* Schema.decodeEffect(Identity.RequestId)(`compaction:${execution.taskId}`),
    )
    if (ready._tag === 'Noop') return yield* finish(ready)
    if (ready._tag === 'Supplied') return yield* place(ready.fingerprint, ready.summary)
    yield* execution.commit((tx) =>
      Effect.gen(function* () {
        yield* stageReady(tx, execution.conversationId, ready)
        yield* execution.progress(tx, Contract.contract, {
          status: 'pending',
          checkpoint: checkpoint(ready),
        })
        yield* updateStatus(tx, execution.conversationId, execution.taskId, {
          phase: 'summarize',
          attempt: ready.envelope.policy.attempt,
        })
        yield* activateFormat(services)
      }),
    )
  })
  const saveRunning = Effect.fnUntraced(function* (
    tx: Transaction.Transaction,
    taskId: Record.TaskId,
    checkpoint: Contract.Checkpoint,
  ) {
    const saved = yield* Transaction.task(tx, taskId)
    if (Option.isNone(saved)) return yield* invalid('Summary invocation task is absent')
    yield* Transaction.putTask(tx, {
      ...saved.value,
      state: {
        status: 'running',
        checkpoint: yield* Task.encode(Contract.CheckpointSchema, checkpoint),
      },
    })
  })
  const summarize = Effect.fn('Compaction.summarize')(function* ({
    checkpoint,
  }: Task.PhaseInput<typeof Contract.InputSchema, typeof Contract.CheckpointSchema, 'summarize'>) {
    const execution = yield* TaskExecution
    const binding = yield* Effect.exit(pinned(checkpoint.envelope))
    if (Exit.isFailure(binding))
      return yield* fail(
        binding.cause,
        checkpoint.started
          ? { envelope: checkpoint.envelope, attempt: checkpoint.attempt }
          : undefined,
      )
    const bound = binding.value
    const fence = yield* Effect.exit(
      execution.commit((tx) =>
        Inbox.validateCompaction(tx, execution.conversationId, checkpoint.envelope.fingerprint),
      ),
    )
    if (Exit.isFailure(fence))
      return yield* fail(
        fence.cause,
        checkpoint.started
          ? { envelope: checkpoint.envelope, attempt: checkpoint.attempt }
          : undefined,
      )
    let attempt = checkpoint.attempt
    if (checkpoint.started) {
      attempt = yield* Schema.decodeEffect(Usage.AttemptId)(attempt + 1)
      yield* execution.commit((tx) =>
        Effect.gen(function* () {
          yield* Usage.record(tx, {
            conversationId: execution.conversationId,
            identity: identity(checkpoint.envelope, checkpoint.attempt),
            outcome: {
              _tag: 'interrupted',
              reason:
                'Summary invocation ended before durable publication; external spend is unknown',
            },
          })
          yield* saveRunning(tx, execution.taskId, { ...checkpoint, attempt, started: false })
        }),
      )
    }
    const restored = yield* Restore.restore(
      bound.capability.configuration,
      checkpoint.envelope.preparation.configuration,
    )
    const scope = yield* Scope.Scope
    const policy = { ...checkpoint.envelope.policy, attempt }
    const context = Context.merge(services.context, restored).pipe(
      Context.add(ProviderAffinity, { id: checkpoint.envelope.preparation.affinity }),
      Context.add(Scope.Scope, scope),
      Context.add(CompactionRequest, policy),
      Context.add(Prepared.PreparedRequest, {
        current: yield* Preparation.own(checkpoint.envelope.preparation),
      }),
    )
    yield* execution.commit((tx) =>
      Effect.gen(function* () {
        yield* Inbox.validateCompaction(
          tx,
          execution.conversationId,
          checkpoint.envelope.fingerprint,
        )
        yield* saveRunning(tx, execution.taskId, { ...checkpoint, attempt, started: true })
        yield* updateStatus(tx, execution.conversationId, execution.taskId, {
          phase: 'summarize',
          attempt,
        })
      }),
    )
    const response = yield* Effect.exit(
      bound.model
        .generateText({ prompt: (yield* Preparation.own(checkpoint.envelope.preparation)).prompt })
        .pipe(Effect.provideContext(context)),
    )
    if (Exit.isSuccess(response)) {
      const usage = ModelStream.usage(response.value.usage, checkpoint.envelope.preparation)
      const summary = yield* Effect.exit(
        Compaction.validateSummary(
          response.value,
          bound.capability.outputBudget === 'local-acceptance'
            ? {
                maximum: policy.maxOutputTokens,
                ...(response.value.usage.outputTokens.total === undefined
                  ? {}
                  : { outputTokens: response.value.usage.outputTokens.total }),
              }
            : undefined,
        ),
      )
      if (Exit.isSuccess(summary))
        return yield* place(
          checkpoint.envelope.fingerprint,
          summary.value,
          checkpoint.envelope,
          attempt,
          usage,
        )
      return yield* fail(summary.cause, { envelope: checkpoint.envelope, attempt, usage })
    }
    if (Cause.hasInterrupts(response.cause)) return yield* Effect.failCause(response.cause)
    const reason = response.cause.reasons.length === 1 ? response.cause.reasons[0] : undefined
    const error =
      reason !== undefined && Cause.isFailReason(reason) && AiError.isAiError(reason.error)
        ? reason.error
        : undefined
    const settings = yield* Settings.getOrDefault.pipe(Effect.provideContext(services.context))
    const until =
      error === undefined
        ? undefined
        : ModelStream.retryAt(error, checkpoint.retries, settings, yield* Clock.currentTimeMillis)
    const known =
      error === undefined
        ? undefined
        : ModelStream.errorUsage(error, checkpoint.envelope.preparation)
    if (until === undefined)
      return yield* fail(response.cause, {
        envelope: checkpoint.envelope,
        attempt,
        ...(known === undefined ? {} : { usage: known }),
      })
    yield* execution.commit((tx) =>
      Effect.gen(function* () {
        yield* Usage.record(tx, {
          conversationId: execution.conversationId,
          identity: identity(checkpoint.envelope, attempt),
          outcome: { _tag: 'failed', error: { message: Cause.pretty(response.cause) } },
          ...(known === undefined ? {} : { usage: known }),
        })
        if (until !== undefined) {
          yield* execution.progress(tx, Contract.contract, {
            status: 'pending',
            checkpoint: {
              phase: 'retry',
              envelope: checkpoint.envelope,
              attempt,
              retries: checkpoint.retries + 1,
              until,
            },
          })
          yield* updateStatus(tx, execution.conversationId, execution.taskId, {
            phase: 'retry',
            attempt,
            retryAt: until,
          })
        }
      }),
    )
  })
  const guard = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(Effect.catchCause((cause) => fail(cause)))
  const abort = ({
    checkpoint,
  }: {
    readonly input: typeof Contract.InputSchema.Type
    readonly checkpoint: Contract.Checkpoint
  }) =>
    Effect.gen(function* () {
      const execution = yield* TaskExecution
      yield* execution.commit((tx) =>
        Effect.gen(function* () {
          if (
            checkpoint.phase === 'summarize' &&
            checkpoint.started &&
            !(yield* Usage.receipts(tx, execution.conversationId)).some(
              (receipt) =>
                receipt.identity._tag === 'requestAttempt' &&
                receipt.identity.requestId === checkpoint.envelope.preparation.requestId &&
                receipt.identity.attempt === checkpoint.attempt &&
                receipt.identity.purpose === 'compaction',
            )
          )
            yield* Usage.record(tx, {
              conversationId: execution.conversationId,
              identity: identity(checkpoint.envelope, checkpoint.attempt),
              outcome: {
                _tag: 'interrupted',
                reason: 'Compaction aborted; unreported external spend is unknown',
              },
            })
          yield* updateStatus(tx, execution.conversationId, execution.taskId)
          yield* execution.progress(tx, Contract.contract, {
            status: 'completing',
            outcome: { status: 'aborted' },
          })
        }),
      )
    })
  return Task.make({
    ...Contract.contract,
    initial: () => ({ phase: 'select' as const }),
    phases: {
      supplied: (value) => guard(place(value.checkpoint.fingerprint, value.checkpoint.summary)),
      select: (value) => guard(select(value)),
      summarize: (value) => guard(summarize(value)),
      retry: (value) =>
        guard(
          Effect.gen(function* () {
            const execution = yield* TaskExecution
            let remaining = value.checkpoint.until - (yield* Clock.currentTimeMillis)
            while (remaining > 0) {
              yield* Effect.sleep(Math.min(remaining, 0x7fffffff))
              remaining = value.checkpoint.until - (yield* Clock.currentTimeMillis)
            }
            const attempt = yield* Schema.decodeEffect(Usage.AttemptId)(
              value.checkpoint.attempt + 1,
            )
            yield* execution.commit((tx) =>
              Effect.gen(function* () {
                yield* execution.progress(tx, Contract.contract, {
                  status: 'pending',
                  checkpoint: {
                    phase: 'summarize',
                    envelope: value.checkpoint.envelope,
                    retries: value.checkpoint.retries,
                    attempt,
                    started: false,
                  },
                })
                yield* updateStatus(tx, execution.conversationId, execution.taskId, {
                  phase: 'summarize',
                  attempt,
                })
              }),
            )
          }),
        ),
    },
    abort,
  })
}
import { PolicySchema as importPolicy } from '../CompactionRequest.js'
import { contextProtocol as importContextProtocol } from '../RunControl.js'
