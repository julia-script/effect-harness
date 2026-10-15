import * as Tracing from './Tracing.js'
import * as PromptUsage from './PromptUsage.js'
import * as ProviderState from './ProviderState.js'
import { ProviderAffinity } from '../ProviderAffinity.js'
import * as Usage from '../Usage.js'
import * as Clock from 'effect/Clock'
import * as Exit from 'effect/Exit'
import * as Cause from 'effect/Cause'
import * as AiError from 'effect/ai/AiError'
import * as LiveState from '../LiveState.js'
import * as ModelStream from './ModelStream.js'
/** Executes one durable submission; scheduling and client admission live in HarnessRuntime. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Equal from 'effect/Equal'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as NativeTool from 'effect/ai/Tool'
import * as NativeToolkit from 'effect/ai/Toolkit'
import type * as Record from '../Record.js'
import * as Task from '../Task.js'
import * as ToolProgress from '../ToolProgress.js'
import type * as Agent from '../Agent.js'
import type * as Execution from '../Execution.js'
import { ExecutionError, type Failure } from '../ExecutionError.js'
import type * as PromptSection from '../PromptSection.js'
import { HarnessError } from '../HarnessError.js'
import { TaskError } from '../TaskError.js'
import type * as Hook from '../Hook.js'
import { HookExecution } from '../HookExecution.js'
import * as Model from '../Model.js'
import * as Prepared from '../PreparedRequest.js'
import * as Preparation from './RequestPreparation.js'
import * as PreparationRestore from './PreparationRestoreFailure.js'
import * as Plan from './ContextPlan.js'
import * as Inventory from '../ToolInventory.js'
import * as Settings from '../Settings.js'
import * as Identity from '../Identity.js'
import type * as Session from '../Session.js'
import * as Tool from '../Tool.js'
import * as Transaction from '../Transaction.js'
import * as Run from './RunState.js'
import * as DeferredExecution from './DeferredExecution.js'
import * as Inbox from './Inbox.js'
import * as Compaction from '../Compaction.js'
import * as CompactionTask from '../CompactionTask.js'
import * as CompactionRuntime from './CompactionRuntime.js'
import { TaskExecution } from '../TaskExecution.js'
import { NativeWriteFailure } from './NativeWriteFailure.js'
import { StreamPublicationFailure } from './StreamPublicationFailure.js'
import { document as agentDocument, target as agentTarget } from './AgentState.js'
import { protect, absent } from './HarnessFailure.js'
import * as Round from './ToolRound.js'
import * as RequestRecord from './RequestRecord.js'
import * as ToolTask from './ToolTask.js'
import * as ToolControl from '../ToolControl.js'
import * as OutputRetention from '../OutputRetention.js'
import * as ProgressPolicy from '../ProgressPolicy.js'

export interface BoundSection {
  readonly section: PromptSection.Any
  readonly extension?: string | undefined
  readonly registration: string
}
export interface Services {
  readonly toolTask: ReturnType<typeof ToolTask.make>
  readonly requireToolRoundFormat: Services['requireSubmissionFormat']
  readonly waitTools: (
    tx: Transaction.Transaction,
    checkpoint: Extract<Run.RunState, { _tag: 'toolRound' }>,
  ) => Effect.Effect<void, import('../TaskError.js').Failure>
  readonly compaction: CompactionRuntime.Services
  readonly waitCompaction: (
    tx: Transaction.Transaction,
    checkpoint: Extract<Run.RunState, { _tag: 'compaction' }>,
  ) => Effect.Effect<void, import('../TaskError.js').Failure>
  readonly cancellationCleanup: import('effect/Scope').Closeable
  readonly nativeModel: Prepared.Binding
  readonly requireStreamFormat: Services['requireSubmissionFormat']
  readonly requireDeferredFormat: Services['requireSubmissionFormat']
  readonly requireRequestFormat: Services['requireSubmissionFormat']
  readonly refresh: Effect.Effect<
    Pick<Services, 'lease' | 'allTools' | 'toolExtensions' | 'sections' | 'runHooks'>,
    import('../RegistryError.js').RegistryError,
    import('effect/Scope').Scope
  >
  readonly requireSubmissionFormat: Effect.Effect<
    import('../StorageFormat.js').Format,
    import('../StorageFormat.js').Failure
  >
  readonly notify: Effect.Effect<void, HarnessError | Failure>
  readonly lease: import('../RegistryScope.js').Lease
  readonly session: Session.Session
  readonly context: Context.Context<never>
  readonly allTools: ReadonlyArray<Tool.Any>
  readonly toolExtensions: ReadonlyMap<string, string>
  readonly sections: ReadonlyArray<BoundSection>
  readonly models: ReadonlyArray<Model.Any>
  readonly nativeDefault: Option.Option<typeof LanguageModel.LanguageModel.Service>
  readonly maxTurns: number
  readonly defaultAgent: Agent.State
  readonly checkOpen: Effect.Effect<void, HarnessError>
  readonly access: (id: Record.ConversationId) => Execution.Access
  readonly runHooks: (
    id: Record.ConversationId,
    agent: Agent.State,
    event: Hook.Event,
    taskId?: Record.TaskId,
  ) => Effect.Effect<Hook.Output<Hook.Name>, HarnessError>
  readonly write: <A, E, R>(
    change: (tx: Transaction.Transaction, task: Record.Task) => Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | import('../TaskError.js').Failure | NativeWriteFailure, R>
  readonly writeCallback: <A, E, R>(
    change: (tx: Transaction.Transaction, task: Record.Task) => Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | import('../TaskError.js').Failure, R>
  readonly saveState: (
    tx: Transaction.Transaction,
    task: Record.Task,
    state: Run.RunState,
  ) => Effect.Effect<void, import('../TaskError.js').Failure>
  readonly complete: (
    tx: Transaction.Transaction,
    outcome:
      | { readonly status: 'completed'; readonly result: Record.EntryId }
      | { readonly status: 'aborted'; readonly reason: typeof ToolTask.unsafeSubagentRecovery },
  ) => Effect.Effect<void, import('../TaskError.js').Failure>
  readonly invocationActive: () => boolean
}

export const run = Effect.fn('RunLoop.run')(function* (services: Services, initial: Record.Task) {
  const {
    session,
    context,
    models,
    nativeDefault,
    maxTurns,
    defaultAgent,
    checkOpen,
    access,
    write: nativeWrite,
    writeCallback,
    saveState,
    invocationActive,
    notify,
  } = services
  let observed:
    | {
        readonly requestId: Identity.RequestId
        readonly attempt: Usage.AttemptId
        readonly usage: Usage.Record
      }
    | undefined
  const write = <A, E, R>(
    change: (tx: Transaction.Transaction, task: Record.Task) => Effect.Effect<A, E, R>,
  ): Effect.Effect<
    A,
    E | import('../TaskError.js').Failure | NativeWriteFailure | StreamPublicationFailure,
    R
  > =>
    nativeWrite(change).pipe(
      Effect.catchCause(
        (
          cause,
        ): Effect.Effect<
          never,
          E | import('../TaskError.js').Failure | NativeWriteFailure | StreamPublicationFailure
        > => {
          const reason = cause.reasons.length === 1 ? cause.reasons[0] : undefined
          return observed !== undefined &&
            reason !== undefined &&
            Cause.isFailReason(reason) &&
            reason.error instanceof NativeWriteFailure
            ? Effect.fail(new StreamPublicationFailure({ error: reason.error, ...observed }))
            : Effect.failCause(cause)
        },
      ),
    )
  let { sections, runHooks, lease } = services
  let task = initial
  let state = yield* Run.get(session, task.state.checkpoint)
  yield* Effect.annotateCurrentSpan({
    'task.id': initial.id,
    'conversation.id': initial.conversationId,
    'submission.id': state.submissionId,
    'run.phase': state.phase,
  })
  if (state._tag !== 'queued') {
    yield* write((tx) =>
      Effect.gen(function* () {
        yield* services.requireSubmissionFormat
        yield* Inbox.adopt(tx, initial, state.submissionId)
      }),
    )
  }
  const clearLive = (tx: Transaction.Transaction) =>
    Effect.gen(function* () {
      const found = yield* Transaction.snapshot(
        tx,
        LiveState.document,
        LiveState.target(task.conversationId),
      )
      if (Option.isSome(found))
        yield* Transaction.setDocument(
          tx,
          LiveState.document,
          LiveState.target(task.conversationId),
          {},
        )
    })
  const identity = (
    preparation: Prepared.Snapshot,
    attempt: Usage.AttemptId,
  ): Usage.ReceiptIdentity => ({
    _tag: 'requestAttempt',
    requestId: preparation.requestId,
    attempt,
    purpose: 'generation',
  })
  const nextAttempt = (attempt: number) => Schema.decodeEffect(Usage.AttemptId)(attempt + 1)
  while (true) {
    yield* checkOpen
    if (state._tag === 'toolRound') {
      const waiting = state
      const execution = yield* TaskExecution
      const outcomes = yield* execution.outcomes(waiting.children)
      const round = yield* write((tx) => Round.read(tx, task.conversationId, waiting.roundKey))
      const admitted = round.slots.flatMap((slot) =>
        slot.taskId === undefined ? [] : [slot.taskId],
      )
      if (
        admitted.length !== waiting.children.length ||
        admitted.some((id, index) => id !== waiting.children[index])
      )
        return yield* Round.invalid('The parent wait does not cover every admitted tool child')
      if (round.ownerTaskId !== task.id || round.assistant !== waiting.assistant || round.applied)
        return yield* Round.invalid('The completed tool round does not match its run checkpoint')
      if (
        outcomes.some(
          (outcome) =>
            outcome.status === 'aborted' && outcome.reason === ToolTask.unsafeSubagentRecovery,
        )
      ) {
        yield* write((tx, currentTask) =>
          Effect.gen(function* () {
            const current = yield* Round.read(tx, task.conversationId, waiting.roundKey)
            if (
              !Schema.toEquivalence(Run.RunStateSchema)(
                yield* Run.read(tx, currentTask.state.checkpoint),
                waiting,
              ) ||
              !Schema.toEquivalence(Round.RoundSchema)(current, round)
            )
              return yield* Round.invalid('The abandoned tool round changed during settlement')
            for (const [index, slot] of current.slots.entries()) {
              if (slot.taskId === undefined) continue
              const child = yield* Transaction.task(tx, slot.taskId)
              const outcome = outcomes[waiting.children.indexOf(slot.taskId)]
              if (
                Option.isNone(child) ||
                child.value.kind !== ToolTask.contract.kind ||
                child.value.version !== ToolTask.contract.version ||
                child.value.background ||
                child.value.state.status !== 'terminal' ||
                child.value.state.outcome === undefined ||
                outcome === undefined ||
                !Schema.toEquivalence(Schema.Json)(child.value.state.outcome, outcome)
              )
                return yield* Round.invalid(
                  'The abandoned round has no authoritative terminal child',
                )
              const input = yield* Task.decode(Round.InputSchema, child.value.input)
              if (input.index !== index || input.roundKey !== waiting.roundKey)
                return yield* Round.invalid('The abandoned child belongs to a different round')
              yield* Round.slot(tx, task.conversationId, slot.taskId, input)
              if (
                outcome?.status !== 'aborted' ||
                outcome.reason !== ToolTask.unsafeSubagentRecovery
              )
                continue
              const progress = yield* Transaction.snapshot(
                tx,
                ToolProgress.document,
                Round.target(task.conversationId, Round.progressKey(slot.taskId)),
              )
              const receipts = (yield* Usage.receipts(tx, task.conversationId)).filter(
                (receipt) =>
                  receipt.identity._tag === 'toolCall' &&
                  receipt.identity.taskId === slot.taskId &&
                  receipt.identity.callId === slot.call.id &&
                  Option.isSome(progress) &&
                  receipt.identity.attempt === progress.value.value.attempt,
              )
              if (
                input.replay !== 'unsafe' ||
                slot.result !== undefined ||
                Option.isNone(progress) ||
                progress.value.value.callId !== Round.progressKey(slot.taskId) ||
                progress.value.value.status !== 'interrupted' ||
                !Schema.toEquivalence(OutputRetention.Policy)(
                  progress.value.value.retention,
                  slot.retention,
                ) ||
                receipts.length !== 1 ||
                receipts[0]?.outcome._tag !== 'interrupted' ||
                receipts[0].outcome.reason !== ToolTask.unsafeSubagentRecovery ||
                receipts[0].usage !== undefined ||
                receipts[0].linkedEntryId !== undefined
              )
                return yield* Round.invalid(
                  'The abandoned child has no matching interrupted accounting',
                )
            }
            yield* Round.write(tx, task.conversationId, waiting.roundKey, {
              ...current,
              applied: true,
            })
            yield* clearLive(tx)
            yield* Inbox.settle(tx, task.conversationId, task.id, waiting.submissionId, {
              reason: 'aborted',
            })
            yield* services.complete(tx, {
              status: 'aborted',
              reason: ToolTask.unsafeSubagentRecovery,
            })
          }),
        )
        yield* notify
        return
      }
      const nextIndex = round.slots.findIndex((slot) => slot.result === undefined)
      if (nextIndex >= 0) {
        if (round.mode !== 'sequential' || round.slots[nextIndex]?.taskId !== undefined)
          return yield* Round.invalid('A terminal tool child did not publish its result slot')
        yield* write((tx) =>
          Effect.gen(function* () {
            const current = yield* Round.read(tx, task.conversationId, waiting.roundKey)
            const saved = current.slots[nextIndex]
            if (saved === undefined || saved.taskId !== undefined || current.applied)
              return yield* Round.invalid('Sequential child admission changed unexpectedly')
            const child = yield* execution.createChild(tx, services.toolTask, {
              ownerTaskId: task.id,
              roundKey: waiting.roundKey,
              index: nextIndex,
              call: saved.call,
              replay: saved.replay,
              preparation: RequestRecord.reference(task.id, current.preparation),
              retention: saved.retention,
              progress: saved.progress,
            })
            yield* Round.write(tx, task.conversationId, waiting.roundKey, {
              ...current,
              slots: current.slots.map((slot, index) =>
                index === nextIndex ? { ...slot, taskId: child.id } : slot,
              ),
            })
            yield* services.waitTools(tx, { ...waiting, children: [...waiting.children, child.id] })
          }),
        )
        return
      }
      const fatal = round.slots.filter((slot) => slot.fatal === true)
      if (fatal.length > 0)
        return yield* new HarnessError({
          reason: 'failed',
          operation: 'tool.round',
          message: fatal
            .flatMap((slot) => slot.result!.diagnostics.map((diagnostic) => diagnostic.message))
            .join('\n'),
        })
      const results = round.slots.map((slot) => slot.result!)
      const receipts = round.slots.map((slot) => slot.receipt!)
      const availableControls = Inventory.inventory(lease.snapshot, 'model', {
        agent: round.preparation.agent,
        preparedTools: round.preparation.allTools,
        selection: 'permitted',
      }).map((item) => item.declaration.name)
      const controls = yield* ToolControl.reduce(
        round.slots.map((slot) => slot.call.id),
        receipts,
        availableControls,
      )
      yield* runHooks(task.conversationId, waiting.agent, { _tag: 'afterTools', results }, task.id)
      const next = yield* write((tx, currentTask) =>
        Effect.gen(function* () {
          const current = yield* Round.read(tx, task.conversationId, waiting.roundKey)
          if (current.applied || current.slots.some((slot) => slot.result === undefined))
            return yield* Round.invalid('The control barrier is already applied or incomplete')
          if (controls.addTools.length > 0) {
            const saved = yield* Transaction.ensureDocument(
              tx,
              agentDocument,
              agentTarget(task.conversationId),
            )
            const agent = { ...defaultAgent, ...saved.value }
            if (!Equal.equals(agent.modelTools, round.preparation.agent.modelTools))
              return yield* new ToolControl.InvalidControl({
                reason: 'invalidRound',
                message: 'Model selection changed after the request was prepared',
              })
            yield* ToolControl.validate(
              { addTools: controls.addTools },
              Inventory.inventory(lease.snapshot, 'model', {
                agent,
                preparedTools: round.preparation.allTools,
                selection: 'permitted',
              }).map((item) => item.declaration.name),
            )
            const selection = agent.modelTools ?? { mode: 'default' as const, names: [] }
            const bindings = new Map(Object.entries(selection.bindings ?? {}))
            for (const name of controls.addTools) {
              const tool = round.preparation.allTools.find(
                (item) => item.declaration.name === name,
              )!
              bindings.set(name, { binding: tool.binding.id, version: tool.binding.version })
            }
            // As-of forks need an entry at this commit even when activation ends the run.
            yield* Transaction.appendEntry(tx, task.conversationId, {
              kind: 'agent.configure',
            })
            yield* Transaction.setDocument(tx, agentDocument, agentTarget(task.conversationId), {
              ...saved.value,
              modelTools: {
                ...selection,
                names: [...new Set([...selection.names, ...controls.addTools])],
                bindings: Object.fromEntries(bindings),
              },
            })
          }
          if (controls.handoff !== undefined)
            yield* Transaction.appendEntry(tx, task.conversationId, {
              kind: 'reset',
              head: 'self',
              model: yield* Schema.encodeEffect(Run.Messages)([
                Prompt.userMessage({ content: [Prompt.textPart({ text: controls.handoff })] }),
              ]),
            })
          const final = controls.terminate || controls.handoff !== undefined
          const selected = yield* Inbox.boundary(tx, task.conversationId, {
            boundary: final ? 'final' : 'postTools',
            taskId: task.id,
            ...(final ? { outcome: { answer: waiting.assistant } } : {}),
          }).pipe(Effect.provideContext(context))
          yield* Round.write(tx, task.conversationId, waiting.roundKey, {
            ...current,
            applied: true,
          })
          if (selected.boundary === 'final') {
            const reset = selected.placed.findLast((item) => item._tag === 'WriteDone')
            let answer: Record.EntryId | undefined
            if (final) answer = waiting.assistant
            else if (reset?._tag === 'WriteDone') answer = reset.entry
            if (answer === undefined)
              return yield* absent('run.reset', 'Reset placement is missing')
            yield* services.complete(tx, { status: 'completed', result: answer })
            return undefined
          }
          const next: Run.RunState = {
            _tag: 'request',
            phase: 'request',
            submissionId: waiting.submissionId,
            agent: waiting.agent,
            ...(waiting.ordinal === undefined ? {} : { ordinal: waiting.ordinal }),
          }
          yield* saveState(tx, currentTask, next)
          return next
        }),
      )
      if (next === undefined) {
        yield* notify
        return
      }
      state = next
      continue
    }
    if (state._tag === 'compaction') {
      const waiting = state
      const execution = yield* TaskExecution
      const [outcome] = yield* execution.outcomes([waiting.child])
      if (
        waiting.overflow !== undefined &&
        (outcome?.status !== 'completed' ||
          !Schema.is(
            Schema.TaggedStruct('Placed', {
              entryId: CompactionTask.ResultSchema.members[1].fields.entryId,
            }),
          )(outcome.result))
      )
        return yield* waiting.overflow
      const {
        child: _child,
        preparation: _preparation,
        attempt: _attempt,
        retries: _retries,
        ...unprepared
      } = waiting
      state = {
        ...unprepared,
        _tag: 'request',
        phase: 'request',
        compacted: waiting.child,
      }
      yield* write((tx, current) => saveState(tx, current, state))
      continue
    }
    if (state._tag === 'retry') {
      const retry = state
      let remaining = retry.until - (yield* Clock.currentTimeMillis)
      while (remaining > 0) {
        yield* Effect.sleep(Math.min(remaining, 0x7fffffff))
        remaining = retry.until - (yield* Clock.currentTimeMillis)
      }
      const { until: _until, ...pinned } = retry
      state = { ...pinned, _tag: 'request', phase: 'request' }
      yield* write((tx, current) => saveState(tx, current, state))
      continue
    }
    if (state._tag === 'streaming') {
      const interrupted = state
      const recorded = yield* write((tx) =>
        Usage.receipts(tx, task.conversationId).pipe(
          Effect.map((receipts) =>
            receipts.find(
              (receipt) =>
                receipt.identity._tag === 'requestAttempt' &&
                receipt.identity.requestId === interrupted.preparation.requestId &&
                receipt.identity.attempt === interrupted.attempt &&
                receipt.identity.purpose === 'generation' &&
                receipt.outcome._tag === 'failed',
            ),
          ),
        ),
      )
      // A terminal observation can precede overflow preflight or an uncertain
      // acknowledgement. Its committed receipt is authoritative on cold recovery.
      if (recorded?.outcome._tag === 'failed')
        return yield* new HarnessError({
          reason: 'failed',
          operation: 'model.recover',
          message: Schema.is(Schema.Struct({ message: Schema.String }))(recorded.outcome.error)
            ? recorded.outcome.error.message
            : 'Recorded generation attempt failed before runtime settlement',
          cause: recorded.outcome.error,
        })
      const attempt = yield* nextAttempt(interrupted.attempt)
      state = yield* write((tx, current) =>
        Effect.gen(function* () {
          let linkedEntryId: Record.EntryId | undefined
          if (interrupted.partial !== undefined)
            linkedEntryId = (yield* Transaction.appendEntry(tx, task.conversationId, {
              kind: 'assistant.interrupted',
              data: yield* Schema.encodeEffect(Schema.toCodecJson(LiveState.PartialSchema))(
                interrupted.partial,
              ),
              ...(interrupted.partial.usage === undefined
                ? {}
                : { usage: interrupted.partial.usage }),
            })).id
          yield* Usage.record(tx, {
            conversationId: task.conversationId,
            identity: identity(interrupted.preparation, interrupted.attempt),
            outcome: {
              _tag: 'interrupted',
              reason:
                'Runtime scope ended before terminal commit; unreported external spend is unknown',
            },
            ...(interrupted.partial?.usage === undefined
              ? {}
              : { usage: interrupted.partial.usage }),
            ...(linkedEntryId === undefined || interrupted.partial?.usage === undefined
              ? {}
              : { linkedEntryId }),
          })
          const { revision: _revision, partial: _partial, ...pinned } = interrupted
          const next: Run.RunState = { ...pinned, _tag: 'request', phase: 'request', attempt }
          yield* clearLive(tx)
          yield* saveState(tx, current, next)
          return next
        }),
      )
      continue
    }
    if (state._tag === 'queued') {
      const queued = state
      state = yield* write((tx, current) =>
        Effect.gen(function* () {
          yield* services.requireSubmissionFormat
          yield* Inbox.placeRun(tx, current, queued)
          const configured = yield* Transaction.snapshot(
            tx,
            agentDocument,
            agentTarget(task.conversationId),
          )
          const next: Run.RunState = {
            _tag: 'request',
            phase: 'request',
            submissionId: queued.submissionId,
            agent: {
              ...defaultAgent,
              ...(Option.isSome(configured) ? configured.value.value : {}),
            },
          }
          yield* saveState(tx, current, next)
          return next
        }),
      )
      continue
    }
    if (state._tag === 'request' || state._tag === 'deferred' || state._tag === 'deferred-start') {
      let requesting = state
      let selected =
        requesting.preparation === undefined
          ? undefined
          : Preparation.selectedModel(requesting.preparation, models)
      let requestContext = Context.add(context, ProviderAffinity, {
        id: requesting.preparation?.affinity ?? '',
      })
      if (requesting.preparation === undefined) {
        const refreshed = yield* services.refresh
        ;({ sections, runHooks, lease } = refreshed)
        const settingsService = yield* Effect.serviceOption(Settings.Settings).pipe(
          Effect.provideContext(context),
        )
        const settings = Option.isSome(settingsService)
          ? yield* settingsService.value.get
          : { ...Settings.defaults, maxTurns }
        const ordinal =
          requesting.compacted === undefined
            ? (requesting.ordinal ?? 0) + 1
            : (requesting.ordinal ?? 1)
        if (!Number.isSafeInteger(ordinal) || ordinal > settings.maxTurns)
          return yield* new HarnessError({
            reason: 'failed',
            operation: 'run.request',
            message: `Conversation exceeded ${settings.maxTurns} model turns`,
          })
        const current = yield* write((tx) =>
          Effect.gen(function* () {
            const configured = yield* Transaction.snapshot(
              tx,
              agentDocument,
              agentTarget(task.conversationId),
            )
            const agent = {
              ...defaultAgent,
              ...(Option.isSome(configured) ? configured.value.value : {}),
            }
            if (agent.extensions === undefined && settings.extensions !== undefined)
              agent.extensions = settings.extensions
            return { agent, view: yield* Inbox.project(tx, task.conversationId) }
          }),
        )
        requesting = { ...requesting, agent: current.agent, ordinal }
        selected =
          requesting.agent.model === undefined
            ? models[0]
            : models.find(
                (model) =>
                  model.definition.ref.provider === requesting.agent.model?.ref.provider &&
                  model.definition.ref.modelId === requesting.agent.model?.ref.modelId,
              )
        if (requesting.agent.model !== undefined && selected === undefined)
          return yield* absent('model.select', 'Configured model is unavailable')
        const model = selected?.languageModel ?? Option.getOrUndefined(nativeDefault)
        if (model === undefined)
          return yield* absent('model.select', 'No generation capability is available')
        const execution =
          settings.stream.deferred === true || typeof settings.stream.deferred === 'object'
            ? ('deferred' as const)
            : ('streaming' as const)
        if (execution === 'deferred') {
          if (selected === undefined)
            return yield* absent('model.deferred', 'Deferred execution requires a model descriptor')
          yield* Model.deferred(selected)
        }
        const requestId = yield* Effect.try({
          try: () => globalThis.crypto.randomUUID(),
          catch: (cause) =>
            new HarnessError({
              reason: 'failed',
              operation: 'request.identity',
              message: 'Cannot allocate preparation identity',
              cause,
            }),
        }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Identity.RequestId)))
        const attempt = yield* Schema.decodeEffect(Usage.AttemptId)(0)
        const affinity = yield* write((tx) =>
          Effect.gen(function* () {
            const saved = yield* Transaction.snapshot(
              tx,
              ProviderState.document,
              ProviderState.target(task.conversationId),
            )
            if (Option.isSome(saved)) return saved.value.value.id
            return (yield* Transaction.ensureDocument(
              tx,
              ProviderState.document,
              ProviderState.target(task.conversationId),
              { seed: yield* ProviderState.fresh },
            )).value.id
          }),
        )
        requestContext = Context.add(context, ProviderAffinity, { id: affinity })
        let options: Schema.JsonObject = {}
        let configured = Context.empty()
        if (selected !== undefined) {
          const codec = Schema.toCodecJson(
            selected.options as Schema.ConstraintCodec<unknown, unknown, never, never>,
          )
          const decoded = yield* Schema.decodeEffect(codec)(
            requesting.agent.model?.options ?? {},
          ).pipe(Effect.provideContext(requestContext))
          options = yield* Schema.encodeEffect(codec)(decoded).pipe(
            Effect.provideContext(requestContext),
            Effect.flatMap(Schema.decodeUnknownEffect(Schema.JsonObject)),
          )
          const configure = selected.configure as (
            options: unknown,
            policy: Model.PreparationPolicy,
          ) => Effect.Effect<Context.Context<never>, import('../ModelError.js').ModelError>
          configured = yield* configure(decoded, { requestId, ordinal, attempt, settings }).pipe(
            Effect.provideContext(requestContext),
          )
        }
        const configurationCodec = selected?.configuration ?? Model.emptyConfiguration
        yield* Schema.decodeEffect(Prepared.BindingSchema)(configurationCodec.binding)
        yield* Schema.decodeEffect(Prepared.BindingSchema)(services.nativeModel)
        if (selected !== undefined)
          yield* Schema.decodeEffect(Model.DefinitionSchema)(selected.definition)
        const configuration = yield* configurationCodec.encode(configured)
        let prompt = current.view.prompt
        const desiredSections: Array<import('../ContextProjection.js').Section> = []
        if (requesting.agent.instructions !== undefined)
          desiredSections.push({
            key: '@effect-harness/instructions',
            text: requesting.agent.instructions,
          })
        for (const bound of sections) {
          if (
            bound.extension !== undefined &&
            requesting.agent.extensions !== undefined &&
            !requesting.agent.extensions.includes(bound.extension)
          )
            continue
          let sectionActive = true
          const guard = Effect.suspend(() =>
            sectionActive && invocationActive()
              ? Effect.void
              : Effect.fail(
                  new ExecutionError({
                    reason: 'revoked',
                    operation: 'section.access',
                    message: 'Section render has ended',
                  }),
                ),
          )
          const native = access(task.conversationId)
          const text = yield* protect(
            'promptSection.render',
            lease.renderSection(bound.section.key, prompt, { binding: bound.registration }).pipe(
              Effect.provideService(HookExecution, {
                conversationId: task.conversationId,
                taskId: task.id,
                snapshot: (document, target) =>
                  guard.pipe(Effect.andThen(native.snapshot(document, target))),
                watch: (document, target) =>
                  Stream.unwrap(guard.pipe(Effect.as(native.watch(document, target)))).pipe(
                    Stream.mapEffect((value) => guard.pipe(Effect.as(value))),
                  ),
                commit: (change) =>
                  guard.pipe(
                    Effect.andThen(
                      writeCallback((tx) =>
                        Effect.gen(function* () {
                          yield* guard
                          const value = yield* change(tx)
                          yield* guard
                          return value
                        }),
                      ).pipe(
                        Effect.mapError((error) =>
                          Schema.is(TaskError)(error)
                            ? new ExecutionError({
                                reason: error.reason === 'revoked' ? 'revoked' : 'invalid',
                                operation: 'section.commit',
                                message: error.message,
                                cause: error,
                              })
                            : error,
                        ),
                      ),
                    ),
                  ),
              }),
              Effect.ensuring(
                Effect.sync(() => {
                  sectionActive = false
                }),
              ),
            ),
          )
          if (text !== undefined) desiredSections.push({ key: bound.section.key, text })
        }
        const offered =
          selected !== undefined && !selected.definition.capabilities.tools
            ? []
            : Preparation.inventory(lease.snapshot, 'model', requesting.agent)
        const offeredTools = yield* Effect.forEach(offered, Preparation.declaration)
        const allInventory = [
          ...Preparation.inventory(lease.snapshot, 'model', requesting.agent, 'permitted'),
          ...Preparation.inventory(lease.snapshot, 'tools', requesting.agent),
        ]
        const allBindings = [
          ...new Map(allInventory.map((item) => [item.declaration.name, item])).values(),
        ]
        const allToolFacts = yield* Effect.forEach(allBindings, Preparation.declaration)
        const planned = yield* write((tx) =>
          Effect.gen(function* () {
            const view = yield* Inbox.project(tx, task.conversationId)
            const plan = yield* Plan.plan(view, {
              sections: desiredSections,
              tools: offeredTools.map((tool) => tool.declaration),
            })
            const entries = yield* Transaction.scanEntries(tx, {
              conversationId: task.conversationId,
              order: 'ascending',
            }).pipe(Stream.runCollect)
            const preview = yield* Preparation.preview(
              view,
              entries,
              plan,
              selected?.positioning ?? { sections: 'leading', tools: 'latest' },
            )
            const usage = yield* Transaction.snapshot(
              tx,
              PromptUsage.document,
              PromptUsage.target(task.conversationId),
            )
            return {
              view,
              plan,
              ...preview,
              baseline: Option.isSome(usage) ? usage.value.value.baseline : undefined,
            }
          }),
        )
        prompt = planned.translation.prompt
        const before = yield* runHooks(
          task.conversationId,
          requesting.agent,
          { _tag: 'beforeRequest', prompt },
          task.id,
        )
        if (before !== undefined && 'content' in before) prompt = before as Prompt.Prompt
        if (requesting.compacted === undefined) {
          const selection = yield* Compaction.select(planned.view, settings.compaction, {
            reason: 'threshold',
            ...(selected?.definition.contextWindow === undefined
              ? {}
              : { contextWindow: selected.definition.contextWindow }),
            prompt,
            tools: offeredTools.map((tool) => tool.declaration),
            ...(selected?.measurement === undefined
              ? {}
              : { overheadTokens: selected.measurement.overheadTokens }),
            ...(planned.baseline !== undefined &&
            selected?.measurement !== undefined &&
            PromptUsage.identity(selected.measurement.identity, offeredTools) ===
              planned.baseline.identity &&
            Schema.toEquivalence(Schema.UndefinedOr(Model.SelectionSchema))(
              planned.baseline.model,
              requesting.agent.model,
            )
              ? { baseline: planned.baseline }
              : {}),
          }).pipe(Effect.provideContext(context))
          if (selection._tag === 'Selected') {
            const ready = yield* CompactionRuntime.prepare(
              services.compaction,
              task.conversationId,
              { reason: 'threshold', blocking: selection.mode === 'blocking' },
              (change) => write((tx) => change(tx)),
              task.id,
              Identity.RequestId.make(`summary:${requestId}`),
              selection,
            )
            if (ready._tag !== 'Noop') {
              const blocking = selection.mode === 'blocking'
              const waiting = yield* write((tx) =>
                Effect.gen(function* () {
                  const child = yield* CompactionRuntime.admit(
                    tx,
                    task.conversationId,
                    { reason: 'threshold', blocking },
                    blocking ? task.id : undefined,
                    ready,
                  )
                  yield* CompactionRuntime.activateFormat(services.compaction)
                  if (!blocking) return undefined
                  const checkpoint: Extract<Run.RunState, { _tag: 'compaction' }> = {
                    ...requesting,
                    _tag: 'compaction',
                    phase: 'compaction',
                    child: child.id,
                  }
                  yield* services.waitCompaction(tx, checkpoint)
                  yield* CompactionRuntime.activateFormat(services.compaction)
                  return checkpoint
                }),
              )
              if (waiting !== undefined) {
                state = waiting
                return
              }
            }
          }
        }
        const translation = yield* Preparation.align(planned.translation, prompt)
        const preparation = yield* Preparation.own({
          requestId,
          ordinal,
          attempt,
          prompt: translation.prompt,
          agent: requesting.agent,
          settings,
          affinity,
          model:
            selected === undefined
              ? { _tag: 'native', binding: services.nativeModel }
              : {
                  _tag: 'descriptor',
                  definition: selected.definition,
                  binding: {
                    id: JSON.stringify([
                      selected.definition.ref.provider,
                      selected.definition.ref.modelId,
                    ]),
                    version: selected.version ?? 1,
                  },
                },
          options,
          execution,
          configuration,
          configurationBinding: configurationCodec.binding,
          registryGeneration: lease.snapshot.generation,
          requiredBindings: lease.snapshot.registrations
            .filter((item) => item.enabled && item.id !== 'native-runtime-run')
            .map((item) => ({ id: item.id, version: item.version })),
          tools: offeredTools,
          allTools: allToolFacts,
          translation,
        })
        requesting = yield* write((tx, current) =>
          Effect.gen(function* () {
            const view = yield* Inbox.project(tx, task.conversationId)
            if (!Equal.equals(view, planned.view))
              return yield* new HarnessError({
                reason: 'invalid',
                operation: 'request.context',
                message: 'Context changed while validating the final request',
              })
            const identities = new Map<Record.EntryId, Record.EntryId>()
            for (const [index, draft] of planned.plan.drafts.entries()) {
              const entry = yield* Transaction.appendEntry(tx, task.conversationId, draft)
              const previewId = planned.identities[index]
              if (previewId !== undefined) identities.set(previewId, entry.id)
            }
            const published = yield* Preparation.own({
              ...preparation,
              translation: {
                ...preparation.translation,
                frames: preparation.translation.frames.map((frame) => ({
                  ...frame,
                  entryId: identities.get(frame.entryId) ?? frame.entryId,
                })),
                toolPositions: preparation.translation.toolPositions.map((position) => ({
                  ...position,
                  entryId: identities.get(position.entryId) ?? position.entryId,
                })),
              },
            })
            const next = { ...requesting, preparation: published }
            // Checkpoint encoding/staging validates before the reader barrier; Session
            // publishes the accepted facts and immutable checkpoint in one native commit.
            yield* saveState(tx, current, next)
            yield* services.requireRequestFormat
            if (execution === 'deferred') yield* services.requireDeferredFormat
            return next
          }),
        )
        state = requesting
      } else {
        yield* Preparation.compatible(
          requesting.preparation,
          lease.snapshot,
          models,
          services.nativeModel,
          Option.isSome(nativeDefault),
        )
      }
      const preparation = requesting.preparation!
      selected = Preparation.selectedModel(preparation, models)
      const available = preparation.tools.flatMap((saved) =>
        lease.snapshot.bindings
          .filter(
            (item) =>
              item.owner.id === saved.binding.id &&
              item.declaration.name === saved.declaration.name,
          )
          .map((item) => item.declaration),
      )
      const model = selected?.languageModel ?? Option.getOrUndefined(nativeDefault)
      if (model === undefined) return yield* absent('model.select', 'Pinned model is unavailable')
      const configurationCodec = selected?.configuration ?? Model.emptyConfiguration
      const configuration = yield* PreparationRestore.restore(
        configurationCodec,
        preparation.configuration,
      )
      requestContext = Context.merge(
        Context.add(context, ProviderAffinity, { id: preparation.affinity }),
        configuration,
      )
      requestContext = Context.add(requestContext, Prepared.PreparedRequest, {
        current: yield* Preparation.own(preparation),
      })
      const nativeToolkit = NativeToolkit.make(
        ...preparation.tools.map((tool) => {
          const declaration = NativeTool.dynamic(tool.declaration.name, {
            description: tool.declaration.description,
            parameters: tool.declaration.parameters,
          })
          return tool.strict === undefined
            ? declaration
            : declaration.annotate(NativeTool.Strict, tool.strict)
        }),
      )
      const attempt = requesting.attempt ?? preparation.attempt
      const retries = requesting.retries ?? 0
      const overflow = (error: AiError.AiError) =>
        (selected?.compaction ?? services.compaction.nativeCompaction)?.isOverflow(error) === true
      const recoverOverflow = Effect.fnUntraced(function* <E>(
        cause: Cause.Cause<E>,
        known?: Usage.Record,
      ) {
        const reason = cause.reasons.length === 1 ? cause.reasons[0] : undefined
        if (
          reason === undefined ||
          !Cause.isFailReason(reason) ||
          !AiError.isAiError(reason.error) ||
          !overflow(reason.error) ||
          requesting.compacted !== undefined
        )
          return false
        const error = reason.error
        if (preparation.execution !== 'deferred') {
          observed =
            known === undefined
              ? undefined
              : { requestId: preparation.requestId, attempt, usage: known }
          // The generation already failed. Persist its charge before any summary
          // selection, hook or configuration can fail or be interrupted. The existing
          // write boundary carries only this operation's deterministic rejection.
          yield* write((tx) =>
            Usage.record(tx, {
              conversationId: task.conversationId,
              identity: identity(preparation, attempt),
              outcome: { _tag: 'failed', error: { message: Cause.pretty(cause) } },
              ...(known === undefined ? {} : { usage: known }),
            }),
          ).pipe(Effect.uninterruptible)
        }
        const policy = yield* Settings.getOrDefault.pipe(Effect.provideContext(context))
        if (!policy.compaction.enabled) return false
        const immutable = yield* write((tx) => Inbox.project(tx, task.conversationId))
        const selection = yield* Compaction.select(immutable, policy.compaction, {
          reason: 'overflow',
          prompt: preparation.prompt,
          tools: preparation.tools.map((tool) => tool.declaration),
          ...(selected?.measurement === undefined
            ? {}
            : { overheadTokens: selected.measurement.overheadTokens }),
        }).pipe(Effect.provideContext(context))
        if (selection._tag === 'Noop') return false
        const ready = yield* CompactionRuntime.prepare(
          services.compaction,
          task.conversationId,
          { reason: 'overflow', blocking: true },
          (change) => write((tx) => change(tx)),
          task.id,
          Identity.RequestId.make(`summary:${preparation.requestId}`),
          selection,
        )
        if (ready._tag === 'Noop') return false
        yield* write((tx) =>
          Effect.gen(function* () {
            if (preparation.execution === 'deferred')
              yield* Usage.record(tx, {
                conversationId: task.conversationId,
                identity: identity(preparation, attempt),
                outcome: { _tag: 'failed', error: { message: Cause.pretty(cause) } },
                ...(known === undefined ? {} : { usage: known }),
              })
            const child = yield* CompactionRuntime.admit(
              tx,
              task.conversationId,
              { reason: 'overflow', blocking: true },
              task.id,
              ready,
            )
            yield* services.waitCompaction(tx, {
              ...requesting,
              _tag: 'compaction',
              phase: 'compaction',
              child: child.id,
              overflow: error,
            })
            yield* clearLive(tx)
            yield* CompactionRuntime.activateFormat(services.compaction)
          }),
        )
        return true
      })
      let response: LanguageModel.GenerateTextResponse<Record<string, NativeTool.Any>, 'encoded'>
      if (preparation.execution === 'deferred') {
        if (selected === undefined)
          return yield* absent('model.deferred', 'Pinned deferred model is unavailable')
        const adapter = yield* Model.deferred(selected)
        const initial =
          requesting._tag === 'deferred' || requesting._tag === 'deferred-start'
            ? requesting
            : {
                ...requesting,
                _tag: 'deferred-start' as const,
                phase: 'deferred-start' as const,
                preparation,
                attempt,
                retries,
                expiresAt: yield* DeferredExecution.expiry(preparation.settings),
              }
        const deferred = yield* Effect.exit(
          DeferredExecution.run(
            initial,
            adapter,
            {
              requestId: preparation.requestId,
              attempt,
              prompt: (yield* Preparation.own(preparation)).prompt,
              toolkit: nativeToolkit,
              configuration,
            },
            (next) =>
              write((tx, current) =>
                Effect.gen(function* () {
                  yield* services.requireDeferredFormat
                  yield* saveState(tx, current, next)
                  yield* Transaction.ensureDocument(
                    tx,
                    LiveState.document,
                    LiveState.target(task.conversationId),
                  )
                  yield* Transaction.setDocument(
                    tx,
                    LiveState.document,
                    LiveState.target(task.conversationId),
                    {
                      generation: {
                        requestId: preparation.requestId,
                        attempt,
                        revision: 0,
                        retryAt: next._tag === 'deferred' ? next.pollAt : next.expiresAt,
                      },
                    },
                  )
                  state = next
                }),
              ),
            Settings.getOrDefault.pipe(Effect.provideContext(context)),
            requesting._tag === 'request',
          ).pipe(Effect.provideContext(requestContext)),
        )
        if (Exit.isFailure(deferred)) {
          const reason = deferred.cause.reasons.length === 1 ? deferred.cause.reasons[0] : undefined
          const known =
            reason !== undefined && Cause.isFailReason(reason) && AiError.isAiError(reason.error)
              ? ModelStream.errorUsage(reason.error, preparation)
              : undefined
          if (yield* recoverOverflow(deferred.cause, known)) return
          return yield* Effect.failCause(deferred.cause)
        }
        const outcome = deferred.value
        response = new LanguageModel.GenerateTextResponse(outcome.response.content)
        observed = {
          requestId: preparation.requestId,
          attempt,
          usage: ModelStream.usage(outcome.response.usage, preparation),
        }
      } else {
        if (requesting._tag !== 'request')
          return yield* absent(
            'model.deferred',
            'Deferred checkpoint requires its pinned execution choice',
          )
        const streaming: Extract<Run.RunState, { _tag: 'streaming' }> = {
          ...requesting,
          _tag: 'streaming',
          phase: 'streaming',
          preparation,
          attempt,
          retries,
          revision: 0,
        }
        yield* write((tx, current) =>
          Effect.gen(function* () {
            yield* services.requireStreamFormat
            yield* saveState(tx, current, streaming)
            yield* Transaction.ensureDocument(
              tx,
              LiveState.document,
              LiveState.target(task.conversationId),
            )
            yield* Transaction.setDocument(
              tx,
              LiveState.document,
              LiveState.target(task.conversationId),
              { generation: { requestId: preparation.requestId, attempt, revision: 0 } },
            )
          }),
        )
        state = streaming
        const progressPolicy = yield* Settings.getOrDefault.pipe(Effect.provideContext(context))
        const streamed = yield* ModelStream.consume(
          model
            .streamText({
              prompt: (yield* Preparation.own(preparation)).prompt,
              toolkit: nativeToolkit,
              disableToolCallResolution: true,
            })
            .pipe(
              Stream.updateContext((current: Context.Context<never>) =>
                Tracing.context(requestContext, current),
              ),
            ),
          preparation,
          attempt,
          progressPolicy.progress.partialIntervalMs,
          (partial, revision) =>
            write((tx, current) =>
              Effect.gen(function* () {
                if (!invocationActive())
                  return yield* new TaskError({
                    reason: 'revoked',
                    operation: 'model.partial',
                    message: 'Model invocation ended',
                  })
                yield* saveState(tx, current, { ...streaming, partial, revision })
                yield* Transaction.setDocument(
                  tx,
                  LiveState.document,
                  LiveState.target(task.conversationId),
                  { generation: { requestId: preparation.requestId, attempt, revision, partial } },
                )
              }),
            ),
        )
        if (Exit.isFailure(streamed.exit)) {
          const cause = streamed.exit.cause
          const reason = cause.reasons.length === 1 ? cause.reasons[0] : undefined
          // Publication failures never enter provider retry/receipt writes. Only a direct,
          // deterministic native rejection can carry this invocation's observed usage.
          if (streamed.publicationFailure) {
            if (
              reason !== undefined &&
              Cause.isFailReason(reason) &&
              reason.error instanceof NativeWriteFailure &&
              streamed.partial?.usage !== undefined
            )
              return yield* new StreamPublicationFailure({
                error: reason.error,
                requestId: preparation.requestId,
                attempt,
                usage: streamed.partial.usage,
              })
            return yield* Effect.failCause(cause)
          }
          const error =
            reason !== undefined && Cause.isFailReason(reason) && AiError.isAiError(reason.error)
              ? reason.error
              : undefined
          const policy = yield* Settings.getOrDefault.pipe(Effect.provideContext(context))
          const until =
            error === undefined
              ? undefined
              : ModelStream.retryAt(error, retries, policy, yield* Clock.currentTimeMillis)
          const known =
            streamed.partial?.usage ??
            (error === undefined ? undefined : ModelStream.errorUsage(error, preparation))
          const isOverflow = error !== undefined && overflow(error)
          if (isOverflow && (yield* recoverOverflow(cause, known))) return
          const followingAttempt =
            until === undefined || isOverflow ? undefined : yield* nextAttempt(attempt)
          const next: Run.RunState | undefined =
            until === undefined || followingAttempt === undefined
              ? undefined
              : {
                  ...requesting,
                  _tag: 'retry',
                  phase: 'retry',
                  preparation,
                  attempt: followingAttempt,
                  retries: retries + 1,
                  until,
                }
          yield* write((tx, current) =>
            Effect.gen(function* () {
              yield* Usage.record(tx, {
                conversationId: task.conversationId,
                identity: identity(preparation, attempt),
                outcome: { _tag: 'failed', error: { message: Cause.pretty(cause) } },
                ...(known === undefined ? {} : { usage: known }),
              })
              if (next === undefined) {
                yield* clearLive(tx)
                yield* saveState(tx, current, { ...requesting, attempt, retries })
              } else {
                yield* saveState(tx, current, next)
                yield* Transaction.setDocument(
                  tx,
                  LiveState.document,
                  LiveState.target(task.conversationId),
                  {
                    generation: {
                      requestId: preparation.requestId,
                      attempt: next.attempt,
                      revision: 0,
                      retryAt: next.until,
                    },
                  },
                )
              }
            }),
          )
          if (next === undefined) return yield* Effect.failCause(cause)
          state = next
          continue
        }
        response = streamed.exit.value
      }
      const appendAssistant = (tx: Transaction.Transaction, draft: Transaction.EntryDraft) =>
        Effect.gen(function* () {
          const entry = yield* Transaction.appendEntry(tx, task.conversationId, draft)
          yield* Usage.record(tx, {
            conversationId: task.conversationId,
            identity: identity(preparation, attempt),
            outcome: { _tag: 'completed', value: { entryId: entry.id } },
            linkedEntryId: entry.id,
            ...(draft.usage === undefined ? {} : { usage: draft.usage }),
          })
          const preparedModel = preparation.model
          const measuredModel =
            preparedModel._tag === 'descriptor'
              ? services.compaction.models.find(
                  (model) =>
                    model.definition.ref.provider === preparedModel.definition.ref.provider &&
                    model.definition.ref.modelId === preparedModel.definition.ref.modelId,
                )
              : undefined
          if (
            measuredModel?.measurement !== undefined &&
            draft.usage?._tag === 'model' &&
            draft.usage.tokens.input !== undefined &&
            (response.finishReason === 'stop' || response.finishReason === 'tool-calls')
          ) {
            yield* Transaction.ensureDocument(
              tx,
              PromptUsage.document,
              PromptUsage.target(task.conversationId),
            )
            yield* Transaction.setDocument(
              tx,
              PromptUsage.document,
              PromptUsage.target(task.conversationId),
              {
                baseline: {
                  identity: PromptUsage.identity(
                    measuredModel.measurement.identity,
                    preparation.tools,
                  ),
                  ...(preparation.agent.model === undefined
                    ? {}
                    : { model: preparation.agent.model }),
                  prompt: preparation.prompt,
                  tools: preparation.tools.map((tool) => tool.declaration),
                  inputTokens: draft.usage.tokens.input,
                },
              },
            )
          }
          yield* clearLive(tx)
          return entry
        })
      if (
        response.toolResults.length > 0 ||
        response.toolCalls.some((call) => call.providerExecuted)
      )
        return yield* new HarnessError({
          reason: 'invalid',
          operation: 'model.response',
          message: 'Providers must return tool intents without executing tools',
        })
      const nativeUsage = response.usage
      const usage = yield* Schema.decodeEffect(Usage.RecordSchema)(
        Usage.model(
          {
            ...(nativeUsage.inputTokens.total === undefined
              ? {}
              : { input: nativeUsage.inputTokens.total }),
            ...(nativeUsage.inputTokens.uncached === undefined
              ? {}
              : { inputUncached: nativeUsage.inputTokens.uncached }),
            ...(nativeUsage.inputTokens.cacheRead === undefined
              ? {}
              : { cacheRead: nativeUsage.inputTokens.cacheRead }),
            ...(nativeUsage.inputTokens.cacheWrite === undefined
              ? {}
              : { cacheWrite: nativeUsage.inputTokens.cacheWrite }),
            ...(nativeUsage.outputTokens.total === undefined
              ? {}
              : { output: nativeUsage.outputTokens.total }),
            ...(nativeUsage.outputTokens.text === undefined
              ? {}
              : { outputText: nativeUsage.outputTokens.text }),
            ...(nativeUsage.outputTokens.reasoning === undefined
              ? {}
              : { reasoning: nativeUsage.outputTokens.reasoning }),
          },
          preparation.model._tag === 'descriptor' ? preparation.model.definition.ref : undefined,
          preparation.model._tag === 'descriptor'
            ? preparation.model.definition.pricing
            : undefined,
        ),
      )
      const responsePrompt = Prompt.fromResponseParts(response.content)
      const assistant = responsePrompt.content.find((message) => message.role === 'assistant')
      if (assistant === undefined || assistant.role !== 'assistant')
        return yield* new HarnessError({
          reason: 'invalid',
          operation: 'model.response',
          message: 'Model produced no assistant message',
        })
      yield* runHooks(
        task.conversationId,
        requesting.agent,
        { _tag: 'afterResponse', message: assistant },
        task.id,
      )
      const calls = yield* Effect.forEach(response.toolCalls, (call) =>
        Effect.gen(function* () {
          return yield* Schema.decodeEffect(Tool.CallSchema)({
            id: call.id,
            name: call.name,
            arguments: yield* Schema.decodeUnknownEffect(Schema.JsonObject)(call.params),
          })
        }),
      )
      if (new Set(calls.map((call) => call.id)).size !== calls.length)
        return yield* new HarnessError({
          reason: 'invalid',
          operation: 'model.response',
          message: 'Duplicate tool call identities',
        })
      if (calls.length === 0) {
        const decision = yield* runHooks(
          task.conversationId,
          requesting.agent,
          { _tag: 'onYield', message: assistant },
          task.id,
        )
        if (decision !== undefined && '_tag' in decision && decision._tag === 'continue') {
          const {
            preparation: _preparation,
            attempt: _attempt,
            retries: _retries,
            compacted: _compacted,
            overflow: _overflow,
            ...unprepared
          } = requesting
          const next: Run.RunState = {
            ...unprepared,
            _tag: 'request',
            phase: 'request',
          }
          yield* write((tx, current) =>
            Effect.gen(function* () {
              yield* appendAssistant(tx, {
                kind: 'assistant',
                usage,
                model: yield* Schema.encodeEffect(Run.Messages)(responsePrompt.content),
              })
              yield* Transaction.appendEntry(tx, task.conversationId, {
                kind: 'input',
                model: yield* Schema.encodeEffect(Run.Messages)([decision.input]),
              })
              yield* saveState(tx, current, next)
            }),
          )
          state = next
          observed = undefined
          continue
        }
        yield* write((tx) =>
          Effect.gen(function* () {
            const entry = yield* appendAssistant(tx, {
              kind: 'assistant',
              usage,
              model: yield* Schema.encodeEffect(Run.Messages)(responsePrompt.content),
            })
            yield* Inbox.boundary(tx, task.conversationId, {
              boundary: 'final',
              taskId: task.id,
              outcome: { answer: entry.id },
            }).pipe(Effect.provideContext(context))
            yield* services.complete(tx, { status: 'completed', result: entry.id })
          }),
        )
        yield* notify
        return
      }
      const settings = yield* Settings.getOrDefault.pipe(Effect.provideContext(context))
      const mode =
        settings.toolExecution === 'parallel' &&
        available.every((tool) => Tool.policy(tool).executionMode === 'parallel')
          ? ('parallel' as const)
          : ('sequential' as const)
      const roundKey = `tool-round:${task.id}:${preparation.requestId}`
      yield* write((tx) =>
        Effect.gen(function* () {
          yield* services.requireToolRoundFormat
          const assistant = yield* appendAssistant(tx, {
            kind: 'assistant',
            usage,
            model: yield* Schema.encodeEffect(Run.Messages)(responsePrompt.content),
          })
          const execution = yield* TaskExecution
          const slots: Round.Slot[] = calls.map((call) => {
            const tool = available.find((tool) => tool.name === call.name)
            return {
              call,
              replay: tool === undefined ? 'unsafe' : Tool.policy(tool).replay,
              retention:
                tool === undefined ? OutputRetention.defaultPolicy : Tool.outputRetention(tool),
              progress: {
                ...(tool === undefined ? ProgressPolicy.defaultPolicy : Tool.progress(tool)),
                minInterval: settings.progress.outputIntervalMs,
              },
            }
          })
          const children: Record.TaskId[] = []
          for (let index = 0; index < slots.length; index++) {
            if (mode === 'sequential' && index > 0) break
            const slot = slots[index]!
            const child = yield* execution.createChild(tx, services.toolTask, {
              ownerTaskId: task.id,
              roundKey,
              index,
              call: slot.call,
              replay: slot.replay,
              preparation: RequestRecord.reference(task.id, preparation),
              retention: slot.retention,
              progress: slot.progress,
            })
            slots[index] = { ...slot, taskId: child.id }
            children.push(child.id)
          }
          yield* Transaction.ensureDocument(
            tx,
            Round.document,
            Round.target(task.conversationId, roundKey),
          )
          yield* Round.write(tx, task.conversationId, roundKey, {
            ownerTaskId: task.id,
            assistant: assistant.id,
            mode,
            preparation,
            applied: false,
            slots,
          })
          yield* services.waitTools(tx, {
            _tag: 'toolRound',
            phase: 'toolRound',
            submissionId: requesting.submissionId,
            agent: requesting.agent,
            ordinal: preparation.ordinal,
            preparation,
            roundKey,
            assistant: assistant.id,
            children,
          })
        }),
      )
      observed = undefined
      return
    }
  }
})
