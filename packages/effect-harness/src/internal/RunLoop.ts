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
import type * as Document from '../Document.js'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Equal from 'effect/Equal'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as NativeTool from 'effect/ai/Tool'
import * as NativeToolkit from 'effect/ai/Toolkit'
import type * as Record from '../Record.js'
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
import * as Session from '../Session.js'
import * as Tool from '../Tool.js'
import { ToolExecution } from '../ToolExecution.js'
import * as ToolResult from '../ToolResult.js'
import * as Transaction from '../Transaction.js'
import * as Run from './RunState.js'
import * as Inbox from './Inbox.js'
import type { NativeWriteFailure } from './NativeWriteFailure.js'
import { document as agentDocument, target as agentTarget } from './AgentState.js'
import { protect, absent } from './HarnessFailure.js'

export interface BoundSection {
  readonly section: PromptSection.Any
  readonly extension?: string | undefined
  readonly registration: string
}
export interface Services {
  readonly nativeModel: Prepared.Binding
  readonly requireStreamFormat: Services['requireSubmissionFormat']
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
    outcome: import('../Record.js').TaskOutcome & {
      readonly status: 'completed'
      readonly result: Record.EntryId
    },
  ) => Effect.Effect<void, import('../TaskError.js').Failure>
  readonly invocationActive: () => boolean
}
const jsonCodec = <S extends Schema.Constraint>(schema: S) => Schema.toCodecJson(schema)

export const run = Effect.fnUntraced(function* (services: Services, initial: Record.Task) {
  const {
    session,
    context,
    models,
    nativeDefault,
    maxTurns,
    defaultAgent,
    checkOpen,
    access,
    write,
    writeCallback,
    saveState,
    invocationActive,
    notify,
  } = services
  let { allTools, toolExtensions, sections, runHooks, lease } = services
  let task = initial
  let state = yield* Run.decode(task.state.checkpoint)
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
          const prompt = yield* Inbox.placeRun(tx, current, queued)
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
            prompt,
          }
          yield* saveState(tx, current, next)
          return next
        }),
      )
      continue
    }
    if (state._tag === 'request') {
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
        ;({ allTools, toolExtensions, sections, runHooks, lease } = refreshed)
        const settingsService = yield* Effect.serviceOption(Settings.Settings).pipe(
          Effect.provideContext(context),
        )
        const settings = Option.isSome(settingsService)
          ? yield* settingsService.value.get
          : { ...Settings.defaults, maxTurns }
        const ordinal = (requesting.ordinal ?? 0) + 1
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
        requesting = { ...requesting, agent: current.agent, prompt: current.view.prompt, ordinal }
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
        let prompt = requesting.prompt
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
        const { modelTools: _modelTools, ...allAgent } = requesting.agent
        const allInventory = [
          ...Preparation.inventory(lease.snapshot, 'model', allAgent),
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
            return { view, plan, ...preview }
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
          .pipe(Stream.provideContext(requestContext)),
        preparation,
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
        const followingAttempt = until === undefined ? undefined : yield* nextAttempt(attempt)
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
      const response = streamed.exit.value
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
      const nextPrompt = Prompt.concat(requesting.prompt, responsePrompt)
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
            ...unprepared
          } = requesting
          const next: Run.RunState = {
            ...unprepared,
            prompt: Prompt.concat(nextPrompt, Prompt.make([decision.input])),
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
      const pending: Run.RunState = {
        _tag: 'tools',
        phase: 'tools',
        submissionId: requesting.submissionId,
        agent: requesting.agent,
        ordinal: preparation.ordinal,
        preparation,
        prompt: nextPrompt,
        calls: calls.map((call) => {
          const tool = available.find((tool) => tool.name === call.name)
          return {
            call,
            replay: tool === undefined ? 'unsafe' : Tool.policy(tool).replay,
            started: false,
          }
        }),
        index: 0,
        results: [],
      }
      yield* write((tx, current) =>
        Effect.gen(function* () {
          yield* appendAssistant(tx, {
            kind: 'assistant',
            usage,
            model: yield* Schema.encodeEffect(Run.Messages)(responsePrompt.content),
          })
          yield* saveState(tx, current, pending)
        }),
      )
      state = pending
      continue
    }
    const toolState = state
    const pending = toolState.calls[toolState.index]
    if (pending === undefined) {
      const next: Run.RunState = {
        _tag: 'request',
        phase: 'request',
        submissionId: toolState.submissionId,
        agent: toolState.agent,
        ...(toolState.ordinal === undefined ? {} : { ordinal: toolState.ordinal }),
        prompt: toolState.prompt,
      }
      yield* runHooks(
        task.conversationId,
        toolState.agent,
        { _tag: 'afterTools', results: toolState.results },
        task.id,
      )
      const selected = yield* write((tx, current) =>
        Effect.gen(function* () {
          const selected = yield* Inbox.boundary(tx, task.conversationId, {
            boundary: 'postTools',
            taskId: task.id,
          }).pipe(Effect.provideContext(context))
          const prompt = (yield* Inbox.project(tx, task.conversationId)).prompt
          if (selected.boundary === 'final') {
            const reset = selected.placed.findLast((item) => item._tag === 'WriteDone')
            if (reset === undefined || reset._tag !== 'WriteDone')
              return yield* absent('run.reset', 'Reset placement is missing')
            yield* services.complete(tx, { status: 'completed', result: reset.entry })
          } else {
            yield* saveState(tx, current, {
              ...next,
              prompt,
            })
          }
          return { selected, prompt }
        }),
      )
      if (selected.selected.boundary === 'final') {
        yield* notify
        return
      }
      state = { ...next, prompt: selected.prompt }
      continue
    }
    let call = pending.call
    let result: import('../ToolResult.js').Result | undefined
    let explicitEnvelope = false
    if (toolState.preparation !== undefined)
      yield* Preparation.compatible(
        toolState.preparation,
        lease.snapshot,
        models,
        services.nativeModel,
        Option.isSome(nativeDefault),
      )
    const binding = toolState.preparation?.tools.find(
      (tool) => tool.declaration.name === call.name,
    )?.binding
    const definition =
      binding === undefined
        ? allTools.find((tool) => tool.name === call.name)
        : lease.snapshot.bindings.find(
            (item) =>
              item.owner.id === binding.id &&
              item.owner.version === binding.version &&
              item.declaration.name === call.name,
          )?.declaration
    const extension = toolExtensions.get(call.name)
    const currentlyAllowed =
      definition !== undefined &&
      (toolState.agent.tools === undefined || toolState.agent.tools.includes(call.name)) &&
      (extension === undefined ||
        toolState.agent.extensions === undefined ||
        toolState.agent.extensions.includes(extension))
    if (!currentlyAllowed)
      result = {
        content: [Prompt.textPart({ text: `Tool ${call.name} is unavailable` })],
        isError: true,
        diagnostics: [],
      }
    if (
      pending.started &&
      (pending.replay !== 'safe' ||
        definition === undefined ||
        Tool.policy(definition).replay !== 'safe')
    )
      result = {
        content: [
          Prompt.textPart({
            text: 'Tool execution was interrupted. It was not repeated because replay is unsafe; its external outcome is unknown.',
          }),
        ],
        isError: true,
        diagnostics: [],
      }
    if (!pending.started && result === undefined) {
      const decision = yield* runHooks(
        task.conversationId,
        toolState.agent,
        { _tag: 'beforeTool', call },
        task.id,
      )
      if (decision !== undefined && '_tag' in decision && decision._tag === 'block')
        result = {
          content: [Prompt.textPart({ text: decision.message })],
          isError: true,
          diagnostics: [],
        }
      if (decision !== undefined && '_tag' in decision && decision._tag === 'replaceArguments')
        call = { ...call, arguments: decision.arguments }
    }
    if (result === undefined) {
      const started: Run.RunState = {
        ...toolState,
        calls: toolState.calls.map((saved, index) =>
          index === toolState.index ? { ...saved, call, started: true } : saved,
        ),
      }
      yield* write((tx, current) => saveState(tx, current, started))
      let toolActive = true
      const guard = Effect.suspend(() =>
        toolActive && invocationActive()
          ? Effect.void
          : Effect.fail(
              new ExecutionError({
                reason: 'revoked',
                operation: 'tool',
                message: 'Tool invocation has ended',
              }),
            ),
      )
      const report = (kind: string, data: Schema.Json) =>
        guard.pipe(
          Effect.andThen(
            writeCallback((tx) =>
              guard.pipe(
                Effect.andThen(Transaction.appendEntry(tx, task.conversationId, { kind, data })),
              ),
            ),
          ),
          Effect.asVoid,
          Effect.mapError((error) =>
            Schema.is(TaskError)(error)
              ? new ExecutionError({
                  reason: error.reason === 'revoked' ? 'revoked' : 'invalid',
                  operation: 'tool.report',
                  message: error.message,
                  cause: error,
                })
              : error,
          ),
        )
      const selected = toolState.preparation?.model
      const modelDefinition = selected?._tag === 'descriptor' ? selected.definition : undefined
      const imageReadPolicy: import('../coding/ImageReadPolicy.js').Policy =
        modelDefinition?.capabilities.images === true && modelDefinition.imageLimits !== undefined
          ? { vision: true, limits: modelDefinition.imageLimits }
          : { vision: false }
      const toolAccess = {
        ...access(task.conversationId),
        ...(toolState.preparation === undefined ? {} : { imageReadPolicy }),
        snapshot: <S extends Document.Codec>(
          document: Document.Document<S>,
          target: Document.Target,
        ) => guard.pipe(Effect.andThen(Session.snapshot(session, document, target))),
        watch: <S extends Document.Codec>(
          document: Document.Document<S>,
          target: Document.Target,
        ) =>
          Stream.unwrap(guard.pipe(Effect.as(Session.watch(session, document, target)))).pipe(
            Stream.mapEffect((value) => guard.pipe(Effect.as(value))),
          ),
        taskId: task.id,
        callId: call.id,
        commit: <A, E, R>(change: (tx: Transaction.Transaction) => Effect.Effect<A, E, R>) =>
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
                        operation: 'tool.commit',
                        message: error.message,
                        cause: error,
                      })
                    : error,
                ),
              ),
            ),
          ),
        output: (chunk: string) => report('tool.output', { callId: call.id, chunk }),
        details: (value: Schema.Json) => report('tool.details', { callId: call.id, value }),
        diagnostic: (value: import('../ToolResult.js').Diagnostic) =>
          report('tool.diagnostic', { callId: call.id, value }),
      }
      result = yield* (
        toolState.preparation === undefined
          ? lease.invokeTool(call)
          : Inventory.invoke(lease, call, 'model', {
              agent: {
                ...toolState.agent,
                toolkits: {
                  inherit: false,
                  registrations: toolState.preparation.requiredBindings.map((item) => item.id),
                  bindings: Object.fromEntries(
                    toolState.preparation.allTools.map((item) => [
                      item.declaration.name,
                      { binding: item.binding.id, version: item.binding.version },
                    ]),
                  ),
                },
              },
            })
      ).pipe(
        Effect.provideService(ToolExecution, toolAccess),
        Effect.ensuring(
          Effect.sync(() => {
            toolActive = false
          }),
        ),
      )
      // A handler may already return the canonical media envelope. Expose its native
      // content to result hooks before the final model-visible projection is encoded.
      const envelope =
        definition !== undefined && Tool.hasResultChannels(definition)
          ? Option.none()
          : Schema.decodeUnknownOption(jsonCodec(ToolResult.Envelope))(result.details)
      if (Option.isSome(envelope)) {
        result = { ...result, content: envelope.value.content }
        explicitEnvelope = true
      }
    }
    const after = yield* runHooks(
      task.conversationId,
      toolState.agent,
      { _tag: 'afterTool', call, result },
      task.id,
    )
    if (after !== undefined && 'isError' in after) result = after
    if (definition !== undefined && Tool.hasResultChannels(definition)) {
      // Hook results contain encoded programmatic JSON, while media parts retain native bytes.
      // Decode against the declared channel schema to validate replacements before committing.
      const schema = (result.isError
        ? definition.failureSchema
        : definition.successSchema) as unknown as Schema.ConstraintCodec<
        unknown,
        unknown,
        never,
        never
      >
      const encoded = yield* Schema.encodeEffect(jsonCodec(ToolResult.ResultSchema))(result)
      yield* lease.withTool(
        call.name,
        (item) =>
          Schema.decodeEffect(jsonCodec(schema))(encoded).pipe(Effect.provideContext(item.context)),
        binding === undefined ? undefined : { binding: binding.id, version: binding.version },
      )
    }
    const plainText = result.content.flatMap((part) =>
      part.type === 'text' && Object.keys(part.options).length === 0 ? [part.text] : [],
    )
    const modelResult =
      !explicitEnvelope && plainText.length === result.content.length
        ? plainText.join('\n')
        : yield* Schema.encodeEffect(jsonCodec(ToolResult.Envelope))({
            _tag: '@effect-harness/ToolContent',
            content: result.content,
          })
    const output = Prompt.toolMessage({
      content: [
        Prompt.toolResultPart({
          id: call.id,
          name: call.name,
          providerExecuted: false,
          isFailure: result.isError,
          result: modelResult,
        }),
      ],
    })
    const next: Run.RunState = {
      ...toolState,
      calls: toolState.calls.map((saved, index) =>
        index === toolState.index ? { ...saved, call, started: true } : saved,
      ),
      index: toolState.index + 1,
      results: [...toolState.results, result],
      prompt: Prompt.concat(toolState.prompt, Prompt.make([output])),
    }
    yield* write((tx, current) =>
      Effect.gen(function* () {
        yield* Transaction.appendEntry(tx, task.conversationId, {
          kind: 'tool.result',
          usage: {
            _tag: 'tool',
            name: call.name,
            ...(result.spend === undefined ? {} : { cost: result.spend }),
          },
          model: yield* Schema.encodeEffect(Run.Messages)([output]),
          data: yield* Schema.encodeEffect(jsonCodec(ToolResult.ResultSchema))(result),
        })
        yield* saveState(tx, current, next)
      }),
    )
    state = next
    const refreshed = yield* Session.task(session, task.id)
    if (Option.isSome(refreshed)) task = refreshed.value
  }
})
