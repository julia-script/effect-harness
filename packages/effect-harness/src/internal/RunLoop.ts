import * as ProviderState from './ProviderState.js'
import { ProviderAffinity } from '../ProviderAffinity.js'
import * as Usage from '../Usage.js'
/** Executes one durable submission; scheduling and client admission live in HarnessRuntime. */
import type * as Document from '../Document.js'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import type * as AiTool from 'effect/ai/Tool'
import type * as AiToolkit from 'effect/ai/Toolkit'
import type * as Record from '../Record.js'
import type * as Agent from '../Agent.js'
import type * as Execution from '../Execution.js'
import { ExecutionError, type Failure } from '../ExecutionError.js'
import type * as PromptSection from '../PromptSection.js'
import { HarnessError } from '../HarnessError.js'
import { TaskError } from '../TaskError.js'
import type * as Hook from '../Hook.js'
import { HookExecution } from '../HookExecution.js'
import type * as Model from '../Model.js'
import * as Session from '../Session.js'
import * as Tool from '../Tool.js'
import { ToolExecution } from '../ToolExecution.js'
import * as ToolResult from '../ToolResult.js'
import * as Toolkit from '../Toolkit.js'
import * as Transaction from '../Transaction.js'
import * as Run from './RunState.js'
import * as Inbox from './Inbox.js'
import type { NativeWriteFailure } from './NativeWriteFailure.js'
import { document as agentDocument, target as agentTarget } from './AgentState.js'
import { protect, absent } from './HarnessFailure.js'

// Schema services are already present in the owning runtime's captured Context.
type GenerationCodec = Schema.Codec<unknown, Schema.Json, never, never>
type GenerationTool = AiTool.Tool<
  string,
  {
    readonly parameters: Schema.Codec<unknown, Schema.JsonObject, never, never>
    readonly success: GenerationCodec
    readonly failure: GenerationCodec
    readonly failureMode: 'error'
  }
>
export interface BoundSection {
  readonly section: PromptSection.Any
  readonly extension?: string | undefined
  readonly registration: string
}
export interface Services {
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
    allTools,
    toolExtensions,
    sections,
    models,
    nativeDefault,
    maxTurns,
    defaultAgent,
    checkOpen,
    access,
    runHooks,
    write,
    writeCallback,
    saveState,
    invocationActive,
    notify,
    lease,
  } = services
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
  let turns = 0
  while (true) {
    yield* checkOpen
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
      turns++
      if (turns > maxTurns)
        return yield* new HarnessError({
          reason: 'failed',
          operation: 'run.request',
          message: `Conversation exceeded ${maxTurns} model turns`,
        })
      const requesting = state
      let prompt = requesting.prompt
      const system: Array<string> = []
      if (requesting.agent.instructions !== undefined) system.push(requesting.agent.instructions)
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
        if (text !== undefined) system.push(text)
      }
      if (system.length > 0)
        prompt = Prompt.concat(
          Prompt.make([Prompt.systemMessage({ content: system.join('\n\n') })]),
          prompt,
        )
      const before = yield* runHooks(
        task.conversationId,
        requesting.agent,
        { _tag: 'beforeRequest', prompt },
        task.id,
      )
      if (before !== undefined && 'content' in before) prompt = before as Prompt.Prompt
      const available = allTools
        .filter(
          (tool) =>
            requesting.agent.tools === undefined || requesting.agent.tools.includes(tool.name),
        )
        .filter((tool) => {
          const owner = toolExtensions.get(tool.name)
          return (
            owner === undefined ||
            requesting.agent.extensions === undefined ||
            requesting.agent.extensions.includes(owner)
          )
        })
      const selected =
        requesting.agent.model === undefined
          ? models[0]
          : models.find(
              (model) =>
                model.definition.ref.provider === requesting.agent.model?.ref.provider &&
                model.definition.ref.modelId === requesting.agent.model?.ref.modelId,
            )
      if (requesting.agent.model !== undefined && selected === undefined)
        return yield* absent('model.select', 'Configured model is unavailable')
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
      let requestContext: Context.Context<never> = Context.add(context, ProviderAffinity, {
        id: affinity,
      })
      const model = selected?.languageModel ?? Option.getOrUndefined(nativeDefault)
      if (model === undefined)
        return yield* absent('model.select', 'No generation capability is available')
      if (selected !== undefined) {
        const decoded = yield* Schema.decodeEffect(
          selected.options as Schema.ConstraintCodec<unknown, unknown, never, never>,
        )(requesting.agent.model?.options ?? {}).pipe(Effect.provideContext(requestContext))
        const configure = selected.configure as (
          options: unknown,
        ) => Effect.Effect<Context.Context<never>, HarnessError>
        requestContext = Context.merge(
          requestContext,
          yield* protect(
            'model.configure',
            configure(decoded).pipe(Effect.provideService(ProviderAffinity, { id: affinity })),
          ),
        )
      }
      const nativeToolkit = Toolkit.make(...available).native as unknown as AiToolkit.Toolkit<
        Record<string, GenerationTool>
      >
      const response = yield* model
        .generateText({ prompt, toolkit: nativeToolkit, disableToolCallResolution: true })
        .pipe(Effect.provideContext(requestContext))
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
          selected?.definition.ref,
          selected?.definition.pricing,
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
        Schema.decodeEffect(Tool.CallSchema)({
          id: call.id,
          name: call.name,
          arguments: call.params,
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
          const next: Run.RunState = {
            ...requesting,
            prompt: Prompt.concat(nextPrompt, Prompt.make([decision.input])),
          }
          yield* write((tx, current) =>
            Effect.gen(function* () {
              yield* Transaction.appendEntry(tx, task.conversationId, {
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
            const entry = yield* Transaction.appendEntry(tx, task.conversationId, {
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
          yield* Transaction.appendEntry(tx, task.conversationId, {
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
    const definition = allTools.find((tool) => tool.name === call.name)
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
        )
      const toolAccess = {
        ...access(task.conversationId),
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
              ),
            ),
          ),
        output: (chunk: string) => report('tool.output', { callId: call.id, chunk }),
        details: (value: Schema.Json) => report('tool.details', { callId: call.id, value }),
        diagnostic: (value: import('../ToolResult.js').Diagnostic) =>
          report('tool.diagnostic', { callId: call.id, value }),
      }
      result = yield* lease.invokeTool(call).pipe(
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
      yield* lease.withTool(call.name, (item) =>
        Schema.decodeEffect(jsonCodec(schema))(encoded).pipe(Effect.provideContext(item.context)),
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
