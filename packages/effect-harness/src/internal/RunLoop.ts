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
import type * as Extension from '../Extension.js'
import { HarnessError } from '../HarnessError.js'
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
  readonly section: Extension.Any['sections'][number]
  readonly context: Context.Context<never>
  readonly extension: string
}
export interface Services {
  readonly notify: Effect.Effect<void, HarnessError | Failure>
  readonly handlers: Toolkit.WithHandler<Record<string, Tool.Any>>
  readonly session: Session.Session
  readonly context: Context.Context<never>
  readonly allTools: ReadonlyArray<Tool.Any>
  readonly extensions: ReadonlyArray<Extension.Any>
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
  ) => Effect.Effect<A, E | Failure, R>
  readonly saveState: (
    tx: Transaction.Transaction,
    task: Record.Task,
    state: Run.RunState,
  ) => Effect.Effect<void, Failure>
  readonly invocationActive: () => boolean
}
const jsonCodec = <S extends Schema.Constraint>(schema: S) => Schema.toCodecJson(schema)

export const run = Effect.fnUntraced(function* (services: Services, initial: Record.Task) {
  const {
    session,
    context,
    allTools,
    extensions,
    sections,
    models,
    nativeDefault,
    maxTurns,
    defaultAgent,
    checkOpen,
    access,
    runHooks,
    write,
    saveState,
    invocationActive,
    notify,
    handlers,
  } = services
  let task = initial
  let state = yield* Run.decode(task.state.checkpoint)
  let turns = 0
  while (true) {
    yield* checkOpen
    if (state._tag === 'queued') {
      const queued = state
      state = yield* write((tx, current) =>
        Effect.gen(function* () {
          const history = yield* Stream.runCollect(
            Transaction.scanEntries(tx, {
              conversationId: task.conversationId,
              order: 'ascending',
            }),
          )
          const messages = yield* Schema.decodeEffect(Run.Messages)(
            history.flatMap((entry) => entry.model ?? []),
          )
          const input = Prompt.userMessage({
            content:
              typeof queued.draft.content === 'string'
                ? [Prompt.textPart({ text: queued.draft.content })]
                : queued.draft.content,
          })
          const entry = yield* Transaction.appendEntry(tx, task.conversationId, {
            kind: 'input',
            head: 'self',
            model: yield* Schema.encodeEffect(Run.Messages)([input]),
          })
          const record = yield* Transaction.submission(tx, queued.submissionId)
          if (
            Option.isNone(record) ||
            record.value.type !== 'input' ||
            record.value.status !== 'queued'
          )
            return yield* absent('run.place', 'Queued submission is missing')
          yield* Transaction.putSubmission(tx, {
            ...record.value,
            _tag: 'InputPlaced',
            status: 'placed',
            entry: entry.id,
          })
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
            prompt: Prompt.make([...messages, input]),
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
          requesting.agent.extensions !== undefined &&
          !requesting.agent.extensions.includes(bound.extension)
        )
          continue
        const render = bound.section.render as (
          prompt: Prompt.Prompt,
        ) => Effect.Effect<string | undefined, HarnessError, HookExecution>
        const text = yield* protect(
          'promptSection.render',
          render(prompt).pipe(
            Effect.updateContext((input: Context.Context<HookExecution>) =>
              Context.merge(bound.context, input),
            ),
            Effect.provideService(HookExecution, {
              ...access(task.conversationId),
              taskId: task.id,
            }),
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
          const owner = extensions.findLast((extension) => extension.tools.includes(tool))
          return (
            owner === undefined ||
            requesting.agent.extensions === undefined ||
            requesting.agent.extensions.includes(owner.name)
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
      let requestContext: Context.Context<never> = context
      const model = selected?.languageModel ?? Option.getOrUndefined(nativeDefault)
      if (model === undefined)
        return yield* absent('model.select', 'No generation capability is available')
      if (selected !== undefined) {
        const decoded = yield* Schema.decodeEffect(
          selected.options as Schema.ConstraintCodec<unknown, unknown, never, never>,
        )(requesting.agent.model?.options ?? {})
        const configure = selected.configure as (
          options: unknown,
        ) => Effect.Effect<Context.Context<never>, HarnessError>
        requestContext = Context.merge(
          context,
          yield* protect('model.configure', configure(decoded)),
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
                head: 'self',
                model: yield* Schema.encodeEffect(Run.Messages)(responsePrompt.content),
              })
              yield* Transaction.appendEntry(tx, task.conversationId, {
                kind: 'input',
                head: 'self',
                model: yield* Schema.encodeEffect(Run.Messages)([decision.input]),
              })
              yield* saveState(tx, current, next)
            }),
          )
          state = next
          continue
        }
        yield* write((tx, current) =>
          Effect.gen(function* () {
            const entry = yield* Transaction.appendEntry(tx, task.conversationId, {
              kind: 'assistant',
              head: 'self',
              model: yield* Schema.encodeEffect(Run.Messages)(responsePrompt.content),
            })
            const found = yield* Transaction.submission(tx, requesting.submissionId)
            if (
              Option.isNone(found) ||
              found.value.type !== 'input' ||
              found.value.status !== 'placed'
            )
              return yield* absent('run.settle', 'Placed submission is missing')
            yield* Transaction.putSubmission(tx, {
              ...found.value,
              _tag: 'InputDone',
              status: 'done',
              answer: entry.id,
            })
            yield* Transaction.putTask(tx, {
              ...current,
              state: {
                status: 'terminal',
                outcome: { status: 'completed', result: entry.id },
              },
            })
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
            head: 'self',
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
      yield* write((tx, current) => saveState(tx, current, next))
      state = next
      continue
    }
    let call = pending.call
    let result: import('../ToolResult.js').Result | undefined
    let explicitEnvelope = false
    const definition = allTools.find((tool) => tool.name === call.name)
    const extension = extensions.findLast(
      (extension) => definition !== undefined && extension.tools.includes(definition),
    )
    const currentlyAllowed =
      definition !== undefined &&
      (toolState.agent.tools === undefined || toolState.agent.tools.includes(call.name)) &&
      (extension === undefined ||
        toolState.agent.extensions === undefined ||
        toolState.agent.extensions.includes(extension.name))
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
            write((tx) => Transaction.appendEntry(tx, task.conversationId, { kind, data })),
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
          guard.pipe(Effect.andThen(write((tx) => change(tx)))),
        output: (chunk: string) => report('tool.output', { callId: call.id, chunk }),
        details: (value: Schema.Json) => report('tool.details', { callId: call.id, value }),
        diagnostic: (value: import('../ToolResult.js').Diagnostic) =>
          report('tool.diagnostic', { callId: call.id, value }),
      }
      result = yield* handlers.invoke(call).pipe(
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
      yield* Schema.decodeEffect(jsonCodec(schema))(
        yield* Schema.encodeEffect(jsonCodec(ToolResult.ResultSchema))(result),
      ).pipe(Effect.provideContext(context))
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
          head: 'self',
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
