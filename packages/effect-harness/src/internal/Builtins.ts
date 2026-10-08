/** Checkpointed conversation turns and tool calls. External work runs outside transactions. */
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Duration from 'effect/Duration'
import * as Prompt from 'effect/ai/Prompt'
import * as Agent from '../Agent.ts'
import * as Executor from '../Executor.ts'
import * as Invocation from '../Invocation.ts'
import * as Hook from '../Hook.ts'
import * as Registry from '../Registry.ts'
import * as ResponseAccumulator from '../ResponseAccumulator.ts'
import * as Task from '../Task.ts'
import { TaskRuntime } from '../TaskRuntime.ts'
import * as ToolRegistration from '../ToolRegistration.ts'
import * as ToolResult from '../ToolResult.ts'
import * as Serialization from '../Serialization.ts'
import * as Record from '../Record.ts'
import { StorageError } from '../StorageError.ts'
import type * as Session from './Session.ts'
import * as State from './ConversationState.ts'
import * as Accounting from './Usage.ts'
import * as Usage from '../Usage.ts'
import * as Model from '../Model.ts'

export interface Options {
  readonly settings: Agent.Settings
  readonly cwd: string
  readonly report: (error: unknown) => Effect.Effect<void>
}
const ToolInput = Schema.Struct({ id: Schema.String, name: Schema.String, args: Schema.Json })
const ToolCheckpoint = Schema.Struct({
  phase: Schema.Literals(['call', 'execute']),
  intent: Schema.optionalKey(ToolRegistration.Intent),
})
const TurnCheckpoint = Schema.Struct({
  phase: Schema.Literals([
    'prepare',
    'request',
    'tools',
    'retry',
    'deferred',
    'compacted',
    'failure',
  ]),
  compacted: Schema.optionalKey(Schema.Boolean),
  attemptStarted: Schema.optionalKey(Schema.Boolean),
  disposition: Schema.optionalKey(Executor.Disposition),
  request: Schema.optionalKey(Executor.Request),
  attempt: Schema.optionalKey(Schema.Int),
  until: Schema.optionalKey(Schema.Finite),
  handle: Schema.optionalKey(Schema.Json),
  children: Schema.optionalKey(Schema.Array(Record.TaskId)),
})
export const turnName = 'harness.turn'
export const toolName = 'harness.tool'

export const make = Effect.fnUntraced(function* (
  session: Session.Session.Service,
  executor: Executor.Executor['Service'],
  options: Options,
  compaction: Task.BoundDefinition,
) {
  const invocation = (
    runtime: TaskRuntime['Service'],
    cwd?: string,
  ): Invocation.Invocation['Service'] => ({
    cwd: cwd ?? options.cwd,
    report: options.report,
    progress: (progress) =>
      runtime
        .commit(
          Effect.fnUntraced(function* (tx) {
            const draft = yield* tx.doc(State.ProgressDoc, { owner: runtime.taskId })
            if (progress.clear) {
              delete draft.output
              delete draft.details
              delete draft.diagnostics
            }
            if (progress.output !== undefined) draft.output = progress.output
            if (progress.details !== undefined) Object.assign(draft, { details: progress.details })
            if (progress.diagnostics !== undefined)
              Object.assign(draft, {
                diagnostics: yield* Schema.encodeEffect(
                  Schema.toCodecJson(Schema.Array(Invocation.Diagnostic)),
                )(progress.diagnostics).pipe(
                  Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(Schema.Json))),
                ),
              })
            return undefined
          }),
        )
        .pipe(Effect.orDie),
  })
  const agentState = Effect.fnUntraced(function* (conversationId: Record.ConversationId) {
    const snapshot = yield* session.snapshot(State.AgentDoc, { owner: conversationId })
    return Option.isSome(snapshot) ? snapshot.value.value : {}
  })
  const settleTool = Effect.fnUntraced(function* (
    runtime: TaskRuntime['Service'],
    input: typeof ToolInput.Type,
    execution: ToolRegistration.Execution,
  ) {
    const result = yield* ToolResult.encode(execution.result)
    const model = yield* State.encodeMessages([
      Prompt.toolMessage({
        content: [
          Prompt.toolResultPart({
            id: input.id,
            name: input.name,
            result,
            isFailure: execution.result.isError === true,
            providerExecuted: false,
          }),
        ],
      }),
    ])
    const data = yield* Schema.encodeEffect(Schema.toCodecJson(ToolRegistration.Execution))(
      execution,
    ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)))
    yield* runtime.commit(
      Effect.fnUntraced(function* (tx) {
        yield* tx.appendEntry(runtime.conversationId, {
          kind: toolName,
          model,
          data,
          byTaskId: runtime.taskId,
        })
        if (execution.result.usage !== undefined)
          yield* Accounting.record(
            tx,
            runtime.conversationId,
            'tools',
            input.name,
            execution.result.usage,
          )
        return Task.complete(data)
      }),
    )
  })
  const tool = yield* Task.bind(
    Task.define({
      name: toolName,
      version: 1,
      input: Schema.toCodecJson(ToolInput),
      checkpoint: Schema.toCodecJson(ToolCheckpoint),
      result: Schema.Json,
      initial: () => ({ phase: 'call' as const }),
      run: Effect.fnUntraced(function* (task) {
        const runtime = yield* TaskRuntime
        const state = yield* agentState(task.conversationId)
        const context = invocation(runtime, state.cwd)
        const agent = yield* executor
          .resolve(state, options.settings)
          .pipe(Effect.provideService(Invocation.Invocation, context))
        let intent = task.checkpoint.intent
        if (task.checkpoint.phase === 'call') {
          const prepared = yield* Effect.result(
            executor
              .prepareTool(agent, task.input)
              .pipe(Effect.provideService(Invocation.Invocation, context)),
          )
          if (Result.isFailure(prepared)) {
            yield* settleTool(runtime, task.input, {
              outcome: 'failed',
              result: {
                isError: true,
                content: [Prompt.textPart({ text: prepared.failure.message })],
              },
            })
            return
          }
          intent = prepared.success
          const checkpoint = yield* Schema.encodeEffect(Schema.toCodecJson(ToolCheckpoint))({
            phase: 'execute',
            intent,
          }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)))
          yield* runtime.checkpoint(checkpoint)
        }
        if (intent === undefined) return Task.fail('Saved tool intent is missing')
        const previous = yield* session.snapshot(State.ProgressDoc, { owner: task.id })
        const partial: Invocation.Result = Option.isSome(previous)
          ? {
              ...(previous.value.value.output === undefined
                ? {}
                : { content: [Prompt.textPart({ text: previous.value.value.output })] }),
              ...(previous.value.value.details === undefined
                ? {}
                : { details: previous.value.value.details }),
            }
          : {}
        const result = yield* executor
          .tool(intent, agent, {
            recovering: task.checkpoint.phase === 'execute',
            previous: partial,
            settings: options.settings,
          })
          .pipe(
            Effect.provideService(Invocation.Invocation, context),
            Effect.catch((error) =>
              Effect.succeed<ToolRegistration.Execution>({
                outcome: 'failed',
                result: { isError: true, content: [Prompt.textPart({ text: error.message })] },
              }),
            ),
          )
        yield* settleTool(runtime, task.input, result)
      }),
      abort: Effect.fnUntraced(function* (task) {
        const runtime = yield* TaskRuntime
        const intent = task.checkpoint.intent ?? {
          id: task.input.id,
          name: task.input.name,
          args: task.input.args,
          replay: 'unsafe' as const,
        }
        const previous = yield* session.snapshot(State.ProgressDoc, { owner: task.id })
        const result = {
          ...ToolRegistration.interruption(intent),
          ...(Option.isSome(previous) && previous.value.value.output !== undefined
            ? { content: [Prompt.textPart({ text: previous.value.value.output })] }
            : {}),
          isError: true,
        }
        yield* settleTool(runtime, task.input, { outcome: 'interrupted', result })
      }),
    }),
  )
  const finish = Effect.fnUntraced(function* (
    runtime: TaskRuntime['Service'],
    message: string,
    aborted = false,
    draft?: Record.Entry.Draft,
    accounting?: { readonly ref: Agent.ModelRef; readonly usage: Usage.Usage },
  ) {
    yield* runtime.commit(
      Effect.fnUntraced(function* (tx) {
        if (draft !== undefined) yield* tx.appendEntry(runtime.conversationId, draft)
        if (accounting !== undefined)
          yield* Accounting.record(
            tx,
            runtime.conversationId,
            'models',
            `${accounting.ref.provider}/${accounting.ref.modelId}`,
            accounting.usage,
          )
        const inbox = yield* tx.doc(State.InboxDoc, { owner: runtime.conversationId })
        for (const id of [...inbox.pending, ...inbox.queue.map((item) => item.submissionId)])
          yield* tx.settleSubmission(id, { status: 'unanswered', reason: message })
        inbox.pending = []
        inbox.queue = []
        delete inbox.active
        return aborted ? Task.aborted(message) : Task.fail(message)
      }),
    )
  })
  const turn = yield* Task.bind(
    Task.define({
      name: turnName,
      version: 1,
      input: Schema.Json,
      checkpoint: Schema.toCodecJson(TurnCheckpoint),
      result: Schema.Json,
      initial: () => ({ phase: 'prepare' as const }),
      run: (task: Task.Snapshot<Schema.Json, typeof TurnCheckpoint.Type>) =>
        Effect.gen(function* () {
          const runtime = yield* TaskRuntime
          const state = yield* agentState(task.conversationId)
          const context = invocation(runtime, state.cwd)
          const agent = yield* executor
            .resolve(state, options.settings)
            .pipe(Effect.provideService(Invocation.Invocation, context))
          if (task.checkpoint.phase === 'compacted')
            return Task.continueWith({ phase: 'prepare' as const, compacted: true })
          const requestCompaction = Effect.fnUntraced(function* (
            reason: 'threshold' | 'overflow',
            background: boolean,
            record?: (tx: Session.Transaction) => Effect.Effect<void, StorageError>,
          ) {
            const prepared = yield* compaction.prepare({ reason })
            yield* runtime.commit(
              Effect.fnUntraced(function* (tx) {
                if (record !== undefined) yield* record(tx)
                const running = (yield* tx.tasks({
                  conversationId: task.conversationId,
                  kind: compaction.name,
                })).find((value) => value.state.status !== 'terminal')
                const id =
                  running?.id ??
                  (yield* tx.createTask({
                    conversationId: task.conversationId,
                    kind: compaction.name,
                    version: compaction.version,
                    input: prepared.input,
                    background,
                    ...(!background ? { owner: task.id } : {}),
                    abortRequested: false,
                    state: { status: 'pending', checkpoint: prepared.checkpoint },
                  }))
                if (background) return undefined
                return Task.wait({ phase: 'compacted' }, [id])
              }),
            )
          })
          if (task.checkpoint.phase === 'failure') {
            const failure = task.checkpoint.disposition
            const request = task.checkpoint.request
            if (failure?._tag !== 'failure' || request === undefined) {
              yield* finish(runtime, 'Saved failed response is invalid')
              return
            }
            const draft: Record.Entry.Draft = {
              kind: 'harness.assistant',
              model: yield* State.encodeMessages(failure.prompt.content),
              byTaskId: task.id,
              data: yield* Schema.encodeEffect(Schema.toCodecJson(State.Data))({
                harness: {
                  status: 'error',
                  usage: failure.usage,
                  ...(failure.error === undefined ? {} : { error: failure.error }),
                },
                message: failure.message,
              }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json))),
            }
            const record = Effect.fnUntraced(function* (tx: Session.Transaction) {
              yield* tx.appendEntry(task.conversationId, draft)
              yield* Accounting.record(
                tx,
                task.conversationId,
                'models',
                `${request.model.provider}/${request.model.modelId}`,
                failure.usage,
              )
            })
            if (
              failure.overflow &&
              options.settings.compaction.enabled &&
              !task.checkpoint.compacted
            ) {
              yield* requestCompaction('overflow', false, record)
              return
            }
            const attempt = (task.checkpoint.attempt ?? 0) + 1
            if (
              failure.retryable &&
              options.settings.retry.enabled &&
              attempt <= options.settings.retry.maxRetries
            ) {
              const until =
                (yield* runtime.now) +
                Duration.toMillis(Agent.retryDelay(options.settings.retry, attempt))
              yield* runtime.commit(
                Effect.fnUntraced(function* (tx) {
                  yield* record(tx)
                  return Task.continueWith({
                    phase: 'retry',
                    attempt,
                    until,
                    compacted: task.checkpoint.compacted ?? false,
                  })
                }),
              )
              return
            }
            yield* finish(runtime, failure.message, false, draft, {
              ref: request.model,
              usage: failure.usage,
            })
            return
          }
          if (task.checkpoint.phase === 'retry') {
            yield* runtime.sleepUntil(task.checkpoint.until ?? 0)
            return Task.continueWith({
              phase: 'prepare' as const,
              attempt: task.checkpoint.attempt ?? 0,
              compacted: task.checkpoint.compacted ?? false,
            })
          }
          if (task.checkpoint.phase === 'tools') {
            const outcomes = yield* runtime.outcomes(task.checkpoint.children ?? [])
            const executions = yield* Effect.forEach(outcomes, (outcome) =>
              Schema.decodeUnknownEffect(
                Schema.Struct({ status: Schema.String, result: Schema.optionalKey(Schema.Json) }),
              )(outcome),
            )
            const aligned = yield* Effect.forEach(
              executions,
              (
                outcome,
              ): Effect.Effect<Option.Option<ToolRegistration.Execution>, Schema.SchemaError> =>
                outcome.result === undefined
                  ? Effect.succeedNone
                  : Schema.decodeEffect(Schema.toCodecJson(ToolRegistration.Execution))(
                      outcome.result,
                    ).pipe(Effect.asSome),
            )
            const settled = aligned.flatMap((execution) =>
              Option.isNone(execution) ? [] : [execution.value],
            )
            const entries = yield* Stream.runCollect(
              session.scanEntries({ conversationId: task.conversationId }),
            )
            const batch = yield* Effect.forEach(
              task.checkpoint.children ?? [],
              Effect.fnUntraced(function* (id, index) {
                const child = yield* runtime.task(id)
                const execution = aligned[index]
                const entry = entries.find(
                  (value) => value.byTaskId === id && value.kind === toolName,
                )
                if (
                  Option.isNone(child) ||
                  execution === undefined ||
                  Option.isNone(execution) ||
                  entry === undefined
                )
                  return []
                const call = yield* Schema.decodeEffect(Schema.toCodecJson(ToolInput))(
                  child.value.input,
                )
                return [
                  {
                    id: call.id,
                    name: call.name,
                    entryId: entry.id,
                    ...execution.value,
                  } satisfies Hook.SettledTool,
                ]
              }),
            )
            yield* Hook.afterTools(Registry.handlers(agent, 'tool'), batch.flat()).pipe(
              Effect.provideService(Invocation.Invocation, context),
            )
            if (settled.some((execution) => execution.result.control?.terminate === true)) {
              yield* finish(runtime, 'Tool requested termination')
              return
            }
            yield* runtime.commit(
              Effect.fnUntraced(function* (tx) {
                const draft = yield* tx.doc(State.AgentDoc, { owner: task.conversationId })
                for (const execution of settled) {
                  const names = execution.result.control?.addTools
                  if (names !== undefined) Object.assign(draft, Agent.addTools(draft, names))
                  if (execution.result.control?.reset !== undefined)
                    yield* tx.appendEntry(task.conversationId, {
                      kind: 'harness.reset',
                      head: 'self',
                      model: yield* State.encodeMessages([
                        Prompt.userMessage({
                          content: [
                            Prompt.textPart({
                              text: execution.result.control.reset.note ?? 'Conversation reset.',
                            }),
                          ],
                        }),
                      ]),
                    })
                }
                yield* State.place(
                  tx,
                  task.conversationId,
                  'steering',
                  options.settings.steeringMode === 'one-at-a-time',
                )
                return Task.continueWith({ phase: 'prepare' as const })
              }),
            )
            return
          }
          if (task.checkpoint.phase === 'prepare') {
            yield* runtime.transaction(
              Effect.fnUntraced(function* (tx) {
                const inbox = yield* tx.doc(State.InboxDoc, { owner: task.conversationId })
                if (inbox.pending.length === 0)
                  yield* State.place(
                    tx,
                    task.conversationId,
                    'all',
                    options.settings.followUpMode === 'one-at-a-time',
                  )
              }),
            )
            const view = yield* State.context(session, task.conversationId)
            const prepared = yield* executor
              .prepare({ state, settings: options.settings, view })
              .pipe(Effect.provideService(Invocation.Invocation, context))
            const prompt = yield* Hook.beforeRequest(
              Registry.handlers(prepared.agent, 'generation'),
              prepared.request.prompt,
            ).pipe(Effect.provideService(Invocation.Invocation, context))
            if (task.checkpoint.compacted !== true) {
              const threshold = yield* executor.compactionThreshold({
                state,
                settings: options.settings,
                view,
                request: { ...prepared.request, prompt },
              })
              if (Option.isSome(threshold)) {
                yield* requestCompaction('threshold', threshold.value === 'background')
                if (threshold.value === 'blocking') return
              }
            }
            const edits = yield* Effect.forEach(
              prepared.plan.edits,
              (edit): Effect.Effect<Record.ContextEdit, Schema.SchemaError> =>
                edit._tag === 'omit'
                  ? Effect.succeed(edit)
                  : State.encodeMessages(edit.messages).pipe(
                      Effect.map((messages) => ({ ...edit, messages })),
                    ),
            )
            const patches = yield* Effect.forEach(prepared.plan.patches, (system) =>
              Schema.encodeEffect(Schema.toCodecJson(State.Data))({ harness: { system } }).pipe(
                Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)),
              ),
            )
            yield* runtime.commit(
              Effect.fnUntraced(function* (tx) {
                let tail = prepared.request.tail
                for (const [index, data] of patches.entries())
                  tail = (yield* tx.appendEntry(task.conversationId, {
                    kind: 'harness.system',
                    data,
                    ...(index === 0 ? { edits } : {}),
                  })).id
                if (patches.length === 0 && edits.length > 0)
                  tail = (yield* tx.appendEntry(task.conversationId, {
                    kind: 'harness.system',
                    edits,
                  })).id
                return Task.continueWith(
                  yield* Schema.encodeEffect(Schema.toCodecJson(TurnCheckpoint))({
                    phase: 'request',
                    request: {
                      ...prepared.request,
                      prompt,
                      ...(tail === undefined ? {} : { tail }),
                    },
                    attempt: task.checkpoint.attempt ?? 0,
                    compacted: task.checkpoint.compacted ?? false,
                  }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json))),
                )
              }),
            )
            return
          }
          const request = task.checkpoint.request
          if (request === undefined) {
            yield* finish(runtime, 'Saved model request is missing')
            return
          }
          if (task.checkpoint.attemptStarted === true) {
            yield* runtime.commit(
              Effect.fn('Builtins.recordInterruptedRequest')(function* (tx) {
                const progress = yield* tx.doc(State.ProgressDoc, { owner: task.id })
                const model =
                  progress.partial === undefined
                    ? []
                    : yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Json))(progress.partial)
                yield* tx.appendEntry(task.conversationId, {
                  kind: 'harness.assistant',
                  model,
                  byTaskId: task.id,
                  data: yield* Schema.encodeEffect(Schema.toCodecJson(State.Data))({
                    harness: { status: 'aborted' },
                    message: 'Model request interrupted before its response was committed',
                  }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json))),
                })
                yield* tx.retire(State.ProgressDoc, { owner: task.id })
                return Task.continueWith(
                  yield* Schema.encodeEffect(Schema.toCodecJson(TurnCheckpoint))({
                    ...task.checkpoint,
                    attemptStarted: false,
                  }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json))),
                )
              }),
            )
            return
          }
          let accumulated = ResponseAccumulator.make()
          let publishedAt = 0
          if (task.checkpoint.phase === 'deferred')
            yield* runtime.sleepUntil(task.checkpoint.until ?? 0)
          const started = yield* Schema.encodeEffect(Schema.toCodecJson(TurnCheckpoint))({
            ...task.checkpoint,
            attemptStarted: true,
          }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)))
          yield* runtime.commit(
            Effect.fn('Builtins.startRequest')(function* (tx) {
              yield* tx.retire(State.ProgressDoc, { owner: task.id })
              return Task.continueWith(started)
            }),
          )
          const stream =
            task.checkpoint.phase === 'deferred'
              ? executor.fetchDeferred(request, task.checkpoint.handle ?? null)
              : executor.generate(request, agent, { skipBeforeRequest: true })
          const streamed = yield* Effect.result(
            Stream.runForEach(stream, (part) =>
              Effect.gen(function* () {
                accumulated = ResponseAccumulator.append(accumulated, part)
                const now = yield* runtime.now
                if (
                  now - publishedAt <
                  Duration.toMillis(options.settings.progress.partialInterval)
                )
                  return
                const partial = ResponseAccumulator.partial(accumulated)
                if (Option.isNone(partial)) return
                const model = yield* State.encodeMessages([partial.value])
                yield* runtime.transaction(
                  Effect.fnUntraced(function* (tx) {
                    Object.assign(yield* tx.doc(State.ProgressDoc, { owner: task.id }), {
                      partial: model,
                    })
                  }),
                )
                publishedAt = now
              }),
            ).pipe(Effect.provideService(Invocation.Invocation, context)),
          )
          if (Result.isFailure(streamed)) {
            if (streamed.failure instanceof StorageError) return yield* streamed.failure
            const policy = yield* executor.classifyFailure(request, streamed.failure)
            const partial = ResponseAccumulator.partial(accumulated)
            return Task.continueWith({
              ...task.checkpoint,
              phase: 'failure' as const,
              disposition: {
                _tag: 'failure' as const,
                ...policy,
                message: Serialization.errorText(streamed.failure),
                error: Model.providerError(streamed.failure, request.model.provider),
                usage: Usage.make(),
                prompt: Option.isSome(partial) ? Prompt.make([partial.value]) : Prompt.empty,
              },
            })
          }
          const disposition = yield* executor
            .classifyResponse(request, agent, accumulated.parts)
            .pipe(Effect.provideService(Invocation.Invocation, context))
          if (disposition._tag === 'deferred')
            return Task.continueWith({
              phase: 'deferred' as const,
              request,
              handle: disposition.decision.handle,
              attempt: task.checkpoint.attempt ?? 0,
              until:
                (yield* runtime.now) +
                Duration.toMillis(disposition.decision.pollAfterMs ?? Duration.millis(0)),
            })
          if (disposition._tag === 'failure')
            return Task.continueWith({ ...task.checkpoint, phase: 'failure' as const, disposition })
          const model = yield* State.encodeMessages(disposition.prompt.content)
          const yielded =
            disposition._tag === 'answer'
              ? yield* Hook.onYield(Registry.handlers(agent, 'generation'), accumulated.parts).pipe(
                  Effect.provideService(Invocation.Invocation, context),
                )
              : Option.none<Prompt.UserMessage>()
          const followUp = Option.isSome(yielded)
            ? yield* State.encodeMessages([yielded.value])
            : undefined
          yield* runtime.commit(
            Effect.fnUntraced(function* (tx) {
              const answer = yield* tx.appendEntry(task.conversationId, {
                kind: 'harness.assistant',
                model,
                byTaskId: task.id,
                data: yield* Schema.encodeEffect(Schema.toCodecJson(State.Data))({
                  harness: {
                    status: disposition._tag === 'tools' ? 'tool-calls' : 'stop',
                    usage: disposition.usage,
                  },
                }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json))),
              })
              yield* Accounting.record(
                tx,
                task.conversationId,
                'models',
                `${request.model.provider}/${request.model.modelId}`,
                disposition.usage,
              )
              if (disposition._tag === 'tools') {
                const children: Array<Record.TaskId> = []
                const sequential =
                  options.settings.toolExecution === 'sequential' ||
                  agent.tools.some(
                    (registration) =>
                      disposition.calls.some((call) => call.name === registration.tool.name) &&
                      registration.metadata.execution === 'sequential',
                  )
                for (const call of disposition.calls) {
                  const prepared = yield* tool.prepare({
                    id: call.id,
                    name: call.name,
                    args: call.params,
                  })
                  const previous = children.at(-1)
                  children.push(
                    yield* tx.createTask({
                      conversationId: task.conversationId,
                      kind: tool.name,
                      version: tool.version,
                      input: prepared.input,
                      owner: task.id,
                      background: false,
                      abortRequested: false,
                      state:
                        sequential && previous !== undefined
                          ? {
                              status: 'waiting',
                              checkpoint: prepared.checkpoint,
                              on: [previous],
                              policy: 'allSettled',
                            }
                          : { status: 'pending', checkpoint: prepared.checkpoint },
                    }),
                  )
                }
                const checkpoint = yield* Schema.encodeEffect(Schema.toCodecJson(TurnCheckpoint))({
                  phase: 'tools',
                  children,
                }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)))
                return Task.wait(checkpoint, children)
              }
              const inbox = yield* tx.doc(State.InboxDoc, { owner: task.conversationId })
              for (const id of inbox.pending)
                yield* tx.settleSubmission(id, { status: 'done', answer: answer.id })
              inbox.pending = []
              if (followUp !== undefined)
                yield* tx.appendEntry(task.conversationId, {
                  kind: 'harness.followUp',
                  model: followUp,
                })
              if (inbox.queue.length > 0 || followUp !== undefined)
                return Task.continueWith({ phase: 'prepare' as const })
              delete inbox.active
              return Task.complete({ answer: answer.id })
            }),
          )
        }).pipe(
          Effect.catch(
            Effect.fnUntraced(function* (error) {
              if (error instanceof StorageError) return yield* error
              yield* finish(yield* TaskRuntime, Serialization.errorText(error))
            }),
          ),
        ),
      abort: Effect.fnUntraced(function* (task) {
        const runtime = yield* TaskRuntime
        if (
          task.checkpoint.phase === 'deferred' &&
          task.checkpoint.request !== undefined &&
          task.checkpoint.handle !== undefined
        )
          yield* executor
            .cancelDeferred(task.checkpoint.request, task.checkpoint.handle)
            .pipe(Effect.catch(options.report))
        const progress = yield* session.snapshot(State.ProgressDoc, { owner: task.id })
        const partial = Option.isSome(progress) ? progress.value.value.partial : undefined
        const draft: Record.Entry.Draft | undefined =
          partial === undefined
            ? undefined
            : {
                kind: 'harness.assistant',
                model: yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Json))(partial),
                byTaskId: task.id,
                data: yield* Schema.encodeEffect(Schema.toCodecJson(State.Data))({
                  harness: { status: 'aborted' },
                }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json))),
              }
        yield* finish(runtime, 'aborted', true, draft)
      }),
    }),
  )
  return {
    turn: {
      ...turn,
      onTerminal: Effect.fnUntraced(function* (
        tx: Session.Transaction,
        task: Record.Task,
        outcome: Task.Outcome<Schema.Json>,
      ) {
        const inbox = yield* tx.doc(State.InboxDoc, { owner: task.conversationId })
        if (inbox.active !== task.id) return
        const reason =
          outcome.status === 'faulted' || outcome.status === 'failed'
            ? outcome.error.message
            : outcome.status
        for (const id of [...inbox.pending, ...inbox.queue.map((item) => item.submissionId)])
          yield* tx.settleSubmission(id, { status: 'unanswered', reason })
        inbox.pending = []
        inbox.queue = []
        delete inbox.active
      }),
    },
    tool,
  }
})
