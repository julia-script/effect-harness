import * as Predicate from 'effect/Predicate'
import * as Array from 'effect/Array'
import type { StorageError } from '../StorageError.ts'
import * as Time from 'effect-harness/Time'
import * as Ref from 'effect/Ref'
import * as Option from 'effect/Option'
import * as Schedule from 'effect/Schedule'
import { ModelRetryError, remaining, policy as retryPolicy } from './ModelRetry.ts'
import * as Serialization from '../Serialization.ts'
import * as Entry from '../Entry.ts'
import type * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import type * as Layer from 'effect/Layer'
import * as Agent from 'effect-harness/Agent'
import * as Transcript from 'effect-harness/Transcript'
import * as Compaction from 'effect-harness/Compaction'
import * as Executor from 'effect-harness/Executor'
import * as Hook from 'effect-harness/Hook'
import * as Invocation from 'effect-harness/Invocation'
import * as Model from 'effect-harness/Model'
import * as Progress from 'effect-harness/Progress'
import * as Registry from 'effect-harness/Registry'
import * as ResponseAccumulator from 'effect-harness/ResponseAccumulator'
import * as ToolRegistration from 'effect-harness/ToolRegistration'
import * as Usage from 'effect-harness/Usage'
import * as DateTime from 'effect/DateTime'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as Response from 'effect/ai/Response'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Activity from 'effect/workflow/Activity'
import * as DurableClock from 'effect/workflow/DurableClock'
import * as Conversation from '../Conversation.ts'
import * as Document from '../Document.ts'
import * as Inbox from '../Inbox.ts'
import * as Ownership from '../Ownership.ts'
import * as Record from '../Record.ts'
import type * as Session from '../Session.ts'
import { SessionDirectory } from '../SessionDirectory.ts'
import { record as recordUsage } from '../Usage.ts'
import {
  ExecutionError,
  InvalidStateError,
  AbortedError,
  NoModelError,
  ModelError,
} from './ExecutionError.ts'
import { Generation, Result } from './Generation.ts'
import * as SubmissionExecutor from './SubmissionExecutor.ts'
import { ToolCall } from './ToolCall.ts'
import * as ToolExecutor from './ToolExecutor.ts'
import { Compaction as CompactionWorkflow } from './Compaction.ts'
import * as CompactionExecutor from './CompactionExecutor.ts'
import { RequestDoc } from './Request.ts'
import * as Cancellation from './Cancellation.ts'
import * as Structured from './Structured.ts'

const Pinned = Schema.Struct({
  request: Executor.Request,
  state: Agent.State,
  settings: Agent.Settings,
})
const Preparation = Schema.Union([
  Schema.TaggedStruct('request', {
    ...Pinned.fields,
    background: Schema.optionalKey(CompactionWorkflow.payloadSchema),
  }),
  Schema.TaggedStruct('compaction', {
    compaction: CompactionWorkflow.payloadSchema,
  }),
])
const ToolPart = Response.ToolCallPart('', Schema.Json)
const ResponseParts = Schema.Array(
  Schema.Union([
    Response.AllParts(Toolkit.make()),
    Schema.Struct({ ...ToolPart.fields, name: Schema.String }),
    Schema.Struct({
      '~effect/ai/Response/Part': ToolPart.fields['~effect/ai/Response/Part'],
      type: Schema.Literal('tool-result'),
      id: Schema.String,
      name: Schema.String,
      providerExecuted: Schema.Boolean,
      metadata: ToolPart.fields.metadata,
      result: Schema.Json,
      encodedResult: Schema.Json,
      isFailure: Schema.Boolean,
      preliminary: Schema.Boolean,
    }),
  ]),
)
const DeferredDecision = Schema.Struct({ handle: Schema.Json, at: Time.DateTimeUtcFromEpochMillis })
const FailureAttempt = Schema.Struct({
  at: Time.DateTimeUtcFromEpochMillis,
  retry: Schema.Boolean,
  compaction: Schema.optionalKey(CompactionWorkflow.payloadSchema),
})
const ResponseStep = Schema.Struct({ disposition: Executor.Disposition, parts: ResponseParts })
const Settlement = Schema.Struct({
  result: Result,
  notify: Schema.Array(Record.SubmissionId),
  next: Schema.optionalKey(Generation.payloadSchema),
  failure: Schema.optionalKey(Schema.Struct({ reason: Schema.String, detail: Schema.String })),
})
const RoundCall = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  args: Schema.Json,
  unavailable: Schema.optionalKey(Schema.toEncoded(Schema.toCodecJson(ToolRegistration.Execution))),
})
const Round = Schema.Struct({
  assistant: Record.EntryId,
  calls: Schema.Array(RoundCall),
  sequential: Schema.Boolean,
})
const codecError = (cause?: unknown) =>
  new ExecutionError({
    reason: new InvalidStateError({
      message: 'Generation data cannot be persisted',
      ...(cause === undefined ? {} : { cause }),
    }),
  })
const domainError = (error: import('../StorageError.ts').StorageError | ExecutionError) =>
  error._tag === 'StorageError' ? SubmissionExecutor.storageError(error) : error

/**
 * Appends saved partial assistant content as aborted and records available usage.
 *
 * @category combinators
 */
export const convertPartial = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  live: Document.Document.Draft<Inbox.LiveState>,
  conversationId: Record.ConversationId,
): Effect.fn.Return<void, StorageError | ExecutionError> {
  const generation = live.generation
  if (generation?.message === undefined) return
  yield* tx.appendEntry(conversationId, {
    kind: 'harness.assistant',
    model: [generation.message],
    data: yield* Schema.encodeEffect(Serialization.json(Entry.AssistantData))({
      harness: {
        status: 'aborted',
        ...(generation.usage === undefined ? {} : { usage: generation.usage }),
      },
    }).pipe(Effect.mapError(codecError)),
  })
  if (generation.model !== undefined && generation.usage !== undefined)
    yield* recordUsage(
      tx,
      conversationId,
      'models',
      `${generation.model.provider}/${generation.model.modelId}`,
      generation.usage,
    )
  delete generation.message
  delete generation.usage
})
const appendAssistant = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  payload: typeof Generation.payloadSchema.Type,
  request: Executor.Request,
  disposition: Exclude<Executor.Disposition, { readonly _tag: 'deferred' }>,
  finishReason?: Response.FinishReason,
) {
  const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(Schema.Array(Prompt.Message)))(
    disposition.prompt.content,
  ).pipe(Effect.mapError(codecError))
  const messages = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Json))(encoded).pipe(
    Effect.mapError(codecError),
  )
  yield* recordUsage(
    tx,
    payload.conversationId,
    'models',
    `${request.model.provider}/${request.model.modelId}`,
    disposition.usage,
  )
  let status: Conversation.Metadata['status'] = 'stop'
  if (disposition._tag === 'failure') status = 'error'
  else if (disposition._tag === 'tools') status = 'tool-calls'
  else if (finishReason === 'length') status = 'length'
  return yield* tx.appendEntry(payload.conversationId, {
    kind: 'harness.assistant',
    byTaskId: payload.taskId,
    model: messages,
    data: yield* Schema.encodeEffect(Serialization.json(Entry.AssistantData))({
      timestamp: yield* DateTime.now,
      harness: {
        status,
        usage: disposition.usage,
        ...(disposition._tag === 'failure' && disposition.error !== undefined
          ? { error: disposition.error }
          : {}),
      },
    }).pipe(Effect.mapError(codecError)),
  })
})

/**
 * Ordinary native Workflow orchestration; named Activities own request replay and domain commits.
 *
 * @category layers
 */
export const layer: Layer.Layer<
  never,
  never,
  | Cancellation.Cancellation
  | Model.Catalog
  | Conversation.Configuration
  | Ownership.Declarations
  | Executor.Executor
  | SessionDirectory
  | WorkflowEngine.WorkflowEngine
> = Generation.toLayer(
  Effect.fnUntraced(function* (payload, executionId) {
    const session = yield* (yield* SessionDirectory)
      .resolve(payload.sessionId)
      .pipe(Effect.mapError(domainError))
    const executor = yield* Executor.Executor
    const catalog = yield* Model.Catalog
    const config = yield* Conversation.Configuration
    let invocation = Invocation.Invocation.of({
      cwd: config.cwd,
      report: config.report,
      progress: () => Effect.void,
    })
    const active = Effect.gen(function* () {
      const taskOption = yield* session.task(payload.taskId).pipe(Effect.mapError(domainError))
      if (Option.isNone(taskOption))
        return yield* new ExecutionError({
          reason: new InvalidStateError({ message: 'Generation projection is absent' }),
        })
      const task = taskOption.value
      if (task.abortRequested || task.state.status === 'terminal')
        return yield* new ExecutionError({
          reason: new AbortedError({ message: 'Generation has ended' }),
        })
      return task
    })
    const finish = Effect.fnUntraced(function* (value: typeof Settlement.Type) {
      yield* Structured.drain(session, payload.taskId, payload.sessionId).pipe(
        Effect.mapError(domainError),
      )
      if (value.next !== undefined) yield* Generation.execute(value.next, { discard: true })
      yield* SubmissionExecutor.notify(session, value.notify)
      return value.result
    })
    const fail = Effect.fnUntraced(function* (reason: string, detail: string) {
      const settled = yield* Activity.make({
        name: 'failure',
        success: Settlement,
        error: ExecutionError,
        execute: session
          .transaction(
            Effect.fnUntraced(function* (tx) {
              const graph = yield* Ownership.readGraph(tx)
              const taskOption = yield* tx.task(payload.taskId)
              if (Option.isNone(taskOption)) return yield* codecError()
              const task = taskOption.value
              if (task.state.status === 'terminal' || task.state.status === 'completing')
                return {
                  result: yield* Schema.decodeUnknownEffect(Result)(task.state.outcome).pipe(
                    Effect.mapError(codecError),
                  ),
                  notify: [],
                }
              const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
              yield* convertPartial(tx, live, payload.conversationId)
              const result: Result = {
                status: reason === 'aborted' ? 'aborted' : 'failed',
                detail,
              }
              yield* Structured.hold(tx, task, result, graph)
              return { result, notify: [], failure: { reason, detail } }
            }),
            { key: `workflow/generation/failure/${executionId}` },
          )
          .pipe(Effect.mapError(domainError)),
      })
      // The live tool slots remain available until owned work has settled. In
      // particular, an abort after restart must reconcile each tool's committed
      // progress before removing the enclosing run.
      const result = yield* finish(settled)
      const notify = yield* Activity.make({
        name: 'failure/end-run',
        success: Schema.Array(Record.SubmissionId),
        error: ExecutionError,
        execute: session
          .transaction(
            Effect.fnUntraced(function* (tx) {
              const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
              return yield* Inbox.endRun(tx, live, payload.taskId, {
                status: 'unanswered',
                reason: settled.failure?.reason ?? reason,
                detail: settled.failure?.detail ?? detail,
              })
            }),
            { key: `workflow/generation/failure/end-run/${executionId}` },
          )
          .pipe(Effect.mapError(domainError)),
      })
      yield* SubmissionExecutor.notify(session, notify)
      return result
    })
    const run = Effect.gen(function* () {
      const compacted = yield* Ref.make(false)
      const cycleNumber = yield* Ref.make(1)
      const attemptComputation = Effect.gen(function* () {
        const attempt = (yield* Schedule.CurrentMetadata).attempt + 1
        while (true) {
          const cycle = yield* Ref.getAndUpdate(cycleNumber, (cycle) => cycle + 1)
          let pinned: typeof Pinned.Type
          for (let preparation = 0; ; preparation++) {
            const planned = yield* Activity.make({
              name: `prepare/${cycle}/${preparation}`,
              success: Preparation,
              error: ExecutionError,
              execute: Cancellation.activity(
                payload,
                session,
                Effect.gen(function* () {
                  // Domain commit can survive a crash before the native Activity reply is cached.
                  // Reuse its exact result before current registry hooks or model planning run again.
                  const receipts = (yield* session.committed.pipe(
                    Effect.mapError(SubmissionExecutor.storageError),
                  )).receipts
                  const preparedReceipt = Array.findFirst(
                    receipts,
                    (receipt) =>
                      receipt.key ===
                      `workflow/generation/prepare/${executionId}/${cycle}/${preparation}`,
                  )
                  if (Option.isSome(preparedReceipt))
                    return yield* Schema.decodeEffect(Schema.toCodecJson(Preparation))(
                      preparedReceipt.value.result,
                    ).pipe(Effect.mapError(codecError))
                  const compactionReceipt = Array.findFirst(
                    receipts,
                    (receipt) =>
                      receipt.key ===
                      `workflow/generation/compact/${executionId}/${cycle}/${preparation}`,
                  )
                  if (Option.isSome(compactionReceipt))
                    return {
                      _tag: 'compaction' as const,
                      compaction: yield* Schema.decodeUnknownEffect(
                        CompactionWorkflow.payloadSchema,
                      )(compactionReceipt.value.result).pipe(Effect.mapError(codecError)),
                    }
                  yield* active
                  const state: Agent.State = yield* session
                    .snapshot(Conversation.AgentDoc, { owner: payload.conversationId })
                    .pipe(
                      Effect.mapError(domainError),
                      Effect.map(
                        Option.match({ onNone: () => ({}), onSome: (snapshot) => snapshot.value }),
                      ),
                    )
                  let providerOption = yield* session
                    .snapshot(Conversation.ProviderDoc, { owner: payload.conversationId })
                    .pipe(Effect.mapError(domainError))
                  if (
                    Option.isNone(providerOption) ||
                    providerOption.value.value.sessionId === ''
                  ) {
                    yield* session
                      .initialize(payload.conversationId)
                      .pipe(Effect.mapError(domainError))
                    providerOption = yield* session
                      .snapshot(Conversation.ProviderDoc, { owner: payload.conversationId })
                      .pipe(Effect.mapError(domainError))
                  }
                  if (Option.isNone(providerOption) || providerOption.value.value.sessionId === '')
                    return yield* new ExecutionError({
                      reason: new InvalidStateError({
                        message:
                          'Provider identity requires Conversation.layer(options) during Session construction',
                      }),
                    })
                  const provider = providerOption.value
                  const view = yield* Conversation.context(session, payload.conversationId).pipe(
                    Effect.mapError(domainError),
                  )
                  const prepared = yield* executor
                    .prepare({
                      state,
                      settings: yield* config.settings.pipe(Effect.mapError(codecError)),
                      view,
                      sessionId: provider.value.sessionId,
                    })
                    .pipe(
                      Effect.provideService(
                        Invocation.Invocation,
                        Invocation.Invocation.of({
                          ...invocation,
                          cwd: state.cwd ?? config.cwd,
                        }),
                      ),
                      Effect.mapError(
                        (error) =>
                          new ExecutionError({
                            reason: new (Predicate.hasProperty(error, 'reason') &&
                              error.reason._tag === 'ModelNoModelError'
                              ? NoModelError
                              : ModelError)({ message: error.message, cause: error }),
                          }),
                      ),
                    )
                  const settings = yield* Schema.decodeEffect(Schema.toType(Agent.Settings))(
                    yield* config.settings.pipe(Effect.mapError(codecError)),
                  ).pipe(Effect.mapError(codecError))
                  const descriptor = yield* catalog.resolve(prepared.request.model).pipe(
                    Effect.mapError(
                      (error) =>
                        new ExecutionError({
                          reason: new NoModelError({ message: error.message, cause: error }),
                        }),
                    ),
                  )
                  const hasCut = Option.isSome(
                    Compaction.selectCut(
                      view,
                      (yield* config.settings.pipe(Effect.mapError(codecError))).compaction
                        .keepRecentTokens,
                      descriptor.estimate,
                    ),
                  )
                  const threshold =
                    (yield* Ref.get(compacted)) || !hasCut
                      ? Option.none()
                      : Compaction.threshold(
                          Transcript.estimate(
                            view,
                            Array.flatMap(prepared.plan.patches, Transcript.systemMessages),
                            descriptor.estimate,
                          ),
                          descriptor.contextWindow,
                          (yield* config.settings.pipe(Effect.mapError(codecError))).compaction,
                        )
                  if (Option.isSome(threshold) && threshold.value === 'blocking') {
                    const child = yield* session
                      .transaction(
                        Effect.fnUntraced(function* (tx) {
                          const taskOption = yield* tx.task(payload.taskId)
                          if (Option.isNone(taskOption) || taskOption.value.abortRequested)
                            return yield* new ExecutionError({
                              reason: new AbortedError({ message: 'Generation aborted' }),
                            })
                          const task = taskOption.value
                          const child = yield* CompactionExecutor.make(tx, {
                            sessionId: payload.sessionId,
                            conversationId: payload.conversationId,
                            reason: 'threshold',
                            owner: payload.taskId,
                          })
                          yield* tx.write({
                            _tag: 'task',
                            value: {
                              ...task,
                              state: {
                                status: 'waiting',
                                on: [child.taskId],
                                policy: 'allSettled',
                              },
                            },
                          })
                          return child
                        }),
                        {
                          key: `workflow/generation/compact/${executionId}/${cycle}/${preparation}`,
                        },
                      )
                      .pipe(Effect.mapError(domainError))
                    return {
                      _tag: 'compaction' as const,
                      compaction: child,
                    }
                  }
                  const encodedRequest = yield* session
                    .transaction(
                      Effect.fnUntraced(function* (tx) {
                        const taskOption = yield* tx.task(payload.taskId)
                        if (Option.isNone(taskOption) || taskOption.value.abortRequested)
                          return yield* new ExecutionError({
                            reason: new AbortedError({ message: 'Generation aborted' }),
                          })
                        const task = taskOption.value
                        const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                        yield* convertPartial(tx, live, payload.conversationId)
                        const background =
                          Option.isSome(threshold) &&
                          threshold.value === 'background' &&
                          (live.compactions?.length ?? 0) === 0
                            ? yield* CompactionExecutor.make(tx, {
                                sessionId: payload.sessionId,
                                conversationId: payload.conversationId,
                                reason: 'background',
                              })
                            : undefined
                        let tail = prepared.request.tail
                        const edits = yield* Schema.decodeUnknownEffect(
                          Schema.Array(Record.ContextEdit),
                        )(prepared.plan.edits).pipe(Effect.mapError(codecError))
                        for (const [index, patch] of prepared.plan.patches.entries()) {
                          const system = yield* Schema.decodeUnknownEffect(Schema.Json)(patch).pipe(
                            Effect.mapError(codecError),
                          )
                          const entry = yield* tx.appendEntry(payload.conversationId, {
                            kind: 'harness.system',
                            data: { harness: { system } },
                            ...(index === 0 && Array.isReadonlyArrayNonEmpty(edits)
                              ? { edits }
                              : {}),
                          })
                          tail = entry.id
                        }
                        live.generation = {
                          attempt,
                          model: yield* Document.copyEffect(prepared.request.model),
                        }
                        const encoded = yield* Schema.encodeEffect(
                          Schema.toCodecJson(Executor.Request),
                        )({
                          ...prepared.request,
                          ...(tail === undefined ? {} : { tail }),
                        }).pipe(Effect.mapError(codecError))
                        const requestDoc = yield* tx.doc(RequestDoc, {
                          owner: payload.taskId,
                          seed: encoded,
                        })
                        requestDoc.request = yield* Document.copyEffect(encoded)
                        delete requestDoc.handle
                        yield* tx.write({
                          _tag: 'task',
                          value: { ...task, state: { status: 'running' } },
                        })
                        return yield* Schema.encodeEffect(Schema.toCodecJson(Preparation))({
                          _tag: 'request',
                          request: { ...prepared.request, ...(tail === undefined ? {} : { tail }) },
                          state,
                          settings,
                          ...(background === undefined ? {} : { background }),
                        }).pipe(Effect.mapError(codecError))
                      }),
                      { key: `workflow/generation/prepare/${executionId}/${cycle}/${preparation}` },
                    )
                    .pipe(Effect.mapError(domainError))
                  return yield* Schema.decodeEffect(Schema.toCodecJson(Preparation))(
                    encodedRequest,
                  ).pipe(Effect.mapError(codecError))
                }),
              ).pipe(Effect.mapError(domainError)),
            })
            if (planned._tag === 'compaction') {
              yield* Ref.set(compacted, true)
              yield* Effect.result(CompactionWorkflow.execute(planned.compaction))
              continue
            }
            pinned = planned
            if (planned.background !== undefined)
              yield* CompactionWorkflow.execute(planned.background, { discard: true })
            break
          }
          invocation = Invocation.Invocation.of({
            ...invocation,
            cwd: pinned.state.cwd ?? config.cwd,
          })
          const agent = yield* executor
            .resolve(pinned.state, pinned.settings)
            .pipe(Effect.provideService(Invocation.Invocation, invocation))
          let poll: { handle: Schema.Json; at: DateTime.Utc } | undefined
          let disposition: Executor.Disposition
          let parts: ReadonlyArray<Response.AnyPart> = []
          for (let fetch = 0; ; fetch++) {
            if (poll !== undefined)
              yield* DurableClock.sleep({
                name: `poll/${cycle}/${fetch}`,
                duration: yield* remaining(poll.at),
                inMemoryThreshold: 0,
              })
            const handle = poll?.handle
            const step = yield* Activity.make({
              name: `response/${cycle}/${fetch}`,
              success: ResponseStep,
              error: ExecutionError,
              execute: Cancellation.activity(
                payload,
                session,
                Effect.scoped(
                  Effect.gen(function* () {
                    yield* active
                    yield* session
                      .transaction(
                        Effect.fnUntraced(function* (tx) {
                          const live = yield* tx.doc(Inbox.LiveDoc, {
                            owner: payload.conversationId,
                          })
                          yield* convertPartial(tx, live, payload.conversationId)
                          live.generation = {
                            attempt,
                            model: yield* Document.copyEffect(pinned.request.model),
                          }
                        }),
                      )
                      .pipe(Effect.mapError(domainError))
                    const responseState = yield* Ref.make(ResponseAccumulator.make())
                    const write = Effect.gen(function* () {
                      const response = yield* Ref.get(responseState)
                      const message = ResponseAccumulator.partial(response)
                      if (Option.isNone(message)) return 0
                      const encoded = yield* Schema.encodeEffect(
                        Schema.toCodecJson(Prompt.AssistantMessage),
                      )(message.value)
                      const finish = Array.findLast(
                        response.parts,
                        (part) => part.type === 'finish',
                      )
                      const descriptor = yield* catalog.resolve(pinned.request.model).pipe(
                        Effect.mapError(
                          (error) =>
                            new ExecutionError({
                              reason: new (Predicate.hasProperty(error, 'reason') &&
                                error.reason._tag === 'ModelNoModelError'
                                ? NoModelError
                                : ModelError)({ message: error.message, cause: error }),
                            }),
                        ),
                      )
                      const usage = Option.isNone(finish)
                        ? undefined
                        : (descriptor.usage?.(finish.value.usage, finish.value.metadata) ??
                          Usage.fromResponse(finish.value.usage))
                      yield* session.transaction(
                        Effect.fnUntraced(function* (tx) {
                          const task = yield* tx.task(payload.taskId)
                          if (
                            Option.exists(
                              task,
                              (value) => value.abortRequested || value.state.status === 'terminal',
                            )
                          )
                            return
                          const live = yield* tx.doc(Inbox.LiveDoc, {
                            owner: payload.conversationId,
                          })
                          if (
                            live.run?.taskId === payload.taskId &&
                            live.generation !== undefined
                          ) {
                            live.generation.message = yield* Document.copyEffect(encoded)
                            if (usage !== undefined)
                              live.generation.usage = yield* Document.copyEffect(usage)
                          }
                        }),
                      )
                      return new TextEncoder().encode(JSON.stringify(encoded)).length
                    }).pipe(Effect.orDie)
                    const progress = yield* Progress.make(write, {
                      minInterval: pinned.settings.progress.partialInterval,
                    }).pipe(Effect.mapError(codecError))
                    const source =
                      handle === undefined
                        ? executor.generate(pinned.request, agent)
                        : executor.fetchDeferred(pinned.request, handle)
                    const streamed = yield* Effect.result(
                      source.pipe(
                        Stream.runForEach(
                          Effect.fnUntraced(function* (part) {
                            yield* Ref.update(responseState, (response) =>
                              ResponseAccumulator.append(response, part),
                            )
                            yield* progress.mark
                          }),
                        ),
                        Effect.provideService(Invocation.Invocation, invocation),
                      ),
                    )
                    const pending = yield* progress.stop
                    yield* Progress.settle(pending, Exit.void)
                    const response = yield* Ref.get(responseState)
                    const parts = yield* Schema.decodeUnknownEffect(ResponseParts)(
                      response.parts,
                    ).pipe(Effect.mapError(codecError))
                    if (
                      streamed._tag === 'Failure' ||
                      !response.parts.some((part) => part.type === 'finish')
                    ) {
                      if (streamed._tag === 'Failure')
                        yield* Effect.logError(
                          'Generation response stream failed',
                          streamed.failure,
                        ).pipe(
                          Effect.annotateLogs({
                            conversationId: payload.conversationId,
                            taskId: payload.taskId,
                          }),
                        )
                      const text =
                        streamed._tag === 'Failure'
                          ? Model.errorText(streamed.failure)
                          : 'Stream ended before a terminal response event'
                      const finish = Array.findLast(
                        response.parts,
                        (part) => part.type === 'finish',
                      )
                      const partial = ResponseAccumulator.partial(response)
                      const descriptor = yield* catalog.resolve(pinned.request.model).pipe(
                        Effect.mapError(
                          (error) =>
                            new ExecutionError({
                              reason: new (Predicate.hasProperty(error, 'reason') &&
                                error.reason._tag === 'ModelNoModelError'
                                ? NoModelError
                                : ModelError)({ message: error.message, cause: error }),
                            }),
                        ),
                      )
                      return {
                        parts,
                        disposition: {
                          _tag: 'failure' as const,
                          prompt: Option.match(partial, {
                            onNone: () => ResponseAccumulator.message(response),
                            onSome: (message) => Prompt.fromMessages([message]),
                          }),
                          usage: Option.isNone(finish)
                            ? Usage.fromResponse({
                                inputTokens: {
                                  uncached: undefined,
                                  total: undefined,
                                  cacheRead: undefined,
                                  cacheWrite: undefined,
                                },
                                outputTokens: {
                                  total: undefined,
                                  text: undefined,
                                  reasoning: undefined,
                                },
                              })
                            : (descriptor.usage?.(finish.value.usage, finish.value.metadata) ??
                              Usage.fromResponse(finish.value.usage)),
                          message: text,
                          error: Model.providerError(
                            streamed._tag === 'Failure' ? streamed.failure : text,
                            pinned.request.model.provider,
                          ),
                          ...(descriptor.classify?.(
                            streamed._tag === 'Failure' ? streamed.failure : text,
                          ) ??
                            Model.classify(
                              streamed._tag === 'Failure' ? streamed.failure : text,
                              pinned.request.model.provider,
                            )),
                        },
                      }
                    }
                    const disposition = yield* executor
                      .classifyResponse(pinned.request, agent, response.parts)
                      .pipe(
                        Effect.provideService(Invocation.Invocation, invocation),
                        Effect.mapError(
                          (error) =>
                            new ExecutionError({
                              reason: new (Predicate.hasProperty(error, 'reason') &&
                                error.reason._tag === 'ModelNoModelError'
                                ? NoModelError
                                : ModelError)({ message: error.message, cause: error }),
                            }),
                        ),
                      )
                    return { disposition, parts }
                  }),
                ),
              ).pipe(Effect.mapError(domainError)),
            })
            disposition = step.disposition
            parts = step.parts
            if (disposition._tag !== 'deferred') break
            const previousAt = poll?.at
            poll = yield* Activity.make({
              name: `deferred/${cycle}/${fetch}`,
              success: DeferredDecision,
              error: ExecutionError,
              execute: session
                .transaction(
                  Effect.fnUntraced(function* (tx) {
                    const at = Model.pollAt(
                      yield* DateTime.now,
                      previousAt,
                      disposition._tag === 'deferred'
                        ? disposition.decision.pollAfterMs
                        : undefined,
                    )
                    const handle =
                      disposition._tag === 'deferred' ? disposition.decision.handle : null
                    const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                    const request = yield* tx.doc(RequestDoc, { owner: payload.taskId })
                    request.handle = yield* Document.copyEffect(handle)
                    live.generation = {
                      attempt,
                      model: yield* Document.copyEffect(pinned.request.model),
                      deferred: { pollAt: DateTime.toEpochMillis(at) },
                    }
                    return { at: DateTime.toEpochMillis(at), handle }
                  }),
                  { key: `workflow/generation/deferred/${executionId}/${cycle}/${fetch}` },
                )
                .pipe(
                  Effect.mapError(domainError),
                  Effect.flatMap(Schema.decodeEffect(DeferredDecision)),
                  Effect.mapError((cause) =>
                    cause instanceof ExecutionError ? cause : codecError(cause),
                  ),
                ),
            })
          }
          if (disposition._tag === 'failure') {
            const failure = disposition
            const failed = yield* Activity.make({
              name: `failure-attempt/${cycle}`,
              success: FailureAttempt,
              error: ExecutionError,
              execute: Effect.gen(function* () {
                let shouldCompact = false
                if (
                  failure.overflow &&
                  !(yield* Ref.get(compacted)) &&
                  (yield* config.settings.pipe(Effect.mapError(codecError))).compaction.enabled
                ) {
                  const cutoff =
                    pinned.request.tail === undefined
                      ? undefined
                      : yield* Schema.decodeEffect(Record.EntryId)(pinned.request.tail).pipe(
                          Effect.mapError(codecError),
                        )
                  const view = yield* Conversation.context(
                    session,
                    payload.conversationId,
                    cutoff,
                  ).pipe(Effect.mapError(domainError))
                  const descriptor = yield* catalog.resolve(pinned.request.model).pipe(
                    Effect.mapError(
                      (error) =>
                        new ExecutionError({
                          reason: new NoModelError({ message: error.message, cause: error }),
                        }),
                    ),
                  )
                  shouldCompact = Option.isSome(
                    Compaction.selectCut(
                      view,
                      (yield* config.settings.pipe(Effect.mapError(codecError))).compaction
                        .keepRecentTokens,
                      descriptor.estimate,
                    ),
                  )
                }
                return yield* session
                  .transaction(
                    Effect.fnUntraced(function* (tx) {
                      const taskOption = yield* tx.task(payload.taskId)
                      if (Option.isNone(taskOption) || taskOption.value.abortRequested)
                        return yield* new ExecutionError({
                          reason: new AbortedError({ message: 'Generation aborted' }),
                        })
                      const task = taskOption.value
                      const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                      const compaction = shouldCompact
                        ? yield* CompactionExecutor.make(tx, {
                            sessionId: payload.sessionId,
                            conversationId: payload.conversationId,
                            reason: 'overflow',
                            owner: payload.taskId,
                          })
                        : undefined
                      yield* appendAssistant(tx, payload, pinned.request, failure)
                      const retry =
                        !failure.overflow &&
                        Agent.isRetryAllowed(
                          (yield* config.settings.pipe(Effect.mapError(codecError))).retry,
                          attempt,
                          failure.retryable,
                        )
                      const at = DateTime.addDuration(
                        yield* DateTime.now,
                        Agent.retryDelay(
                          (yield* config.settings.pipe(Effect.mapError(codecError))).retry,
                          attempt,
                        ),
                      )
                      if (compaction !== undefined) {
                        delete live.generation
                        yield* tx.write({
                          _tag: 'task',
                          value: {
                            ...task,
                            state: {
                              status: 'waiting',
                              on: [compaction.taskId],
                              policy: 'allSettled',
                            },
                          },
                        })
                      } else if (retry)
                        live.generation = {
                          attempt,
                          retry: { at: DateTime.toEpochMillis(at), error: failure.message },
                        }
                      else delete live.generation
                      return {
                        at: DateTime.toEpochMillis(at),
                        retry,
                        ...(compaction === undefined ? {} : { compaction }),
                      }
                    }),
                    { key: `workflow/generation/attempt/${executionId}/${cycle}` },
                  )
                  .pipe(
                    Effect.mapError(domainError),
                    Effect.flatMap(Schema.decodeEffect(FailureAttempt)),
                    Effect.mapError((cause) =>
                      cause instanceof ExecutionError ? cause : codecError(cause),
                    ),
                  )
              }),
            })
            if (failed.compaction !== undefined) {
              yield* Ref.set(compacted, true)
              const summary = yield* Effect.result(CompactionWorkflow.execute(failed.compaction))
              if (summary._tag === 'Failure' || summary.success.entryId === undefined)
                return yield* fail('model_error', failure.message)
              continue
            }
            if (!failed.retry) return yield* fail('model_error', failure.message)
            return yield* new ModelRetryError({ name: `retry/${cycle}`, at: failed.at })
          }
          if (disposition._tag === 'answer') {
            const answer = disposition
            const continuation = yield* Activity.make({
              name: 'on-yield',
              success: Schema.OptionFromUndefinedOr(Prompt.UserMessage),
              error: ExecutionError,
              execute: Cancellation.activity(
                payload,
                session,
                Hook.onYield(Registry.handlers(agent, 'generation'), parts),
              ).pipe(
                Effect.mapError(domainError),
                Effect.provideService(Invocation.Invocation, invocation),
              ),
            })
            const settlement = yield* Activity.make({
              name: 'answer',
              success: Settlement,
              error: ExecutionError,
              execute: session
                .transaction(
                  Effect.fnUntraced(function* (tx) {
                    const graph = yield* Ownership.readGraph(tx)
                    const taskOption = yield* tx.task(payload.taskId)
                    if (Option.isNone(taskOption) || taskOption.value.abortRequested)
                      return yield* new ExecutionError({
                        reason: new AbortedError({ message: 'Generation aborted' }),
                      })
                    const task = taskOption.value
                    const boundary = yield* Inbox.prepare(
                      tx,
                      payload.conversationId,
                      yield* config.settings.pipe(Effect.mapError(codecError)),
                    )
                    const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                    const entry = yield* appendAssistant(
                      tx,
                      payload,
                      pinned.request,
                      answer,
                      Option.getOrUndefined(
                        Option.map(
                          Array.findLast(parts, (part) => part.type === 'finish'),
                          (part) => part.reason,
                        ),
                      ),
                    )
                    const selected = yield* Inbox.apply(tx, boundary, 'final', yield* DateTime.now)
                    let next: typeof Generation.payloadSchema.Type | undefined
                    let notify = [...selected.settled]
                    if (
                      Option.isSome(continuation) &&
                      Array.isReadonlyArrayEmpty(selected.users) &&
                      !selected.reset
                    ) {
                      const message = yield* Schema.encodeEffect(
                        Schema.toCodecJson(Prompt.UserMessage),
                      )(continuation.value).pipe(Effect.mapError(codecError))
                      yield* tx.appendEntry(payload.conversationId, {
                        kind: 'harness.user',
                        model: [message],
                      })
                      next = yield* SubmissionExecutor.makeGeneration(tx, {
                        sessionId: payload.sessionId,
                        conversationId: payload.conversationId,
                        inputs: live.run?.inputs ?? [],
                        runId: payload.runId,
                      })
                    } else {
                      notify.push(
                        ...(yield* Inbox.endRun(tx, live, payload.taskId, {
                          status: 'done',
                          answer: entry.id,
                        })),
                      )
                      if (Array.isReadonlyArrayNonEmpty(selected.users))
                        next = yield* SubmissionExecutor.makeGeneration(tx, {
                          sessionId: payload.sessionId,
                          conversationId: payload.conversationId,
                          inputs: selected.users,
                        })
                    }
                    const result: Result = { status: 'answered', answer: entry.id }
                    yield* Structured.hold(tx, task, result, graph)
                    return { result, notify, ...(next === undefined ? {} : { next }) }
                  }),
                  { key: `workflow/generation/answer/${executionId}` },
                )
                .pipe(Effect.mapError(domainError)),
            })
            return yield* finish(settlement)
          }
          const tools = disposition
          const round = yield* Activity.make({
            name: 'tool-round',
            success: Round,
            error: ExecutionError,
            execute: session
              .transaction(
                Effect.fnUntraced(function* (tx) {
                  const taskOption = yield* tx.task(payload.taskId)
                  if (Option.isNone(taskOption) || taskOption.value.abortRequested)
                    return yield* new ExecutionError({
                      reason: new AbortedError({ message: 'Generation aborted' }),
                    })
                  const task = taskOption.value
                  const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                  const assistant = yield* appendAssistant(tx, payload, pinned.request, tools)
                  const offered = new Set(pinned.request.tools.map((tool) => tool.name))
                  const calls: Array<typeof RoundCall.Type> = []
                  live.tools = []
                  for (const call of tools.calls) {
                    const args = yield* Schema.decodeUnknownEffect(Schema.Json)(call.params).pipe(
                      Effect.mapError(codecError),
                    )
                    const unavailable: ToolRegistration.Execution | undefined = offered.has(
                      call.name,
                    )
                      ? undefined
                      : { outcome: 'unavailable', result: ToolRegistration.unavailable(call.name) }
                    live.tools.push({ callId: call.id, name: call.name, status: 'pending' })
                    if (unavailable !== undefined)
                      yield* ToolExecutor.appendResult(
                        tx,
                        {
                          conversationId: payload.conversationId,
                          assistantId: assistant.id,
                          callId: call.id,
                          name: call.name,
                        },
                        unavailable,
                      )
                    calls.push({
                      id: call.id,
                      name: call.name,
                      args,
                      ...(unavailable === undefined
                        ? {}
                        : {
                            unavailable: yield* Schema.encodeEffect(
                              Schema.toCodecJson(ToolRegistration.Execution),
                            )(unavailable).pipe(Effect.mapError(codecError)),
                          }),
                    })
                  }
                  delete live.generation
                  yield* tx.write({
                    _tag: 'task',
                    value: { ...task, state: { status: 'running' } },
                  })
                  return {
                    assistant: assistant.id,
                    calls,
                    sequential:
                      ToolRegistration.executionMode(
                        Array.filter(agent.tools, (tool) =>
                          calls.some((call) => call.name === tool.tool.name),
                        ),
                        (yield* config.settings.pipe(Effect.mapError(codecError))).toolExecution,
                      ) === 'sequential',
                  }
                }),
                { key: `workflow/generation/round/${executionId}` },
              )
              .pipe(Effect.mapError(domainError)),
          })
          const executions = yield* Effect.forEach(
            round.calls,
            Effect.fnUntraced(function* (call, index) {
              if (call.unavailable !== undefined)
                return yield* Schema.decodeEffect(Schema.toCodecJson(ToolRegistration.Execution))(
                  call.unavailable,
                ).pipe(Effect.mapError(codecError))
              const child = yield* Activity.make({
                name: `start-tool/${index}`,
                success: ToolCall.payloadSchema,
                error: ExecutionError,
                execute: session
                  .transaction(
                    Effect.fnUntraced(function* (tx) {
                      const ownerOption = yield* tx.task(payload.taskId)
                      if (Option.isNone(ownerOption) || ownerOption.value.abortRequested)
                        return yield* new ExecutionError({
                          reason: new AbortedError({ message: 'Generation aborted' }),
                        })
                      const owner = ownerOption.value
                      const taskId = yield* tx.mint(Record.TaskId)
                      const child = {
                        sessionId: payload.sessionId,
                        conversationId: payload.conversationId,
                        taskId,
                        generationTaskId: payload.taskId,
                        assistantId: round.assistant,
                        callId: call.id,
                        name: call.name,
                        arguments: call.args,
                      }
                      const binding: Ownership.Binding = {
                        workflow: ToolCall._tag,
                        executionId: yield* ToolCall.executionId(child),
                        payload: child,
                      }
                      yield* tx.write({
                        _tag: 'task',
                        value: {
                          id: taskId,
                          conversationId: payload.conversationId,
                          owner: payload.taskId,
                          kind: 'harness.tool',
                          version: 1,
                          input: binding,
                          background: false,
                          abortRequested: false,
                          state: { status: 'pending' },
                        },
                      })
                      const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                      const slot = Array.findFirst(
                        live.tools ?? [],
                        (slot) => slot.callId === call.id,
                      )
                      if (Option.isSome(slot)) slot.value.taskId = taskId
                      yield* tx.write({
                        _tag: 'task',
                        value: {
                          ...owner,
                          state: {
                            status: 'waiting',
                            on: [...(owner.state.on ?? []), taskId],
                            policy: 'allSettled',
                          },
                        },
                      })
                      return child
                    }),
                    { key: `workflow/generation/tool/${executionId}/${index}` },
                  )
                  .pipe(Effect.mapError(domainError)),
              })
              yield* ToolCall.execute(child)
              const task = yield* session.task(child.taskId).pipe(Effect.mapError(domainError))
              return (yield* Schema.decodeEffect(Schema.toCodecJson(ToolExecutor.Outcome))(
                task.pipe(
                  Option.map((value) => value.state.outcome ?? null),
                  Option.getOrElse(() => null),
                ),
              ).pipe(Effect.mapError(codecError))).execution
            }),
            { concurrency: round.sequential ? 1 : config.toolConcurrency },
          )
          yield* Activity.make({
            name: 'after-tools',
            error: ExecutionError,
            execute: Cancellation.activity(
              payload,
              session,
              Effect.gen(function* () {
                const live = yield* session
                  .snapshot(Inbox.LiveDoc, { owner: payload.conversationId })
                  .pipe(Effect.mapError(domainError))
                const results: Array<Hook.SettledTool> = []
                for (const [index, call] of round.calls.entries()) {
                  const entryId = live.pipe(
                    Option.flatMap((snapshot) =>
                      Array.findFirst(
                        snapshot.value.tools ?? [],
                        (slot) => slot.callId === call.id,
                      ),
                    ),
                    Option.flatMap((slot) => Option.fromUndefinedOr(slot.entry)),
                  )
                  const execution = executions[index]
                  if (Option.isNone(entryId) || execution === undefined) return yield* codecError()
                  results.push({
                    id: call.id,
                    name: call.name,
                    entryId: entryId.value,
                    ...execution,
                  })
                }
                yield* Hook.afterTools(Registry.handlers(agent, 'generation'), results)
              }),
            ).pipe(
              Effect.mapError(domainError),
              Effect.provideService(Invocation.Invocation, invocation),
            ),
          })
          const controls = ToolRegistration.controls(executions)
          const settlement = yield* Activity.make({
            name: 'after-round',
            success: Settlement,
            error: ExecutionError,
            execute: session
              .transaction(
                Effect.fnUntraced(function* (tx) {
                  const graph = yield* Ownership.readGraph(tx)
                  const taskOption = yield* tx.task(payload.taskId)
                  if (Option.isNone(taskOption) || taskOption.value.abortRequested)
                    return yield* new ExecutionError({
                      reason: new AbortedError({ message: 'Generation aborted' }),
                    })
                  const task = taskOption.value
                  const boundary = yield* Inbox.prepare(
                    tx,
                    payload.conversationId,
                    yield* config.settings.pipe(Effect.mapError(codecError)),
                  )
                  const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                  if (Array.isReadonlyArrayNonEmpty(controls.addTools)) {
                    const agent = yield* tx.doc(Conversation.AgentDoc, {
                      owner: payload.conversationId,
                    })
                    Object.assign(agent, Agent.addTools(agent, controls.addTools))
                  }
                  if (controls.reset !== undefined) {
                    const reset = yield* Conversation.resetDraft(controls.reset.note)
                    const entry = yield* tx.appendEntry(payload.conversationId, reset)
                    boundary.head = entry.id
                  }
                  const selected = yield* Inbox.apply(
                    tx,
                    boundary,
                    controls.terminate || controls.reset !== undefined ? 'final' : 'postTools',
                    yield* DateTime.now,
                  )
                  const notify = [...selected.settled]
                  let next: typeof Generation.payloadSchema.Type | undefined
                  if (controls.terminate || controls.reset !== undefined || selected.reset) {
                    notify.push(
                      ...(yield* Inbox.endRun(
                        tx,
                        live,
                        payload.taskId,
                        selected.reset && controls.reset === undefined
                          ? { status: 'unanswered', reason: 'reset' }
                          : { status: 'done', answer: round.assistant },
                      )),
                    )
                    if (Array.isReadonlyArrayNonEmpty(selected.users))
                      next = yield* SubmissionExecutor.makeGeneration(tx, {
                        sessionId: payload.sessionId,
                        conversationId: payload.conversationId,
                        inputs: selected.users,
                      })
                  } else
                    next = yield* SubmissionExecutor.makeGeneration(tx, {
                      sessionId: payload.sessionId,
                      conversationId: payload.conversationId,
                      inputs: [...(live.run?.inputs ?? payload.inputs), ...selected.users],
                      runId: payload.runId,
                    })
                  const result: Result = { status: 'tools', answer: round.assistant }
                  yield* Structured.hold(tx, task, result, graph)
                  return { result, notify, ...(next === undefined ? {} : { next }) }
                }),
                { key: `workflow/generation/after-round/${executionId}` },
              )
              .pipe(Effect.mapError(domainError)),
          })
          return yield* finish(settlement)
        }
      })
      const attemptModel: Effect.Effect<
        Result,
        ExecutionError,
        Effect.Services<typeof attemptComputation>
      > = attemptComputation.pipe(
        Effect.retry(retryPolicy),
        Effect.catchTag('ModelRetryError', (cause) => Effect.fail(codecError(cause))),
      )
      return yield* attemptModel
    })
    // A settlement can commit before the engine caches its Activity reply.
    // Register that same native Activity before task-terminal fencing or current
    // model/registry lookup, then finish its recorded notifications and children.
    const receipts = (yield* session.committed.pipe(Effect.mapError(domainError))).receipts
    for (const name of ['answer', 'after-round'] as const) {
      const saved = Array.findFirst(
        receipts,
        (receipt) => receipt.key === `workflow/generation/${name}/${executionId}`,
      )
      if (Option.isSome(saved)) {
        const settlement = yield* Activity.make({
          name,
          success: Settlement,
          error: ExecutionError,
          execute: Schema.decodeUnknownEffect(Settlement)(saved.value.result).pipe(
            Effect.mapError(codecError),
          ),
        })
        return yield* finish(settlement)
      }
    }
    const failed = Array.findFirst(
      receipts,
      (receipt) => receipt.key === `workflow/generation/failure/${executionId}`,
    )
    if (Option.isSome(failed)) {
      const settled = yield* Schema.decodeUnknownEffect(Settlement)(failed.value.result).pipe(
        Effect.mapError(codecError),
      )
      return yield* fail(
        settled.failure?.reason ??
          (settled.result.status === 'aborted' ? 'aborted' : 'model_error'),
        settled.failure?.detail ?? settled.result.detail ?? '',
      )
    }
    return yield* Cancellation.run(payload, session, run).pipe(
      Effect.mapError(domainError),
      Effect.catchTag('ExecutionError', (error) => {
        switch (error.reason._tag) {
          case 'NoModelError':
            return fail('no_model', error.message)
          case 'AbortedError':
            return fail('aborted', error.message)
          case 'ConversationBusyError':
          case 'RequestConflictError':
          case 'ToolUnavailableError':
          case 'InvalidArgumentsError':
          case 'ModelError':
          case 'ContextOverflowError':
          case 'ClosedError':
          case 'StorageError':
          case 'InvalidStateError':
            return fail('model_error', error.message)
        }
      }),
    )
  }),
)
