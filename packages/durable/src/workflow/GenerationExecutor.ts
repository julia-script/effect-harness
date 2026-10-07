import * as Time from '@effect-harness/harness/Time'
import * as Ref from 'effect/Ref'
import * as Schedule from 'effect/Schedule'
import { ModelRetry, remaining, policy as retryPolicy } from './ModelRetry.ts'
import * as Serialization from '../Serialization.ts'
import * as Entry from '../Entry.ts'
import type * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import type * as Layer from 'effect/Layer'
import * as Agent from '@effect-harness/harness/Agent'
import * as ContextDomain from '@effect-harness/harness/Context'
import * as CompactionDomain from '@effect-harness/harness/Compaction'
import * as Harness from '@effect-harness/harness/Executor'
import * as Hook from '@effect-harness/harness/Hook'
import * as Invocation from '@effect-harness/harness/Invocation'
import * as Model from '@effect-harness/harness/Model'
import * as Progress from '@effect-harness/harness/Progress'
import * as Registry from '@effect-harness/harness/Registry'
import * as Response from '@effect-harness/harness/Response'
import * as Tool from '@effect-harness/harness/Tool'
import * as Totals from '@effect-harness/harness/Usage'
import * as DateTime from 'effect/DateTime'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as AiResponse from 'effect/ai/Response'
import * as Toolkit from 'effect/ai/Toolkit'
import * as ClusterSchema from 'effect/cluster/ClusterSchema'
import * as Activity from 'effect/workflow/Activity'
import * as DurableClock from 'effect/workflow/DurableClock'
import * as Conversation from '../Conversation.ts'
import * as Document from '../Document.ts'
import * as Inbox from '../Inbox.ts'
import * as Ownership from '../Ownership.ts'
import * as Record from '../Record.ts'
import type * as Session from '../Session.ts'
import { SessionDirectory } from '../SessionDirectory.ts'
import * as Usage from '../Usage.ts'
import {
  ExecutionError,
  InvalidState,
  Aborted,
  ExecutionErrorCodec,
  NoModel,
  ModelError,
} from './ExecutionError.ts'
import { Generation, Result } from './Generation.ts'
import * as SubmissionExecutor from './SubmissionExecutor.ts'
import { ToolCall } from './ToolCall.ts'
import * as ToolExecutor from './ToolExecutor.ts'
import { Compaction } from './Compaction.ts'
import * as CompactionExecutor from './CompactionExecutor.ts'
import { RequestDoc } from './Request.ts'
import * as Cancellation from './Cancellation.ts'
import * as Structured from './Structured.ts'

const Pinned = Schema.Struct({
  request: Harness.Request,
  state: Agent.State,
  settings: Agent.Settings,
})
const Preparation = Schema.Union([
  Schema.Struct({
    type: Schema.Literal('request'),
    ...Pinned.fields,
    background: Schema.optionalKey(Compaction.payloadSchema),
  }),
  Schema.Struct({ type: Schema.Literal('compaction'), compaction: Compaction.payloadSchema }),
])
const ToolPart = AiResponse.ToolCallPart('', Schema.Json)
const ResponseParts = Schema.Array(
  Schema.Union([
    AiResponse.AllParts(Toolkit.make()),
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
const ResponseStep = Schema.Struct({ disposition: Harness.Disposition, parts: ResponseParts })
const Settlement = Schema.Struct({
  result: Result,
  notify: Schema.Array(Record.SubmissionId),
  next: Schema.optionalKey(Generation.payloadSchema),
})
const RoundCall = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  args: Schema.Json,
  unavailable: Schema.optionalKey(Schema.toEncoded(Schema.toCodecJson(Tool.Execution))),
})
const Round = Schema.Struct({
  assistant: Record.EntryId,
  calls: Schema.Array(RoundCall),
  sequential: Schema.Boolean,
})
const codecError = (cause?: unknown) =>
  new ExecutionError({
    reason: new InvalidState({
      message: 'Generation data cannot be persisted',
      ...(cause === undefined ? {} : { cause }),
    }),
  })
const domainError = (error: import('../StorageError.ts').StorageError | ExecutionError) =>
  error._tag === 'StorageError' ? SubmissionExecutor.storageError(error) : error

export const convertPartial = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  live: Document.Draft<Inbox.LiveState>,
  conversationId: Record.ConversationId,
) {
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
    yield* Usage.record(
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
  request: Harness.Request,
  disposition: Exclude<Harness.Disposition, { readonly type: 'deferred' }>,
  finishReason?: AiResponse.FinishReason,
) {
  const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(Schema.Array(Prompt.Message)))(
    disposition.prompt.content,
  ).pipe(Effect.mapError(codecError))
  const messages = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Json))(encoded).pipe(
    Effect.mapError(codecError),
  )
  yield* Usage.record(
    tx,
    payload.conversationId,
    'models',
    `${request.model.provider}/${request.model.modelId}`,
    disposition.usage,
  )
  let status: Conversation.Metadata['status'] = 'stop'
  if (disposition.type === 'failure') status = 'error'
  else if (disposition.type === 'tools') status = 'tool-calls'
  else if (finishReason === 'length') status = 'length'
  return yield* tx.appendEntry(payload.conversationId, {
    kind: 'harness.assistant',
    byTaskId: payload.taskId,
    model: messages,
    data: yield* Schema.encodeEffect(Serialization.json(Entry.AssistantData))({
      timestamp: yield* DateTime.now,
      harness: { status, usage: disposition.usage },
    }).pipe(Effect.mapError(codecError)),
  })
})

/** Ordinary native Workflow orchestration; named Activities own request replay and domain commits. */
export const layer: Layer.Layer<
  never,
  never,
  | Cancellation.Cancellation
  | Model.Catalog
  | Conversation.Configuration
  | Ownership.Declarations
  | Harness.Executor
  | SessionDirectory
  | WorkflowEngine.WorkflowEngine
> = Generation.toLayer(
  Effect.fnUntraced(function* (payload, executionId) {
    const session = yield* (yield* SessionDirectory)
      .resolve(payload.sessionId)
      .pipe(Effect.mapError(domainError))
    const executor = yield* Harness.Executor
    const catalog = yield* Model.Catalog
    const config = yield* Conversation.Configuration
    let invocation = Invocation.Invocation.of({
      cwd: config.cwd,
      report: config.report,
      progress: () => Effect.void,
    })
    const active = Effect.gen(function* () {
      const task = yield* session.task(payload.taskId).pipe(Effect.mapError(domainError))
      if (task === undefined)
        return yield* new ExecutionError({
          reason: new InvalidState({ message: 'Generation projection is absent' }),
        })
      if (task.abortRequested || task.state.status === 'terminal')
        return yield* new ExecutionError({
          reason: new Aborted({ message: 'Generation has ended' }),
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
        error: ExecutionErrorCodec,
        execute: session
          .transaction(
            Effect.fnUntraced(function* (tx) {
              const graph = yield* Ownership.readGraph(tx)
              const task = yield* tx.task(payload.taskId)
              if (task === undefined) return yield* codecError()
              if (task.state.status === 'terminal' || task.state.status === 'completing')
                return {
                  result: yield* Schema.decodeUnknownEffect(Result)(task.state.outcome).pipe(
                    Effect.mapError(codecError),
                  ),
                  notify: [],
                }
              const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
              yield* convertPartial(tx, live, payload.conversationId)
              const result: typeof Result.Type = {
                status: reason === 'aborted' ? 'aborted' : 'failed',
                detail,
              }
              yield* Structured.hold(tx, task, result, graph)
              return { result, notify: [] }
            }),
            { key: `workflow/generation/failure/${executionId}` },
          )
          .pipe(Effect.mapError(domainError)),
      }).annotate(ClusterSchema.WithTransaction, true)
      // The live tool slots remain available until owned work has settled. In
      // particular, an abort after restart must reconcile each tool's committed
      // progress before removing the enclosing run.
      const result = yield* finish(settled)
      const notify = yield* Activity.make({
        name: 'failure/end-run',
        success: Schema.Array(Record.SubmissionId),
        error: ExecutionErrorCodec,
        execute: session
          .transaction(
            Effect.fnUntraced(function* (tx) {
              const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
              return yield* Inbox.endRun(tx, live, payload.taskId, {
                status: 'unanswered',
                reason,
                detail,
              })
            }),
            { key: `workflow/generation/failure/end-run/${executionId}` },
          )
          .pipe(Effect.mapError(domainError)),
      }).annotate(ClusterSchema.WithTransaction, true)
      yield* SubmissionExecutor.notify(session, notify)
      return result
    })
    const run = Effect.gen(function* () {
      const compacted = yield* Ref.make(false)
      const cycleNumber = yield* Ref.make(1)
      const attemptModel = Effect.fnUntraced(function* () {
        const attempt = (yield* Schedule.CurrentMetadata).attempt + 1
        while (true) {
          const cycle = yield* Ref.getAndUpdate(cycleNumber, (cycle) => cycle + 1)
          let pinned: typeof Pinned.Type
          for (let preparation = 0; ; preparation++) {
            const planned = yield* Activity.make({
              name: `prepare/${cycle}/${preparation}`,
              success: Preparation,
              error: ExecutionErrorCodec,
              execute: Cancellation.activity(
                payload,
                session,
                Effect.gen(function* () {
                  yield* active
                  // Domain commit can survive a crash before the native Activity reply is cached.
                  // Reuse its exact result before current registry hooks or model planning run again.
                  const receipts = (yield* session.committed.pipe(
                    Effect.mapError(SubmissionExecutor.storageError),
                  )).receipts
                  const preparedReceipt = receipts.find(
                    (receipt) =>
                      receipt.key ===
                      `workflow/generation/prepare/${executionId}/${cycle}/${preparation}`,
                  )
                  if (preparedReceipt !== undefined)
                    return yield* Schema.decodeEffect(Schema.toCodecJson(Preparation))(
                      preparedReceipt.result,
                    ).pipe(Effect.mapError(codecError))
                  const compactionReceipt = receipts.find(
                    (receipt) =>
                      receipt.key ===
                      `workflow/generation/compact/${executionId}/${cycle}/${preparation}`,
                  )
                  if (compactionReceipt !== undefined)
                    return {
                      type: 'compaction' as const,
                      compaction: yield* Schema.decodeUnknownEffect(Compaction.payloadSchema)(
                        compactionReceipt.result,
                      ).pipe(Effect.mapError(codecError)),
                    }
                  const state =
                    (yield* session
                      .snapshot(Conversation.AgentDoc, { owner: payload.conversationId })
                      .pipe(Effect.mapError(domainError)))?.value ?? {}
                  let provider = yield* session
                    .snapshot(Conversation.ProviderDoc, { owner: payload.conversationId })
                    .pipe(Effect.mapError(domainError))
                  if (provider === undefined || provider.value.sessionId === '') {
                    yield* session
                      .initialize(payload.conversationId)
                      .pipe(Effect.mapError(domainError))
                    provider = yield* session
                      .snapshot(Conversation.ProviderDoc, { owner: payload.conversationId })
                      .pipe(Effect.mapError(domainError))
                  }
                  if (provider === undefined || provider.value.sessionId === '')
                    return yield* new ExecutionError({
                      reason: new InvalidState({
                        message: 'Provider identity requires Conversation.layerCreation',
                      }),
                    })
                  const view = yield* Conversation.context(session, payload.conversationId).pipe(
                    Effect.mapError(domainError),
                  )
                  const prepared = yield* executor
                    .prepare({
                      state,
                      settings: config.settings,
                      view,
                      sessionId: provider.value.sessionId,
                    })
                    .pipe(
                      Effect.provideService(Invocation.Invocation, {
                        ...invocation,
                        cwd: state.cwd ?? config.cwd,
                      }),
                      Effect.mapError(
                        (error) =>
                          new ExecutionError({
                            reason: new ('reason' in error && error.reason._tag === 'ModelNoModel'
                              ? NoModel
                              : ModelError)({ message: error.message, cause: error }),
                          }),
                      ),
                    )
                  const settings = yield* Schema.decodeEffect(Schema.toType(Agent.Settings))(
                    config.settings,
                  ).pipe(Effect.mapError(codecError))
                  const descriptor = yield* catalog.resolve(prepared.request.model).pipe(
                    Effect.mapError(
                      (error) =>
                        new ExecutionError({
                          reason: new NoModel({ message: error.message, cause: error }),
                        }),
                    ),
                  )
                  const hasCut =
                    CompactionDomain.selectCut(
                      view,
                      config.settings.compaction.keepRecentTokens,
                      descriptor.estimate,
                    ) !== undefined
                  const threshold =
                    (yield* Ref.get(compacted)) || !hasCut
                      ? undefined
                      : CompactionDomain.threshold(
                          ContextDomain.estimate(
                            view,
                            prepared.plan.patches.flatMap(ContextDomain.systemMessages),
                            descriptor.estimate,
                          ),
                          descriptor.contextWindow,
                          config.settings.compaction,
                        )
                  if (threshold === 'blocking') {
                    const child = yield* session
                      .transaction(
                        Effect.fnUntraced(function* (tx) {
                          const task = yield* tx.task(payload.taskId)
                          if (task === undefined || task.abortRequested)
                            return yield* new ExecutionError({
                              reason: new Aborted({ message: 'Generation aborted' }),
                            })
                          const child = yield* CompactionExecutor.create(
                            tx,
                            payload.sessionId,
                            payload.conversationId,
                            'threshold',
                            payload.taskId,
                          )
                          yield* tx.write({
                            type: 'task',
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
                    return { type: 'compaction' as const, compaction: child }
                  }
                  const encodedRequest = yield* session
                    .transaction(
                      Effect.fnUntraced(function* (tx) {
                        const task = yield* tx.task(payload.taskId)
                        if (task === undefined || task.abortRequested)
                          return yield* new ExecutionError({
                            reason: new Aborted({ message: 'Generation aborted' }),
                          })
                        const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                        yield* convertPartial(tx, live, payload.conversationId)
                        const background =
                          threshold === 'background' && (live.compactions?.length ?? 0) === 0
                            ? yield* CompactionExecutor.create(
                                tx,
                                payload.sessionId,
                                payload.conversationId,
                                'background',
                              )
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
                            ...(index === 0 && edits.length > 0 ? { edits } : {}),
                          })
                          tail = entry.id
                        }
                        live.generation = {
                          attempt,
                          model: yield* Document.copyEffect(prepared.request.model),
                        }
                        const encoded = yield* Schema.encodeEffect(
                          Schema.toCodecJson(Harness.Request),
                        )({ ...prepared.request, ...(tail === undefined ? {} : { tail }) }).pipe(
                          Effect.mapError(codecError),
                        )
                        const requestDoc = yield* tx.doc(RequestDoc, {
                          owner: payload.taskId,
                          seed: encoded,
                        })
                        requestDoc.request = yield* Document.copyEffect(encoded)
                        delete requestDoc.handle
                        yield* tx.write({
                          type: 'task',
                          value: { ...task, state: { status: 'running' } },
                        })
                        return yield* Schema.encodeEffect(Schema.toCodecJson(Preparation))({
                          type: 'request',
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
            if (planned.type === 'compaction') {
              yield* Ref.set(compacted, true)
              yield* Effect.result(Compaction.execute(planned.compaction))
              continue
            }
            pinned = planned
            if (planned.background !== undefined)
              yield* Compaction.execute(planned.background, { discard: true })
            break
          }
          invocation = { ...invocation, cwd: pinned.state.cwd ?? config.cwd }
          const agent = yield* executor
            .resolve(pinned.state, pinned.settings)
            .pipe(Effect.provideService(Invocation.Invocation, invocation))
          let poll: { handle: Schema.Json; at: DateTime.Utc } | undefined
          let disposition: Harness.Disposition
          let parts: ReadonlyArray<AiResponse.AnyPart> = []
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
              error: ExecutionErrorCodec,
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
                    const responseState = yield* Ref.make(Response.empty())
                    const write = Effect.gen(function* () {
                      const response = yield* Ref.get(responseState)
                      const message = Response.partial(response)
                      if (message === undefined) return 0
                      const encoded = yield* Schema.encodeEffect(
                        Schema.toCodecJson(Prompt.AssistantMessage),
                      )(message)
                      const finish = response.parts.findLast((part) => part.type === 'finish')
                      const descriptor = yield* catalog.resolve(pinned.request.model).pipe(
                        Effect.mapError(
                          (error) =>
                            new ExecutionError({
                              reason: new ('reason' in error && error.reason._tag === 'ModelNoModel'
                                ? NoModel
                                : ModelError)({ message: error.message, cause: error }),
                            }),
                        ),
                      )
                      const usage =
                        finish === undefined
                          ? undefined
                          : (descriptor.usage?.(finish.usage, finish.metadata) ??
                            Totals.fromResponse(finish.usage))
                      yield* session.transaction(
                        Effect.fnUntraced(function* (tx) {
                          const task = yield* tx.task(payload.taskId)
                          if (task?.abortRequested || task?.state.status === 'terminal') return
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
                    const progress = yield* Progress.make(
                      write,
                      pinned.settings.progress.partialIntervalMs,
                    ).pipe(Effect.mapError(codecError))
                    const source =
                      handle === undefined
                        ? executor.generate(pinned.request, agent)
                        : executor.fetchDeferred(pinned.request, handle)
                    const streamed = yield* Effect.result(
                      source.pipe(
                        Stream.runForEach(
                          Effect.fnUntraced(function* (part) {
                            yield* Ref.update(responseState, (response) =>
                              Response.append(response, part),
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
                      const text =
                        streamed._tag === 'Failure'
                          ? Model.errorText(streamed.failure)
                          : 'Stream ended before a terminal response event'
                      const finish = response.parts.findLast((part) => part.type === 'finish')
                      const partial = Response.partial(response)
                      const descriptor = yield* catalog.resolve(pinned.request.model).pipe(
                        Effect.mapError(
                          (error) =>
                            new ExecutionError({
                              reason: new ('reason' in error && error.reason._tag === 'ModelNoModel'
                                ? NoModel
                                : ModelError)({ message: error.message, cause: error }),
                            }),
                        ),
                      )
                      return {
                        parts,
                        disposition: {
                          type: 'failure' as const,
                          prompt:
                            partial === undefined
                              ? Response.message(response)
                              : Prompt.fromMessages([partial]),
                          usage:
                            finish === undefined
                              ? Totals.fromResponse({
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
                              : (descriptor.usage?.(finish.usage, finish.metadata) ??
                                Totals.fromResponse(finish.usage)),
                          message: text,
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
                              reason: new ('reason' in error && error.reason._tag === 'ModelNoModel'
                                ? NoModel
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
            if (disposition.type !== 'deferred') break
            const previousAt = poll?.at
            poll = yield* Activity.make({
              name: `deferred/${cycle}/${fetch}`,
              success: Schema.Struct({ handle: Schema.Json, at: Time.EpochMillis }),
              error: ExecutionErrorCodec,
              execute: Effect.gen(function* () {
                const at = Model.pollAt(
                  yield* DateTime.now,
                  previousAt,
                  disposition.type === 'deferred' ? disposition.decision.pollAfterMs : undefined,
                )
                const handle = disposition.type === 'deferred' ? disposition.decision.handle : null
                yield* session
                  .transaction(
                    Effect.fnUntraced(function* (tx) {
                      const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                      const request = yield* tx.doc(RequestDoc, { owner: payload.taskId })
                      request.handle = yield* Document.copyEffect(handle)
                      live.generation = {
                        attempt,
                        model: yield* Document.copyEffect(pinned.request.model),
                        deferred: { pollAt: DateTime.toEpochMillis(at) },
                      }
                    }),
                  )
                  .pipe(Effect.mapError(domainError))
                return { at, handle }
              }),
            }).annotate(ClusterSchema.WithTransaction, true)
          }
          if (disposition.type === 'failure') {
            const failure = disposition
            const failed = yield* Activity.make({
              name: `failure-attempt/${cycle}`,
              success: Schema.Struct({
                at: Time.EpochMillis,
                retry: Schema.Boolean,
                compaction: Schema.optionalKey(Compaction.payloadSchema),
              }),
              error: ExecutionErrorCodec,
              execute: Effect.gen(function* () {
                let shouldCompact = false
                if (
                  failure.overflow &&
                  !(yield* Ref.get(compacted)) &&
                  config.settings.compaction.enabled
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
                          reason: new NoModel({ message: error.message, cause: error }),
                        }),
                    ),
                  )
                  shouldCompact =
                    CompactionDomain.selectCut(
                      view,
                      config.settings.compaction.keepRecentTokens,
                      descriptor.estimate,
                    ) !== undefined
                }
                return yield* session
                  .transaction(
                    Effect.fnUntraced(function* (tx) {
                      const task = yield* tx.task(payload.taskId)
                      if (task === undefined || task.abortRequested)
                        return yield* new ExecutionError({
                          reason: new Aborted({ message: 'Generation aborted' }),
                        })
                      const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                      const compaction = shouldCompact
                        ? yield* CompactionExecutor.create(
                            tx,
                            payload.sessionId,
                            payload.conversationId,
                            'overflow',
                            payload.taskId,
                          )
                        : undefined
                      yield* appendAssistant(tx, payload, pinned.request, failure)
                      const retry =
                        !failure.overflow &&
                        Agent.shouldRetry(config.settings.retry, attempt, failure.retryable)
                      const at = DateTime.addDuration(
                        yield* DateTime.now,
                        Agent.retryDelay(config.settings.retry, attempt),
                      )
                      if (compaction !== undefined) {
                        delete live.generation
                        yield* tx.write({
                          type: 'task',
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
                    Effect.flatMap(
                      Schema.decodeEffect(
                        Schema.Struct({
                          at: Time.EpochMillis,
                          retry: Schema.Boolean,
                          compaction: Schema.optionalKey(Compaction.payloadSchema),
                        }),
                      ),
                    ),
                    Effect.mapError((cause) =>
                      cause instanceof ExecutionError ? cause : codecError(cause),
                    ),
                  )
              }),
            }).annotate(ClusterSchema.WithTransaction, true)
            if (failed.compaction !== undefined) {
              yield* Ref.set(compacted, true)
              const summary = yield* Effect.result(Compaction.execute(failed.compaction))
              if (summary._tag === 'Failure' || summary.success.entryId === undefined)
                return yield* fail('model_error', failure.message)
              continue
            }
            if (!failed.retry) return yield* fail('model_error', failure.message)
            return yield* new ModelRetry({ name: `retry/${cycle}`, at: failed.at })
          }
          if (disposition.type === 'answer') {
            const answer = disposition
            const continuation = yield* Activity.make({
              name: 'on-yield',
              success: Schema.UndefinedOr(Prompt.UserMessage),
              error: ExecutionErrorCodec,
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
              error: ExecutionErrorCodec,
              execute: session
                .transaction(
                  Effect.fnUntraced(function* (tx) {
                    const graph = yield* Ownership.readGraph(tx)
                    const task = yield* tx.task(payload.taskId)
                    if (task === undefined || task.abortRequested)
                      return yield* new ExecutionError({
                        reason: new Aborted({ message: 'Generation aborted' }),
                      })
                    const boundary = yield* Inbox.prepare(
                      tx,
                      payload.conversationId,
                      config.settings,
                    )
                    const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                    const entry = yield* appendAssistant(
                      tx,
                      payload,
                      pinned.request,
                      answer,
                      parts.findLast((part) => part.type === 'finish')?.reason,
                    )
                    const selected = yield* Inbox.apply(tx, boundary, 'final', yield* DateTime.now)
                    let next: typeof Generation.payloadSchema.Type | undefined
                    let notify = [...selected.settled]
                    if (
                      continuation !== undefined &&
                      selected.users.length === 0 &&
                      !selected.reset
                    ) {
                      const message = yield* Schema.encodeEffect(
                        Schema.toCodecJson(Prompt.UserMessage),
                      )(continuation).pipe(Effect.mapError(codecError))
                      yield* tx.appendEntry(payload.conversationId, {
                        kind: 'harness.user',
                        model: [message],
                      })
                      next = yield* SubmissionExecutor.createGeneration(
                        tx,
                        payload.sessionId,
                        payload.conversationId,
                        live.run?.inputs ?? [],
                        payload.runId,
                      )
                    } else {
                      notify.push(
                        ...(yield* Inbox.endRun(tx, live, payload.taskId, {
                          status: 'done',
                          answer: entry.id,
                        })),
                      )
                      if (selected.users.length > 0)
                        next = yield* SubmissionExecutor.createGeneration(
                          tx,
                          payload.sessionId,
                          payload.conversationId,
                          selected.users,
                        )
                    }
                    const result: typeof Result.Type = { status: 'answered', answer: entry.id }
                    yield* Structured.hold(tx, task, result, graph)
                    return { result, notify, ...(next === undefined ? {} : { next }) }
                  }),
                  { key: `workflow/generation/answer/${executionId}` },
                )
                .pipe(Effect.mapError(domainError)),
            }).annotate(ClusterSchema.WithTransaction, true)
            return yield* finish(settlement)
          }
          const tools = disposition
          const round = yield* Activity.make({
            name: 'tool-round',
            success: Round,
            error: ExecutionErrorCodec,
            execute: session
              .transaction(
                Effect.fnUntraced(function* (tx) {
                  const task = yield* tx.task(payload.taskId)
                  if (task === undefined || task.abortRequested)
                    return yield* new ExecutionError({
                      reason: new Aborted({ message: 'Generation aborted' }),
                    })
                  const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                  const assistant = yield* appendAssistant(tx, payload, pinned.request, tools)
                  const offered = new Set(pinned.request.tools.map((tool) => tool.name))
                  const calls: Array<typeof RoundCall.Type> = []
                  live.tools = []
                  for (const call of tools.calls) {
                    const args = yield* Schema.decodeUnknownEffect(Schema.Json)(call.params).pipe(
                      Effect.mapError(codecError),
                    )
                    const unavailable: Tool.Execution | undefined = offered.has(call.name)
                      ? undefined
                      : { outcome: 'unavailable', result: Tool.unavailable(call.name) }
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
                              Schema.toCodecJson(Tool.Execution),
                            )(unavailable).pipe(Effect.mapError(codecError)),
                          }),
                    })
                  }
                  delete live.generation
                  yield* tx.write({
                    type: 'task',
                    value: { ...task, state: { status: 'running' } },
                  })
                  return {
                    assistant: assistant.id,
                    calls,
                    sequential:
                      Tool.executionMode(
                        agent.tools.filter((tool) =>
                          calls.some((call) => call.name === tool.tool.name),
                        ),
                        config.settings.toolExecution,
                      ) === 'sequential',
                  }
                }),
                { key: `workflow/generation/round/${executionId}` },
              )
              .pipe(Effect.mapError(domainError)),
          }).annotate(ClusterSchema.WithTransaction, true)
          const executions = yield* Effect.forEach(
            round.calls,
            Effect.fnUntraced(function* (call, index) {
              if (call.unavailable !== undefined)
                return yield* Schema.decodeEffect(Schema.toCodecJson(Tool.Execution))(
                  call.unavailable,
                ).pipe(Effect.mapError(codecError))
              const child = yield* Activity.make({
                name: `start-tool/${index}`,
                success: ToolCall.payloadSchema,
                error: ExecutionErrorCodec,
                execute: session
                  .transaction(
                    Effect.fnUntraced(function* (tx) {
                      const owner = yield* tx.task(payload.taskId)
                      if (owner === undefined || owner.abortRequested)
                        return yield* new ExecutionError({
                          reason: new Aborted({ message: 'Generation aborted' }),
                        })
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
                        type: 'task',
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
                      const slot = live.tools?.find((slot) => slot.callId === call.id)
                      if (slot !== undefined) slot.taskId = taskId
                      yield* tx.write({
                        type: 'task',
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
              }).annotate(ClusterSchema.WithTransaction, true)
              yield* ToolCall.execute(child)
              const task = yield* session.task(child.taskId).pipe(Effect.mapError(domainError))
              return (yield* Schema.decodeEffect(Schema.toCodecJson(ToolExecutor.Outcome))(
                task?.state.outcome ?? null,
              ).pipe(Effect.mapError(codecError))).execution
            }),
            { concurrency: round.sequential ? 1 : config.toolConcurrency },
          )
          yield* Activity.make({
            name: 'after-tools',
            error: ExecutionErrorCodec,
            execute: Cancellation.activity(
              payload,
              session,
              Effect.gen(function* () {
                const live = yield* session
                  .snapshot(Inbox.LiveDoc, { owner: payload.conversationId })
                  .pipe(Effect.mapError(domainError))
                const results: Hook.SettledTool[] = []
                for (const [index, call] of round.calls.entries()) {
                  const entryId = live?.value.tools?.find((slot) => slot.callId === call.id)?.entry
                  const execution = executions[index]
                  if (entryId === undefined || execution === undefined) return yield* codecError()
                  results.push({ id: call.id, name: call.name, entryId, ...execution })
                }
                yield* Hook.afterTools(Registry.handlers(agent, 'generation'), results)
              }),
            ).pipe(
              Effect.mapError(domainError),
              Effect.provideService(Invocation.Invocation, invocation),
            ),
          })
          const controls = Tool.controls(executions)
          const settlement = yield* Activity.make({
            name: 'after-round',
            success: Settlement,
            error: ExecutionErrorCodec,
            execute: session
              .transaction(
                Effect.fnUntraced(function* (tx) {
                  const graph = yield* Ownership.readGraph(tx)
                  const task = yield* tx.task(payload.taskId)
                  if (task === undefined || task.abortRequested)
                    return yield* new ExecutionError({
                      reason: new Aborted({ message: 'Generation aborted' }),
                    })
                  const boundary = yield* Inbox.prepare(tx, payload.conversationId, config.settings)
                  const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                  if (controls.addTools.length > 0) {
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
                    if (selected.users.length > 0)
                      next = yield* SubmissionExecutor.createGeneration(
                        tx,
                        payload.sessionId,
                        payload.conversationId,
                        selected.users,
                      )
                  } else
                    next = yield* SubmissionExecutor.createGeneration(
                      tx,
                      payload.sessionId,
                      payload.conversationId,
                      [...(live.run?.inputs ?? payload.inputs), ...selected.users],
                      payload.runId,
                    )
                  const result: typeof Result.Type = { status: 'tools', answer: round.assistant }
                  yield* Structured.hold(tx, task, result, graph)
                  return { result, notify, ...(next === undefined ? {} : { next }) }
                }),
                { key: `workflow/generation/after-round/${executionId}` },
              )
              .pipe(Effect.mapError(domainError)),
          }).annotate(ClusterSchema.WithTransaction, true)
          return yield* finish(settlement)
        }
      })
      return yield* attemptModel().pipe(
        Effect.retry(retryPolicy),
        Effect.catchTag('ModelRetry', (cause) => Effect.fail(codecError(cause))),
      )
    })
    return yield* Cancellation.run(payload, session, run).pipe(
      Effect.mapError(domainError),
      Effect.catch((error) => {
        let reason = 'model_error'
        if (error.reason._tag === 'NoModel') reason = 'no_model'
        else if (error.reason._tag === 'Aborted') reason = 'aborted'
        return fail(reason, error.message)
      }),
    )
  }),
)
