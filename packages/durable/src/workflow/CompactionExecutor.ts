/**
 * Native compaction execution, retries and atomic summary settlement.
 */
import * as result from 'effect/Result'
// effect-review-allow P9-namespace-alias-equals-module: the native Workflow Result schema is an imported binding in this module.
import * as Arr from 'effect/Array'
import { tagged } from '../internal/legacyTag.ts'
import type { StorageError } from '../StorageError.ts'
import * as Option from 'effect/Option'
import * as Time from '@effect-harness/harness/Time'
import * as Schedule from 'effect/Schedule'
import { ModelRetry, policy as retryPolicy } from './ModelRetry.ts'
import * as Identity from '../Identity.ts'
import type * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import type * as Layer from 'effect/Layer'
import * as Agent from '@effect-harness/harness/Agent'
import * as Executor from '@effect-harness/harness/Executor'
import * as Invocation from '@effect-harness/harness/Invocation'
import * as Usage from '@effect-harness/harness/Usage'
import * as DateTime from 'effect/DateTime'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Activity from 'effect/workflow/Activity'
import * as Conversation from '../Conversation.ts'
import type * as Document from '../Document.ts'
import * as Inbox from '../Inbox.ts'
import * as Ownership from '../Ownership.ts'
import * as Record from '../Record.ts'
import type * as Session from '../Session.ts'
import { SessionDirectory } from '../SessionDirectory.ts'
import { record as recordUsage } from '../Usage.ts'
import { Compaction, Result } from './Compaction.ts'
import * as Cancellation from './Cancellation.ts'
import * as Structured from './Structured.ts'
import {
  ExecutionError,
  InvalidState,
  Aborted,
  ExecutionErrorCodec,
  NoModel,
  ModelError,
} from './ExecutionError.ts'
import { Submission, EntryDraft } from './Submission.ts'
import * as SubmissionExecutor from './SubmissionExecutor.ts'

const Prepared = Schema.Union([
  tagged('none', { type: Schema.tag('none') }),
  tagged('summary', {
    type: Schema.tag('summary'),
    firstKept: Record.EntryId,
    summary: Schema.String,
  }),
  tagged('request', { type: Schema.tag('request'), request: Executor.SummaryRequest }),
])
const SummarySubmission = Schema.Struct({
  ...Submission.payloadSchema.fields,
  submission: tagged('write', { type: Schema.tag('write'), entry: EntryDraft }),
})
const Settlement = Schema.Struct({
  result: Result,
  notify: Schema.Array(Record.SubmissionId),
  submission: Schema.optionalKey(SummarySubmission),
})
const Attempt = Schema.Union([
  tagged('summary', { type: Schema.tag('summary'), summary: Executor.Summary }),
  tagged('failure', {
    type: Schema.tag('failure'),
    message: Schema.String,
    retryable: Schema.Boolean,
    usage: Schema.optionalKey(Usage.Usage),
  }),
])
const domainError = (error: import('../StorageError.ts').StorageError | ExecutionError) =>
  error._tag === 'StorageError' ? SubmissionExecutor.storageError(error) : error
const invalid = (cause?: unknown) =>
  new ExecutionError({
    reason: new InvalidState({
      message: 'Invalid compaction state',
      ...(cause === undefined ? {} : { cause }),
    }),
  })

/**
 * Creates the domain projection and returns an ordinary Compaction Workflow payload.
 *
 * @category constructors
 */
export const make = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  sessionId: Identity.SessionId,
  conversationId: Record.ConversationId,
  reason: (typeof Compaction.payloadSchema.Type)['reason'],
  owner?: Record.TaskId,
  instructions?: string,
): Effect.fn.Return<typeof Compaction.payloadSchema.Type, StorageError | ExecutionError> {
  if (owner !== undefined) {
    const taskOption = yield* tx.task(owner)
    if (
      Option.isNone(taskOption) ||
      taskOption.value.abortRequested ||
      taskOption.value.state.status === 'terminal'
    )
      return yield* new ExecutionError({
        reason: new Aborted({ message: 'Compaction owner has ended' }),
      })
  }
  const taskId = yield* tx.mint(Record.TaskId)
  const payload = {
    sessionId,
    conversationId,
    taskId,
    reason,
    blocking: owner !== undefined,
    ...(instructions === undefined ? {} : { instructions }),
  }
  const binding: Ownership.Binding = {
    workflow: Compaction._tag,
    executionId: yield* Compaction.executionId(payload),
    payload,
  }
  yield* tx.write({
    _tag: 'task',
    type: 'task',
    value: {
      id: taskId,
      conversationId,
      kind: 'harness.compaction',
      version: 1,
      input: binding,
      ...(owner === undefined ? {} : { owner }),
      background: owner === undefined && reason !== 'manual',
      abortRequested: false,
      state: { status: 'pending' },
    },
  })
  const live = yield* tx.doc(Inbox.LiveDoc, { owner: conversationId })
  live.compactions ??= []
  live.compactions.push({
    taskId,
    reason: reason === 'background' ? 'threshold' : reason,
    blocking: payload.blocking,
    attempt: 1,
  })
  return payload
})

/**
 * Pinned summary requests and absolute retry deadlines are native cached Activities.
 *
 * @category layers
 */
export const layer: Layer.Layer<
  never,
  never,
  | Cancellation.Cancellation
  | Conversation.Configuration
  | Ownership.Declarations
  | Executor.Executor
  | SessionDirectory
  | WorkflowEngine.WorkflowEngine
> = Compaction.toLayer(
  Effect.fnUntraced(function* (payload, executionId) {
    const session = yield* (yield* SessionDirectory)
      .resolve(payload.sessionId)
      .pipe(Effect.mapError(SubmissionExecutor.storageError))
    const executor = yield* Executor.Executor
    const config = yield* Conversation.Configuration
    const invocation = Invocation.Invocation.of({
      cwd: config.cwd,
      report: config.report,
      progress: () => Effect.void,
    })
    const active = Effect.gen(function* () {
      const taskOption = yield* session
        .task(payload.taskId)
        .pipe(Effect.mapError(SubmissionExecutor.storageError))
      if (Option.isNone(taskOption)) return yield* invalid()
      const task = taskOption.value
      if (task.abortRequested || task.state.status === 'terminal')
        return yield* new ExecutionError({
          reason: new Aborted({ message: 'Compaction has ended' }),
        })
      return task
    })
    const removeStatus = (live: Document.Draft<Inbox.LiveState>) => {
      if (live.compactions !== undefined)
        live.compactions = Arr.filter(
          live.compactions,
          (status) => status.taskId !== payload.taskId,
        )
    }
    const complete = Effect.fnUntraced(function* (summary?: {
      firstKept: Record.EntryId
      text: string
    }) {
      const settlement = yield* Activity.make({
        name: 'placement',
        success: Settlement,
        error: ExecutionErrorCodec,
        execute: session
          .transaction(
            Effect.fnUntraced(function* (tx) {
              const graph = yield* Ownership.readGraph(tx)
              const taskOption = yield* tx.task(payload.taskId)
              if (Option.isNone(taskOption)) return yield* invalid()
              const task = taskOption.value
              if (task.abortRequested)
                return yield* new ExecutionError({
                  reason: new Aborted({ message: 'Compaction aborted' }),
                })
              const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
              let result: Result = {}
              let notify: ReadonlyArray<Record.SubmissionId> = []
              let submission: typeof SummarySubmission.Type | undefined
              if (summary !== undefined) {
                const message = Prompt.userMessage({
                  content: [
                    Prompt.textPart({
                      text: `The conversation history before this point was compacted into the following summary:\n\n<summary>\n${summary.text}\n</summary>`,
                    }),
                  ],
                })
                const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.UserMessage))(
                  message,
                ).pipe(Effect.mapError(invalid))
                const entry = {
                  kind: 'harness.compaction',
                  head: summary.firstKept,
                  model: [encoded],
                  data: { reason: payload.reason === 'background' ? 'threshold' : payload.reason },
                }
                if (payload.blocking)
                  result = { entryId: (yield* tx.appendEntry(payload.conversationId, entry)).id }
                else {
                  submission = {
                    sessionId: payload.sessionId,
                    conversationId: payload.conversationId,
                    requestId: Identity.RequestId.make(`compaction:${payload.taskId}`),
                    submission: { _tag: 'write', type: 'write', entry },
                  }
                  const admitted = yield* SubmissionExecutor.admitInTransaction(
                    tx,
                    config,
                    submission,
                    yield* Submission.executionId(submission),
                  )
                  result = { submissionId: admitted.id }
                  notify = admitted.notify
                }
              }
              removeStatus(live)
              yield* Structured.hold(tx, task, { status: 'completed', result }, graph)
              return { result, notify, ...(submission === undefined ? {} : { submission }) }
            }),
            { key: `workflow/compaction/placement/${executionId}` },
          )
          .pipe(Effect.mapError(domainError)),
      })
      yield* Structured.drain(session, payload.taskId, payload.sessionId).pipe(
        Effect.mapError(domainError),
      )
      if (settlement.submission !== undefined)
        yield* Submission.execute(settlement.submission, { discard: true })
      yield* SubmissionExecutor.notify(session, settlement.notify)
      return settlement.result
    })
    const run = Effect.gen(function* () {
      const prepared = yield* Activity.make({
        name: 'selection',
        success: Prepared,
        error: ExecutionErrorCodec,
        execute: Cancellation.activity(
          payload,
          session,
          Effect.gen(function* () {
            yield* active
            const state: Agent.State = yield* session
              .snapshot(Conversation.AgentDoc, { owner: payload.conversationId })
              .pipe(
                Effect.mapError(SubmissionExecutor.storageError),
                Effect.map(
                  Option.match({ onNone: () => ({}), onSome: (snapshot) => snapshot.value }),
                ),
              )
            let providerOption = yield* session
              .snapshot(Conversation.ProviderDoc, { owner: payload.conversationId })
              .pipe(Effect.mapError(SubmissionExecutor.storageError))
            if (Option.isNone(providerOption) || providerOption.value.value.sessionId === '') {
              yield* session
                .initialize(payload.conversationId)
                .pipe(Effect.mapError(SubmissionExecutor.storageError))
              providerOption = yield* session
                .snapshot(Conversation.ProviderDoc, { owner: payload.conversationId })
                .pipe(Effect.mapError(SubmissionExecutor.storageError))
            }
            if (Option.isNone(providerOption) || providerOption.value.value.sessionId === '')
              return yield* new ExecutionError({
                reason: new InvalidState({
                  message: 'Provider identity requires Conversation.layerCreation',
                }),
              })
            const provider = providerOption.value
            const view = yield* Conversation.context(session, payload.conversationId).pipe(
              Effect.mapError(domainError),
            )
            const value = yield* executor
              .prepareCompaction({
                state,
                settings: config.settings,
                view,
                reason: payload.reason === 'background' ? 'threshold' : payload.reason,
                instructions: payload.instructions,
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
            return yield* Schema.decodeUnknownEffect(Schema.toType(Prepared))({
              ...value,
              type: value._tag,
            }).pipe(Effect.mapError(invalid))
          }),
        ).pipe(Effect.mapError(domainError)),
      })
      if (prepared.type === 'none') return yield* complete()
      if (prepared.type === 'summary')
        return yield* complete({ firstKept: prepared.firstKept, text: prepared.summary })
      const pinned = prepared.request
      const attemptModel = Effect.fnUntraced(function* () {
        const attempt = (yield* Schedule.CurrentMetadata).attempt + 1
        const response = yield* Activity.make({
          name: `summary/${attempt}`,
          success: Attempt,
          error: ExecutionErrorCodec,
          execute: Cancellation.activity(
            payload,
            session,
            Effect.gen(function* () {
              yield* active
              yield* session
                .transaction(
                  Effect.fnUntraced(function* (tx) {
                    const taskOption = yield* tx.task(payload.taskId)
                    if (Option.isNone(taskOption) || taskOption.value.abortRequested)
                      return yield* new ExecutionError({
                        reason: new Aborted({ message: 'Compaction aborted' }),
                      })
                    const task = taskOption.value
                    const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                    const status = Arr.findFirst(
                      live.compactions ?? [],
                      (status) => status.taskId === payload.taskId,
                    )
                    if (Option.isSome(status)) {
                      status.value.attempt = attempt
                      delete status.value.retry
                    }
                    yield* tx.write({
                      _tag: 'task',
                      type: 'task',
                      value: { ...task, state: { status: 'running' } },
                    })
                  }),
                )
                .pipe(Effect.mapError(domainError))
              const attemptResult = yield* Effect.result(executor.compact({ ...pinned, attempt }))
              return yield* result.match(attemptResult, {
                onSuccess: (summary) =>
                  Effect.succeed({ _tag: 'summary' as const, type: 'summary' as const, summary }),
                onFailure: (error) =>
                  executor.classifyFailure(pinned.request, error).pipe(
                    Effect.mapError(
                      (failure) =>
                        new ExecutionError({
                          reason: new (failure.reason._tag === 'ModelNoModel'
                            ? NoModel
                            : ModelError)({ message: failure.message, cause: failure }),
                        }),
                    ),
                    Effect.map((classification) => ({
                      _tag: 'failure' as const,
                      type: 'failure' as const,
                      message: error.message,
                      retryable: classification.retryable,
                      ...('usage' in error && error.usage !== undefined
                        ? { usage: error.usage }
                        : {}),
                    })),
                  ),
              })
            }),
          ).pipe(Effect.mapError(domainError)),
        })
        const decision = yield* Activity.make({
          name: `usage/${attempt}`,
          success: Schema.Struct({ at: Time.EpochMillis, retry: Schema.Boolean }),
          error: ExecutionErrorCodec,
          execute: session
            .transaction(
              Effect.fnUntraced(function* (tx) {
                const taskOption = yield* tx.task(payload.taskId)
                if (Option.isNone(taskOption) || taskOption.value.abortRequested)
                  return yield* new ExecutionError({
                    reason: new Aborted({ message: 'Compaction aborted' }),
                  })

                const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                const usage = response.type === 'summary' ? response.summary.usage : response.usage
                if (usage !== undefined)
                  yield* recordUsage(
                    tx,
                    payload.conversationId,
                    'models',
                    `${pinned.request.model.provider}/${pinned.request.model.modelId}`,
                    usage,
                  )
                const retry =
                  response.type === 'failure' &&
                  Agent.isRetryAllowed(config.settings.retry, attempt, response.retryable)
                const at = DateTime.addDuration(
                  yield* DateTime.now,
                  Agent.retryDelay(config.settings.retry, attempt),
                )
                const status = Arr.findFirst(
                  live.compactions ?? [],
                  (status) => status.taskId === payload.taskId,
                )
                if (Option.isSome(status) && retry && response.type === 'failure')
                  status.value.retry = { at: DateTime.toEpochMillis(at), error: response.message }
                return { at: DateTime.toEpochMillis(at), retry }
              }),
              { key: `workflow/compaction/usage/${executionId}/${attempt}` },
            )
            .pipe(
              Effect.mapError(domainError),
              Effect.flatMap(
                Schema.decodeEffect(Schema.Struct({ at: Time.EpochMillis, retry: Schema.Boolean })),
              ),
              Effect.mapError((cause) =>
                cause instanceof ExecutionError ? cause : invalid(cause),
              ),
            ),
        })
        if (response.type === 'summary')
          return yield* complete({
            firstKept: yield* Schema.decodeEffect(Record.EntryId)(pinned.firstKept).pipe(
              Effect.mapError(invalid),
            ),
            text: response.summary.summary,
          })
        if (!decision.retry)
          return yield* new ExecutionError({
            reason: new ModelError({ message: response.message, cause: response }),
          })
        return yield* new ModelRetry({ name: `retry/${attempt}`, at: decision.at })
      })
      return yield* attemptModel().pipe(
        Effect.retry(retryPolicy),
        Effect.catchTag('ModelRetry', (cause) => Effect.fail(invalid(cause))),
      )
    })
    // Settlement receipts restore native Activities before terminal-task fencing.
    const receipts = (yield* session.committed.pipe(Effect.mapError(domainError))).receipts
    const placement = Arr.findFirst(
      receipts,
      (receipt) => receipt.key === `workflow/compaction/placement/${executionId}`,
    )
    if (Option.isSome(placement)) return yield* complete()
    const failed = Arr.findFirst(
      receipts,
      (receipt) => receipt.key === `workflow/compaction/failure/${executionId}`,
    )
    if (Option.isSome(failed)) {
      const encoded = yield* Activity.make({
        name: 'failed',
        success: Schema.Json,
        error: ExecutionErrorCodec,
        execute: Effect.succeed(failed.value.result),
      })
      const error = yield* Schema.decodeEffect(Schema.toCodecJson(ExecutionErrorCodec))(
        encoded,
      ).pipe(Effect.mapError(invalid))
      yield* Structured.drain(session, payload.taskId, payload.sessionId).pipe(
        Effect.mapError(domainError),
      )
      return yield* error
    }
    return yield* Cancellation.run(payload, session, run).pipe(
      Effect.mapError(domainError),
      Effect.tapError((error) =>
        Activity.make({
          name: 'failed',
          success: Schema.Json,
          error: ExecutionErrorCodec,
          execute: session
            .transaction(
              Effect.fnUntraced(function* (tx) {
                const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(ExecutionErrorCodec))(
                  error,
                ).pipe(Effect.mapError(invalid))
                const graph = yield* Ownership.readGraph(tx)
                const taskOption = yield* tx.task(payload.taskId)
                if (
                  Option.isNone(taskOption) ||
                  taskOption.value.state.status === 'terminal' ||
                  taskOption.value.state.status === 'completing'
                )
                  return encoded
                const task = taskOption.value
                removeStatus(yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId }))
                yield* Structured.hold(
                  tx,
                  task,
                  {
                    status: error.reason._tag === 'Aborted' ? 'aborted' : 'failed',
                    message: error.message,
                    reason: error.code,
                  },
                  graph,
                )
                return encoded
              }),
              { key: `workflow/compaction/failure/${executionId}` },
            )
            .pipe(Effect.mapError(domainError)),
        }).pipe(
          Effect.andThen(Structured.drain(session, payload.taskId, payload.sessionId)),
          Effect.mapError(domainError),
        ),
      ),
    )
  }),
)
