import * as Predicate from 'effect/Predicate'
/**
 * Native compaction execution, retries and atomic summary settlement.
 */
import { rejected } from '../StorageError.ts'
import * as Result from 'effect/Result'
import * as Array from 'effect/Array'
import type { StorageError } from '../StorageError.ts'
import * as Option from 'effect/Option'
import * as Time from 'effect-harness/Time'
import * as Schedule from 'effect/Schedule'
import { ModelRetryError, policy as retryPolicy } from './ModelRetry.ts'
import * as Identity from '../Identity.ts'
import type * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import type * as Layer from 'effect/Layer'
import * as Agent from 'effect-harness/Agent'
import * as Executor from 'effect-harness/Executor'
import * as Invocation from 'effect-harness/Invocation'
import * as Usage from 'effect-harness/Usage'
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
import { Compaction, Result as WorkflowResult } from './Compaction.ts'
import * as Cancellation from './Cancellation.ts'
import * as Structured from './Structured.ts'
import {
  ExecutionError,
  InvalidStateError,
  AbortedError,
  NoModelError,
  ModelError,
} from './ExecutionError.ts'
import { Submission, EntryDraft } from './Submission.ts'
import * as SubmissionExecutor from './SubmissionExecutor.ts'

const Prepared = Schema.Union([
  Schema.TaggedStruct('none', {}),
  Schema.TaggedStruct('summary', {
    firstKept: Record.EntryId,
    summary: Schema.String,
  }),
  Schema.TaggedStruct('request', { request: Executor.SummaryRequest }),
])
const SummarySubmission = Schema.Struct({
  ...Submission.payloadSchema.fields,
  submission: Schema.TaggedStruct('write', { entry: EntryDraft }),
})
const Settlement = Schema.Struct({
  result: WorkflowResult,
  notify: Schema.Array(Record.SubmissionId),
  submission: Schema.optionalKey(SummarySubmission),
})
const CompactionDecision = Schema.Struct({
  at: Time.DateTimeUtcFromEpochMillis,
  retry: Schema.Boolean,
})
const Attempt = Schema.Union([
  Schema.TaggedStruct('summary', { summary: Executor.Summary }),
  Schema.TaggedStruct('failure', {
    message: Schema.String,
    retryable: Schema.Boolean,
    usage: Schema.optionalKey(Usage.Usage),
  }),
])
const domainError = (error: import('../StorageError.ts').StorageError | ExecutionError) =>
  error._tag === 'StorageError' ? SubmissionExecutor.storageError(error) : error
const invalid = (cause?: unknown) =>
  new ExecutionError({
    reason: new InvalidStateError({
      message: 'Invalid compaction state',
      ...(cause === undefined ? {} : { cause }),
    }),
  })

/**
 * Creates the domain projection and returns an ordinary Compaction Workflow payload.
 *
 * @category constructors
 */
/** Compaction creation data; tx remains the sole resource subject.
 * @category models
 */
export interface Options {
  readonly sessionId: Identity.SessionId
  readonly conversationId: Record.ConversationId
  readonly reason: (typeof Compaction.payloadSchema.Type)['reason']
  readonly owner?: Record.TaskId | undefined
  readonly instructions?: string | undefined
}
export const make = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  options: Options,
): Effect.fn.Return<typeof Compaction.payloadSchema.Type, StorageError | ExecutionError> {
  const { sessionId, conversationId, reason, owner, instructions } = options
  if (owner !== undefined) {
    const taskOption = yield* tx.task(owner)
    if (
      Option.isNone(taskOption) ||
      taskOption.value.abortRequested ||
      taskOption.value.state.status === 'terminal'
    )
      return yield* new ExecutionError({
        reason: new AbortedError({ message: 'Compaction owner has ended' }),
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
          reason: new AbortedError({ message: 'Compaction has ended' }),
        })
      return task
    })
    const removeStatus = (live: Document.Document.Draft<Inbox.LiveState>) => {
      if (live.compactions !== undefined)
        live.compactions = Array.filter(
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
        error: ExecutionError,
        execute: session
          .transaction(
            Effect.fnUntraced(function* (tx) {
              const graph = yield* Ownership.readGraph(tx)
              const taskOption = yield* tx.task(payload.taskId)
              if (Option.isNone(taskOption)) return yield* invalid()
              const task = taskOption.value
              if (task.abortRequested)
                return yield* new ExecutionError({
                  reason: new AbortedError({ message: 'Compaction aborted' }),
                })
              const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
              let result: WorkflowResult = {}
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
                    submission: { _tag: 'write', entry },
                  }
                  const submissionExecutionId = yield* Submission.executionId(submission)
                  const existing = yield* SubmissionExecutor.existingAdmission(
                    tx,
                    submission,
                    submissionExecutionId,
                  )
                  const admitted = Option.isSome(existing)
                    ? existing.value
                    : yield* SubmissionExecutor.admitInTransaction(
                        tx,
                        yield* config.settings.pipe(
                          Effect.mapError((cause) =>
                            rejected('Invalid host settings', undefined, cause),
                          ),
                        ),
                        submission,
                        submissionExecutionId,
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
        error: ExecutionError,
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
                reason: new InvalidStateError({
                  message:
                    'Provider identity requires Conversation.layer(options) during Session construction',
                }),
              })
            const provider = providerOption.value
            const view = yield* Conversation.context(session, payload.conversationId).pipe(
              Effect.mapError(domainError),
            )
            const value = yield* executor
              .prepareCompaction({
                state,
                settings: yield* config.settings.pipe(
                  Effect.mapError(
                    (cause) =>
                      new ExecutionError({
                        reason: new InvalidStateError({ message: 'Invalid host settings', cause }),
                      }),
                  ),
                ),
                view,
                reason: payload.reason === 'background' ? 'threshold' : payload.reason,
                instructions: payload.instructions,
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
                        : ModelError)({
                        message: error.message,
                        cause: error,
                      }),
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
      switch (prepared._tag) {
        case 'none':
          return yield* complete()
        case 'summary':
          return yield* complete({ firstKept: prepared.firstKept, text: prepared.summary })
        case 'request':
          break
      }
      const pinned = prepared.request
      const attemptComputation = Effect.gen(function* () {
        const attempt = (yield* Schedule.CurrentMetadata).attempt + 1
        const response = yield* Activity.make({
          name: `summary/${attempt}`,
          success: Attempt,
          error: ExecutionError,
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
                        reason: new AbortedError({ message: 'Compaction aborted' }),
                      })
                    const task = taskOption.value
                    const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                    const status = Array.findFirst(
                      live.compactions ?? [],
                      (status) => status.taskId === payload.taskId,
                    )
                    if (Option.isSome(status)) {
                      status.value.attempt = attempt
                      delete status.value.retry
                    }
                    yield* tx.write({
                      _tag: 'task',
                      value: { ...task, state: { status: 'running' } },
                    })
                  }),
                )
                .pipe(Effect.mapError(domainError))
              const attemptResult = yield* Effect.result(executor.compact({ ...pinned, attempt }))
              return yield* Result.match(attemptResult, {
                onSuccess: (summary) => Effect.succeed({ _tag: 'summary' as const, summary }),
                onFailure: (error) =>
                  executor.classifyFailure(pinned.request, error).pipe(
                    Effect.mapError(
                      (failure) =>
                        new ExecutionError({
                          reason: new (failure.reason._tag === 'ModelNoModelError'
                            ? NoModelError
                            : ModelError)({
                            message: failure.message,
                            cause: failure,
                          }),
                        }),
                    ),
                    Effect.map((classification) => ({
                      _tag: 'failure' as const,
                      message: error.message,
                      retryable: classification.retryable,
                      ...(Predicate.hasProperty(error, 'usage') && error.usage !== undefined
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
          success: CompactionDecision,
          error: ExecutionError,
          execute: session
            .transaction(
              Effect.fnUntraced(function* (tx) {
                const taskOption = yield* tx.task(payload.taskId)
                if (Option.isNone(taskOption) || taskOption.value.abortRequested)
                  return yield* new ExecutionError({
                    reason: new AbortedError({ message: 'Compaction aborted' }),
                  })

                const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                const usage = response._tag === 'summary' ? response.summary.usage : response.usage
                if (usage !== undefined)
                  yield* recordUsage(
                    tx,
                    payload.conversationId,
                    'models',
                    `${pinned.request.model.provider}/${pinned.request.model.modelId}`,
                    usage,
                  )
                const retry =
                  response._tag === 'failure' &&
                  Agent.isRetryAllowed(
                    (yield* config.settings.pipe(
                      Effect.mapError(
                        (cause) =>
                          new ExecutionError({
                            reason: new InvalidStateError({
                              message: 'Invalid host settings',
                              cause,
                            }),
                          }),
                      ),
                    )).retry,
                    attempt,
                    response.retryable,
                  )
                const at = DateTime.addDuration(
                  yield* DateTime.now,
                  Agent.retryDelay(
                    (yield* config.settings.pipe(
                      Effect.mapError(
                        (cause) =>
                          new ExecutionError({
                            reason: new InvalidStateError({
                              message: 'Invalid host settings',
                              cause,
                            }),
                          }),
                      ),
                    )).retry,
                    attempt,
                  ),
                )
                const status = Array.findFirst(
                  live.compactions ?? [],
                  (status) => status.taskId === payload.taskId,
                )
                if (Option.isSome(status) && retry && response._tag === 'failure')
                  status.value.retry = { at: DateTime.toEpochMillis(at), error: response.message }
                return { at: DateTime.toEpochMillis(at), retry }
              }),
              { key: `workflow/compaction/usage/${executionId}/${attempt}` },
            )
            .pipe(
              Effect.mapError(domainError),
              Effect.flatMap(Schema.decodeEffect(CompactionDecision)),
              Effect.mapError((cause) =>
                cause instanceof ExecutionError ? cause : invalid(cause),
              ),
            ),
        })
        if (response._tag === 'summary')
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
        return yield* new ModelRetryError({ name: `retry/${attempt}`, at: decision.at })
      })
      const attemptModel: Effect.Effect<
        WorkflowResult,
        ExecutionError,
        Effect.Services<typeof attemptComputation>
      > = attemptComputation.pipe(
        Effect.retry(retryPolicy),
        Effect.catchTag('ModelRetryError', (cause) => Effect.fail(invalid(cause))),
      )
      return yield* attemptModel
    })
    // Settlement receipts restore native Activities before terminal-task fencing.
    const receipts = (yield* session.committed.pipe(Effect.mapError(domainError))).receipts
    const placement = Array.findFirst(
      receipts,
      (receipt) => receipt.key === `workflow/compaction/placement/${executionId}`,
    )
    if (Option.isSome(placement)) return yield* complete()
    const failed = Array.findFirst(
      receipts,
      (receipt) => receipt.key === `workflow/compaction/failure/${executionId}`,
    )
    if (Option.isSome(failed)) {
      const encoded = yield* Activity.make({
        name: 'failed',
        success: Schema.Json,
        error: ExecutionError,
        execute: Effect.succeed(failed.value.result),
      })
      const error = yield* Schema.decodeEffect(Schema.toCodecJson(ExecutionError))(encoded).pipe(
        Effect.mapError(invalid),
      )
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
          error: ExecutionError,
          execute: session
            .transaction(
              Effect.fnUntraced(function* (tx) {
                const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(ExecutionError))(
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
                    status: error.reason._tag === 'AbortedError' ? 'aborted' : 'failed',
                    message: error.message,
                    reason: error.reason._tag,
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
