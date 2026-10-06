import type * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import type * as Layer from 'effect/Layer'
import * as Agent from '@effect-harness/harness/Agent'
import * as Harness from '@effect-harness/harness/Executor'
import * as Invocation from '@effect-harness/harness/Invocation'
import * as Totals from '@effect-harness/harness/Usage'
import * as Clock from 'effect/Clock'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
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
import { Compaction, Result } from './Compaction.ts'
import * as Cancellation from './Cancellation.ts'
import * as Structured from './Structured.ts'
import { ExecutionError } from './ExecutionError.ts'
import { Submission, EntryDraft } from './Submission.ts'
import * as SubmissionExecutor from './SubmissionExecutor.ts'

const Prepared = Schema.Union([
  Schema.Struct({ type: Schema.Literal('none') }),
  Schema.Struct({
    type: Schema.Literal('summary'),
    firstKept: Record.EntryId,
    summary: Schema.String,
  }),
  Schema.Struct({ type: Schema.Literal('request'), request: Harness.SummaryRequest }),
])
const SummarySubmission = Schema.Struct({
  ...Submission.payloadSchema.fields,
  submission: Schema.Struct({ type: Schema.Literal('write'), entry: EntryDraft }),
})
const Settlement = Schema.Struct({
  result: Result,
  notify: Schema.Array(Record.SubmissionId),
  submission: Schema.optionalKey(SummarySubmission),
})
const Attempt = Schema.Union([
  Schema.Struct({ type: Schema.Literal('summary'), summary: Harness.Summary }),
  Schema.Struct({
    type: Schema.Literal('failure'),
    message: Schema.String,
    retryable: Schema.Boolean,
    usage: Schema.optionalKey(Totals.Usage),
  }),
])
const domainError = (error: import('../StorageError.ts').StorageError | ExecutionError) =>
  error._tag === 'StorageError' ? SubmissionExecutor.storageError(error) : error
const invalid = () =>
  new ExecutionError({ reason: 'invalid_state', message: 'Invalid compaction state' })

/** Creates the domain projection and returns an ordinary Compaction Workflow payload. */
export const create = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  sessionId: string,
  conversationId: Record.ConversationId,
  reason: (typeof Compaction.payloadSchema.Type)['reason'],
  owner?: Record.TaskId,
  instructions?: string,
) {
  if (owner !== undefined) {
    const task = yield* tx.task(owner)
    if (task === undefined || task.abortRequested || task.state.status === 'terminal')
      return yield* new ExecutionError({ reason: 'aborted', message: 'Compaction owner has ended' })
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

/** Pinned summary requests and absolute retry deadlines are native cached Activities. */
export const layer: Layer.Layer<
  never,
  never,
  | Cancellation.Cancellation
  | Conversation.Configuration
  | Ownership.Declarations
  | Harness.Executor
  | SessionDirectory
  | WorkflowEngine.WorkflowEngine
> = Compaction.toLayer(
  Effect.fnUntraced(function* (payload, executionId) {
    const session = yield* (yield* SessionDirectory)
      .resolve(payload.sessionId)
      .pipe(Effect.mapError(SubmissionExecutor.storageError))
    const executor = yield* Harness.Executor
    const config = yield* Conversation.Configuration
    const invocation = Invocation.Invocation.of({
      cwd: config.cwd,
      report: config.report,
      progress: () => Effect.void,
    })
    const active = Effect.gen(function* () {
      const task = yield* session
        .task(payload.taskId)
        .pipe(Effect.mapError(SubmissionExecutor.storageError))
      if (task === undefined) return yield* invalid()
      if (task.abortRequested || task.state.status === 'terminal')
        return yield* new ExecutionError({ reason: 'aborted', message: 'Compaction has ended' })
      return task
    })
    const removeStatus = (live: Document.Draft<Inbox.LiveState>) => {
      if (live.compactions !== undefined)
        live.compactions = live.compactions.filter((status) => status.taskId !== payload.taskId)
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
              const task = yield* tx.task(payload.taskId)
              if (task === undefined) return yield* invalid()
              if (task.abortRequested)
                return yield* new ExecutionError({
                  reason: 'aborted',
                  message: 'Compaction aborted',
                })
              const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
              let result: typeof Result.Type = {}
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
                    requestId: `compaction:${payload.taskId}`,
                    submission: { type: 'write', entry },
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
      }).annotate(ClusterSchema.WithTransaction, true)
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
            const state =
              (yield* session
                .snapshot(Conversation.AgentDoc, { owner: payload.conversationId })
                .pipe(Effect.mapError(SubmissionExecutor.storageError)))?.value ?? {}
            let provider = yield* session
              .snapshot(Conversation.ProviderDoc, { owner: payload.conversationId })
              .pipe(Effect.mapError(SubmissionExecutor.storageError))
            if (provider === undefined || provider.value.sessionId === '') {
              yield* session
                .initialize(payload.conversationId)
                .pipe(Effect.mapError(SubmissionExecutor.storageError))
              provider = yield* session
                .snapshot(Conversation.ProviderDoc, { owner: payload.conversationId })
                .pipe(Effect.mapError(SubmissionExecutor.storageError))
            }
            if (provider === undefined || provider.value.sessionId === '')
              return yield* new ExecutionError({
                reason: 'invalid_state',
                message: 'Provider identity requires Conversation.layerCreation',
              })
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
                      reason:
                        'reason' in error && error.reason === 'no_model'
                          ? 'no_model'
                          : 'model_error',
                      message: error.message,
                    }),
                ),
              )
            return yield* Schema.decodeEffect(Prepared)(value).pipe(Effect.mapError(invalid))
          }),
        ).pipe(Effect.mapError(domainError)),
      })
      if (prepared.type === 'none') return yield* complete()
      if (prepared.type === 'summary')
        return yield* complete({ firstKept: prepared.firstKept, text: prepared.summary })
      const pinned = prepared.request
      for (let attempt = 1; ; attempt++) {
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
                    const task = yield* tx.task(payload.taskId)
                    if (task === undefined || task.abortRequested)
                      return yield* new ExecutionError({
                        reason: 'aborted',
                        message: 'Compaction aborted',
                      })
                    const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                    const status = live.compactions?.find(
                      (status) => status.taskId === payload.taskId,
                    )
                    if (status !== undefined) {
                      status.attempt = attempt
                      delete status.retry
                    }
                    yield* tx.write({
                      type: 'task',
                      value: { ...task, state: { status: 'running' } },
                    })
                  }),
                )
                .pipe(Effect.mapError(domainError))
              const result = yield* Effect.result(executor.compact({ ...pinned, attempt }))
              if (result._tag === 'Success')
                return { type: 'summary' as const, summary: result.success }
              const error = result.failure
              return {
                type: 'failure' as const,
                message: error.message,
                retryable: (yield* executor.classifyFailure(pinned.request, error).pipe(
                  Effect.mapError(
                    (failure) =>
                      new ExecutionError({
                        reason: failure.reason === 'no_model' ? 'no_model' : 'model_error',
                        message: failure.message,
                      }),
                  ),
                )).retryable,
                ...('usage' in error && error.usage !== undefined ? { usage: error.usage } : {}),
              }
            }),
          ).pipe(Effect.mapError(domainError)),
        })
        const decision = yield* Activity.make({
          name: `usage/${attempt}`,
          success: Schema.Struct({ at: Schema.Finite, retry: Schema.Boolean }),
          error: ExecutionError,
          execute: session
            .transaction(
              Effect.fnUntraced(function* (tx) {
                const task = yield* tx.task(payload.taskId)
                if (task === undefined || task.abortRequested)
                  return yield* new ExecutionError({
                    reason: 'aborted',
                    message: 'Compaction aborted',
                  })
                const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                const usage = response.type === 'summary' ? response.summary.usage : response.usage
                if (usage !== undefined)
                  yield* Usage.record(
                    tx,
                    payload.conversationId,
                    'models',
                    `${pinned.request.model.provider}/${pinned.request.model.modelId}`,
                    usage,
                  )
                const retry =
                  response.type === 'failure' &&
                  Agent.shouldRetry(config.settings.retry, attempt, response.retryable)
                const at =
                  (yield* Clock.currentTimeMillis) +
                  Agent.retryDelay(config.settings.retry, attempt)
                const status = live.compactions?.find((status) => status.taskId === payload.taskId)
                if (status !== undefined && retry && response.type === 'failure')
                  status.retry = { at, error: response.message }
                return { at, retry }
              }),
              { key: `workflow/compaction/usage/${executionId}/${attempt}` },
            )
            .pipe(Effect.mapError(domainError)),
        }).annotate(ClusterSchema.WithTransaction, true)
        if (response.type === 'summary')
          return yield* complete({
            firstKept: yield* Schema.decodeEffect(Record.EntryId)(pinned.firstKept).pipe(
              Effect.mapError(invalid),
            ),
            text: response.summary.summary,
          })
        if (!decision.retry)
          return yield* new ExecutionError({ reason: 'model_error', message: response.message })
        yield* DurableClock.sleep({
          name: `retry/${attempt}`,
          duration: Math.max(0, decision.at - (yield* Clock.currentTimeMillis)),
          inMemoryThreshold: 0,
        })
      }
    })
    return yield* Cancellation.run(payload, session, run).pipe(
      Effect.mapError(domainError),
      Effect.tapError((error) =>
        Activity.make({
          name: 'failed',
          error: ExecutionError,
          execute: session
            .transaction(
              Effect.fnUntraced(function* (tx) {
                const graph = yield* Ownership.readGraph(tx)
                const task = yield* tx.task(payload.taskId)
                if (
                  task === undefined ||
                  task.state.status === 'terminal' ||
                  task.state.status === 'completing'
                )
                  return
                removeStatus(yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId }))
                yield* Structured.hold(
                  tx,
                  task,
                  {
                    status: error.reason === 'aborted' ? 'aborted' : 'failed',
                    message: error.message,
                    reason: error.reason,
                  },
                  graph,
                )
              }),
              { key: `workflow/compaction/failure/${executionId}` },
            )
            .pipe(Effect.mapError(domainError)),
        })
          .annotate(ClusterSchema.WithTransaction, true)
          .pipe(
            Effect.andThen(Structured.drain(session, payload.taskId, payload.sessionId)),
            Effect.mapError(domainError),
          ),
      ),
    )
  }),
)
