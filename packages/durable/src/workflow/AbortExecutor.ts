/**
 * Native abort execution and domain cancellation settlement.
 */
import * as Arr from 'effect/Array'
import * as Option from 'effect/Option'
import type * as Layer from 'effect/Layer'
import * as Executor from '@effect-harness/harness/Executor'
import * as Invocation from '@effect-harness/harness/Invocation'
import * as Effect from 'effect/Effect'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Activity from 'effect/workflow/Activity'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Conversation from '../Conversation.ts'
import * as Inbox from '../Inbox.ts'
import * as Ownership from '../Ownership.ts'
import * as Record from '../Record.ts'
import { SessionDirectory } from '../SessionDirectory.ts'
import { Abort } from './Abort.ts'
import * as Cancellation from './Cancellation.ts'
import { ExecutionError, ExecutionErrorCodec, InvalidState } from './ExecutionError.ts'
import { convertPartial } from './GenerationExecutor.ts'
import { RequestDoc } from './Request.ts'
import * as SubmissionExecutor from './SubmissionExecutor.ts'
import { ToolCall } from './ToolCall.ts'
import * as ToolExecutor from './ToolExecutor.ts'

const Marked = Schema.Struct({
  tasks: Schema.Array(Record.Task),
  conversations: Schema.Array(Record.Conversation),
  notify: Schema.Array(Record.SubmissionId),
  deferred: Schema.Array(
    Schema.Struct({
      taskId: Record.TaskId,
      request: Schema.Json,
      handle: Schema.optionalKey(Schema.Json),
    }),
  ),
})
const domainError = (error: import('../StorageError.ts').StorageError | ExecutionError) =>
  error._tag === 'StorageError' ? SubmissionExecutor.storageError(error) : error

/**
 * Durable intent precedes cancellation; bottom-up reconciliation also handles suspended or absent code.
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
> = Abort.toLayer(
  Effect.fnUntraced(function* (payload, executionId) {
    const session = yield* (yield* SessionDirectory)
      .resolve(payload.sessionId)
      .pipe(Effect.mapError(SubmissionExecutor.storageError))
    const executor = yield* Executor.Executor
    const config = yield* Conversation.Configuration
    const declarations = yield* Ownership.Declarations
    const engine = yield* WorkflowEngine.WorkflowEngine
    const invocation = Invocation.Invocation.of({
      cwd: config.cwd,
      report: config.report,
      progress: () => Effect.void,
    })
    const marked = yield* Activity.make({
      name: 'mark',
      success: Marked,
      error: ExecutionErrorCodec,
      execute: session
        .transaction(
          Effect.fnUntraced(function* (tx) {
            const graph = yield* Ownership.readGraph(tx)
            const target: Ownership.Target =
              payload.target.type === 'conversation'
                ? { _tag: 'conversation', kind: 'conversation', id: payload.target.id }
                : { _tag: 'task', kind: 'task', id: payload.target.id }
            const reachedOption = Ownership.reach(graph, target, payload.background)
            if (Option.isNone(reachedOption))
              return yield* new ExecutionError({
                reason: new InvalidState({ message: 'Abort target is absent' }),
              })
            const reached = reachedOption.value
            const deferred: Array<(typeof Marked.Type.deferred)[number]> = []
            // Read all per-task handles under this same native SQL lease before writes.
            const requests = yield* Effect.forEach(
              reached.tasks,
              (task) => session.snapshot(RequestDoc, { owner: task.id }),
              { concurrency: 16 },
            )
            for (const [index, task] of reached.tasks.entries()) {
              const request = requests[index]
              if (
                request !== undefined &&
                Option.isSome(request) &&
                request.value.value.handle !== undefined
              )
                deferred.push({ taskId: task.id, ...request.value.value })
            }
            // Acquire every inbox draft before the first table write in a multi-conversation commit.
            for (const conversation of reached.conversations)
              yield* tx.doc(Inbox.InboxDoc, { owner: conversation.id })
            const notify: Array<Record.SubmissionId> = []
            for (const conversation of reached.conversations)
              notify.push(...(yield* Inbox.withdraw(tx, conversation.id)))
            for (const task of reached.tasks)
              if (!task.abortRequested)
                yield* tx.write({
                  _tag: 'task',
                  type: 'task',
                  value: { ...task, abortRequested: true },
                })
            return { ...reached, deferred, notify }
          }),
          { key: `workflow/abort/mark/${executionId}` },
        )
        .pipe(Effect.mapError(domainError)),
    })

    yield* SubmissionExecutor.notify(session, marked.notify)

    // Provider cancellation is recoverable native Activity work, never an unjournaled network finalizer.
    for (const request of marked.deferred) {
      yield* Activity.make({
        name: `provider-cancel/${request.taskId}`,
        execute: Effect.gen(function* () {
          const pinned = yield* Schema.decodeEffect(Schema.toCodecJson(Executor.Request))(
            request.request,
          )
          if (request.handle !== undefined) yield* executor.cancelDeferred(pinned, request.handle)
        }).pipe(
          Effect.provideService(Invocation.Invocation, invocation),
          Effect.catch((error) => config.report(error)),
        ),
      })
    }
    yield* Cancellation.cancel(payload.sessionId, marked)

    const notify: Array<Record.SubmissionId> = [...marked.notify]
    for (const previous of marked.tasks) {
      const settled = yield* Activity.make({
        name: `reconcile/${previous.id}`,
        success: Schema.Array(Record.SubmissionId),
        error: ExecutionErrorCodec,
        execute: session
          .transaction(
            Effect.fnUntraced(function* (tx) {
              const taskOption = yield* tx.task(previous.id)
              if (Option.isNone(taskOption) || taskOption.value.state.status === 'terminal')
                return []
              const task = taskOption.value
              const live = yield* tx.doc(Inbox.LiveDoc, { owner: task.conversationId })
              let outcome: Record.Json =
                task.state.status === 'completing'
                  ? (task.state.outcome ?? null)
                  : { status: 'aborted' }
              const ids: Array<Record.SubmissionId> = []
              if (task.kind === 'harness.tool' && task.state.status !== 'completing') {
                const binding = yield* Schema.decodeUnknownEffect(Ownership.Binding)(
                  task.input,
                ).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ExecutionError({
                        reason: new InvalidState({ message: 'Aborted tool has no binding', cause }),
                      }),
                  ),
                )
                const input = yield* Schema.decodeEffect(
                  Schema.toCodecJson(ToolCall.payloadSchema),
                )(binding.payload).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ExecutionError({
                        reason: new InvalidState({
                          message: 'Aborted tool input is invalid',
                          cause,
                        }),
                      }),
                  ),
                )
                const slot = Arr.findFirst(live.tools ?? [], (slot) => slot.callId === input.callId)
                const execution = {
                  outcome: 'interrupted' as const,
                  result: {
                    isError: true,
                    content: [
                      Prompt.textPart({
                        text:
                          Option.getOrUndefined(Option.map(slot, (found) => found.output)) ??
                          'Tool execution interrupted',
                      }),
                    ],
                    ...(Option.isNone(slot) || slot.value.details === undefined
                      ? {}
                      : { details: slot.value.details }),
                    ...(Option.isNone(slot) || slot.value.diagnostics === undefined
                      ? {}
                      : { diagnostics: slot.value.diagnostics }),
                  },
                }
                const entry = yield* ToolExecutor.appendResult(tx, input, execution)
                outcome = yield* Schema.encodeEffect(Schema.toCodecJson(ToolExecutor.Outcome))({
                  execution,
                  receipt: { status: 'aborted', entryId: entry.id },
                }).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ExecutionError({
                        reason: new InvalidState({ message: 'Aborted result is invalid', cause }),
                      }),
                  ),
                )
              } else if (task.kind === 'harness.generation') {
                yield* convertPartial(tx, live, task.conversationId)
                ids.push(
                  ...(yield* Inbox.endRun(tx, live, task.id, {
                    status: 'unanswered',
                    reason: 'aborted',
                    ...(payload.reason === undefined ? {} : { detail: payload.reason }),
                  })),
                )
              } else if (task.kind === 'harness.compaction' && live.compactions !== undefined)
                live.compactions = Arr.filter(
                  live.compactions,
                  (status) => status.taskId !== task.id,
                )
              yield* tx.write({
                _tag: 'task',
                type: 'task',
                value: { ...task, abortRequested: true, state: { status: 'terminal', outcome } },
              })
              return ids
            }),
            { key: `workflow/abort/reconcile/${executionId}/${previous.id}` },
          )
          .pipe(Effect.mapError(domainError)),
      })
      notify.push(...settled)
    }
    // Every admitted receipt is reconciled after restart even if another invocation already settled its task.
    const finalState = yield* session.committed.pipe(
      Effect.mapError(SubmissionExecutor.storageError),
    )
    const conversations = new Set(marked.conversations.map((conversation) => conversation.id))
    for (const task of marked.tasks) conversations.add(task.conversationId)
    notify.push(
      ...Arr.filter(
        finalState.submissions,
        (submission) =>
          conversations.has(submission.conversationId) &&
          (submission.status === 'done' || submission.status === 'unanswered'),
      ).map((submission) => submission.id),
    )
    yield* SubmissionExecutor.notify(session, Arr.dedupe(notify))
    for (const task of marked.tasks) {
      const binding = yield* Schema.decodeUnknownEffect(Ownership.Binding)(task.input).pipe(
        Effect.result,
      )
      if (binding._tag === 'Failure') continue
      const declaration = declarations.get(binding.success.workflow)
      if (Option.isSome(declaration))
        yield* engine.interrupt(declaration.value, binding.success.executionId)
    }
    if (payload.target.type === 'conversation')
      yield* Conversation.awaitIdle(session, payload.target.id).pipe(Effect.mapError(domainError))
    return { reached: marked.tasks.map((task) => task.id) }
  }),
)
