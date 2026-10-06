import type * as Layer from 'effect/Layer'
import * as Harness from '@effect-harness/harness/Executor'
import * as Invocation from '@effect-harness/harness/Invocation'
import * as Tool from '@effect-harness/harness/Tool'
import * as ToolResult from '@effect-harness/harness/ToolResult'
import * as Clock from 'effect/Clock'
import * as Effect from 'effect/Effect'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Activity from 'effect/workflow/Activity'
import type * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Conversation from '../Conversation.ts'
import * as Document from '../Document.ts'
import * as Inbox from '../Inbox.ts'
import * as Ownership from '../Ownership.ts'
import * as Record from '../Record.ts'
import type * as Session from '../Session.ts'
import { SessionDirectory } from '../SessionDirectory.ts'
import * as Usage from '../Usage.ts'
import { ExecutionError } from './ExecutionError.ts'
import { storageError } from './SubmissionExecutor.ts'
import { ToolCall, Result } from './ToolCall.ts'
import * as Cancellation from './Cancellation.ts'
import * as Structured from './Structured.ts'

export const IntentDoc = Document.define({
  kind: 'harness.tool-intent',
  version: 1,
  scope: 'task',
  schema: Schema.Struct({ intent: Tool.Intent, started: Schema.Boolean }),
  initial: (seed) => ({ intent: Schema.decodeUnknownSync(Tool.Intent)(seed), started: false }),
})
export const Outcome = Schema.Struct({ execution: Tool.Execution, receipt: Result })
const Prepared = Schema.Union([
  Schema.Struct({ type: Schema.Literal('intent'), intent: Tool.Intent }),
  Schema.Struct({ type: Schema.Literal('rejected'), message: Schema.String }),
])
const invalid = (message: string) => new ExecutionError({ reason: 'invalid_state', message })
const codecError = () => invalid('Tool result cannot be persisted')

/** A native tool result entry also covers an unoffered call, which has no executable task. */
export const appendResult = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  payload: Pick<
    typeof ToolCall.payloadSchema.Type,
    'conversationId' | 'assistantId' | 'callId' | 'name'
  > & { readonly taskId?: Record.TaskId },
  execution: Tool.Execution,
) {
  const result = yield* ToolResult.encode(execution.result).pipe(Effect.mapError(codecError))
  const model = yield* Schema.encodeEffect(Schema.toCodecJson(Schema.Array(Prompt.Message)))([
    Prompt.toolMessage({
      content: [
        Prompt.toolResultPart({
          id: payload.callId,
          name: payload.name,
          result,
          isFailure: execution.result.isError === true,
          providerExecuted: false,
        }),
      ],
    }),
  ]).pipe(Effect.mapError(codecError))
  const encodedExecution = yield* Schema.encodeEffect(Schema.toCodecJson(Tool.Execution))(
    execution,
  ).pipe(Effect.mapError(codecError))
  const entry = yield* tx.appendEntry(payload.conversationId, {
    kind: 'harness.tool',
    model: yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Json))(model).pipe(
      Effect.mapError(codecError),
    ),
    ...(payload.taskId === undefined ? {} : { byTaskId: payload.taskId }),
    data: {
      timestamp: yield* Clock.currentTimeMillis,
      assistantId: payload.assistantId,
      callId: payload.callId,
      name: payload.name,
      execution: encodedExecution,
    },
  })
  const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
  const slot = live.tools?.find((slot) => slot.callId === payload.callId)
  if (slot !== undefined) {
    slot.status = 'done'
    slot.entry = entry.id
  }
  if (execution.result.usage !== undefined)
    yield* Usage.record(tx, payload.conversationId, 'tools', payload.name, execution.result.usage)
  return entry
})

/** Committed progress only: an acknowledgement cannot expose an uncommitted preview. */
const progress =
  (session: Session.Service, payload: typeof ToolCall.payloadSchema.Type) =>
  (value: Invocation.Progress): Effect.Effect<void> =>
    session
      .transaction(
        Effect.fnUntraced(function* (tx) {
          const task = yield* tx.task(payload.taskId)
          if (task === undefined || task.state.status === 'terminal' || task.abortRequested) return
          const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
          const slot = live.tools?.find((slot) => slot.callId === payload.callId)
          if (slot === undefined) return
          if (value.clear === true) {
            delete slot.output
            delete slot.details
            delete slot.diagnostics
            delete slot.droppedBytes
            delete slot.droppedLines
          }
          if (value.output !== undefined) slot.output = value.output
          if (value.details !== undefined) slot.details = Document.copy(value.details)
          if (value.diagnostics !== undefined)
            slot.diagnostics = [...(slot.diagnostics ?? []), ...Document.copy(value.diagnostics)]
          if (value.droppedBytes !== undefined) slot.droppedBytes = value.droppedBytes
          if (value.droppedLines !== undefined) slot.droppedLines = value.droppedLines
        }),
      )
      .pipe(Effect.asVoid, Effect.orDie)

/** Registers an ordinary native child Workflow. Its intent commits before any handler side effect. */
export const layer: Layer.Layer<
  never,
  never,
  | Cancellation.Cancellation
  | Conversation.Configuration
  | Ownership.Declarations
  | Harness.Executor
  | SessionDirectory
  | WorkflowEngine.WorkflowEngine
> = ToolCall.toLayer(
  Effect.fnUntraced(function* (payload, executionId) {
    const session = yield* (yield* SessionDirectory)
      .resolve(payload.sessionId)
      .pipe(Effect.mapError(storageError))
    const executor = yield* Harness.Executor
    const config = yield* Conversation.Configuration
    const settlementContext = yield* Effect.context<
      Cancellation.Cancellation | Ownership.Declarations | WorkflowEngine.WorkflowEngine
    >()
    const state =
      (yield* session
        .snapshot(Conversation.AgentDoc, { owner: payload.conversationId })
        .pipe(Effect.mapError(storageError)))?.value ?? {}
    const invocation = Invocation.Invocation.of({
      cwd: state.cwd ?? config.cwd,
      report: config.report,
      progress: progress(session, payload),
    })
    const agent = yield* executor
      .resolve(state, config.settings)
      .pipe(Effect.provideService(Invocation.Invocation, invocation))
    const prepared = yield* Activity.make({
      name: 'intent',
      success: Prepared,
      error: ExecutionError,
      execute: Cancellation.activity(
        payload,
        session,
        Effect.gen(function* () {
          const task = yield* session.task(payload.taskId).pipe(Effect.mapError(storageError))
          if (task?.abortRequested)
            return yield* new ExecutionError({
              reason: 'aborted',
              message: 'Tool aborted before intent',
            })
          const persisted = yield* session
            .snapshot(IntentDoc, { owner: payload.taskId })
            .pipe(Effect.mapError(storageError))
          if (persisted !== undefined)
            return { type: 'intent' as const, intent: persisted.value.intent }
          const intent = yield* Effect.result(
            executor
              .prepareTool(agent, {
                id: payload.callId,
                name: payload.name,
                args: payload.arguments,
              })
              .pipe(
                Effect.provideService(Invocation.Invocation, invocation),
                Effect.provide(Ownership.layerCurrent(payload, session)),
              ),
          )
          if (intent._tag === 'Failure')
            return { type: 'rejected' as const, message: intent.failure.message }
          yield* session
            .transaction((tx) => tx.doc(IntentDoc, { owner: payload.taskId, seed: intent.success }))
            .pipe(Effect.mapError(storageError))
          return { type: 'intent' as const, intent: intent.success }
        }),
      ).pipe(
        Effect.mapError((error) => (error._tag === 'StorageError' ? storageError(error) : error)),
      ),
    })

    const settle = Effect.fnUntraced(function* (execution: Tool.Execution) {
      return yield* session
        .transaction(
          Effect.fnUntraced(function* (tx) {
            const graph = yield* Ownership.readGraph(tx)
            const task = yield* tx.task(payload.taskId)
            if (task === undefined) return yield* invalid('Tool task projection is absent')
            if (task.state.status === 'terminal' || task.state.status === 'completing')
              return task.state.outcome ?? null
            const entry = yield* appendResult(tx, payload, execution)
            let status: (typeof Result.Type)['status'] =
              execution.outcome === 'completed' ? 'completed' : 'failed'
            if (task.abortRequested || execution.outcome === 'interrupted') status = 'aborted'
            const receipt: typeof Result.Type = {
              status,
              entryId: entry.id,
              ...(execution.result.control === undefined
                ? {}
                : { control: execution.result.control }),
            }
            const outcome = yield* Schema.encodeEffect(Schema.toCodecJson(Outcome))({
              execution,
              receipt,
            }).pipe(Effect.mapError(codecError))
            yield* Structured.hold(tx, task, outcome, graph)
            return outcome
          }),
          { key: `workflow/tool/settle/${executionId}` },
        )
        .pipe(
          Effect.andThen(Structured.drain(session, payload.taskId, payload.sessionId)),
          Effect.mapError((error) => (error._tag === 'StorageError' ? storageError(error) : error)),
        )
    })

    const outcome = yield* Activity.make({
      name: 'execute-and-settle',
      success: Outcome,
      error: ExecutionError,
      execute: Effect.gen(function* () {
        const task = yield* session.task(payload.taskId).pipe(Effect.mapError(storageError))
        if (task?.state.status === 'terminal' || task?.state.status === 'completing') {
          yield* Structured.drain(session, payload.taskId, payload.sessionId).pipe(
            Effect.mapError((error) =>
              error._tag === 'StorageError' ? storageError(error) : error,
            ),
          )
          return yield* Schema.decodeEffect(Schema.toCodecJson(Outcome))(
            task.state.outcome ?? null,
          ).pipe(Effect.mapError(codecError))
        }
        let committed: Record.Json | undefined
        const commit = (execution: Tool.Execution) =>
          settle(execution).pipe(
            Effect.provideContext(settlementContext),
            Effect.tap((value) =>
              Effect.sync(() => {
                committed = value
              }),
            ),
            Effect.asVoid,
            Effect.orDie,
          )
        if (prepared.type === 'rejected') {
          yield* commit({
            outcome: 'failed',
            result: { isError: true, content: [Prompt.textPart({ text: prepared.message })] },
          })
        } else {
          const recovering = yield* session
            .transaction(
              Effect.fnUntraced(function* (tx) {
                const task = yield* tx.task(payload.taskId)
                if (task === undefined) return yield* invalid('Tool task projection is absent')
                if (task.abortRequested)
                  return yield* new ExecutionError({
                    reason: 'aborted',
                    message: 'Tool aborted before execution',
                  })
                const doc = yield* tx.doc(IntentDoc, { owner: payload.taskId })
                const before = doc.started
                doc.started = true
                const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                const slot = live.tools?.find((slot) => slot.callId === payload.callId)
                if (slot !== undefined) slot.status = 'running'
                yield* tx.write({
                  type: 'task',
                  value: {
                    ...task,
                    state: { status: 'running', checkpoint: { arguments: doc.intent.args } },
                  },
                })
                return before
              }),
            )
            .pipe(
              Effect.mapError((error) =>
                error._tag === 'StorageError' ? storageError(error) : error,
              ),
            )
          const previous = (yield* session
            .snapshot(Inbox.LiveDoc, { owner: payload.conversationId })
            .pipe(Effect.mapError(storageError)))?.value.tools?.find(
            (slot) => slot.callId === payload.callId,
          )
          const execution = yield* Cancellation.run(
            payload,
            session,
            executor
              .tool(prepared.intent, agent, {
                recovering,
                settings: config.settings,
                commit,
                previous: {
                  content:
                    previous?.output === undefined
                      ? []
                      : [Prompt.textPart({ text: previous.output })],
                  ...(previous?.details === undefined ? {} : { details: previous.details }),
                  diagnostics: previous?.diagnostics ?? [],
                },
              })
              .pipe(Effect.provideService(Invocation.Invocation, invocation)),
          ).pipe(
            Effect.catchTags({
              ExecutionError: () =>
                Effect.gen(function* () {
                  const live = yield* session
                    .snapshot(Inbox.LiveDoc, { owner: payload.conversationId })
                    .pipe(Effect.mapError(storageError))
                  const slot = live?.value.tools?.find((slot) => slot.callId === payload.callId)
                  return {
                    outcome: 'interrupted' as const,
                    result: {
                      isError: true,
                      content: [
                        Prompt.textPart({ text: slot?.output ?? 'Tool execution interrupted' }),
                      ],
                      ...(slot?.details === undefined ? {} : { details: slot.details }),
                      ...(slot?.diagnostics === undefined ? {} : { diagnostics: slot.diagnostics }),
                    },
                  }
                }),
              StorageError: (error) => Effect.fail(storageError(error)),
            }),
          )
          if (committed === undefined) yield* commit(execution)
        }
        return yield* Schema.decodeEffect(Schema.toCodecJson(Outcome))(committed ?? null).pipe(
          Effect.mapError(codecError),
        )
      }),
    })
    return outcome.receipt
  }),
)
