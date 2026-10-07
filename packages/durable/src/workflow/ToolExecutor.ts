import * as Arr from 'effect/Array'
import type * as Agent from '@effect-harness/harness/Agent'
import type { StorageError } from '../StorageError.ts'
import * as Option from 'effect/Option'
import * as Entry from '../Entry.ts'
import * as Serialization from '../Serialization.ts'
import { ToolCheckpoint } from './Outcome.ts'
import * as Layer from 'effect/Layer'
import * as Harness from '@effect-harness/harness/Executor'
import * as Invocation from '@effect-harness/harness/Invocation'
import * as Tool from '@effect-harness/harness/Tool'
import * as ToolResult from '@effect-harness/harness/ToolResult'
import * as DateTime from 'effect/DateTime'
import * as Ref from 'effect/Ref'
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
import * as Session from '../Session.ts'
import { SessionDirectory } from '../SessionDirectory.ts'
import * as Usage from '../Usage.ts'
import { ExecutionError, InvalidState, ExecutionErrorCodec, Aborted } from './ExecutionError.ts'
import { storageError } from './SubmissionExecutor.ts'
import { ToolCall, Result } from './ToolCall.ts'
import * as Cancellation from './Cancellation.ts'
import * as Structured from './Structured.ts'

export const IntentDoc = Document.defineUnsafe({
  kind: 'harness.tool-intent',
  version: 1,
  scope: 'task',
  schema: Document.jsonObjectCodec(Schema.Struct({ intent: Tool.Intent, started: Schema.Boolean })),
  initial: (seed) => ({ intent: Schema.decodeUnknownSync(Tool.Intent)(seed), started: false }),
})
export const Outcome = Schema.Struct({ execution: Tool.Execution, receipt: Result })
const Prepared = Schema.Union([
  Schema.Struct({ type: Schema.Literal('intent'), intent: Tool.Intent }),
  Schema.Struct({ type: Schema.Literal('rejected'), message: Schema.String }),
])
const invalid = (message: string, cause?: unknown) =>
  new ExecutionError({
    reason: new InvalidState({ message, ...(cause === undefined ? {} : { cause }) }),
  })
const codecError = (cause: unknown) => invalid('Tool result cannot be persisted', cause)

/** A native tool result entry also covers an unoffered call, which has no executable task. */
export const appendResult = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  payload: Pick<
    typeof ToolCall.payloadSchema.Type,
    'conversationId' | 'assistantId' | 'callId' | 'name'
  > & { readonly taskId?: Record.TaskId },
  execution: Tool.Execution,
): Effect.fn.Return<Record.Entry, StorageError | ExecutionError> {
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
  const entry = yield* tx.appendEntry(payload.conversationId, {
    kind: 'harness.tool',
    model: yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Json))(model).pipe(
      Effect.mapError(codecError),
    ),
    ...(payload.taskId === undefined ? {} : { byTaskId: payload.taskId }),
    data: yield* Schema.encodeEffect(Serialization.json(Entry.ToolResultData))({
      timestamp: yield* DateTime.now,
      assistantId: payload.assistantId,
      callId: payload.callId,
      name: payload.name,
      execution,
    }).pipe(Effect.mapError(codecError)),
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
          const taskOption = yield* tx.task(payload.taskId)
          if (
            Option.isNone(taskOption) ||
            taskOption.value.state.status === 'terminal' ||
            taskOption.value.abortRequested
          )
            return

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
          if (value.details !== undefined) slot.details = yield* Document.copyEffect(value.details)
          if (value.diagnostics !== undefined)
            slot.diagnostics = [
              ...(slot.diagnostics ?? []),
              ...(yield* Document.copyEffect(value.diagnostics)),
            ]
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
    const state: Agent.State = yield* session
      .snapshot(Conversation.AgentDoc, { owner: payload.conversationId })
      .pipe(
        Effect.mapError(storageError),
        Effect.map(Option.match({ onNone: () => ({}), onSome: (snapshot) => snapshot.value })),
      )
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
      error: ExecutionErrorCodec,
      execute: Cancellation.activity(
        payload,
        session,
        Effect.gen(function* () {
          const task = yield* session.task(payload.taskId).pipe(Effect.mapError(storageError))
          if (Option.exists(task, (value) => value.abortRequested))
            return yield* new ExecutionError({
              reason: new Aborted({ message: 'Tool aborted before intent' }),
            })
          const persisted = yield* session
            .snapshot(IntentDoc, { owner: payload.taskId })
            .pipe(Effect.mapError(storageError))
          if (Option.isSome(persisted))
            return { type: 'intent' as const, intent: persisted.value.value.intent }
          const intent = yield* Effect.result(
            executor
              .prepareTool(agent, {
                id: payload.callId,
                name: payload.name,
                args: payload.arguments,
              })
              .pipe(
                Effect.provideService(Invocation.Invocation, invocation),
                Effect.provide(
                  Ownership.layerCurrent(payload).pipe(
                    Layer.provide(Layer.succeed(Session.Session, session)),
                  ),
                ),
              ),
          )
          if (intent._tag === 'Failure')
            return { type: 'rejected' as const, message: intent.failure.message }
          yield* session
            .transaction(
              Effect.fnUntraced(function* (tx) {
                return yield* tx.doc(IntentDoc, {
                  owner: payload.taskId,
                  seed: yield* Schema.encodeEffect(Serialization.json(Tool.Intent))(
                    intent.success,
                  ).pipe(Effect.mapError(codecError)),
                })
              }),
            )
            .pipe(
              Effect.mapError((cause) =>
                cause instanceof ExecutionError ? cause : storageError(cause),
              ),
            )
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
            const taskOption = yield* tx.task(payload.taskId)
            if (Option.isNone(taskOption)) return yield* invalid('Tool task projection is absent')
            const task = taskOption.value
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
                : {
                    control: yield* Schema.encodeEffect(Serialization.object(Invocation.Control))(
                      execution.result.control,
                    ).pipe(Effect.mapError(codecError)),
                  }),
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
      error: ExecutionErrorCodec,
      execute: Effect.gen(function* () {
        const task = yield* session.task(payload.taskId).pipe(Effect.mapError(storageError))
        if (
          Option.isSome(task) &&
          (task.value.state.status === 'terminal' || task.value.state.status === 'completing')
        ) {
          yield* Structured.drain(session, payload.taskId, payload.sessionId).pipe(
            Effect.mapError((error) =>
              error._tag === 'StorageError' ? storageError(error) : error,
            ),
          )
          return yield* Schema.decodeEffect(Schema.toCodecJson(Outcome))(
            task.value.state.outcome ?? null,
          ).pipe(Effect.mapError(codecError))
        }
        const committed = yield* Ref.make<Record.Json | undefined>(undefined)
        const commit = (execution: Tool.Execution) =>
          settle(execution).pipe(
            Effect.provideContext(settlementContext),
            Effect.tap((value) => Ref.set(committed, value)),
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
                const taskOption = yield* tx.task(payload.taskId)
                if (Option.isNone(taskOption))
                  return yield* invalid('Tool task projection is absent')
                const task = taskOption.value
                if (task.abortRequested)
                  return yield* new ExecutionError({
                    reason: new Aborted({ message: 'Tool aborted before execution' }),
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
                    state: {
                      status: 'running',
                      checkpoint: ToolCheckpoint.make({ arguments: doc.intent.args }),
                    },
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
            .pipe(Effect.mapError(storageError))).pipe(
            Option.flatMap((snapshot) =>
              Arr.findFirst(snapshot.value.tools ?? [], (slot) => slot.callId === payload.callId),
            ),
          )
          const execution = yield* Cancellation.run(
            payload,
            session,
            executor
              .tool(prepared.intent, agent, {
                recovering,
                settings: config.settings,
                commit,
                previous: Option.match(previous, {
                  onNone: () => ({ content: [], diagnostics: [] }),
                  onSome: (previous) => ({
                    content:
                      previous.output === undefined
                        ? []
                        : [Prompt.textPart({ text: previous.output })],
                    ...(previous.details === undefined ? {} : { details: previous.details }),
                    diagnostics: previous.diagnostics ?? [],
                  }),
                }),
              })
              .pipe(Effect.provideService(Invocation.Invocation, invocation)),
          ).pipe(
            Effect.catchTags({
              ExecutionError: () =>
                Effect.gen(function* () {
                  const live = yield* session
                    .snapshot(Inbox.LiveDoc, { owner: payload.conversationId })
                    .pipe(Effect.mapError(storageError))
                  const slot = live.pipe(
                    Option.flatMap((snapshot) =>
                      Arr.findFirst(
                        snapshot.value.tools ?? [],
                        (slot) => slot.callId === payload.callId,
                      ),
                    ),
                  )
                  return {
                    outcome: 'interrupted' as const,
                    result: {
                      isError: true,
                      content: [
                        Prompt.textPart({
                          text: slot.pipe(
                            Option.map((value) => value.output ?? 'Tool execution interrupted'),
                            Option.getOrElse(() => 'Tool execution interrupted'),
                          ),
                        }),
                      ],
                      ...Option.match(slot, {
                        onNone: () => ({}),
                        onSome: (slot) =>
                          slot.details === undefined ? {} : { details: slot.details },
                      }),
                      ...Option.match(slot, {
                        onNone: () => ({}),
                        onSome: (slot) =>
                          slot.diagnostics === undefined ? {} : { diagnostics: slot.diagnostics },
                      }),
                    },
                  }
                }),
              StorageError: (error) => Effect.fail(storageError(error)),
            }),
            Effect.mapError((cause) =>
              cause instanceof ExecutionError
                ? cause
                : invalid('Tool terminal projection failed', cause),
            ),
          )
          if ((yield* Ref.get(committed)) === undefined) yield* commit(execution)
        }
        return yield* Schema.decodeEffect(Schema.toCodecJson(Outcome))(
          (yield* Ref.get(committed)) ?? null,
        ).pipe(Effect.mapError(codecError))
      }),
    })
    return outcome.receipt
  }),
)
