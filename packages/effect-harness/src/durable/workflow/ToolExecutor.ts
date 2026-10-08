/**
 * Pinned tool intent, progress and terminal execution settlement.
 */
import * as Result from 'effect/Result'
import * as Array from 'effect/Array'
import type * as Agent from 'effect-harness/Agent'
import type { StorageError } from '../StorageError.ts'
import * as Option from 'effect/Option'
import * as Entry from '../Entry.ts'
import * as Serialization from '../Serialization.ts'
import { ToolCheckpoint } from './Outcome.ts'
import * as Layer from 'effect/Layer'
import * as Executor from 'effect-harness/Executor'
import * as Invocation from 'effect-harness/Invocation'
import * as ToolRegistration from 'effect-harness/ToolRegistration'
import * as ToolResult from 'effect-harness/ToolResult'
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
import type * as Record from '../Record.ts'
import * as Session from '../Session.ts'
import { SessionDirectory } from '../SessionDirectory.ts'
import * as Usage from '../Usage.ts'
import { ExecutionError, InvalidStateError, AbortedError } from './ExecutionError.ts'
import { storageError } from './SubmissionExecutor.ts'
import { ToolCall, Result as WorkflowResult } from './ToolCall.ts'
import * as Cancellation from './Cancellation.ts'
import * as Structured from './Structured.ts'

/**
 * Pinned tool intent document definition.
 *
 * @category models
 */
export const IntentDoc = Document.defineUnsafe({
  kind: 'harness.tool-intent',
  version: 1,
  scope: 'task',
  schema: Serialization.object(
    Schema.Struct({ intent: ToolRegistration.Intent, started: Schema.Boolean }),
  ),
  // effect-nit-allow P4-decode-effect-at-boundary: this synchronous document initializer may throw; Session.transaction catches its seed decoder with Effect.try before commit.
  initial: (seed) => ({
    intent: Schema.decodeUnknownSync(ToolRegistration.Intent)(seed),
    started: false,
  }),
})
/**
 * Schema for a decoded tool execution and its terminal Workflow receipt.
 *
 * @category schemas
 */
export const Outcome = Schema.Struct({
  execution: ToolRegistration.Execution,
  receipt: WorkflowResult,
})
/**
 * Decoded value validated by the `Outcome` schema.
 *
 * @category models
 */
export type Outcome = typeof Outcome.Type

const Prepared = Schema.Union([
  Schema.TaggedStruct('intent', { intent: ToolRegistration.Intent }),
  Schema.TaggedStruct('rejected', { message: Schema.String }),
])
const invalid = (message: string, cause?: unknown) =>
  new ExecutionError({
    reason: new InvalidStateError({ message, ...(cause === undefined ? {} : { cause }) }),
  })
const codecError = (cause: unknown) => invalid('Tool result cannot be persisted', cause)

/**
 * Appends a native tool-result entry and records its available usage.
 *
 * **Details**
 *
 * An unoffered call also receives a result entry even when it has no executable task.
 *
 * @category combinators
 */
export const appendResult = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  payload: Pick<
    typeof ToolCall.payloadSchema.Type,
    'conversationId' | 'assistantId' | 'callId' | 'name'
  > & { readonly taskId?: Record.TaskId | undefined },
  execution: ToolRegistration.Execution,
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
  const slot = Array.findFirst(live.tools ?? [], (slot) => slot.callId === payload.callId)
  if (Option.isSome(slot)) {
    slot.value.status = 'done'
    slot.value.entry = entry.id
  }
  if (execution.result.usage !== undefined)
    yield* Usage.record(tx, payload.conversationId, 'tools', payload.name, execution.result.usage)
  return entry
})

/**
 * Registers an ordinary native child Workflow.
 *
 * **Details**
 *
 * Its intent commits before any handler side effect.
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
> = Layer.unwrap(
  Effect.gen(function* () {
    const directory = yield* SessionDirectory
    const executor = yield* Executor.Executor
    const config = yield* Conversation.Configuration
    return ToolCall.toLayer(
      Effect.fnUntraced(function* (payload, executionId) {
        const session = yield* directory
          .resolve(payload.sessionId)
          .pipe(Effect.mapError(storageError))
        /** Committed progress only: an acknowledgement cannot expose an uncommitted preview. */
        const progress = (value: Invocation.Progress): Effect.Effect<void> =>
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
                const slot = Array.findFirst(
                  live.tools ?? [],
                  (slot) => slot.callId === payload.callId,
                )
                if (Option.isNone(slot)) return
                if (value.clear === true) {
                  delete slot.value.output
                  delete slot.value.details
                  delete slot.value.diagnostics
                  delete slot.value.droppedBytes
                  delete slot.value.droppedLines
                }
                if (value.output !== undefined) slot.value.output = value.output
                if (value.details !== undefined)
                  slot.value.details = yield* Document.copyEffect(value.details)
                if (value.diagnostics !== undefined)
                  slot.value.diagnostics = [
                    ...(slot.value.diagnostics ?? []),
                    ...(yield* Document.copyEffect(value.diagnostics)),
                  ]
                if (value.droppedBytes !== undefined) slot.value.droppedBytes = value.droppedBytes
                if (value.droppedLines !== undefined) slot.value.droppedLines = value.droppedLines
              }),
            )
            .pipe(Effect.asVoid, Effect.orDie)

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
          progress,
        })
        const agent = yield* executor
          .resolve(state, yield* config.settings.pipe(Effect.mapError(codecError)))
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
              if (Option.exists(task, (value) => value.abortRequested))
                return yield* new ExecutionError({
                  reason: new AbortedError({ message: 'Tool aborted before intent' }),
                })
              const persisted = yield* session
                .snapshot(IntentDoc, { owner: payload.taskId })
                .pipe(Effect.mapError(storageError))
              if (Option.isSome(persisted))
                return {
                  _tag: 'intent' as const,
                  intent: persisted.value.value.intent,
                }
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
                      // effect-nit-allow P3-layer-memo-by-reference: Current.check and the captured Session belong to this invocation and terminate with its scope.
                      { local: true },
                    ),
                  ),
              )
              return yield* Result.match(intent, {
                onFailure: (failure) =>
                  Effect.succeed({
                    _tag: 'rejected' as const,
                    message: failure.message,
                  }),
                onSuccess: (success) =>
                  session
                    .transaction((tx) =>
                      Schema.encodeEffect(Serialization.json(ToolRegistration.Intent))(
                        success,
                      ).pipe(
                        Effect.mapError(codecError),
                        Effect.flatMap((seed) =>
                          tx.doc(IntentDoc, { owner: payload.taskId, seed }),
                        ),
                      ),
                    )
                    .pipe(
                      Effect.mapError((cause) =>
                        cause instanceof ExecutionError ? cause : storageError(cause),
                      ),
                      Effect.as({
                        _tag: 'intent' as const,
                        intent: success,
                      }),
                    ),
              })
            }),
          ).pipe(
            Effect.mapError((error) =>
              error._tag === 'StorageError' ? storageError(error) : error,
            ),
          ),
        })

        const settle = (execution: ToolRegistration.Execution) =>
          session
            .transaction(
              Effect.fnUntraced(function* (tx) {
                const graph = yield* Ownership.readGraph(tx)
                const taskOption = yield* tx.task(payload.taskId)
                if (Option.isNone(taskOption))
                  return yield* invalid('Tool task projection is absent')
                const task = taskOption.value
                if (task.state.status === 'terminal' || task.state.status === 'completing')
                  return task.state.outcome ?? null
                const entry = yield* appendResult(tx, payload, execution)
                let status: WorkflowResult['status'] =
                  execution.outcome === 'completed' ? 'completed' : 'failed'
                if (task.abortRequested || execution.outcome === 'interrupted') status = 'aborted'
                const receipt: WorkflowResult = {
                  status,
                  entryId: entry.id,
                  ...(execution.result.control === undefined
                    ? {}
                    : {
                        control: yield* Schema.encodeEffect(
                          Serialization.object(Invocation.Control),
                        )(execution.result.control).pipe(Effect.mapError(codecError)),
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
              Effect.mapError((error) =>
                error._tag === 'StorageError' ? storageError(error) : error,
              ),
            )

        const outcome = yield* Activity.make({
          name: 'execute-and-settle',
          success: Outcome,
          error: ExecutionError,
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
            const committed = yield* Ref.make<Schema.Json | undefined>(undefined)
            const commit = (execution: ToolRegistration.Execution) =>
              settle(execution).pipe(
                Effect.provideContext(settlementContext),
                Effect.tap((value) => Ref.set(committed, value)),
                Effect.asVoid,
                Effect.orDie,
              )
            if (prepared._tag === 'rejected') {
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
                        reason: new AbortedError({ message: 'Tool aborted before execution' }),
                      })
                    const doc = yield* tx.doc(IntentDoc, { owner: payload.taskId })
                    const before = doc.started
                    doc.started = true
                    const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
                    const slot = Array.findFirst(
                      live.tools ?? [],
                      (slot) => slot.callId === payload.callId,
                    )
                    if (Option.isSome(slot)) slot.value.status = 'running'
                    yield* tx.write({
                      _tag: 'task',
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
                  Array.findFirst(
                    snapshot.value.tools ?? [],
                    (slot) => slot.callId === payload.callId,
                  ),
                ),
              )
              const execution = yield* Cancellation.run(
                payload,
                session,
                executor
                  .tool(prepared.intent, agent, {
                    recovering,
                    settings: yield* config.settings.pipe(Effect.mapError(codecError)),
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
                  ExecutionError: Effect.fnUntraced(function* () {
                    const live = yield* session
                      .snapshot(Inbox.LiveDoc, { owner: payload.conversationId })
                      .pipe(Effect.mapError(storageError))
                    const slot = live.pipe(
                      Option.flatMap((snapshot) =>
                        Array.findFirst(
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
  }),
)
