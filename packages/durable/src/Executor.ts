/**
 * Native Workflow executor Layer composition.
 *
 * @since 0.0.0
 */
import * as Arr from 'effect/Array'
import type * as Record from './Record.ts'
import type * as Model from '@effect-harness/harness/Model'
import type * as Executor from '@effect-harness/harness/Executor'
import type { SessionDirectory } from './SessionDirectory.ts'
import * as Option from 'effect/Option'
import * as Identity from './Identity.ts'
import * as DateTime from 'effect/DateTime'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Conversation from './Conversation.ts'
import * as Inbox from './Inbox.ts'
import * as Ownership from './Ownership.ts'
import { Abort } from './workflow/Abort.ts'
import * as AbortExecutor from './workflow/AbortExecutor.ts'
import * as Cancellation from './workflow/Cancellation.ts'
import { Compaction } from './workflow/Compaction.ts'
import * as CompactionExecutor from './workflow/CompactionExecutor.ts'
import { ExecutionError, InvalidState } from './workflow/ExecutionError.ts'
import { Generation } from './workflow/Generation.ts'
import * as GenerationExecutor from './workflow/GenerationExecutor.ts'
import * as Structured from './workflow/Structured.ts'
import { Submission } from './workflow/Submission.ts'
import * as SubmissionExecutor from './workflow/SubmissionExecutor.ts'
import { ToolCall } from './workflow/ToolCall.ts'
import * as ToolExecutor from './workflow/ToolExecutor.ts'

/**
 * Built-in native declarations; applications may extend Declarations with their own ordinary Workflows.
 *
 * @category combinators
 * @since 0.0.0
 */
export const workflows = [Submission, Generation, ToolCall, Compaction, Abort] as const

/**
 * Pending work in owned conversations is dispatched and joined using native Workflow executions.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerConversationDrain: Layer.Layer<
  Structured.DrainConversations,
  never,
  Conversation.Configuration | WorkflowEngine.WorkflowEngine
> = Layer.effect(
  Structured.DrainConversations,
  Effect.gen(function* () {
    const config = yield* Conversation.Configuration
    const engine = yield* WorkflowEngine.WorkflowEngine
    return Structured.DrainConversations.of({
      drain: Effect.fnUntraced(function* (session, owner, conversation, _submissions, sessionId) {
        if (Structured.isFailed(owner.state.outcome))
          yield* Abort.execute({
            sessionId,
            requestId: Identity.RequestId.make(`owned-drain:${owner.id}:${conversation.id}`),
            target: { _tag: 'conversation', type: 'conversation', id: conversation.id },
            background: false,
          }).pipe(Effect.provideService(WorkflowEngine.WorkflowEngine, engine))
        const boundary = yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const prepared = yield* Inbox.prepare(tx, conversation.id, config.settings)
            const live = yield* tx.doc(Inbox.LiveDoc, { owner: conversation.id })
            const running = Option.fromUndefinedOr(live.run?.taskId)
            const task = yield* Option.match(running, {
              onNone: () => Effect.succeed(Option.none<Record.Task>()),
              onSome: (id) => tx.task(id),
            })
            if (Option.isSome(task) && task.value.state.status !== 'terminal')
              return { binding: task.value.input, notify: [] }
            const selected = yield* Inbox.apply(tx, prepared, 'final', yield* DateTime.now)
            const generation = Arr.isReadonlyArrayNonEmpty(selected.users)
              ? yield* SubmissionExecutor.makeGeneration(
                  tx,
                  sessionId,
                  conversation.id,
                  selected.users,
                )
              : undefined
            return { ...(generation === undefined ? {} : { generation }), notify: selected.settled }
          }),
        )
        yield* SubmissionExecutor.notify(session, boundary.notify).pipe(
          Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
        )
        if ('binding' in boundary) {
          const binding = yield* Schema.decodeUnknownEffect(Ownership.Binding)(
            boundary.binding,
          ).pipe(
            Effect.mapError(
              (cause) =>
                new ExecutionError({
                  reason: new InvalidState({
                    message: 'Owned conversation generation has no binding',
                    cause,
                  }),
                }),
            ),
          )
          const input = yield* Schema.decodeEffect(Schema.toCodecJson(Generation.payloadSchema))(
            binding.payload,
          ).pipe(
            Effect.mapError(
              (cause) =>
                new ExecutionError({
                  reason: new InvalidState({
                    message: 'Owned conversation generation input is invalid',
                    cause,
                  }),
                }),
            ),
          )
          yield* Generation.execute(input).pipe(
            Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
          )
        } else if (boundary.generation !== undefined)
          yield* Generation.execute(boundary.generation).pipe(
            Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
          )
      }),
    })
  }),
)

/**
 * Registers harness executors with the application's standard WorkflowEngine.
 *
 * **Details**
 *
 * Provide SessionDirectory, Conversation.Configuration and the generic harness/provider Layers. For custom owned work, provide an extended Ownership.Declarations instead of the built-in declarations Layer.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerExecutors: Layer.Layer<
  Structured.DrainConversations | Cancellation.Cancellation,
  never,
  | Model.Catalog
  | Conversation.Configuration
  | Executor.Executor
  | SessionDirectory
  | WorkflowEngine.WorkflowEngine
  | Ownership.Declarations
> = Layer.mergeAll(
  SubmissionExecutor.layer,
  GenerationExecutor.layer,
  ToolExecutor.layer,
  CompactionExecutor.layer,
  AbortExecutor.layer,
).pipe(Layer.provideMerge(layerConversationDrain), Layer.provideMerge(Cancellation.layer))

/**
 * layer service Layer.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer: Layer.Layer<
  Structured.DrainConversations | Cancellation.Cancellation | Ownership.Declarations,
  never,
  | Model.Catalog
  | Conversation.Configuration
  | Executor.Executor
  | SessionDirectory
  | WorkflowEngine.WorkflowEngine
> = layerExecutors.pipe(Layer.provideMerge(Ownership.layerDeclarations(workflows)))
