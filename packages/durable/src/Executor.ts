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

/** Built-in native declarations; applications may extend Declarations with their own ordinary Workflows. */
export const workflows = [Submission, Generation, ToolCall, Compaction, Abort] as const

/** Pending work in owned conversations is dispatched and joined using native Workflow executions. */
export const layerConversationDrain = Layer.effect(
  Structured.DrainConversations,
  Effect.gen(function* () {
    const config = yield* Conversation.Configuration
    const engine = yield* WorkflowEngine.WorkflowEngine
    return Structured.DrainConversations.of({
      drain: Effect.fnUntraced(function* (session, owner, conversation, _submissions, sessionId) {
        if (Structured.failed(owner.state.outcome))
          yield* Abort.execute({
            sessionId,
            requestId: Identity.RequestId.make(`owned-drain:${owner.id}:${conversation.id}`),
            target: { type: 'conversation', id: conversation.id },
            background: false,
          }).pipe(Effect.provideService(WorkflowEngine.WorkflowEngine, engine))
        const boundary = yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const prepared = yield* Inbox.prepare(tx, conversation.id, config.settings)
            const live = yield* tx.doc(Inbox.LiveDoc, { owner: conversation.id })
            const running = live.run?.taskId
            const task = running === undefined ? undefined : yield* tx.task(running)
            if (task !== undefined && task.state.status !== 'terminal')
              return { binding: task.input, notify: [] }
            const selected = yield* Inbox.apply(tx, prepared, 'final', yield* DateTime.now)
            const generation =
              selected.users.length > 0
                ? yield* SubmissionExecutor.createGeneration(
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
 * Register harness executors with the application's standard WorkflowEngine.
 * Provide SessionDirectory, Conversation.Configuration and the generic harness/provider Layers.
 * For custom owned work, provide an extended Ownership.Declarations instead of the built-in declarations Layer.
 */
export const layerExecutors = Layer.mergeAll(
  SubmissionExecutor.layer,
  GenerationExecutor.layer,
  ToolExecutor.layer,
  CompactionExecutor.layer,
  AbortExecutor.layer,
).pipe(Layer.provideMerge(layerConversationDrain), Layer.provideMerge(Cancellation.layer))

export const layer = layerExecutors.pipe(Layer.provideMerge(Ownership.layerDeclarations(workflows)))
