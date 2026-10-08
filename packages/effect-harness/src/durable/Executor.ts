import * as Predicate from 'effect/Predicate'
import { rejected } from './StorageError.ts'
/**
 * Native Workflow executor Layer composition.
 */
import * as Arr from 'effect/Array'
import type * as Model from 'effect-harness/Model'
import type * as Executor from 'effect-harness/Executor'
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
import { ExecutionError, InvalidStateError } from './workflow/ExecutionError.ts'
import { Generation } from './workflow/Generation.ts'
import * as GenerationExecutor from './workflow/GenerationExecutor.ts'
import * as Structured from './workflow/Structured.ts'
import { Submission } from './workflow/Submission.ts'
import * as SubmissionExecutor from './workflow/SubmissionExecutor.ts'
import { ToolCall } from './workflow/ToolCall.ts'
import * as ToolExecutor from './workflow/ToolExecutor.ts'

/**
 * Built-in native Workflow declarations for submission, generation, tools, compaction and
 * abort.
 *
 * **Details**
 *
 * Include these values when constructing extended Ownership.Declarations for custom owned
 * work.
 *
 * @category combinators
 */
export const workflows = [Submission, Generation, ToolCall, Compaction, Abort] as const

/**
 * Pending work in owned conversations is dispatched and joined using native Workflow executions.
 *
 * @category layers
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
      // effect-nit-allow B-no-service-arguments: DrainConversations invokes this callback with its exact selected scoped Session; capture host config/engine at registration and preserve per-invocation resource identity.
      drain: Effect.fnUntraced(function* (session, owner, conversation, _submissions, sessionId) {
        if (Structured.isFailed(owner.state.outcome))
          yield* Abort.execute({
            sessionId,
            requestId: Identity.RequestId.make(`owned-drain:${owner.id}:${conversation.id}`),
            target: { _tag: 'conversation', id: conversation.id },
            background: false,
          }).pipe(Effect.provideService(WorkflowEngine.WorkflowEngine, engine))
        const boundary = yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const prepared = yield* Inbox.prepare(
              tx,
              conversation.id,
              yield* config.settings.pipe(
                Effect.mapError((cause) => rejected('Invalid host settings', undefined, cause)),
              ),
            )
            const live = yield* tx.doc(Inbox.LiveDoc, { owner: conversation.id })
            const running = Option.fromUndefinedOr(live.run?.taskId)
            const task = yield* Option.match(running, {
              onNone: () => Effect.succeedNone,
              onSome: (id) => tx.task(id),
            })
            if (Option.isSome(task) && task.value.state.status !== 'terminal')
              return { binding: task.value.input, notify: [] }
            const selected = yield* Inbox.apply(tx, prepared, 'final', yield* DateTime.now)
            const generation = Arr.isReadonlyArrayNonEmpty(selected.users)
              ? yield* SubmissionExecutor.makeGeneration(tx, {
                  sessionId: sessionId,
                  conversationId: conversation.id,
                  inputs: selected.users,
                })
              : undefined
            return { ...(generation === undefined ? {} : { generation }), notify: selected.settled }
          }),
        )
        yield* SubmissionExecutor.notify(session, boundary.notify).pipe(
          Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
        )
        if (Predicate.hasProperty(boundary, 'binding')) {
          const binding = yield* Schema.decodeUnknownEffect(Ownership.Binding)(
            boundary.binding,
          ).pipe(
            Effect.mapError(
              (cause) =>
                new ExecutionError({
                  reason: new InvalidStateError({
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
                  reason: new InvalidStateError({
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
 * Registers built-in handlers using the application’s ownership declarations.
 *
 * **When to use**
 *
 * Use when custom Workflows share ownership with built-in harness work.
 *
 * **Details**
 *
 * Provide Ownership.Declarations containing the built-ins and your custom declarations. This
 * Layer installs handlers and cancellation/draining services in the supplied native engine.
 *
 * @see {@link workflows} for the built-in declaration list.
 * @category layers
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
 * Registers the five built-in Workflow executors and their ownership declarations.
 *
 * **When to use**
 *
 * Use when only the built-in Submission, Generation, ToolCall, Compaction and Abort
 * Workflows need domain ownership.
 *
 * **Details**
 *
 * Consumes the application’s native WorkflowEngine, SessionDirectory, conversation
 * Configuration, model catalogue and generic Executor. Execution, replay and timers remain
 * owned by Effect Workflow.
 *
 * @see {@link layerExecutors} for custom owned Workflow declarations.
 * @category layers
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
