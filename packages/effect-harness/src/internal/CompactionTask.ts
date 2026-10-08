/** Checkpointed summary preparation, inference and atomic context placement. */
import * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import type * as AiError from 'effect/ai/AiError'
import * as Agent from '../Agent.ts'
import * as Executor from '../Executor.ts'
import * as Invocation from '../Invocation.ts'
import type { ModelError } from '../ModelError.ts'
import * as Task from '../Task.ts'
import { TaskRuntime } from '../TaskRuntime.ts'
import * as Transcript from '../Transcript.ts'
import * as Usage from '../Usage.ts'
import * as Record from '../Record.ts'
import * as Serialization from '../Serialization.ts'
import { UsageDoc } from '../internal/Usage.ts'
import type { StorageError } from '../StorageError.ts'
import * as ConversationState from './ConversationState.ts'
import type * as Session from './Session.ts'

export const Input = Schema.Struct({
  reason: Schema.Literals(['manual', 'threshold', 'overflow']),
  instructions: Schema.optionalKey(Schema.String),
})
const Boundary = {
  head: Schema.optionalKey(Record.EntryId),
  tail: Record.EntryId,
  firstKept: Record.EntryId,
}
const Checkpoint = Serialization.json(
  Schema.Union([
    Schema.Struct({ phase: Schema.Literal('prepare') }),
    Schema.Struct({
      phase: Schema.Literal('summarize'),
      head: Boundary.head,
      request: Executor.SummaryRequest,
      retryAt: Schema.optionalKey(Schema.Finite),
    }),
    Schema.Struct({
      phase: Schema.Literal('place'),
      ...Boundary,
      summary: Schema.String,
    }),
  ]),
)
type Checkpoint = typeof Checkpoint.Type

/** Construct one compaction definition with the owning session and native executor. */
export const make = Effect.fn('CompactionTask.make')(function* (
  _session: Session.Service,
  executor: Executor.Executor['Service'],
  options: {
    readonly settings: Agent.Settings
    readonly cwd: string
    readonly report: (error: unknown) => Effect.Effect<void>
  },
): Effect.fn.Return<Task.BoundDefinition> {
  const retire = (tx: Session.Transaction, taskId: Record.TaskId) =>
    tx.retire(ConversationState.ProgressDoc, { owner: taskId })
  const account = Effect.fn('CompactionTask.account')(function* (
    tx: Session.Transaction,
    task: Record.Task,
    request: Executor.SummaryRequest,
    usage: Usage.Usage,
  ) {
    const ledger = yield* tx.doc(UsageDoc, { owner: task.conversationId })
    const key = `${request.request.model.provider}/${request.request.model.modelId}`
    ledger.models[key] = Usage.add(
      Object.hasOwn(ledger.models, key) ? (ledger.models[key] ?? Usage.make()) : Usage.make(),
      usage,
    )
  })
  const failure = Effect.fn('CompactionTask.failure')(function* (
    error: ModelError | AiError.AiError | Schema.SchemaError,
  ) {
    const runtime = yield* TaskRuntime
    yield* runtime.commit(
      Effect.fn('CompactionTask.failCommit')(function* (tx) {
        yield* retire(tx, runtime.taskId)
        return Task.fail(error.message)
      }),
    )
    return undefined
  })
  const definition = Task.define<
    typeof Input,
    typeof Checkpoint,
    typeof Schema.Json,
    ModelError | AiError.AiError | Schema.SchemaError | StorageError,
    never
  >({
    name: 'harness.compaction',
    version: 1,
    input: Input,
    checkpoint: Checkpoint,
    result: Schema.Json,
    initial: (): Checkpoint => ({ phase: 'prepare' }),
    run: Effect.fn('CompactionTask.run')(
      function* (
        task: Task.Snapshot<typeof Input.Type, Checkpoint>,
      ): Effect.fn.Return<
        Task.Transition<Checkpoint, Schema.Json> | undefined,
        ModelError | AiError.AiError | Schema.SchemaError | StorageError,
        TaskRuntime
      > {
        const runtime = yield* TaskRuntime
        const checkpoint = task.checkpoint
        if (checkpoint.phase === 'prepare') {
          const selected = yield* runtime.transaction(
            Effect.fn('CompactionTask.select')(function* (tx) {
              const state = yield* tx.doc(ConversationState.AgentDoc, {
                owner: task.conversationId,
              })
              // JSON encoding detaches the draft before the transaction revokes it.
              const detached = yield* Schema.encodeEffect(Serialization.json(Agent.State))(state)
              const entries = yield* tx
                .scanEntries({ conversationId: task.conversationId })
                .pipe(
                  Stream.runCollect,
                  Effect.flatMap(Effect.forEach(ConversationState.projectEntry)),
                )
              return { state: detached, view: Transcript.derive(entries) }
            }),
          )
          const state = yield* Schema.decodeEffect(Serialization.json(Agent.State))(selected.state)
          const prepared = yield* executor
            .prepareCompaction({
              state,
              settings: options.settings,
              view: selected.view,
              reason: task.input.reason,
              instructions: task.input.instructions,
            })
            .pipe(
              Effect.provideService(
                Invocation.Invocation,
                Invocation.Invocation.of({
                  cwd: state.cwd ?? options.cwd,
                  report: options.report,
                  progress: () => Effect.void,
                }),
              ),
            )
          if (prepared._tag === 'none') {
            yield* runtime.commit(
              Effect.fn('CompactionTask.skip')(function* (tx) {
                yield* retire(tx, task.id)
                return Task.complete({ placed: false, reason: 'not_needed' })
              }),
            )
            return
          }
          const head = selected.view.head?.id
          if (prepared._tag === 'request')
            return Task.continueWith({
              phase: 'summarize',
              ...(head === undefined ? {} : { head }),
              request: prepared.request,
            } satisfies Checkpoint)
          const tail = yield* Schema.decodeEffect(Record.EntryId)(
            Math.max(...selected.view.entries.map((entry) => entry.id)),
          )
          return Task.continueWith({
            phase: 'place',
            ...(head === undefined ? {} : { head }),
            tail,
            firstKept: prepared.firstKept,
            summary: prepared.summary,
          } satisfies Checkpoint)
        }
        if (checkpoint.phase === 'summarize') {
          if (checkpoint.retryAt !== undefined) yield* runtime.sleepUntil(checkpoint.retryAt)
          const result = yield* Effect.result(executor.compact(checkpoint.request))
          if (Result.isSuccess(result)) {
            const next: Checkpoint = {
              phase: 'place',
              ...(checkpoint.head === undefined ? {} : { head: checkpoint.head }),
              tail: checkpoint.request.tail,
              firstKept: checkpoint.request.firstKept,
              summary: result.success.summary,
            }
            const encoded = yield* Schema.encodeEffect(Checkpoint)(next)
            yield* runtime.commit(
              Effect.fn('CompactionTask.saveSummary')(function* (tx, current) {
                yield* account(tx, current, checkpoint.request, result.success.usage)
                return Task.continueWith(encoded)
              }),
            )
            return
          }
          const classification = yield* executor.classifyFailure(
            checkpoint.request.request,
            result.failure,
          )
          const retry = Agent.isRetryAllowed(
            options.settings.retry,
            checkpoint.request.attempt,
            classification.retryable,
          )
          const usage = result.failure._tag === 'ModelError' ? result.failure.usage : undefined
          const retryAt =
            (yield* runtime.now) +
            Duration.toMillis(Agent.retryDelay(options.settings.retry, checkpoint.request.attempt))
          const encoded = retry
            ? yield* Schema.encodeEffect(Checkpoint)({
                phase: 'summarize',
                ...(checkpoint.head === undefined ? {} : { head: checkpoint.head }),
                request: { ...checkpoint.request, attempt: checkpoint.request.attempt + 1 },
                retryAt,
              })
            : undefined
          yield* runtime.commit(
            Effect.fn('CompactionTask.recordAttempt')(function* (tx, current) {
              if (usage !== undefined) yield* account(tx, current, checkpoint.request, usage)
              if (encoded !== undefined) return Task.continueWith(encoded)
              yield* retire(tx, task.id)
              return Task.fail(result.failure.message)
            }),
          )
          return
        }
        const model = yield* ConversationState.encodeMessages([
          Prompt.userMessage({
            content: [
              Prompt.textPart({
                text: `The conversation history before this point was compacted into the following summary:\n\n<summary>\n${checkpoint.summary}\n</summary>`,
              }),
            ],
          }),
        ])
        yield* runtime.commit(
          Effect.fn('CompactionTask.place')(function* (tx) {
            const entries = yield* tx
              .scanEntries({ conversationId: task.conversationId })
              .pipe(
                Stream.runCollect,
                Effect.flatMap(Effect.forEach(ConversationState.projectEntry)),
              )
            const view = Transcript.derive(entries)
            const stale =
              view.head?.id !== checkpoint.head ||
              !view.entries.some((entry) => entry.id === checkpoint.firstKept) ||
              !view.entries.some((entry) => entry.id === checkpoint.tail)
            yield* retire(tx, task.id)
            if (stale) return Task.complete({ placed: false, reason: 'stale' })
            const entry = yield* tx.appendEntry(task.conversationId, {
              kind: 'harness.compaction',
              head: checkpoint.firstKept,
              model,
              data: { reason: task.input.reason },
            })
            return Task.complete({ placed: true, entryId: entry.id })
          }),
        )
      },
      Effect.catchTags({ ModelError: failure, AiError: failure, SchemaError: failure }),
    ),
    abort: Effect.fn('CompactionTask.abort')(function* (task) {
      const runtime = yield* TaskRuntime
      yield* runtime.commit(
        Effect.fn('CompactionTask.abortCommit')(function* (tx) {
          yield* retire(tx, task.id)
          return Task.aborted()
        }),
      )
      return undefined
    }),
  })
  return yield* Task.bind(definition)
})
