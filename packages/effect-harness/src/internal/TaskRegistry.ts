/** Scheduler adapter over canonical registry snapshots, never a second mutable registry. */
import * as Effect from 'effect/Effect'
import type * as Context from 'effect/Context'
import * as Option from 'effect/Option'
import type * as Schema from 'effect/Schema'
import * as Task from '../Task.js'
import * as TaskResolution from '../TaskResolution.js'
import type * as Record from '../Record.js'
import type * as Transaction from '../Transaction.js'
import { TaskError } from '../TaskError.js'
import { SessionError, type Failure } from '../SessionError.js'
import * as Runtime from './TransactionRuntime.js'
import * as Value from './DocumentValue.js'
import * as Domain from '../Record.js'
import * as TaskTransitions from './TaskTransitions.js'
import type * as RegistryScope from '../RegistryScope.js'

export interface Binding<T extends Task.Any = Task.Any> {
  readonly declaration: T
  readonly owner: RegistryScope.Owner
  readonly context: Context.Context<TaskResolution.DecodingServices<T>>
}
export interface Snapshot<T extends Task.Any = Task.Any> {
  readonly tasks: ReadonlyArray<Binding<T>>
}
/** The existing legacy executor retains this kind until scheduler integration migrates it safely. */
export const RESERVED_KINDS: ReadonlyArray<string> = Object.freeze(['harness.run'])

/** Validate before canonical publication; duplicate task kinds never use tool-style last-wins. */
export const validate = Effect.fnUntraced(function* (tasks: ReadonlyArray<Task.Any>) {
  const kinds = new Set<string>()
  for (const definition of tasks) {
    yield* Task.validate(definition)
    if (
      RESERVED_KINDS.includes(definition.kind) ||
      definition.kind.startsWith('effect-harness/') ||
      kinds.has(definition.kind)
    )
      return yield* new TaskError({
        reason: 'invalidDefinition',
        operation: 'task.registry',
        message: `Task kind ${definition.kind} is reserved or duplicated`,
      })
    kinds.add(definition.kind)
  }
})

/**
 * Consume the canonical snapshot; inspection never acquires a lease or runs migrations.
 * Hold its Registry lease while inspecting codecs that use captured resources. Only the
 * chosen binding's Context is provided, and the result retains its owner and declaration.
 */
export const inspect = <T extends Task.Any>(
  snapshot: Snapshot<T>,
  record: Record.Task,
  failure?: TaskResolution.MigrationFailure,
): Effect.Effect<TaskResolution.Inspection<T> & { readonly binding: Binding<T> | undefined }> => {
  const binding = snapshot.tasks.find((item) => item.declaration.kind === record.kind)
  if (binding === undefined)
    return TaskResolution.inspectMissing(record).pipe(
      Effect.map((result) => ({ ...result, binding })),
    )
  if (
    !binding.owner.enabled &&
    record.state.status !== 'terminal' &&
    record.state.status !== 'completing'
  )
    return Effect.succeed({
      _tag: 'blocked',
      binding,
      error: TaskResolution.blocked(record, 'disabledDefinition', 'Task registration is disabled'),
    })
  return TaskResolution.inspect(binding.declaration, record, failure).pipe(
    Effect.provideContext(binding.context),
    Effect.map((result) => ({ ...result, binding })),
  )
}

const migrationReason = (
  cause: TaskError | Schema.SchemaError,
): TaskResolution.Blocked['reason'] => {
  if (cause._tag !== 'TaskError') return 'migrationFailed'
  if (cause.reason === 'newerVersion') return 'newerVersion'
  if (cause.reason === 'missingMigration') return 'missingMigration'
  return 'migrationFailed'
}

/**
 * Only scheduler code can import this transaction capability (internal package routes are denied).
 * Reread the current draft under its semaphore; validate migration and both codecs completely
 * before staging one replacement. Ownership, policy, status, wait edges and memos are preserved.
 * Pure migration failure/defect/interruption leaves all earlier drafts intact. This is not a
 * reservation: the scheduler must pin a registry lease and recheck invocation/abort gates on
 * Session's line before every phase, progress/memo write, or fresh abort invocation.
 */
export const upgrade = Effect.fnUntraced(function* <
  I extends Schema.Constraint,
  C extends Task.CheckpointCodec,
  S extends Schema.Constraint,
  F extends Schema.Constraint,
  H extends Task.Phases<I, C>,
  B extends Task.Handler<I, C> | undefined,
  R,
  EBuild,
>(
  tx: Transaction.Transaction,
  definition: Task.Task<I, C, S, F, H, B, R, EBuild>,
  id: Record.TaskId,
): Effect.fn.Return<
  Record.Task,
  Failure | TaskResolution.Blocked,
  I['DecodingServices'] | I['EncodingServices'] | C['DecodingServices'] | C['EncodingServices']
> {
  return yield* Runtime.use(tx, (state) =>
    Effect.gen(function* () {
      const found = yield* Runtime.task(state, id)
      if (Option.isNone(found))
        return yield* new SessionError({
          reason: 'notFound',
          operation: 'task.migrate',
          message: 'Task is absent',
        })
      const previous = found.value
      if (
        previous.kind !== definition.kind ||
        previous.state.status === 'terminal' ||
        previous.state.status === 'completing'
      )
        return yield* TaskResolution.blocked(
          previous,
          'incompatible',
          'Only a matching live task can migrate',
        )
      const wire = yield* Task.migrate(definition, {
        version: previous.version,
        input: previous.input,
        checkpoint: previous.state.checkpoint,
      }).pipe(
        Effect.mapError((cause) =>
          TaskResolution.blocked(
            previous,
            migrationReason(cause),
            'Task migration could not prepare a compatible record',
            cause,
          ),
        ),
      )
      if (previous.version === definition.version) return previous
      const next = yield* Value.copy(Domain.Task, {
        ...previous,
        version: definition.version,
        input: wire.input,
        state: { ...previous.state, checkpoint: wire.checkpoint },
      })
      if (!TaskTransitions.canReplace(previous, next, true))
        return yield* TaskResolution.blocked(
          previous,
          'incompatible',
          'Migration changed immutable task state',
        )
      const result = yield* Value.copy(Domain.Task, next)
      yield* Runtime.active(state)
      state.tasks.set(id, next)
      return result
    }),
  )
})
