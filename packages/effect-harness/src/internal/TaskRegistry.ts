/** Scheduler adapter over canonical registry snapshots, never a second mutable registry. */
import * as Effect from 'effect/Effect'
import * as Context from 'effect/Context'
import * as Cause from 'effect/Cause'
import * as Scope from 'effect/Scope'
import { TaskExecution } from '../TaskExecution.js'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Task from '../Task.js'
import * as TaskResolution from '../TaskResolution.js'
import type * as Record from '../Record.js'
import type * as Transaction from '../Transaction.js'
import { TaskError } from '../TaskError.js'
import { StorageError } from '../StorageError.js'
import { SessionError, type Failure } from '../SessionError.js'
import * as Runtime from './TransactionRuntime.js'
import * as Value from './DocumentValue.js'
import * as Domain from '../Record.js'
import * as TaskTransitions from './TaskTransitions.js'
import type * as RegistryScope from '../RegistryScope.js'

const nativeReason = (
  reason: Cause.Reason<unknown>,
): reason is Cause.Fail<StorageError | SessionError> =>
  Cause.isFailReason(reason) &&
  (Schema.is(StorageError)(reason.error) || Schema.is(SessionError)(reason.error))

/** Closed over exactly one typed registration's services. No Task.Any callback is invoked. */
export interface Executable {
  readonly initialize: (
    input: unknown,
  ) => Effect.Effect<
    { readonly input: Schema.Json; readonly checkpoint: Schema.Json },
    TaskError | Schema.SchemaError
  >
  readonly inspect: (
    record: Record.Task,
    failure?: TaskResolution.MigrationFailure,
  ) => Effect.Effect<TaskResolution.Inspection<Task.Any>>
  readonly upgrade: (
    tx: Transaction.Transaction,
    id: Record.TaskId,
  ) => Effect.Effect<Record.Task, Failure | TaskResolution.Blocked>
  readonly invoke: (
    record: Record.Task,
    mode: 'run' | 'abort',
  ) => Effect.Effect<PhaseResult, Failure, TaskExecution | Scope.Scope>
}
export type PhaseResult =
  | { readonly _tag: 'returned' }
  | { readonly _tag: 'interrupted' }
  | { readonly _tag: 'outcome'; readonly outcome: Record.TaskOutcome }

export const compile = <
  I extends Schema.Constraint,
  C extends Task.CheckpointCodec,
  S extends Schema.Constraint,
  F extends Schema.Constraint,
  H extends Task.Phases<I, C>,
  B extends Task.Handler<I, C> | undefined,
  R,
  EBuild,
>(
  self: Task.Task<I, C, S, F, H, B, R, EBuild>,
  captured: Context.Context<Task.ExecutionServices<I, C, S, F, H, B>>,
): Executable => {
  const fault = (message: string): PhaseResult => ({
    _tag: 'outcome',
    outcome: { status: 'faulted', error: { message } },
  })
  const normalize = (
    cause: Cause.Cause<unknown>,
  ): Effect.Effect<PhaseResult, Failure, F['EncodingServices']> => {
    if (Cause.hasInterruptsOnly(cause)) return Effect.succeed({ _tag: 'interrupted' })
    // Mixed causes retain their entire diagnostic; a defect must never become an abort.
    if (cause.reasons.length !== 1 || !Cause.isFailReason(cause.reasons[0]!))
      return Effect.succeed(fault(Cause.pretty(cause)))
    const reason = cause.reasons[0]
    if (nativeReason(reason)) return Effect.failCause(Cause.fromReasons([reason]))
    const failure = reason.error
    return Schema.encodeUnknownEffect(Schema.toCodecJson(self.failure))(failure).pipe(
      Effect.map((result): PhaseResult => ({
        _tag: 'outcome',
        outcome: { status: 'failed', error: { message: Cause.pretty(cause) }, result },
      })),
      Effect.catchCause((encoding) =>
        Effect.succeed(
          Cause.hasInterruptsOnly(encoding)
            ? ({ _tag: 'interrupted' } as const)
            : fault(Cause.pretty(cause)),
        ),
      ),
    )
  }
  return {
    initialize: (input) =>
      Schema.encodeUnknownEffect(Schema.toCodecJson(self.input))(input).pipe(
        Effect.flatMap((wire) => Task.decode(self.input, wire)),
        Effect.flatMap((value) => Task.initialize(self, value)),
        Effect.updateContext((_context: Context.Context<never>) => captured),
      ),
    inspect: (record, failure) =>
      TaskResolution.inspect(self, record, failure).pipe(
        Effect.updateContext((_context: Context.Context<never>) => captured),
      ),
    upgrade: (tx, id) =>
      upgrade(tx, self, id).pipe(
        Effect.updateContext((_context: Context.Context<never>) => captured),
      ),
    invoke: (record, mode) =>
      Effect.gen(function* () {
        const input = yield* Task.decode(self.input, record.input)
        const checkpoint = yield* Task.decodeCheckpoint(self.checkpoint, record.state.checkpoint)
        // The validated exhaustive discriminator selects a concrete H member. This is the
        // sole heterogeneous phase dispatch bridge; its E/R come from this exact Task's
        // phantom channels and are satisfied by captured services plus fresh invocation keys.
        const handler = (
          mode === 'abort' ? self.abort : self.phases[checkpoint.phase as keyof H]
        ) as
          | ((value: {
              readonly input: I['Type']
              readonly checkpoint: C['Type']
            }) => Effect.Effect<
              void,
              Task.Error<typeof self>,
              Task.ExecutionServices<I, C, S, F, H, B> | TaskExecution | Scope.Scope
            >)
          | undefined
        if (handler === undefined) {
          if (mode === 'abort')
            return {
              _tag: 'outcome',
              outcome: { status: 'aborted' },
            } as const
          return fault('Task checkpoint names an unavailable phase')
        }
        yield* Effect.suspend(() => handler({ input, checkpoint }))
        return { _tag: 'returned' } as const
      }).pipe(
        Effect.catchCause(normalize),
        Effect.updateContext((invocation: Context.Context<TaskExecution | Scope.Scope>) =>
          Context.merge(
            captured,
            Context.make(TaskExecution, Context.get(invocation, TaskExecution)).pipe(
              Context.add(Scope.Scope, Context.get(invocation, Scope.Scope)),
            ),
          ),
        ),
      ),
  }
}

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
