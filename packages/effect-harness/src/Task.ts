/** Named, versioned task definitions and their durable phase transitions. */
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import { StorageError } from './StorageError.ts'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Record from './Record.ts'
import type * as Session from './internal/Session.ts'
import { TaskRuntime } from './TaskRuntime.ts'

/** Specialize the persisted outcome codec for a task's result schema. */
export const outcome = <S extends Schema.Constraint>(result: S) =>
  Schema.Union([
    Schema.Struct({ status: Schema.tag('completed'), result }),
    Schema.Struct({
      status: Schema.tag('failed'),
      error: Schema.Struct({ message: Schema.String }),
      result: Schema.optionalKey(result),
    }),
    Schema.Struct({
      status: Schema.tag('aborted'),
      reason: Schema.optionalKey(Schema.String),
      result: Schema.optionalKey(result),
    }),
    Schema.Struct({ status: Schema.tag('orphaned'), reason: Schema.String }),
    Schema.Struct({
      status: Schema.tag('faulted'),
      error: Schema.Struct({ message: Schema.String }),
    }),
  ])
/** JSON outcome codec usable by persistence and remote application transports. */
export const Outcome = Record.TaskOutcome

/** A persisted task completion. */
export type Outcome<A = Schema.Json> =
  | { readonly status: 'completed'; readonly result: A }
  | { readonly status: 'failed'; readonly error: { readonly message: string }; readonly result?: A }
  | { readonly status: 'aborted'; readonly reason?: string; readonly result?: A }
  | { readonly status: 'orphaned'; readonly reason: string }
  | { readonly status: 'faulted'; readonly error: { readonly message: string } }

/** Joining all children preserves their individual outcomes. */
export const JoinPolicy = Schema.Literals(['failFast', 'allSettled'])
export type JoinPolicy = typeof JoinPolicy.Type

/** The state a phase atomically commits after completing its external work. */
export type Transition<S, A> =
  | { readonly status: 'running'; readonly checkpoint: S }
  | {
      readonly status: 'waiting'
      readonly checkpoint: S
      readonly on: ReadonlyArray<Record.TaskId>
      readonly policy: JoinPolicy
    }
  | { readonly status: 'terminal'; readonly outcome: Outcome<A> }

/** Specialize the phase-transition codec at the task-definition boundary. */
export const transition = <S extends Schema.Constraint, A extends Schema.Constraint>(
  checkpoint: S,
  result: A,
) =>
  Schema.Union([
    Schema.Struct({ status: Schema.tag('running'), checkpoint }),
    Schema.Struct({
      status: Schema.tag('waiting'),
      checkpoint,
      on: Schema.Array(Record.TaskId),
      policy: JoinPolicy,
    }),
    Schema.Struct({ status: Schema.tag('terminal'), outcome: outcome(result) }),
  ])
/** JSON transition codec for a storage or transport boundary. */
export const Transition = transition(Schema.Json, Schema.Json)

/** Continue at a replacement durable checkpoint. */
export const continueWith = <S>(checkpoint: S): Transition<S, never> => ({
  status: 'running',
  checkpoint,
})
/** Park the invocation until all referenced tasks finish. */
export const wait = <S>(
  checkpoint: S,
  on: ReadonlyArray<Record.TaskId>,
  policy: JoinPolicy = 'allSettled',
): Transition<S, never> => ({ status: 'waiting', checkpoint, on, policy })
/** Complete the task, holding its outcome until ordinary children finish. */
export const complete = <A>(result: A): Transition<never, A> => ({
  status: 'terminal',
  outcome: { status: 'completed', result },
})
/** End a task with an application failure. */
export const fail = (message: string): Transition<never, never> => ({
  status: 'terminal',
  outcome: { status: 'failed', error: { message } },
})
/** Complete cancellation after the task's abort handler has cleaned up. */
export const aborted = (reason?: string): Transition<never, never> => ({
  status: 'terminal',
  outcome: { status: 'aborted', ...(reason === undefined ? {} : { reason }) },
})

/** Phase input decoded against the installed task definition. */
export interface Snapshot<I, S> {
  readonly id: Record.TaskId
  readonly conversationId: Record.ConversationId
  readonly input: I
  readonly checkpoint: S
  readonly abortRequested: boolean
}

/** Decode the immutable data passed into a phase handler. */
export const snapshot = <I extends Schema.Constraint, S extends Schema.Constraint>(
  input: I,
  checkpoint: S,
) =>
  Schema.Struct({
    id: Record.TaskId,
    conversationId: Record.ConversationId,
    input,
    checkpoint,
    abortRequested: Schema.Boolean,
  })

/** Typed declaration; host dependencies are captured when the definition is bound. */
export interface Definition<
  I extends Schema.Constraint,
  S extends Schema.ConstraintCodec<{ readonly phase: string }, Schema.Json, unknown, unknown>,
  A extends Schema.Constraint,
  E,
  R,
> {
  readonly name: string
  readonly version: number
  readonly input: I
  readonly checkpoint: S
  readonly result: A
  readonly initial: (input: I['Type']) => S['Type']
  readonly run: (
    task: Snapshot<I['Type'], S['Type']>,
  ) => Effect.Effect<Transition<S['Type'], A['Type']> | void, E, R | TaskRuntime>
  readonly abort?: (
    task: Snapshot<I['Type'], S['Type']>,
  ) => Effect.Effect<Transition<S['Type'], A['Type']> | void, E, R | TaskRuntime>
}

/** Declare a task without acquiring resources or capturing a runtime. */
export const define = <
  I extends Schema.Constraint,
  S extends Schema.ConstraintCodec<{ readonly phase: string }, Schema.Json, unknown, unknown>,
  A extends Schema.Constraint,
  E,
  R,
>(
  definition: Definition<I, S, A, E, R>,
): Definition<I, S, A, E, R> => definition

export class TaskError extends Schema.TaggedError<TaskError>('effect-harness/Task/TaskError')(
  'TaskError',
  { message: Schema.String },
) {}

/** Validated executable adapter used by the scheduler; schema environments have been captured. */
export interface BoundDefinition {
  readonly name: string
  readonly version: number
  /** Internal domain settlement shares the terminal commit; no external work belongs here. */
  readonly onTerminal?: (
    tx: Session.Transaction,
    task: Record.Task,
    outcome: Outcome<Schema.Json>,
  ) => Effect.Effect<void, StorageError>
  readonly prepare: (
    input: unknown,
  ) => Effect.Effect<
    { readonly input: Schema.Json; readonly checkpoint: Schema.Json },
    Schema.SchemaError
  >
  readonly run: (
    record: Record.Task,
    abort: boolean,
  ) => Effect.Effect<
    Transition<Schema.Json, Schema.Json> | void,
    TaskError | StorageError | Schema.SchemaError,
    TaskRuntime
  >
}

/**
 * Capture host and codec services once; invocation identity is supplied separately by the scheduler.
 *
 * Schema encodings must be JSON values, so invalid custom encodings fail before admission.
 */
export const bind = Effect.fn('Task.bind')(function* <
  I extends Schema.Constraint,
  S extends Schema.ConstraintCodec<{ readonly phase: string }, Schema.Json, unknown, unknown>,
  A extends Schema.Constraint,
  E,
  R,
>(definition: Definition<I, S, A, E, R>) {
  const context = yield* Effect.context<
    | Exclude<R, TaskRuntime>
    | I['DecodingServices']
    | I['EncodingServices']
    | S['DecodingServices']
    | S['EncodingServices']
    | A['EncodingServices']
  >()
  const encodeJson = <T extends Schema.Constraint>(schema: T, value: T['Type']) =>
    Schema.encodeEffect(schema)(value).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)))
  const prepare = Effect.fn('Task.prepare')(function* (value: unknown) {
    const input = yield* Schema.decodeUnknownEffect(definition.input)(value)
    return {
      input: yield* encodeJson(definition.input, input),
      checkpoint: yield* encodeJson(definition.checkpoint, definition.initial(input)),
    }
  })
  const run = Effect.fn('Task.run')(function* (record: Record.Task, abort: boolean) {
    const input = yield* Schema.decodeEffect(definition.input)(record.input)
    const checkpoint = yield* Schema.decodeUnknownEffect(definition.checkpoint)(
      record.state.checkpoint,
    )
    const snapshot: Snapshot<I['Type'], S['Type']> = {
      id: record.id,
      conversationId: record.conversationId,
      input,
      checkpoint,
      abortRequested: record.abortRequested,
    }
    const transition = yield* abort
      ? (definition.abort?.(snapshot) ?? Effect.succeed(aborted()))
      : definition.run(snapshot)
    if (transition === undefined) return undefined
    if (transition.status === 'terminal') {
      const outcome = transition.outcome
      if (outcome.status === 'completed')
        return {
          status: 'terminal',
          outcome: {
            status: 'completed',
            result: yield* encodeJson(definition.result, outcome.result),
          },
        } as const
      if (
        (outcome.status === 'failed' || outcome.status === 'aborted') &&
        outcome.result !== undefined
      ) {
        return {
          status: 'terminal',
          outcome: { ...outcome, result: yield* encodeJson(definition.result, outcome.result) },
        } as const
      }
      if (outcome.status === 'failed' || outcome.status === 'aborted') {
        const { result: _result, ...rest } = outcome
        return { status: 'terminal', outcome: rest } as const
      }
      return { status: 'terminal', outcome } as const
    }
    return {
      ...transition,
      checkpoint: yield* encodeJson(definition.checkpoint, transition.checkpoint),
    }
  })
  const bound: BoundDefinition = {
    name: definition.name,
    version: definition.version,
    prepare: (input) => prepare(input).pipe(Effect.provideContext(context)),
    run: Effect.fn('Task.boundRun')(function* (record, abort) {
      const invocation = yield* TaskRuntime
      // effect-nit-allow P1-no-unproven-as: TypeScript cannot prove R is covered by
      // Exclude<R, TaskRuntime> | TaskRuntime. Host services were captured above;
      // this exact invocation service supplies the excluded member before dispatch.
      const completeContext = Context.add(context, TaskRuntime, invocation) as Context.Context<
        | R
        | TaskRuntime
        | I['DecodingServices']
        | I['EncodingServices']
        | S['DecodingServices']
        | S['EncodingServices']
        | A['EncodingServices']
      >
      return yield* run(record, abort).pipe(
        Effect.provideContext(completeContext),
        Effect.catchCause((cause): Effect.Effect<never, TaskError | StorageError> => {
          if (Cause.hasInterrupts(cause)) return Effect.interrupt
          const failure = Cause.findErrorOption(cause)
          if (Option.isSome(failure) && failure.value instanceof StorageError)
            return Effect.fail(failure.value)
          return Effect.fail(new TaskError({ message: Cause.pretty(cause) }))
        }),
      )
    }),
  }
  return bound
})
