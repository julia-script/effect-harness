/** Read-only compatibility checks. Inspection never initializes, migrates, or invokes task handlers. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Record from './Record.js'
import * as Task from './Task.js'
import * as TaskPolicy from './TaskPolicy.js'

export class Blocked extends Schema.TaggedError<Blocked>()('TaskResolutionBlocked', {
  reason: Schema.Literals([
    'missingDefinition',
    'newerVersion',
    'missingMigration',
    'incompatible',
    'migrationFailed',
    'unsupportedFormat',
    'disabledDefinition',
  ]),
  taskId: Record.TaskId,
  kind: Schema.String,
  storedVersion: Schema.Finite,
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

/** A failed attempt belongs to an exact definition identity; replacement definitions may retry. */
export interface MigrationFailure<T extends Task.Any = Task.Any> {
  readonly taskId: Record.TaskId
  readonly definition: T
  readonly error: Blocked
}
export type Inspection<T extends Task.Any> =
  | { readonly _tag: 'blocked'; readonly error: Blocked }
  | { readonly _tag: 'inactive'; readonly record: Record.Task }
  | { readonly _tag: 'migrationRequired'; readonly record: Record.Task; readonly definition: T }
  | {
      readonly _tag: 'compatible'
      readonly record: Record.Task
      readonly definition: T
      readonly input: Task.Input<T>
      readonly checkpoint: Task.Checkpoint<T>
    }
export type DecodingServices<T extends Task.Any> =
  | T['input']['DecodingServices']
  | T['checkpoint']['DecodingServices']

export const blocked = (
  record: Record.Task,
  reason: Blocked['reason'],
  message: string,
  cause?: unknown,
): Blocked =>
  new Blocked({
    reason,
    taskId: record.id,
    kind: record.kind,
    storedVersion: record.version,
    message,
    ...(cause === undefined ? {} : { cause }),
  })

/** Missing definitions require no application codec services. Terminal/held tasks stay inactive. */
export const inspectMissing = (
  input: Record.Task,
): Effect.Effect<
  | { readonly _tag: 'blocked'; readonly error: Blocked }
  | { readonly _tag: 'inactive'; readonly record: Record.Task }
> =>
  Effect.suspend(() => {
    if (
      input.schedulerFormat !== undefined &&
      !Schema.is(TaskPolicy.SchedulerFormatSchema)(input.schedulerFormat)
    )
      return Effect.succeed({
        _tag: 'blocked' as const,
        error: blocked(input, 'unsupportedFormat', 'Task requires unsupported scheduler semantics'),
      })
    return Task.decode(Record.Task, input).pipe(
      Effect.map((record) =>
        record.state.status === 'terminal' || record.state.status === 'completing'
          ? { _tag: 'inactive' as const, record }
          : {
              _tag: 'blocked' as const,
              error: blocked(record, 'missingDefinition', 'Task definition is absent'),
            },
      ),
      Effect.catch((cause) =>
        Effect.succeed({
          _tag: 'blocked' as const,
          error: blocked(input, 'incompatible', 'Stored task record is incompatible', cause),
        }),
      ),
    )
  })

/**
 * Inspect one pinned definition. Exact-version codecs may require their declared services;
 * older wire payloads are not decoded with newer codecs. Waiting/abort readiness is a
 * scheduler decision, so compatible does not imply runnable. Held/terminal receipts are
 * inactive and never migrated. Supply the scheduler's prior failure to report it without retry.
 */
export const inspect = <T extends Task.Any>(
  definition: T | undefined,
  input: Record.Task,
  failure?: MigrationFailure,
): Effect.Effect<Inspection<T>, never, DecodingServices<T>> => {
  const check = Effect.gen(function* () {
    if (
      input.schedulerFormat !== undefined &&
      !Schema.is(TaskPolicy.SchedulerFormatSchema)(input.schedulerFormat)
    )
      return {
        _tag: 'blocked',
        error: blocked(input, 'unsupportedFormat', 'Task requires unsupported scheduler semantics'),
      } as const
    if (definition === undefined) return yield* inspectMissing(input)
    const record = yield* Task.decode(Record.Task, input)
    if (record.state.status === 'terminal' || record.state.status === 'completing')
      return { _tag: 'inactive', record } as const
    yield* Task.validate(definition)
    if (definition.kind !== record.kind)
      return {
        _tag: 'blocked',
        error: blocked(record, 'incompatible', 'Task kind differs from its definition'),
      } as const
    if (record.version > definition.version)
      return {
        _tag: 'blocked',
        error: blocked(record, 'newerVersion', 'Stored task requires a newer definition'),
      } as const
    if (record.version < definition.version) {
      if (failure?.taskId === record.id && failure.definition === definition)
        return { _tag: 'blocked', error: failure.error } as const
      if (definition.migrate === undefined)
        return {
          _tag: 'blocked',
          error: blocked(record, 'missingMigration', 'Older task requires a direct migration'),
        } as const
      return { _tag: 'migrationRequired', record, definition } as const
    }
    const decodedInput = yield* Task.decode<T['input']>(definition.input, record.input)
    const checkpoint = yield* Task.decodeCheckpoint<T['checkpoint']>(
      definition.checkpoint,
      record.state.checkpoint,
    )
    return { _tag: 'compatible', record, definition, input: decodedInput, checkpoint } as const
  })
  return check.pipe(
    Effect.catch((cause) =>
      Effect.succeed({
        _tag: 'blocked' as const,
        error: blocked(
          input,
          'incompatible',
          'Task definition or stored payload is incompatible',
          cause,
        ),
      }),
    ),
  )
}
