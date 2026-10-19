/** Semantic task contract failures, distinct from declared domain failures. */
import * as Schema from 'effect/Schema'
import type { Failure as SessionFailure } from './SessionError.js'

export class TaskError extends Schema.TaggedError<TaskError>()('TaskError', {
  reason: Schema.Literals([
    'invalidDefinition',
    'invalidCheckpoint',
    'missingMigration',
    'newerVersion',
    'migrationFailed',
    'revoked',
    'notFound',
    'noProgress',
    'faulted',
  ]),
  operation: Schema.String,
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

/** Invocation operations retain storage, session and schema failures. */
export type Failure = TaskError | SessionFailure
