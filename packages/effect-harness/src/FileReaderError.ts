import * as Schema from 'effect/Schema'

/** Semantic failures of scoped positional readers, including native backend failures. */
export class FileReaderError extends Schema.TaggedError<FileReaderError>()('FileReaderError', {
  reason: Schema.Literals(['invalid', 'unsupported', 'notFile', 'closed', 'filesystem']),
  operation: Schema.String,
  path: Schema.String,
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}
