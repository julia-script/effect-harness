import * as Schema from 'effect/Schema'

/** Invalid host policy, a failed input copy, or exhaustion of the safe revision counter. */
export class SettingsError extends Schema.TaggedError<SettingsError>()('SettingsError', {
  reason: Schema.Literals(['invalid', 'copy', 'revision']),
  operation: Schema.Literals(['make', 'update']),
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}
