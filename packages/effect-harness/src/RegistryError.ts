/** Semantic registration, selection and lease failures. Layer failures retain their own types. */
import * as Schema from 'effect/Schema'

export class RegistryError extends Schema.TaggedError<RegistryError>()('RegistryError', {
  reason: Schema.Literals([
    'invalidRegistration',
    'duplicateTool',
    'duplicateTask',
    'invalidTask',
    'invalidSection',
    'notFound',
    'disabled',
    'incompatibleVersion',
    'revoked',
  ]),
  operation: Schema.String,
  message: Schema.String,
  registration: Schema.optionalKey(Schema.String),
  cause: Schema.optionalKey(Schema.Defect()),
}) {}
