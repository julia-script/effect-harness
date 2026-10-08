/**
 * Extension registry failures with their original causes.
 */
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'
/**
 * Semantic registry failure with its retained cause.
 *
 * @category errors
 */
export class RegistryFailure extends Schema.TaggedError<RegistryFailure>(
  '@effect-harness/harness/RegistryError/RegistryFailure',
)('RegistryFailure', { message: Schema.String, cause: SchemaField.optional(Schema.Defect()) }) {}
/**
 * Schema for registry error reason.
 *
 * @category schemas
 */
export const RegistryErrorReason = Schema.Union([RegistryFailure])
/**
 * Decoded value validated by the `RegistryErrorReason` schema.
 *
 * @category models
 */
export type RegistryErrorReason = typeof RegistryErrorReason.Type
/**
 * Semantic registry error with its retained cause.
 *
 * @category errors
 */
export class RegistryError extends Schema.TaggedError<RegistryError>(
  '@effect-harness/harness/RegistryError/RegistryError',
)('RegistryError', { reason: RegistryErrorReason }) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
}
