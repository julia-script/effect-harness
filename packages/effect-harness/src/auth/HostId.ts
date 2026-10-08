/**
 * Nominal host identities owned by credential storage.
 */
import * as Schema from 'effect/Schema'

/**
 * A nonempty persisted host identity.
 *
 * @category models
 */
export const HostId = Schema.NonEmptyString.pipe(Schema.brand('@effect-harness/auth/HostId'))

/**
 * A host identity owned by credential storage.
 *
 * @category models
 */
export type HostId = typeof HostId.Type
