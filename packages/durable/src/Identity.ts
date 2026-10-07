/**
 * Nominal session, request and run identifiers.
 */
import * as Schema from 'effect/Schema'

/**
 * Durable directory identity; native execution and provider session IDs remain external strings.
 *
 * @category schemas
 */
export const SessionId = Schema.String.pipe(
  Schema.brand('@effect-harness/durable/Identity/SessionId'),
)
/**
 * Decoded value validated by the `SessionId` schema.
 *
 * @category models
 */
export type SessionId = typeof SessionId.Type
/**
 * Persisted admission identity, encoded without normalization.
 *
 * @category schemas
 */
export const RequestId = Schema.String.pipe(
  Schema.brand('@effect-harness/durable/Identity/RequestId'),
)
/**
 * Decoded value validated by the `RequestId` schema.
 *
 * @category models
 */
export type RequestId = typeof RequestId.Type
/**
 * Correlates the generations of one durable run.
 *
 * @category schemas
 */
export const RunId = Schema.String.pipe(Schema.brand('@effect-harness/durable/Identity/RunId'))
/**
 * Decoded value validated by the `RunId` schema.
 *
 * @category models
 */
export type RunId = typeof RunId.Type
