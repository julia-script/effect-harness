/**
 * Nominal session, request and run identifiers.
 *
 * @since 0.0.0
 */
import * as Schema from 'effect/Schema'

/**
 * Durable directory identity; native execution and provider session IDs remain external strings.
 *
 * @category schemas
 * @since 0.0.0
 */
export const SessionId = Schema.String.pipe(
  Schema.brand('@effect-harness/durable/Identity/SessionId'),
)
/**
 * SessionId contract.
 *
 * @category models
 * @since 0.0.0
 */
export type SessionId = typeof SessionId.Type
/**
 * Persisted admission identity, encoded without normalization.
 *
 * @category schemas
 * @since 0.0.0
 */
export const RequestId = Schema.String.pipe(
  Schema.brand('@effect-harness/durable/Identity/RequestId'),
)
/**
 * RequestId contract.
 *
 * @category models
 * @since 0.0.0
 */
export type RequestId = typeof RequestId.Type
/**
 * Correlates the generations of one durable run.
 *
 * @category schemas
 * @since 0.0.0
 */
export const RunId = Schema.String.pipe(Schema.brand('@effect-harness/durable/Identity/RunId'))
/**
 * RunId contract.
 *
 * @category models
 * @since 0.0.0
 */
export type RunId = typeof RunId.Type
