import * as Schema from 'effect/Schema'

/** Durable directory identity; native execution and provider session IDs remain external strings. */
export const SessionId = Schema.String.pipe(
  Schema.brand('@effect-harness/durable/Identity/SessionId'),
)
export type SessionId = typeof SessionId.Type
/** Persisted admission identity, encoded without normalization. */
export const RequestId = Schema.String.pipe(
  Schema.brand('@effect-harness/durable/Identity/RequestId'),
)
export type RequestId = typeof RequestId.Type
/** Correlates the generations of one durable run. */
export const RunId = Schema.String.pipe(Schema.brand('@effect-harness/durable/Identity/RunId'))
export type RunId = typeof RunId.Type
