/** Summary-only native wire policy. It never changes the conversation's provider affinity. */
import * as Context from 'effect/Context'
import * as Schema from 'effect/Schema'
import * as Identity from './Identity.js'
import * as Usage from './Usage.js'

export const PolicySchema = Schema.Struct({
  requestId: Identity.RequestId,
  attempt: Usage.AttemptId,
  maxOutputTokens: Schema.Int.check(
    Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
  ),
  cacheRetention: Schema.Literal('none'),
  deferred: Schema.Literal(false),
})
export type Policy = typeof PolicySchema.Type
/** Adapters omit explicit caching directives; remote automatic caching is outside this contract. */
export class CompactionRequest extends Context.Service<CompactionRequest, Policy>()(
  'effect-harness/CompactionRequest',
) {}
