import * as Schema from 'effect/Schema'
import type * as Session from './Session.js'

/** Invalid bounds or canonical facts; storage and schema failures retain their native identity. */
export class ContextProjectionError extends Schema.TaggedError<ContextProjectionError>()(
  'ContextProjectionError',
  {
    reason: Schema.Literals(['notFound', 'invalid']),
    message: Schema.String,
  },
) {}

export type Failure = ContextProjectionError | Session.Failure
