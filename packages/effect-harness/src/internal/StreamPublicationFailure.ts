import * as Schema from 'effect/Schema'
import * as Usage from '../Usage.js'
import { NativeWriteFailure } from './NativeWriteFailure.js'

/** Known observation carried only by this invocation's escaping native publication rejection. */
export class StreamPublicationFailure extends Schema.TaggedError<StreamPublicationFailure>()(
  'StreamPublicationFailure',
  {
    error: NativeWriteFailure,
    requestId: Schema.String,
    attempt: Usage.AttemptId,
    usage: Usage.RecordSchema,
  },
) {}
