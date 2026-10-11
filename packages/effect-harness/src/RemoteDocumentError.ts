import * as Schema from 'effect/Schema'

/** Declared failures of remote document framing, continuity and operation lifetime. */
export class RemoteDocumentError extends Schema.TaggedError<RemoteDocumentError>()(
  'RemoteDocumentError',
  {
    reason: Schema.Literals([
      'unsupported',
      'invalid',
      'protocol',
      'continuity',
      'transport',
      'oversize',
      'expired',
      'closed',
      'revoked',
    ]),
    operation: Schema.String,
    message: Schema.String,
  },
) {}
