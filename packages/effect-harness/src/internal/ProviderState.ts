import * as Document from '../Document.js'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import { SessionError } from '../SessionError.js'
import type * as Record from '../Record.js'

/** Isolated boundary to Web Crypto, available in supported Node/Bun/browser runtimes. */
export const fresh = Effect.try({
  try: () => globalThis.crypto.randomUUID(),
  catch: (cause) =>
    new SessionError({
      reason: 'invalid',
      operation: 'provider.affinity',
      message: 'Could not allocate provider affinity',
      cause,
    }),
})
export const document = Document.define({
  kind: 'harness.provider',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: Schema.Struct({ id: Schema.NonEmptyString }),
  initial: (seed) => ({ id: typeof seed === 'string' ? seed : '' }),
})
export const target = (conversationId: Record.ConversationId): Document.Target => ({
  scope: { _tag: 'conversation', conversationId },
})
