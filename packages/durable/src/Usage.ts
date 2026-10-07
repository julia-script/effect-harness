/**
 * Persisted model and tool accounting documents.
 */
import { constTrue } from 'effect/Function'
import type { StorageError } from './StorageError.ts'
import * as Option from 'effect/Option'
import * as Usage from '@effect-harness/harness/Usage'
import * as Effect from 'effect/Effect'
import * as Document from './Document.ts'
import type * as Record from './Record.ts'
import type * as Session from './Session.ts'

/**
 * Conversation accounting document definition.
 *
 * @category models
 */
export const UsageDoc = Document.defineUnsafe({
  kind: 'harness.usage',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: Document.jsonObjectCodec(Usage.State),
  initial: Usage.empty,
  checkpointWhen: constTrue,
})

/**
 * Records one usage increment in the conversation ledger.
 *
 * @category combinators
 */
export const record = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  conversationId: Record.ConversationId,
  bucket: keyof Usage.State,
  key: string,
  usage: Usage.Usage,
): Effect.fn.Return<void, StorageError> {
  const state = yield* tx.doc(UsageDoc, { owner: conversationId })
  const totals = state[bucket]
  const previous = Object.hasOwn(totals, key) ? totals[key] : undefined
  // Draft assignment defines own properties, including __proto__ and inherited Object names.
  totals[key] = Usage.add(previous ?? Usage.zero(), usage)
})

/**
 * Includes only each conversation's own ledger; inherited transcript entries are never counted again.
 *
 * @category combinators
 */
export const sessionTotals = Effect.fnUntraced(function* (
  session: Session.Service,
): Effect.fn.Return<Usage.State, StorageError> {
  const states: Array<Usage.State> = []
  let cursor: Record.Cursor | undefined
  do {
    const page = yield* session.scanConversations({}, 100, cursor)
    // Independent own ledgers share one authoritative snapshot per compatible page batch.
    const snapshots = yield* Effect.forEach(
      page.items,
      (conversation) => session.snapshot(UsageDoc, { owner: conversation.id }),
      { concurrency: 16 },
    )
    for (const snapshot of snapshots) if (Option.isSome(snapshot)) states.push(snapshot.value.value)
    cursor = page.next
  } while (cursor !== undefined)
  return Usage.sum(states)
})
