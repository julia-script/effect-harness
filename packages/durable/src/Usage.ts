import * as Totals from '@effect-harness/harness/Usage'
import * as Effect from 'effect/Effect'
import * as Document from './Document.ts'
import type * as Record from './Record.ts'
import type * as Session from './Session.ts'

/** A conversation's own spend persists through reset; forks begin with an empty ledger. */
export const UsageDoc = Document.defineUnsafe({
  kind: 'harness.usage',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: Document.jsonObjectCodec(Totals.State),
  initial: Totals.empty,
  checkpointWhen: () => true,
})

export const record = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  conversationId: Record.ConversationId,
  bucket: keyof Totals.State,
  key: string,
  usage: Totals.Usage,
) {
  const state = yield* tx.doc(UsageDoc, { owner: conversationId })
  const totals = state[bucket]
  const previous = Object.hasOwn(totals, key) ? totals[key] : undefined
  // Draft assignment defines own properties, including __proto__ and inherited Object names.
  totals[key] = Totals.add(previous ?? Totals.zero(), usage)
})

/** Include only each conversation's own ledger; inherited transcript entries are never counted again. */
export const sessionTotals = Effect.fnUntraced(function* (session: Session.Service) {
  const states: Totals.State[] = []
  let cursor: Record.Cursor | undefined
  do {
    const page = yield* session.scanConversations({}, 100, cursor)
    // Independent own ledgers share one authoritative snapshot per compatible page batch.
    const snapshots = yield* Effect.forEach(
      page.items,
      (conversation) => session.snapshot(UsageDoc, { owner: conversation.id }),
      { concurrency: 16 },
    )
    for (const snapshot of snapshots) if (snapshot !== undefined) states.push(snapshot.value)
    cursor = page.next
  } while (cursor !== undefined)
  return Totals.sum(states)
})
