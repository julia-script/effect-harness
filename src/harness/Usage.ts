/**
 * The built-in `$usage` document, ported from pi's `harness/usage.ts`: the ledger of one
 * conversation's own spend. Every built-in writer of assistant entries, and every summarization
 * attempt of a compaction, adds its usage in the commit that records it.
 */
import { Effect } from 'effect'
import { cast } from 'effect/Function'
import type { Draft } from '../delta/Tracker.ts'
import type { Usage } from '../model/Message.ts'
import * as Document from '../session/Document.ts'
import type { Tx } from '../session/Session.ts'
import type { ConversationId } from '../storage/schema/Ids.ts'
import { strictJson } from '../internal/strictJson.ts'
import type { UsageState as State } from '../protocol/ViewState.ts'

export { UsageState as State } from '../protocol/ViewState.ts'

/** One per conversation; every fork starts empty. */
export const document = Document.make<State>({
  kind: '$usage',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  initial: () => Effect.succeed({ models: {}, tools: {} }),
  checkpointWhen: () => Effect.succeed(true),
})

/** Add every counter of `usage` to `total`; optional counters once either side reports them. */
export const add = (total: Draft<Usage>, usage: Usage): void => {
  total.input += usage.input
  total.output += usage.output
  total.cacheRead += usage.cacheRead
  total.cacheWrite += usage.cacheWrite
  total.totalTokens += usage.totalTokens
  if (usage.cacheWrite1h !== undefined)
    total.cacheWrite1h = (total.cacheWrite1h ?? 0) + usage.cacheWrite1h
  if (usage.reasoning !== undefined) total.reasoning = (total.reasoning ?? 0) + usage.reasoning
  total.cost.input += usage.cost.input
  total.cost.output += usage.cost.output
  total.cost.cacheRead += usage.cost.cacheRead
  total.cost.cacheWrite += usage.cost.cacheWrite
  total.cost.total += usage.cost.total
}

/** Add `usage` to one bucket of the conversation's ledger, in the commit that records it. */
export const record = (
  tx: Tx,
  conversationId: ConversationId,
  bucket: keyof State,
  key: string,
  usage: Usage,
) =>
  Effect.map(tx.doc(document, conversationId), (state) => {
    const totals = state[bucket]
    // Own keys only: a tool may be called `toString`.
    const total = Object.hasOwn(totals, key) ? totals[key] : undefined
    if (total === undefined) totals[key] = cast(strictJson({ ...usage }))
    else add(total, usage)
  })

/** The sum of ledgers, bucket by bucket and key by key. */
export const sum = (states: ReadonlyArray<State>): State => {
  const total: Draft<State> = { models: {}, tools: {} }
  for (const state of states) {
    for (const bucket of ['models', 'tools'] as const) {
      const totals = total[bucket]
      for (const [key, usage] of Object.entries(state[bucket])) {
        const current = Object.hasOwn(totals, key) ? totals[key] : undefined
        if (current === undefined) totals[key] = cast(strictJson({ ...usage }))
        else add(current, usage)
      }
    }
  }
  return total
}
