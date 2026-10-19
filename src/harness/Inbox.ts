/**
 * The built-in `pi.inbox` document, ported from pi's `harness/inbox.ts`: the submissions of one
 * conversation that wait for a boundary, in ID order. A boundary places them: every write, the
 * first or all steers, and at the final boundary the first or all follow-ups.
 */
import { Effect, Schema } from 'effect'
import * as Document from '../session/Document.ts'
import { EntryDraft } from '../storage/schema/Entry.ts'
import { SubmissionId } from '../storage/schema/Ids.ts'
import { InputEntry, UserInput } from './Submission.ts'

/** Queued user input, with the entry it is placed as when not `pi.user`. */
export const InputItem = Schema.Struct({
  id: SubmissionId,
  mode: Schema.Literals(['steer', 'followUp']),
  content: UserInput,
  entry: Schema.optionalKey(InputEntry),
})
export type InputItem = typeof InputItem.Type

/** A queued entry write. */
export const WriteItem = Schema.Struct({
  id: SubmissionId,
  mode: Schema.Literal('write'),
  entry: EntryDraft,
})
export type WriteItem = typeof WriteItem.Type

export const Item = Schema.Union([InputItem, WriteItem])
export type Item = typeof Item.Type

export const State = Schema.Struct({ items: Schema.Array(Item) })
export type State = typeof State.Type

/** One per conversation; every fork starts empty. Stored as a complete base whenever empty. */
export const document = Document.make<State>({
  kind: 'pi.inbox',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  initial: () => Effect.succeed({ items: [] }),
  checkpointWhen: (value) => Effect.succeed(value.items.length === 0),
})
