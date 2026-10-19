/**
 * The built-in `pi.live` document, ported from pi's `harness/live.ts`: run control of one
 * conversation, and the presentation of its running generation and tool round. Its Schemas live
 * in `ViewState`, which a client shares.
 */
import { Effect } from 'effect'
import * as Document from '../session/Document.ts'
import type { LiveState as State } from './ViewState.ts'

export {
  CompactionFailure,
  CompactionReason,
  CompactionStatus,
  Generation,
  LiveState as State,
  Run,
  ToolSlot,
} from './ViewState.ts'

/** One per conversation; every fork starts idle. */
export const document = Document.make<State>({
  kind: 'pi.live',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  initial: () => Effect.succeed({}),
  // A complete base whenever nothing streams into the document (spec §8.2): no generation and no
  // running call, so the delta chain spans at most one attempt or one round.
  checkpointWhen: (value) =>
    Effect.succeed(
      value.generation === undefined &&
        !(value.tools ?? []).some((slot) => slot.status === 'running'),
    ),
})
