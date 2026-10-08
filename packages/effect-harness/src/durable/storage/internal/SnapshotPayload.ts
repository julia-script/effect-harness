/** Shared state and retained-frame payload for durable snapshot adapters. */
import * as Schema from 'effect/Schema'
import * as Record from '../../Record.ts'
export const SnapshotPayload = Schema.Struct({
  state: Record.State,
  frames: Schema.Array(Record.Frame),
})
