/** Serializable snapshots and committed changes for application-owned transports. */
import * as Schema from 'effect/Schema'
import * as Record from './Record.ts'

export const Snapshot = Schema.Struct({
  revision: Record.JournalCursor,
  conversation: Record.Conversation,
  entries: Schema.Array(Record.Entry),
  tasks: Schema.Array(Record.Task),
  submissions: Schema.Array(Record.Submission),
  documents: Schema.Array(
    Schema.Struct({ record: Record.Document, version: Schema.Int, value: Schema.JsonObject }),
  ),
})
export type Snapshot = typeof Snapshot.Type
export const Change = Schema.Union([
  Schema.TaggedStruct('snapshot', { value: Snapshot }),
  Schema.TaggedStruct('reset', { value: Snapshot }),
  Schema.TaggedStruct('commit', { frame: Record.Frame }),
])
export type Change = typeof Change.Type
