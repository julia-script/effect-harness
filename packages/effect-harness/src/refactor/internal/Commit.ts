/** Adapts stored rows to the existing pure commit validator. */
import * as Records from '../../internal/records.ts'
import type * as LegacyError from '../../StorageError.ts'
import * as Errors from '../StorageError.js'
import type * as Metadata from './Metadata.js'
import type * as Row from './Row.js'

export const mapError = (operation: string) => (cause: LegacyError.StorageError) => {
  const tag = cause.reason._tag
  let reason: Errors.StorageError['reason'] = 'invalid'
  if (tag === 'ConflictError') reason = 'conflict'
  if (tag === 'NotFoundError') reason = 'notFound'
  if (tag === 'CorruptError') reason = 'corrupt'
  return Errors.make(reason, operation, cause.message, cause)
}
export const rowsOf = (state: Records.State): Array<Row.Row> => [
  ...state.conversations.map((value): Row.Row => ({ _tag: 'conversation', value })),
  ...state.entries.map((value): Row.Row => ({ _tag: 'entry', value })),
  ...state.tasks.map((value): Row.Row => ({ _tag: 'task', value })),
  ...state.submissions.map((value): Row.Row => ({ _tag: 'submission', value })),
  ...state.documents.map((value): Row.Row => ({ _tag: 'document', value })),
]
export const stateOf = (metadata: Metadata.Metadata, rows: Iterable<Row.Row>): Records.State => {
  let state: Records.State = {
    ...Records.emptyState(),
    nextId: metadata.nextId,
    nextSeq: metadata.nextSeq,
  }
  for (const row of rows) {
    switch (row._tag) {
      case 'conversation':
        state = { ...state, conversations: [...state.conversations, row.value] }
        break
      case 'entry':
        state = { ...state, entries: [...state.entries, row.value] }
        break
      case 'task':
        state = { ...state, tasks: [...state.tasks, row.value] }
        break
      case 'submission':
        state = { ...state, submissions: [...state.submissions, row.value] }
        break
      case 'document':
        state = { ...state, documents: [...state.documents, row.value] }
        break
    }
  }
  return state
}
