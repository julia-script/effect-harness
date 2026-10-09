/** Stored record schemas and the fields used to select them. */
import * as Schema from 'effect/Schema'
import * as Domain from '../../Record.ts'
import type * as Data from '../Record.js'

export const Row = Schema.Union([
  Schema.TaggedStruct('conversation', { value: Domain.Conversation }),
  Schema.TaggedStruct('entry', {
    value: Schema.Struct({ entry: Domain.Entry, commitSeq: Domain.Seq }),
  }),
  Schema.TaggedStruct('task', { value: Domain.Task }),
  Schema.TaggedStruct('submission', { value: Domain.Submission }),
  Schema.TaggedStruct('document', { value: Domain.StoredDocument }),
])
export type Row = typeof Row.Type
export type Kind = Row['_tag']
export interface Filter {
  readonly conversationId?: number | undefined
  readonly kind?: string | undefined
  readonly ownerConversationId?: number | undefined
  readonly ownerTaskId?: number | undefined
  readonly status?: string | undefined
  readonly abortRequested?: boolean | undefined
  readonly background?: boolean | undefined
  readonly requestId?: string | undefined
  readonly address?: string | undefined
  readonly scope?: string | undefined
  readonly minId?: number | undefined
  readonly maxId?: number | undefined
  readonly at?: Data.DocumentPoint | undefined
}
export const idOf = (row: Row): number => {
  if (row._tag === 'entry') return row.value.entry.id
  if (row._tag === 'document') return row.value.record.id
  return row.value.id
}
export const indexOf = (row: Row) => {
  const base: {
    conversationId: number | null
    kind: string | null
    ownerConversationId: number | null
    ownerTaskId: number | null
    status: string | null
    background: boolean | null
    abortRequested: boolean | null
    requestId: string | null
    address: string | null
    scope: string | null
    createdAt: number | null
    retiredAt: number | null
  } = {
    conversationId: null,
    kind: null,
    ownerConversationId: null,
    ownerTaskId: null,
    status: null,
    background: null,
    abortRequested: null,
    requestId: null,
    address: null,
    scope: null,
    createdAt: null,
    retiredAt: null,
  }
  switch (row._tag) {
    case 'conversation':
      return {
        ...base,
        ownerConversationId: row.value.owner?.conversationId ?? null,
        ownerTaskId: row.value.owner?.taskId ?? null,
      }
    case 'entry':
      return { ...base, conversationId: row.value.entry.conversationId, kind: row.value.entry.kind }
    case 'task':
      return {
        ...base,
        conversationId: row.value.conversationId,
        kind: row.value.kind,
        ownerTaskId: row.value.owner ?? null,
        status: row.value.state.status,
        background: row.value.background,
        abortRequested: row.value.abortRequested,
      }
    case 'submission':
      return {
        ...base,
        conversationId: row.value.conversationId,
        status: row.value.status,
        requestId: row.value.requestId ?? null,
      }
    case 'document':
      return {
        ...base,
        kind: row.value.record.kind,
        address: Domain.addressKey(row.value.record),
        scope: Domain.scopeKey(row.value.record.scope),
        createdAt: row.value.record.createdAt,
        retiredAt: row.value.record.retiredAt ?? null,
      }
  }
}
export const matches = (row: Row, filter: Filter): boolean => {
  const id = idOf(row)
  if (filter.minId !== undefined && id < filter.minId) return false
  if (filter.maxId !== undefined && id > filter.maxId) return false
  const index = indexOf(row)
  if (filter.at !== undefined && row._tag === 'document') {
    if (filter.at === 'current') {
      if (row.value.record.retiredAt !== undefined) return false
    } else if (
      row.value.record.createdAt > filter.at ||
      (row.value.record.retiredAt !== undefined && row.value.record.retiredAt <= filter.at)
    )
      return false
  }
  for (const key of [
    'conversationId',
    'kind',
    'ownerConversationId',
    'ownerTaskId',
    'status',
    'abortRequested',
    'background',
    'requestId',
    'address',
    'scope',
  ] as const) {
    if (filter[key] !== undefined && filter[key] !== index[key]) return false
  }
  return true
}
