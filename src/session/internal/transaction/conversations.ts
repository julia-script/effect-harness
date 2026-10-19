import { Effect } from 'effect'
import type { ConversationRecord } from '../../../storage/schema/Conversation.ts'
import type { DocumentPoint, DocumentRecord } from '../../../storage/schema/Document.ts'
import { DocumentId, ROOT_CONVERSATION_ID, ConversationId } from '../../../storage/schema/Ids.ts'
import type { EntryId } from '../../../storage/schema/Ids.ts'
import type { Cursor, DocumentQuery } from '../../../storage/schema/Scan.ts'
import type { ConversationOwnership } from '../../Session.ts'
import { addressId } from '../documents.ts'
import { assertOpen, currentTask, rejected } from './state.ts'
import type { ForkCopy, TxState } from './state.ts'

const SCAN_PAGE_SIZE = 256

type ConversationParent = NonNullable<ConversationRecord['parent']>
type ForkPolicy = 'asOf' | 'current'
type ConversationDocument = Extract<DocumentRecord, { readonly history: 'latest' | 'rewindable' }>

/** The child record copying one parent document. */
const copyRecord = (
  source: ConversationDocument,
  id: DocumentId,
  conversationId: ConversationId,
): ForkCopy['record'] => {
  const identity = {
    id,
    kind: source.kind,
    ...(source.key === undefined ? {} : { key: source.key }),
    scope: { kind: 'conversation' as const, conversationId },
  }
  return source.history === 'latest'
    ? { ...identity, history: 'latest', fork: source.fork }
    : { ...identity, history: 'rewindable', fork: source.fork }
}

/** Every document alive in the scope at `at`, across scan pages. */
export const scanDocuments = Effect.fnUntraced(function* (
  state: TxState,
  scope: DocumentQuery['scope'],
  at: DocumentPoint,
) {
  const { guard, storage } = state.host
  const records: Array<DocumentRecord> = []
  let cursor: Cursor | undefined
  do {
    const page = yield* guard(storage.scanDocuments({ scope, at }, SCAN_PAGE_SIZE, cursor))
    records.push(...page.items)
    cursor = page.next
  } while (cursor !== undefined)
  return records
})

/**
 * Select every document a fork copies: `asOf` documents of the conversation that owns the fork
 * entry, as of that entry's commit, and `current` documents of the parent as they are now.
 */
const prepareForkCopies = Effect.fnUntraced(function* (
  state: TxState,
  parent: ConversationParent,
  childId: ConversationId,
) {
  const { guard, storage } = state.host
  const entry = yield* guard(storage.visibleEntry(parent.conversationId, parent.at))
  if (entry === undefined) {
    return yield* rejected(
      `Entry ${parent.at} is not visible from conversation ${parent.conversationId}`,
    )
  }
  const copies: Array<ForkCopy> = []
  const addresses = new Set<string>()
  const selections: ReadonlyArray<readonly [ConversationId, DocumentPoint, ForkPolicy]> = [
    [entry.entry.conversationId, entry.commitSeq, 'asOf'],
    [parent.conversationId, 'current', 'current'],
  ]
  for (const [conversationId, at, policy] of selections) {
    const scope = { kind: 'conversation' as const, conversationId }
    for (const source of yield* scanDocuments(state, scope, at)) {
      if (!('history' in source) || source.fork !== policy) continue
      const record = copyRecord(source, yield* guard(storage.mintId(DocumentId)), childId)
      const address = addressId(record)
      if (addresses.has(address)) {
        const member = record.key === undefined ? record.kind : `${record.kind}/${record.key}`
        return yield* rejected(`Fork selects multiple source documents for ${member}`)
      }
      addresses.add(address)
      copies.push({ record, source: { id: source.id, at } })
    }
  }
  return copies
})

/** Stage a new, forked, or root conversation record. */
export const stageConversation = Effect.fnUntraced(function* (
  state: TxState,
  ownership: ConversationOwnership,
  parent?: ConversationParent,
  reservedId?: ConversationId,
) {
  const { guard, storage } = state.host
  const id = reservedId ?? (yield* guard(storage.mintId(ConversationId)))
  yield* assertOpen(state)
  let owner: ConversationRecord['owner']
  if (ownership.kind === 'task') {
    const task = yield* currentTask(state, ownership.taskId)
    yield* assertOpen(state)
    if (task === undefined) {
      return yield* rejected(`Conversation owner task ${ownership.taskId} does not exist`)
    }
    owner = { conversationId: task.conversationId, taskId: ownership.taskId }
  }
  const record: ConversationRecord = {
    id,
    ...(parent === undefined ? {} : { parent }),
    ...(owner === undefined ? {} : { owner }),
  }
  const copies = parent === undefined ? [] : yield* prepareForkCopies(state, parent, id)
  yield* assertOpen(state)
  for (const copy of copies) {
    state.forkSourceDocuments.add(copy.source.id)
    const { kind, scope, key } = copy.record
    const address = key === undefined ? { kind, scope } : { kind, scope, key }
    const entry = {
      addressId: addressId(address),
      address,
      target: { kind: 'fork-copy' as const, ...copy },
      retireOnCommit: false,
    }
    state.documents.push(entry)
    state.latestDocuments.set(entry.addressId, entry)
  }
  if (parent !== undefined) state.forkSourceConversations.add(parent.conversationId)
  state.createdConversations.add(id)
  state.writes.push({ type: 'conversation', value: record })
  return record
})

export const stageRootConversation = (state: TxState) =>
  stageConversation(state, { kind: 'ownerless' }, undefined, ROOT_CONVERSATION_ID)

export const stageFork = (
  state: TxState,
  conversationId: ConversationId,
  at: EntryId,
  ownership: ConversationOwnership,
) => stageConversation(state, ownership, { conversationId, at })
