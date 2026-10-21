/**
 * The conversation view mount, ported from pi's `harness/view.ts`: one conversation's view, built
 * in a commit and advanced from every later commit's publication.
 */
import { Effect } from 'effect'
import type { CommitPublication, Tx } from '../../../session/Session.ts'
import { TransactionError } from '../../../session/SessionError.ts'
import type { Op } from '../../../storage/schema/Document.ts'
import type { EntryRecord } from '../../../storage/schema/Entry.ts'
import type { ConversationId, DocumentId } from '../../../storage/schema/Ids.ts'
import type { JsonObject } from '../../../storage/schema/Json.ts'
import * as Agent from '../../Agent.ts'
import * as Inbox from '../../Inbox.ts'
import * as Live from '../../Live.ts'
import * as Usage from '../../Usage.ts'
import type { ConversationView } from '../../../protocol/ViewState.ts'
import { read } from '../context.ts'

type Path = ReadonlyArray<string | number>
type NonEmptyPath = readonly [string | number, ...Array<string | number>]

const MOUNTED = [Agent.document, Live.document, Inbox.document, Usage.document] as const
const MOUNTED_KINDS: ReadonlySet<string> = new Set(MOUNTED.map((token) => token.definition.kind))

/**
 * The document incarnation and definition version a view shows of one kind. A view built from a
 * snapshot learns the incarnation's ID from its first change.
 */
export interface Incarnation {
  readonly id: DocumentId | undefined
  readonly version: number
}

/** One conversation's view; `docs` tracks which incarnation of each kind it shows. */
export interface View {
  value: ConversationView
  readonly docs: Map<string, Incarnation>
}

/** Build the view of `id` in a commit, before its first table write. */
export const build = Effect.fnUntraced(function* (tx: Tx, id: ConversationId) {
  const conversation = yield* tx.conversation(id)
  if (conversation === undefined) {
    return yield* new TransactionError({ message: `Conversation ${id} does not exist` })
  }
  const { entries } = yield* read(tx, id)
  const shown = yield* Effect.all([
    tx.snapshot(Agent.document, id),
    tx.snapshot(Live.document, id),
    tx.snapshot(Inbox.document, id),
    tx.snapshot(Usage.document, id),
  ])
  const docs: Record<string, JsonObject> = {}
  const incarnations = new Map<string, Incarnation>()
  for (const [index, value] of shown.entries()) {
    const definition = MOUNTED[index]?.definition
    if (value === undefined || definition === undefined) continue
    docs[definition.kind] = value
    incarnations.set(definition.kind, { id: undefined, version: definition.version })
  }
  const view: View = { value: { conversation, entries, docs }, docs: incarnations }
  return view
})

type DocumentChange = Extract<CommitPublication['changes'][number], { readonly type: 'document' }>

/** `op` moved under `prefix`; a root replacement becomes a set of the prefix. */
const prefixed = (op: Op, prefix: NonEmptyPath): Op => {
  const at = (path: Path): NonEmptyPath => [...prefix, ...path] as const
  if (op[0] === 'r') return ['s', prefix, op[1]]
  if (op[0] === 'p') return ['p', at(op[1]), op[2], op[3], op[4]]
  if (op[0] === 'm') return ['m', at(op[1]), op[2]]
  if (op[0] === 's') return ['s', at(op[1]), op[2]]
  if (op[0] === 'd') return ['d', at(op[1])]
  if (op[0] === 'a') return ['a', at(op[1]), op[2]]
  return ['t', at(op[1]), op[2]]
}

/**
 * The view operations of one document change. An update of the shown incarnation and version
 * applies its operations; a retirement of it deletes the kind; anything else sets it whole.
 */
const documentOps = (view: View, change: DocumentChange): ReadonlyArray<Op> => {
  const kind = change.record.kind
  const path: NonEmptyPath = ['docs', kind]
  const shown = view.docs.get(kind)
  const same = shown !== undefined && (shown.id ?? change.record.id) === change.record.id
  if (change.value === null) {
    if (!same) return []
    view.docs.delete(kind)
    return [['d', path]]
  }
  const version = change.version ?? 0
  const applies = same && shown.version === version && change.ops.length > 0
  view.docs.set(kind, { id: change.record.id, version })
  return applies ? change.ops.map((op) => prefixed(op, path)) : [['s', path, change.value]]
}

/** Whether `change` is of a document `id`'s view shows. */
const isShown = (id: ConversationId, change: CommitPublication['changes'][number]) =>
  change.type === 'document' &&
  change.conversationId === id &&
  change.record.key === undefined &&
  MOUNTED_KINDS.has(change.record.kind)

/** The entry operations of one appended entry, and the entries after it. */
const entryOps = (
  entries: ReadonlyArray<EntryRecord>,
  entry: EntryRecord,
): readonly [Op, ReadonlyArray<EntryRecord>] => {
  if (entry.head === undefined)
    return [
      ['p', ['entries'], entries.length, 0, [entry]],
      [...entries, entry],
    ]
  // A head marker keeps the entries from its head that are not markers, a suffix, and goes first.
  const target = entry.head
  const found = entries.findIndex((each) => each.head === undefined && each.id >= target)
  const kept = found < 0 ? entries.length : found
  return [
    ['p', ['entries'], 0, kept, [entry]],
    [entry, ...entries.slice(kept)],
  ]
}

/**
 * Advance `view` of conversation `id` by one publication; the operations it applied, document
 * operations first. Entries are published in ID order.
 */
export const advance = (id: ConversationId, view: View, publication: CommitPublication) => {
  const docOps: Array<Op> = []
  const entryOpsOf: Array<Op> = []
  let entries = view.value.entries
  let docs = view.value.docs
  for (const change of publication.changes) {
    if (change.type === 'entry' && change.value.conversationId === id) {
      const [op, next] = entryOps(entries, change.value)
      entryOpsOf.push(op)
      entries = next
    } else if (change.type === 'document' && isShown(id, change)) {
      const ops = documentOps(view, change)
      if (ops.length === 0) continue
      docOps.push(...ops)
      const { [change.record.kind]: _replaced, ...rest } = docs
      docs = change.value === null ? rest : { ...rest, [change.record.kind]: change.value }
    }
  }
  const ops = [...docOps, ...entryOpsOf]
  if (ops.length > 0) view.value = { ...view.value, entries, docs }
  return ops
}
