import { Effect } from 'effect'
import { applyBatches, isJsonObject } from '../delta.ts'
import { StorageRequestError } from '../../StorageError.ts'
import type {
  DocumentContent,
  DocumentCreate,
  DocumentPoint,
  DocumentRecord,
  Op,
  StoredDocument,
} from '../../schema/Document.ts'
import type { Seq } from '../../schema/Ids.ts'
import type { StorageWrite } from '../../schema/Write.ts'
import { StorageError } from '../../StorageError.ts'
import {
  getOrCreate,
  insertMapId,
  insertSorted,
  isAliveAt,
  isCurrentOnly,
  recordAddressKey,
  scopeKey,
  semantics,
} from './state.ts'
import type { DocumentAddressIndex, DocumentRevision, State, StoredDocumentState } from './state.ts'

/** Every command one commit gives one document. */
export interface DocumentAction {
  create?: DocumentCreate
  content?: DocumentContent
  retire: boolean
}

/** A write once its document copy, if it was one, is resolved to a create. */
export type ResolvedWrite = Exclude<StorageWrite, { readonly type: 'document.copy' }>

const reject = (message: string) => new StorageError({ message })

/** The ops of the deltas after a base, or `undefined` when one crosses a version boundary. */
const deltasAfter = (
  revisions: ReadonlyArray<DocumentRevision>,
  version: number,
): ReadonlyArray<ReadonlyArray<Op>> | undefined => {
  const ops = revisions.flatMap((revision) =>
    revision.kind === 'delta' && revision.version === version ? [revision.ops] : [],
  )
  return ops.length === revisions.length ? ops : undefined
}

/** Replay one incarnation at a point. The returned value is detached; the record is not. */
export const materializeDocument = (
  state: State,
  id: number,
  at: DocumentPoint,
): Effect.Effect<StoredDocument | undefined, StorageRequestError | StorageError> => {
  const stored = state.documents.get(id)
  if (stored === undefined) return Effect.undefined
  if (at !== 'current' && isCurrentOnly(stored.record)) {
    const message = `Document ${id} does not retain historical content`
    return Effect.fail(new StorageRequestError({ message }))
  }
  if (!isAliveAt(stored.record, at)) return Effect.undefined
  const revisions =
    at === 'current' ? stored.revisions : stored.revisions.filter((revision) => revision.seq <= at)
  const baseIndex = revisions.findLastIndex((revision) => revision.kind === 'base')
  const base = revisions[baseIndex]
  if (base?.kind !== 'base') return Effect.fail(reject(`Document ${id} is missing a required base`))
  // Every delta after a base must share its version; a version change needs a new base.
  const deltas = deltasAfter(revisions.slice(baseIndex + 1), base.version)
  if (deltas === undefined) {
    return Effect.fail(reject(`Document ${id} crosses a stored version boundary without a base`))
  }
  return Effect.flatMap(applyBatches(base.value, deltas), (value) =>
    isJsonObject(value)
      ? Effect.succeed({
          record: stored.record,
          version: base.version,
          value,
          deltasSinceBase: deltas.length,
        })
      : Effect.fail(reject(`Document ${id} does not materialize to an object`)),
  )
}

/** Reads one incarnation at a point, as `materializeDocument` does for a backend. */
export type Materialize<E, R = never> = (
  id: number,
  at: DocumentPoint,
) => Effect.Effect<StoredDocument | undefined, E, R>

const copiedBase = Effect.fnUntraced(function* <E, R>(
  materialize: Materialize<E, R>,
  write: Extract<StorageWrite, { readonly type: 'document.copy' }>,
  changed: ReadonlySet<number>,
) {
  const { source, record } = write
  if (changed.has(source.id)) {
    return yield* reject(`Fork source document ${source.id} is changed in the copy batch`)
  }
  const stored = yield* materialize(source.id, source.at)
  if (stored === undefined) {
    return yield* reject(`Fork source document ${source.id} cannot be read`)
  }
  if (
    stored.record.scope.kind !== 'conversation' ||
    record.scope.kind !== 'conversation' ||
    stored.record.kind !== record.kind ||
    stored.record.key !== record.key ||
    semantics(stored.record) !== semantics(record)
  ) {
    return yield* reject(`Fork source document ${source.id} does not match the copied record`)
  }
  const create: ResolvedWrite = {
    type: 'document.create',
    record,
    content: { kind: 'base', version: stored.version, value: stored.value },
  }
  return create
})

const changedDocumentIds = (writes: ReadonlyArray<StorageWrite>): ReadonlySet<number> => {
  const changed = new Set<number>()
  for (const write of writes) {
    if (write.type === 'document.create' || write.type === 'document.copy') {
      changed.add(write.record.id)
    } else if (write.type === 'document.change' || write.type === 'document.retire') {
      changed.add(write.id)
    }
  }
  return changed
}

/** Turn each `document.copy` into a `document.create` of its source's materialized base. */
export const resolveDocumentCopies = <E, R>(
  materialize: Materialize<E, R>,
  writes: ReadonlyArray<StorageWrite>,
): Effect.Effect<ReadonlyArray<ResolvedWrite>, StorageError, R> => {
  const changed = changedDocumentIds(writes)
  return Effect.forEach(writes, (write) =>
    write.type === 'document.copy'
      ? copiedBase(materialize, write, changed).pipe(
          Effect.mapError(
            (cause) =>
              new StorageError({ message: `Document copy ${write.record.id} was rejected`, cause }),
          ),
        )
      : Effect.succeed(write),
  )
}

const actionFor = (actions: Map<number, DocumentAction>, id: number): DocumentAction =>
  getOrCreate(actions, id, (): DocumentAction => ({ retire: false }))

/** Group a batch's document commands by document, rejecting conflicting commands. */
export const prepareDocumentActions = Effect.fnUntraced(function* (
  writes: ReadonlyArray<StorageWrite>,
) {
  const actions = new Map<number, DocumentAction>()
  for (const write of writes) {
    if (write.type === 'document.create') {
      const action = actionFor(actions, write.record.id)
      if (action.create !== undefined || action.content !== undefined) {
        return yield* reject(`Document ${write.record.id} has more than one content command`)
      }
      action.create = write.record
      action.content = write.content
    } else if (write.type === 'document.change') {
      const action = actionFor(actions, write.id)
      if (action.content !== undefined) {
        return yield* reject(`Document ${write.id} has more than one content command`)
      }
      action.content = write.content
    } else if (write.type === 'document.retire') {
      const action = actionFor(actions, write.id)
      if (action.retire) return yield* reject(`Document ${write.id} is retired more than once`)
      action.retire = true
    }
  }
  return actions
})

/** Why one document's actions cannot apply, or its record when they can. */
const checkAction = (
  state: State,
  id: number,
  action: DocumentAction,
): Effect.Effect<DocumentRecord | DocumentCreate, StorageError> => {
  const existing = state.documents.get(id)
  const record = action.create ?? existing?.record
  if (record === undefined) return Effect.fail(reject(`Unknown document: ${id}`))
  if (action.create !== undefined && existing !== undefined) {
    return Effect.fail(reject(`Document ${id} already exists`))
  }
  if (existing?.record.retiredAt !== undefined) {
    return Effect.fail(reject(`Document ${id} is retired`))
  }
  if (action.content?.kind === 'delta') {
    const previous = existing?.revisions.at(-1)
    if (previous === undefined) return Effect.fail(reject(`Document ${id} delta has no base`))
    if (previous.version !== action.content.version) {
      return Effect.fail(reject(`Document ${id} version transition requires a base`))
    }
  }
  return Effect.succeed(record)
}

/** Reject actions on unknown or retired documents, and addresses left with two current incarnations. */
export const checkDocumentActions = (
  state: State,
  actions: ReadonlyMap<number, DocumentAction>,
): Effect.Effect<void, StorageError> =>
  Effect.flatMap(
    Effect.forEach(actions, ([id, action]) =>
      Effect.map(checkAction(state, id, action), (record) => ({ id, action, record })),
    ),
    (checked) => {
      const liveCounts = new Map<string, number>()
      for (const { id, action, record } of checked) {
        const key = recordAddressKey(record)
        const currentId = state.documentAddresses.get(key)?.currentId
        let live = liveCounts.get(key) ?? (currentId === undefined ? 0 : 1)
        if (action.retire && currentId === id) live--
        if (action.create !== undefined && !action.retire) live++
        liveCounts.set(key, live)
      }
      return [...liveCounts.values()].some((live) => live > 1)
        ? Effect.fail(reject('Document address already has a current incarnation'))
        : Effect.void
    },
  )

const createDocument = (
  state: State,
  create: DocumentCreate,
  content: DocumentContent,
  retire: boolean,
  seq: Seq,
): StoredDocumentState => {
  // Created and retired in one commit gives an empty lifetime.
  const record: DocumentRecord = retire
    ? { ...create, createdAt: seq, retiredAt: seq }
    : { ...create, createdAt: seq }
  const stored: StoredDocumentState = { record, revisions: [{ ...content, seq }] }
  state.recordTypes.set(record.id, 'document')
  state.nextId = Math.max(state.nextId, record.id + 1)
  state.documents.set(record.id, stored)
  const address = getOrCreate(
    state.documentAddresses,
    recordAddressKey(record),
    (): DocumentAddressIndex => ({ ids: [] }),
  )
  insertSorted(address.ids, record.id)
  if (!retire) address.currentId = record.id
  insertMapId(state.documentIdsByScope, scopeKey(record.scope), record.id)
  return stored
}

const changeDocument = (stored: StoredDocumentState, content: DocumentContent, seq: Seq): void => {
  const revision: DocumentRevision = { ...content, seq }
  // Current-only documents drop the history a new base supersedes.
  if (revision.kind === 'base' && isCurrentOnly(stored.record)) stored.revisions = [revision]
  else stored.revisions.push(revision)
}

const retireDocument = (state: State, stored: StoredDocumentState, seq: Seq): void => {
  stored.record = { ...stored.record, retiredAt: seq }
  const address = state.documentAddresses.get(recordAddressKey(stored.record))
  if (address?.currentId === stored.record.id) delete address.currentId
}

/** Apply checked document actions. Nothing here may fail. */
export const applyDocumentActions = (
  state: State,
  actions: ReadonlyMap<number, DocumentAction>,
  seq: Seq,
): void => {
  for (const [id, action] of actions) {
    let stored = state.documents.get(id)
    if (action.create !== undefined && action.content !== undefined) {
      stored = createDocument(state, action.create, action.content, action.retire, seq)
    } else if (stored !== undefined) {
      if (action.content !== undefined) changeDocument(stored, action.content, seq)
      if (action.retire) retireDocument(state, stored, seq)
    }
    if (stored !== undefined && action.retire && isCurrentOnly(stored.record)) {
      stored.revisions = []
    }
  }
}
