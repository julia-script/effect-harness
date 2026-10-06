import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Record from '../Record.ts'
import { rejected, StorageError } from '../StorageError.ts'

/** Copies JSON data through property access so scoped draft proxies are safely detached. */
export const detached = <A>(value: A): A => {
  // Persisted facts are ordinary JSON and use the optimized native path. Scoped drafts
  // contain Proxy values; only that unsupported-clone case needs recursive unwrapping.
  try {
    return structuredClone(value)
  } catch (cause) {
    if (!(cause instanceof DOMException) || cause.name !== 'DataCloneError') throw cause
  }
  const copies = new WeakMap<object, object>()
  const copy = (input: unknown): unknown => {
    if (input === null || typeof input !== 'object') return input
    const existing = copies.get(input)
    if (existing !== undefined) return existing
    const output: object = Array.isArray(input) ? [] : {}
    if (Array.isArray(input)) Reflect.set(output, 'length', input.length)
    copies.set(input, output)
    for (const key of Reflect.ownKeys(input)) {
      if (Array.isArray(input) && key === 'length') continue
      const descriptor = Object.getOwnPropertyDescriptor(input, key)
      if (descriptor?.enumerable !== true) continue
      Object.defineProperty(output, key, {
        value: copy(Reflect.get(input, key)),
        enumerable: true,
        configurable: true,
        writable: true,
      })
    }
    return output
  }
  // Every object is rebuilt; the generic type preserves the caller's validated record shape.
  return copy(value) as A
}
export const validate = Effect.fnUntraced(function* <S extends Schema.Constraint>(
  schema: S,
  value: unknown,
) {
  return yield* Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError((cause) => rejected('Invalid durable value', 'invalid', cause)),
  )
})

export const applyOps = Effect.fnUntraced(function* (
  value: Record.JsonObject,
  ops: ReadonlyArray<Record.Op>,
) {
  const result = yield* Effect.try({
    try: () => {
      let result = detached(value)
      for (const op of ops) {
        if (op[0] === 'replace') {
          result = detached(op[1])
          continue
        }
        const path = op[1]
        if (path.length === 0) throw new TypeError('Non-replacement operation needs a path')
        let current: unknown = result
        for (const segment of path.slice(0, -1)) {
          if (current === null || typeof current !== 'object' || !Object.hasOwn(current, segment))
            throw new TypeError('Invalid operation path')
          current = Reflect.get(current, segment)
        }
        const key = path.at(-1)
        if (key === undefined || current === null || typeof current !== 'object')
          throw new TypeError('Invalid operation target')
        if (
          Array.isArray(current) &&
          (typeof key !== 'number' || !Number.isSafeInteger(key) || key < 0 || key > current.length)
        )
          throw new TypeError('Invalid array operation')
        if (op[0] === 'delete') {
          if (Array.isArray(current) && typeof key === 'number') current.splice(key, 1)
          else Reflect.deleteProperty(current, key)
        } else
          Object.defineProperty(current, key, {
            value: detached(op[2]),
            enumerable: true,
            configurable: true,
            writable: true,
          })
      }
      return result
    },
    catch: (cause) => rejected('Invalid document operation', 'corrupt', cause),
  })
  return yield* validate(Schema.JsonObject, result)
})

export const materialize = Effect.fnUntraced(function* (
  document: Record.StoredDocument,
  at: Record.Point,
): Effect.fn.Return<import('../Document.ts').Snapshot | undefined, StorageError> {
  if (at !== 'current' && Record.currentOnly(document.record))
    return yield* rejected('Document does not retain historical content')
  if (!Record.isAlive(document.record, at)) return undefined
  const revisions = document.revisions.filter((revision) => at === 'current' || revision.seq <= at)
  const baseIndex = revisions.findLastIndex((revision) => revision.content.kind === 'base')
  const base = revisions[baseIndex]
  if (base === undefined || base.content.kind !== 'base')
    return yield* rejected('Document is missing a required base', 'corrupt')
  let value = detached(base.content.value)
  for (const revision of revisions.slice(baseIndex + 1)) {
    if (revision.content.kind !== 'delta' || revision.content.version !== base.content.version)
      return yield* rejected('Document crosses a stored version boundary without a base', 'corrupt')
    value = yield* applyOps(value, revision.content.ops)
  }
  return {
    record: detached(document.record),
    version: base.content.version,
    value,
    deltasSinceBase: revisions.length - baseIndex - 1,
  }
})

const sameScope = (a: Record.Scope, b: Record.Scope) => Record.scopeKey(a) === Record.scopeKey(b)
export const visibleEntries = Effect.fnUntraced(function* (
  state: Record.State,
  conversationId: Record.ConversationId,
  min = 0,
  max = Number.MAX_SAFE_INTEGER,
) {
  const entries: Array<Record.Entry> = []
  const seen = new Set<number>()
  let current = conversationId
  let cap = max
  while (true) {
    if (seen.has(current)) return yield* rejected('Conversation ancestry is cyclic', 'corrupt')
    seen.add(current)
    const conversation = state.conversations.find((item) => item.id === current)
    if (conversation === undefined) return yield* rejected('Unknown conversation', 'not_found')
    entries.push(
      ...state.entries
        .filter(
          (item) =>
            item.entry.conversationId === current && item.entry.id >= min && item.entry.id <= cap,
        )
        .map((item) => detached(item.entry)),
    )
    if (conversation.parent === undefined) break
    cap = Math.min(cap, conversation.parent.at)
    current = conversation.parent.conversationId
  }
  return entries.sort((a, b) => b.id - a.id)
})

export const page = <A extends { readonly id: number }>(
  items: ReadonlyArray<A>,
  limit: number,
  cursor?: Record.Cursor,
): Effect.Effect<Record.Page<A>, StorageError> => {
  if (
    !Number.isSafeInteger(limit) ||
    limit <= 0 ||
    (cursor !== undefined && !Number.isSafeInteger(cursor.after))
  )
    return Effect.fail(rejected('Invalid scan size or cursor'))
  const kept = items
    .filter((item) => cursor === undefined || item.id > cursor.after)
    .sort((a, b) => a.id - b.id)
  const shown = kept.slice(0, limit)
  const last = shown.at(-1)
  return Effect.succeed({
    items: detached(shown),
    ...(kept.length > limit && last !== undefined ? { next: { after: last.id } } : {}),
  })
}

export const applyWrites = Effect.fnUntraced(function* (
  state: Record.State,
  input: ReadonlyArray<Record.Write>,
): Effect.fn.Return<Record.State, StorageError> {
  const writes = yield* validate(Schema.Array(Record.Write), input)
  const seq = yield* validate(Record.Seq, state.nextSeq)
  const conversations = new Map(state.conversations.map((item) => [item.id, detached(item)]))
  const entries = new Map(state.entries.map((item) => [item.entry.id, detached(item)]))
  const tasks = new Map(state.tasks.map((item) => [item.id, detached(item)]))
  const submissions = new Map(state.submissions.map((item) => [item.id, detached(item)]))
  const documents = new Map(state.documents.map((item) => [item.record.id, detached(item)]))
  const ids = new Map<number, string>()
  for (const [kind, values] of [
    ['conversation', state.conversations],
    ['task', state.tasks],
    ['submission', state.submissions],
  ] as const)
    for (const item of values) ids.set(item.id, kind)
  for (const item of state.entries) ids.set(item.entry.id, 'entry')
  for (const item of state.documents) ids.set(item.record.id, 'document')
  let nextId = state.nextId
  const changed = new Set(
    writes.flatMap((write) => {
      if (write.type === 'document.change' || write.type === 'document.retire') return [write.id]
      if (write.type === 'document.create' || write.type === 'document.copy')
        return [write.record.id]
      return []
    }),
  )
  const contentCommands = new Set<number>()
  const retired = new Set<number>()
  for (const write of writes) {
    if (write.type === 'document.retire') {
      if (retired.has(write.id)) return yield* rejected('Document is retired more than once')
      retired.add(write.id)
      continue
    }
    let id: number
    if ('value' in write) id = write.value.id
    else if ('record' in write) id = write.record.id
    else id = write.id
    if (
      id === 1 &&
      (write.type !== 'conversation' ||
        write.value.parent !== undefined ||
        write.value.owner !== undefined)
    )
      return yield* rejected('ID 1 is reserved for the root conversation')
    const kind = write.type.startsWith('document.') ? 'document' : write.type
    if (write.type !== 'document.change') {
      const existing = ids.get(id)
      if (
        existing !== undefined &&
        (existing !== kind || kind === 'conversation' || kind === 'entry' || kind === 'document')
      )
        return yield* rejected(`ID ${id} already belongs to ${existing}`, 'conflict')
      ids.set(id, kind)
      nextId = Math.max(nextId, id + 1)
    }
    switch (write.type) {
      case 'conversation':
        conversations.set(write.value.id, detached(write.value))
        break
      case 'entry':
        entries.set(write.value.id, { entry: detached(write.value), commitSeq: seq })
        break
      case 'task':
        tasks.set(write.value.id, detached(write.value))
        break
      case 'submission':
        submissions.set(write.value.id, detached(write.value))
        break
      case 'document.create':
      case 'document.copy': {
        if (contentCommands.has(id))
          return yield* rejected('Document has more than one content command')
        contentCommands.add(id)
        if (write.record.scope.kind === 'conversation') {
          if (
            write.record.history === undefined ||
            write.record.fork === undefined ||
            (write.record.history === 'latest' && write.record.fork === 'asOf')
          )
            return yield* rejected('Invalid conversation document semantics')
        } else if (write.record.history !== undefined || write.record.fork !== undefined)
          return yield* rejected('Only conversation documents specify history and fork')
        let content: Record.Content
        if (write.type === 'document.copy') {
          if (changed.has(write.source.id))
            return yield* rejected('Document copy source is changed in the copy batch')
          const source = documents.get(write.source.id)
          if (source === undefined)
            return yield* rejected('Document copy source is absent', 'not_found')
          const stored = yield* materialize(source, write.source.at)
          if (
            stored === undefined ||
            source.record.scope.kind !== 'conversation' ||
            write.record.scope.kind !== 'conversation' ||
            source.record.kind !== write.record.kind ||
            source.record.key !== write.record.key ||
            source.record.history !== write.record.history ||
            source.record.fork !== write.record.fork
          )
            return yield* rejected('Document copy source does not match')
          content = { kind: 'base', version: stored.version, value: stored.value }
        } else content = write.content
        if (content.kind !== 'base') return yield* rejected('Document creation requires a base')
        documents.set(write.record.id, {
          record: { ...write.record, createdAt: seq },
          revisions: [{ seq, content: detached(content) }],
        })
        break
      }
      case 'document.change': {
        if (contentCommands.has(id))
          return yield* rejected('Document has more than one content command')
        contentCommands.add(id)
        const previous = documents.get(write.id)
        if (previous === undefined || previous.record.retiredAt !== undefined)
          return yield* rejected('Document is absent or retired', 'not_found')
        const latest = previous.revisions.at(-1)
        if (
          write.content.kind === 'delta' &&
          (latest === undefined || latest.content.version !== write.content.version)
        )
          return yield* rejected('Document version transition requires a base')
        if (write.content.kind === 'delta') {
          const snapshot = yield* materialize(previous, 'current')
          if (snapshot === undefined) return yield* rejected('Document is absent')
          yield* applyOps(snapshot.value, write.content.ops)
        }
        if (write.publicationOps !== undefined) {
          const snapshot = yield* materialize(previous, 'current')
          if (snapshot === undefined || snapshot.version !== write.content.version)
            return yield* rejected('Publication operations require the same document version')
          const published = yield* applyOps(snapshot.value, write.publicationOps)
          const persisted =
            write.content.kind === 'base'
              ? write.content.value
              : yield* applyOps(snapshot.value, write.content.ops)
          if (!Json.equal(published, persisted))
            return yield* rejected('Publication operations differ from persisted document')
        }
        const revisions =
          write.content.kind === 'base' && Record.currentOnly(previous.record)
            ? []
            : previous.revisions
        documents.set(write.id, {
          record: previous.record,
          revisions: [...revisions, { seq, content: detached(write.content) }],
        })
        break
      }
    }
  }
  for (const id of retired) {
    const previous = documents.get(id as Record.DocumentId)
    if (previous === undefined || previous.record.retiredAt !== undefined)
      return yield* rejected('Document is absent or retired', 'not_found')
    documents.set(previous.record.id, {
      record: { ...previous.record, retiredAt: seq },
      revisions: Record.currentOnly(previous.record) ? [] : previous.revisions,
    })
  }
  const addresses = new Set<string>()
  for (const document of documents.values()) {
    if (document.record.retiredAt !== undefined) continue
    const address = Record.addressKey(document.record)
    if (addresses.has(address))
      return yield* rejected('Document address already has a current incarnation', 'conflict')
    addresses.add(address)
  }
  return {
    ...state,
    nextId,
    nextSeq: seq + 1,
    conversations: [...conversations.values()],
    entries: [...entries.values()],
    tasks: [...tasks.values()],
    submissions: [...submissions.values()],
    documents: [...documents.values()],
  }
})
export const findDocument = (state: Record.State, address: Record.Address, at: Record.Point) =>
  state.documents.find(
    (item) =>
      Record.addressKey(item.record) === Record.addressKey(address) &&
      Record.isAlive(item.record, at),
  )
export const documentsInScope = (state: Record.State, scope: Record.Scope, at: Record.Point) =>
  state.documents.filter(
    (item) => sameScope(item.record.scope, scope) && Record.isAlive(item.record, at),
  )

/** Validates the authoritative snapshot at a persistence boundary, including retained history. */
export const validateState = Effect.fnUntraced(function* (
  input: unknown,
): Effect.fn.Return<Record.State, StorageError> {
  const state = yield* validate(Record.State, input).pipe(
    Effect.mapError((cause) => rejected('Invalid persisted durable state', 'corrupt', cause)),
  )
  const ids = new Set<number>()
  const allIds: ReadonlyArray<number> = [
    ...state.conversations.map((r) => r.id),
    ...state.entries.map((r) => r.entry.id),
    ...state.tasks.map((r) => r.id),
    ...state.submissions.map((r) => r.id),
    ...state.documents.map((r) => r.record.id),
  ]
  for (const id of allIds) {
    if (ids.has(id) || id >= state.nextId)
      return yield* rejected('Persisted ID namespace or allocator is corrupt', 'corrupt')
    ids.add(id)
  }
  if (
    allIds.includes(1) &&
    !state.conversations.some(
      (record) => record.id === 1 && record.parent === undefined && record.owner === undefined,
    )
  )
    return yield* rejected('Persisted reserved root ID is corrupt', 'corrupt')
  const addresses = new Set<string>()
  for (const document of state.documents) {
    const record = document.record
    if (
      record.createdAt >= state.nextSeq ||
      (record.retiredAt !== undefined &&
        (record.retiredAt < record.createdAt || record.retiredAt >= state.nextSeq))
    )
      return yield* rejected('Persisted document lifetime is corrupt', 'corrupt')
    if (record.scope.kind === 'conversation') {
      if (
        record.history === undefined ||
        record.fork === undefined ||
        (record.history === 'latest' && record.fork === 'asOf')
      )
        return yield* rejected('Persisted document policy is corrupt', 'corrupt')
    } else if (record.history !== undefined || record.fork !== undefined)
      return yield* rejected('Persisted document policy is corrupt', 'corrupt')
    if (
      !Record.currentOnly(record) &&
      (record.retiredAt === undefined || record.retiredAt > record.createdAt)
    )
      yield* materialize(document, record.createdAt)
    let previous = 0
    for (const revision of document.revisions) {
      if (
        revision.seq < record.createdAt ||
        revision.seq <= previous ||
        revision.seq >= state.nextSeq ||
        (record.retiredAt !== undefined && revision.seq > record.retiredAt)
      )
        return yield* rejected('Persisted document revision sequence is corrupt', 'corrupt')
      previous = revision.seq
      if (revision.content.kind === 'base' && !Record.currentOnly(record))
        yield* materialize(document, revision.seq)
    }
    if (record.retiredAt === undefined) {
      const key = Record.addressKey(record)
      if (addresses.has(key))
        return yield* rejected('Persisted document addresses overlap', 'corrupt')
      addresses.add(key)
      yield* materialize(document, 'current')
    } else if (!Record.currentOnly(record) && record.retiredAt > record.createdAt)
      yield* materialize(document, (record.retiredAt - 1) as Record.Seq)
  }
  const keys = new Set<string>()
  for (const receipt of state.receipts) {
    if (
      (receipt.resultIsVoid === true && receipt.result !== null) ||
      keys.has(receipt.key) ||
      receipt.seq >= state.nextSeq
    )
      return yield* rejected('Persisted operation receipts are corrupt', 'corrupt')
    keys.add(receipt.key)
  }
  for (const entry of state.entries)
    if (entry.commitSeq >= state.nextSeq)
      return yield* rejected('Persisted entry commit sequence is corrupt', 'corrupt')
  return state
})
import * as Json from '@effect-harness/harness/Json'
