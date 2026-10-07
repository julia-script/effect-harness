import * as Order from 'effect/Order'
import * as Predicate from 'effect/Predicate'
import { dual } from 'effect/Function'
import * as Option from 'effect/Option'
import * as Effect from 'effect/Effect'
import * as Arr from 'effect/Array'
import * as Schema from 'effect/Schema'
import * as Result from 'effect/Result'
import * as Record from '../../Record.ts'
import * as Document from '../../Document.ts'
import { rejected, StorageError, Invalid, Corrupt, NotFound, Conflict } from '../../StorageError.ts'

export class CloneError extends Schema.TaggedError<CloneError>(
  '@effect-harness/durable/storage/State/CloneError',
)('CloneError', { message: Schema.String, cause: Schema.Defect() }) {}

/** Copies values, including scoped JSON proxies, without throwing native clone/access failures. */
export const detached = <A>(self: A): Result.Result<A, CloneError> => {
  // Persisted facts are ordinary JSON and use the optimized native path. Scoped drafts
  // contain Proxy values; only that unsupported-clone case needs recursive unwrapping.
  const native = Result.try({ try: () => structuredClone(self), catch: (cause) => cause })
  if (Result.isSuccess(native)) return Result.succeed(native.success)
  const cause = native.failure
  if (!(cause instanceof DOMException) || cause.name !== 'DataCloneError')
    return Result.fail(new CloneError({ message: 'Cannot detach durable value', cause }))
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
  return Result.try({
    try: () => copy(self) as A,
    catch: (cause) => new CloneError({ message: 'Cannot detach durable proxy', cause }),
  })
}
/** Synchronous twin for protocols whose callbacks cannot return a Result. */
export const detachedUnsafe = <A>(self: A): A => Result.getOrThrow(detached(self))
/** Lifts cloning into the storage failure channel at Effect boundaries. */
export const detachedEffect = <A>(self: A): Effect.Effect<A, StorageError> =>
  Effect.suspend(() => Effect.fromResult(detached(self))).pipe(
    Effect.mapError((error) => rejected(error.message, undefined, error.cause)),
  )
export const validate = <S extends Schema.Constraint>(
  schema: S,
  value: unknown,
): Effect.Effect<S['Type'], StorageError, S['DecodingServices']> =>
  Effect.suspend(() => Schema.decodeUnknownEffect(schema)(value)).pipe(
    Effect.mapError((cause) => rejected('Invalid durable value', Invalid, cause)),
  )

const applyOpsImpl = Effect.fnUntraced(function* (
  self: Record.JsonObject,
  ops: ReadonlyArray<Record.Op>,
): Effect.fn.Return<Record.JsonObject, StorageError> {
  const validOps = yield* validate(Schema.Array(Record.Op), ops).pipe(
    Effect.mapError((cause) => rejected('Invalid document operation', Corrupt, cause)),
  )
  // effect-review-allow P2-no-throw-in-effect-code: synchronous operation TypeErrors are confined to this catching thunk and become corrupt StorageError failures.
  const result = yield* Effect.try({
    // effect-review-allow P1-throw-only-in-unsafe-orthrow: this synchronous
    // catching thunk maps every native clone/operation throw into StorageError.
    try: () => {
      let result = detachedUnsafe(self)
      for (const op of validOps) {
        if (op[0] === 'replace') {
          result = detachedUnsafe(op[1])
          continue
        }
        const path = op[1]
        let current: unknown = result
        for (const segment of path.slice(0, -1)) {
          if (!Predicate.isObjectOrArray(current) || !Object.hasOwn(current, segment))
            throw new TypeError('Invalid operation path')
          current = Reflect.get(current, segment)
        }
        const key = Arr.lastNonEmpty(path)
        if (!Predicate.isObjectOrArray(current)) throw new TypeError('Invalid operation target')
        if (
          Array.isArray(current) &&
          (!Predicate.isNumber(key) ||
            !Number.isSafeInteger(key) ||
            key < 0 ||
            key > current.length)
        )
          throw new TypeError('Invalid array operation')
        if (op[0] === 'delete') {
          if (Array.isArray(current) && Predicate.isNumber(key)) current.splice(key, 1)
          else Reflect.deleteProperty(current, key)
        } else
          Object.defineProperty(current, key, {
            value: detachedUnsafe(op[2]),
            enumerable: true,
            configurable: true,
            writable: true,
          })
      }
      return result
    },
    catch: (cause) => rejected('Invalid document operation', Corrupt, cause),
  })
  return yield* validate(Schema.JsonObject, result)
})

const materializeImpl = Effect.fnUntraced(function* (
  self: Record.StoredDocument,
  at: Record.Point,
): Effect.fn.Return<Option.Option<import('../../Document.ts').Snapshot>, StorageError> {
  if (at !== 'current' && Record.isCurrentOnly(self.record))
    return yield* rejected('Document does not retain historical content')
  if (!Record.isAlive(self.record, at)) return Option.none()
  const revisions = Arr.filter(self.revisions, (revision) => at === 'current' || revision.seq <= at)
  const baseIndex = Arr.findLastIndex(
    revisions,
    (revision) => revision.content.kind === 'base',
  ).pipe(Option.getOrElse(() => -1))
  const base = revisions[baseIndex]
  if (base === undefined || base.content.kind !== 'base')
    return yield* rejected('Document is missing a required base', Corrupt)
  let value = yield* detachedEffect(base.content.value)
  for (const revision of revisions.slice(baseIndex + 1)) {
    if (revision.content.kind !== 'delta' || revision.content.version !== base.content.version)
      return yield* rejected('Document crosses a stored version boundary without a base', Corrupt)
    value = yield* applyOps(value, revision.content.ops)
  }
  return Option.some(
    Document.makeSnapshot({
      record: yield* detachedEffect(self.record),
      version: base.content.version,
      value,
      deltasSinceBase: revisions.length - baseIndex - 1,
    }),
  )
})

const sameScope = (a: Record.Scope, b: Record.Scope) => Record.scopeKey(a) === Record.scopeKey(b)
const visibleEntriesImpl = Effect.fnUntraced(function* (
  self: Record.State,
  conversationId: Record.ConversationId,
  min = 0,
  max = Number.MAX_SAFE_INTEGER,
): Effect.fn.Return<ReadonlyArray<Record.Entry>, StorageError> {
  const entries: Array<Record.Entry> = []
  const seen = new Set<number>()
  let current = conversationId
  let cap = max
  while (true) {
    if (seen.has(current)) return yield* rejected('Conversation ancestry is cyclic', Corrupt)
    seen.add(current)
    const conversationOption = Arr.findFirst(self.conversations, (item) => item.id === current)
    if (Option.isNone(conversationOption)) return yield* rejected('Unknown conversation', NotFound)
    const conversation = conversationOption.value
    entries.push(
      ...(yield* detachedEffect(
        Arr.filter(
          self.entries,
          (item) =>
            item.entry.conversationId === current && item.entry.id >= min && item.entry.id <= cap,
        ).map((item) => item.entry),
      )),
    )
    if (conversation.parent === undefined) break
    cap = Math.min(cap, conversation.parent.at)
    current = conversation.parent.conversationId
  }
  return Arr.sort(
    entries,
    Order.flip(Order.mapInput(Order.Number, (item: (typeof entries)[number]) => item.id)),
  )
})

const pageImpl = <A extends { readonly id: number }>(
  self: ReadonlyArray<A>,
  limit: number,
  cursor?: Record.Cursor,
): Effect.Effect<Record.Page<A>, StorageError> => {
  if (
    !Number.isSafeInteger(limit) ||
    limit <= 0 ||
    (cursor !== undefined && !Number.isSafeInteger(cursor.after))
  )
    return Effect.fail(rejected('Invalid scan size or cursor'))
  const kept = Arr.sortWith(
    Arr.filter(self, (item) => cursor === undefined || item.id > cursor.after),
    (item: A) => item.id,
    Order.Number,
  )
  const shown = kept.slice(0, limit)
  const last = shown.at(-1)
  return detachedEffect(shown).pipe(
    Effect.map((self) =>
      Record.makePage({
        items: self,
        ...(kept.length > limit && last !== undefined ? { next: { after: last.id } } : {}),
      }),
    ),
  )
}

const applyWritesImpl = Effect.fnUntraced(function* (
  self: Record.State,
  input: ReadonlyArray<Record.Write>,
): Effect.fn.Return<Record.State, StorageError> {
  const writes = yield* validate(Schema.Array(Record.Write), input)
  const seq = yield* validate(Record.Seq, self.nextSeq)
  const conversations = new Map(
    (yield* detachedEffect(self.conversations)).map((item) => [item.id, item]),
  )
  const entries = new Map(
    (yield* detachedEffect(self.entries)).map((item) => [item.entry.id, item]),
  )
  const tasks = new Map((yield* detachedEffect(self.tasks)).map((item) => [item.id, item]))
  const submissions = new Map(
    (yield* detachedEffect(self.submissions)).map((item) => [item.id, item]),
  )
  const documents = new Map(
    (yield* detachedEffect(self.documents)).map((item) => [item.record.id, item]),
  )
  const ids = new Map<number, string>()
  for (const [kind, values] of [
    ['conversation', self.conversations],
    ['task', self.tasks],
    ['submission', self.submissions],
  ] as const)
    for (const item of values) ids.set(item.id, kind)
  for (const item of self.entries) ids.set(item.entry.id, 'entry')
  for (const item of self.documents) ids.set(item.record.id, 'document')
  let nextId = self.nextId
  const changed = new Set(
    Arr.flatMap(writes, (write) => {
      if (write.type === 'document.change' || write.type === 'document.retire') return [write.id]
      if (write.type === 'document.create' || write.type === 'document.copy')
        return [write.record.id]
      return []
    }),
  )
  const contentCommands = new Set<number>()
  const retired = new Set<Record.DocumentId>()
  for (const write of writes) {
    if (write.type === 'document.retire') {
      if (retired.has(write.id)) return yield* rejected('Document is retired more than once')
      retired.add(write.id)
      continue
    }
    let id: number
    if (Predicate.hasProperty(write, 'value')) id = write.value.id
    else if (Predicate.hasProperty(write, 'record')) id = write.record.id
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
        return yield* rejected(`ID ${id} already belongs to ${existing}`, Conflict)
      ids.set(id, kind)
      nextId = Math.max(nextId, id + 1)
    }
    switch (write.type) {
      case 'conversation':
        conversations.set(write.value.id, yield* detachedEffect(write.value))
        break
      case 'entry':
        entries.set(write.value.id, { entry: yield* detachedEffect(write.value), commitSeq: seq })
        break
      case 'task':
        tasks.set(write.value.id, yield* detachedEffect(write.value))
        break
      case 'submission':
        submissions.set(write.value.id, yield* detachedEffect(write.value))
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
            return yield* rejected('Document copy source is absent', NotFound)
          const storedOption = yield* materialize(source, write.source.at)
          if (
            Option.isNone(storedOption) ||
            source.record.scope.kind !== 'conversation' ||
            write.record.scope.kind !== 'conversation' ||
            source.record.kind !== write.record.kind ||
            source.record.key !== write.record.key ||
            source.record.history !== write.record.history ||
            source.record.fork !== write.record.fork
          )
            return yield* rejected('Document copy source does not match')
          const stored = storedOption.value
          content = { _tag: 'base', kind: 'base', version: stored.version, value: stored.value }
        } else content = write.content
        if (content.kind !== 'base') return yield* rejected('Document creation requires a base')
        documents.set(write.record.id, {
          record: { ...write.record, createdAt: seq },
          revisions: [{ seq, content: yield* detachedEffect(content) }],
        })
        break
      }
      case 'document.change': {
        if (contentCommands.has(id))
          return yield* rejected('Document has more than one content command')
        contentCommands.add(id)
        const previous = documents.get(write.id)
        if (previous === undefined || previous.record.retiredAt !== undefined)
          return yield* rejected('Document is absent or retired', NotFound)
        const latest = previous.revisions.at(-1)
        if (
          write.content.kind === 'delta' &&
          (latest === undefined || latest.content.version !== write.content.version)
        )
          return yield* rejected('Document version transition requires a base')
        if (write.content.kind === 'delta') {
          const snapshotOption = yield* materialize(previous, 'current')
          if (Option.isNone(snapshotOption)) return yield* rejected('Document is absent')
          const snapshot = snapshotOption.value
          yield* applyOps(snapshot.value, write.content.ops)
        }
        if (write.publicationOps !== undefined) {
          const snapshotOption = yield* materialize(previous, 'current')
          if (
            Option.isNone(snapshotOption) ||
            snapshotOption.value.version !== write.content.version
          )
            return yield* rejected('Publication operations require the same document version')
          const snapshot = snapshotOption.value
          const published = yield* applyOps(snapshot.value, write.publicationOps)
          const persisted =
            write.content.kind === 'base'
              ? write.content.value
              : yield* applyOps(snapshot.value, write.content.ops)
          if (!Json.equals(published, persisted))
            return yield* rejected('Publication operations differ from persisted document')
        }
        const revisions =
          write.content.kind === 'base' && Record.isCurrentOnly(previous.record)
            ? []
            : previous.revisions
        documents.set(write.id, {
          record: previous.record,
          revisions: [...revisions, { seq, content: yield* detachedEffect(write.content) }],
        })
        break
      }
    }
  }
  for (const id of retired) {
    const previous = documents.get(id)
    if (previous === undefined || previous.record.retiredAt !== undefined)
      return yield* rejected('Document is absent or retired', NotFound)
    documents.set(previous.record.id, {
      record: { ...previous.record, retiredAt: seq },
      revisions: Record.isCurrentOnly(previous.record) ? [] : previous.revisions,
    })
  }
  const addresses = new Set<string>()
  for (const document of documents.values()) {
    if (document.record.retiredAt !== undefined) continue
    const address = Record.addressKey(document.record)
    if (addresses.has(address))
      return yield* rejected('Document address already has a current incarnation', Conflict)
    addresses.add(address)
  }
  return {
    ...self,
    nextId,
    nextSeq: seq + 1,
    conversations: [...conversations.values()],
    entries: [...entries.values()],
    tasks: [...tasks.values()],
    submissions: [...submissions.values()],
    documents: [...documents.values()],
  }
})
const findDocumentImpl = (
  self: Record.State,
  address: Record.Address,
  at: Record.Point,
): Option.Option<Record.StoredDocument> =>
  Arr.findFirst(
    self.documents,
    (item) =>
      Record.addressKey(item.record) === Record.addressKey(address) &&
      Record.isAlive(item.record, at),
  )
const documentsInScopeImpl = (
  self: Record.State,
  scope: Record.Scope,
  at: Record.Point,
): ReadonlyArray<Record.StoredDocument> =>
  Arr.filter(
    self.documents,
    (item) => sameScope(item.record.scope, scope) && Record.isAlive(item.record, at),
  )

/** Validate arithmetic cursors without narrowing the allocator exhaustion sentinel. */
export const applyWrites: {
  (
    input: ReadonlyArray<Record.Write>,
  ): (self: Record.State) => Effect.Effect<Record.State, StorageError>
  (
    self: Record.State,
    input: ReadonlyArray<Record.Write>,
  ): Effect.Effect<Record.State, StorageError>
} = dual(2, applyWritesImpl)

export const cursor = (nextSeq: number): Effect.Effect<Record.Seq | 0, StorageError> =>
  validate(Record.JournalCursor, nextSeq - 1).pipe(
    Effect.mapError((cause) => rejected('Invalid computed journal cursor', Corrupt, cause)),
  )
/** Validates the authoritative snapshot at a persistence boundary, including retained history. */
export const validateState = Effect.fnUntraced(function* (
  input: unknown,
): Effect.fn.Return<Record.State, StorageError> {
  const state = yield* validate(Record.State, input).pipe(
    Effect.mapError((cause) => rejected('Invalid persisted durable state', Corrupt, cause)),
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
      return yield* rejected('Persisted ID namespace or allocator is corrupt', Corrupt)
    ids.add(id)
  }
  if (
    Arr.contains(allIds, 1) &&
    !state.conversations.some(
      (record) => record.id === 1 && record.parent === undefined && record.owner === undefined,
    )
  )
    return yield* rejected('Persisted reserved root ID is corrupt', Corrupt)
  const addresses = new Set<string>()
  for (const document of state.documents) {
    const record = document.record
    if (
      record.createdAt >= state.nextSeq ||
      (record.retiredAt !== undefined &&
        (record.retiredAt < record.createdAt || record.retiredAt >= state.nextSeq))
    )
      return yield* rejected('Persisted document lifetime is corrupt', Corrupt)
    if (record.scope.kind === 'conversation') {
      if (
        record.history === undefined ||
        record.fork === undefined ||
        (record.history === 'latest' && record.fork === 'asOf')
      )
        return yield* rejected('Persisted document policy is corrupt', Corrupt)
    } else if (record.history !== undefined || record.fork !== undefined)
      return yield* rejected('Persisted document policy is corrupt', Corrupt)
    if (
      !Record.isCurrentOnly(record) &&
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
        return yield* rejected('Persisted document revision sequence is corrupt', Corrupt)
      previous = revision.seq
      if (revision.content.kind === 'base' && !Record.isCurrentOnly(record))
        yield* materialize(document, revision.seq)
    }
    if (record.retiredAt === undefined) {
      const key = Record.addressKey(record)
      if (addresses.has(key))
        return yield* rejected('Persisted document addresses overlap', Corrupt)
      addresses.add(key)
      yield* materialize(document, 'current')
    } else if (!Record.isCurrentOnly(record) && record.retiredAt > record.createdAt)
      yield* materialize(
        document,
        yield* validate(Record.Seq, record.retiredAt - 1).pipe(
          Effect.mapError((cause) => rejected('Invalid retirement boundary', Corrupt, cause)),
        ),
      )
  }
  const keys = new Set<string>()
  for (const receipt of state.receipts) {
    if (
      (receipt.resultIsVoid === true && receipt.result !== null) ||
      keys.has(receipt.key) ||
      receipt.seq >= state.nextSeq
    )
      return yield* rejected('Persisted operation receipts are corrupt', Corrupt)
    keys.add(receipt.key)
  }
  for (const entry of state.entries)
    if (entry.commitSeq >= state.nextSeq)
      return yield* rejected('Persisted entry commit sequence is corrupt', Corrupt)
  return state
})
import * as Json from '@effect-harness/harness/Json'

export const applyOps: {
  (
    ops: ReadonlyArray<Record.Op>,
  ): (self: Record.JsonObject) => Effect.Effect<Record.JsonObject, StorageError>
  (
    self: Record.JsonObject,
    ops: ReadonlyArray<Record.Op>,
  ): Effect.Effect<Record.JsonObject, StorageError>
} = dual(2, applyOpsImpl)

export const materialize: {
  (
    at: Record.Point,
  ): (self: Record.StoredDocument) => Effect.Effect<Option.Option<Document.Snapshot>, StorageError>
  (
    self: Record.StoredDocument,
    at: Record.Point,
  ): Effect.Effect<Option.Option<Document.Snapshot>, StorageError>
} = dual(2, materializeImpl)

export const visibleEntries: {
  (
    conversationId: Record.ConversationId,
    min?: number,
    max?: number,
  ): (self: Record.State) => Effect.Effect<ReadonlyArray<Record.Entry>, StorageError>
  (
    self: Record.State,
    conversationId: Record.ConversationId,
    min?: number,
    max?: number,
  ): Effect.Effect<ReadonlyArray<Record.Entry>, StorageError>
} = dual((args) => Predicate.hasProperty(args[0], 'conversations'), visibleEntriesImpl)

export const page: {
  (
    limit: number,
    cursor?: Record.Cursor,
  ): <A extends { readonly id: number }>(
    self: ReadonlyArray<A>,
  ) => Effect.Effect<Record.Page<A>, StorageError>
  <A extends { readonly id: number }>(
    self: ReadonlyArray<A>,
    limit: number,
    cursor?: Record.Cursor,
  ): Effect.Effect<Record.Page<A>, StorageError>
} = dual((args) => Array.isArray(args[0]), pageImpl)

export const findDocument: {
  (
    address: Record.Address,
    at: Record.Point,
  ): (self: Record.State) => Option.Option<Record.StoredDocument>
  (
    self: Record.State,
    address: Record.Address,
    at: Record.Point,
  ): Option.Option<Record.StoredDocument>
} = dual(3, findDocumentImpl)

export const documentsInScope: {
  (
    scope: Record.Scope,
    at: Record.Point,
  ): (self: Record.State) => ReadonlyArray<Record.StoredDocument>
  (self: Record.State, scope: Record.Scope, at: Record.Point): ReadonlyArray<Record.StoredDocument>
} = dual(3, documentsInScopeImpl)
