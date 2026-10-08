import * as MutableHashMap from 'effect/MutableHashMap'
import * as Predicate from 'effect/Predicate'
import { dual } from 'effect/Function'
import * as Option from 'effect/Option'
import * as Effect from 'effect/Effect'
import * as Arr from 'effect/Array'
import * as Schema from 'effect/Schema'
import * as Result from 'effect/Result'
import * as Record from '../Record.ts'
import * as Document from '../Document.ts'
import {
  rejected,
  type StorageError,
  InvalidError,
  CorruptError,
  NotFoundError,
  ConflictError,
} from '../StorageError.ts'

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
  const copyUnsafe = (input: unknown): unknown => {
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
        value: copyUnsafe(Reflect.get(input, key)),
        enumerable: true,
        configurable: true,
        writable: true,
      })
    }
    return output
  }
  // Every object is rebuilt; the generic type preserves the caller's validated record shape.
  return Result.try({
    try: () => copyUnsafe(self) as A,
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
    Effect.mapError((cause) => rejected('Invalid durable value', InvalidError, cause)),
  )

const applyOpsImpl = Effect.fnUntraced(function* (
  self: Schema.JsonObject,
  ops: ReadonlyArray<Record.Op>,
): Effect.fn.Return<Schema.JsonObject, StorageError> {
  const validOps = yield* validate(Schema.Array(Record.Op), ops).pipe(
    Effect.mapError((cause) => rejected('Invalid document operation', CorruptError, cause)),
  )
  // effect-nit-allow P2-no-throw-in-effect-code: synchronous operation TypeErrors are confined to this catching thunk and become corrupt StorageError failures.
  const result = yield* Effect.try({
    // effect-nit-allow P1-throw-only-in-unsafe-orthrow: this synchronous
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
    catch: (cause) => rejected('Invalid document operation', CorruptError, cause),
  })
  return yield* validate(Schema.JsonObject, result)
})

const materializeImpl = Effect.fnUntraced(function* (
  self: Record.StoredDocument,
  at: Record.Point,
): Effect.fn.Return<Option.Option<import('../Document.ts').Document.Snapshot>, StorageError> {
  if (at !== 'current' && Record.isCurrentOnly(self.record))
    return yield* rejected('Document does not retain historical content')
  if (!Record.isAlive(self.record, at)) return Option.none()
  const revisions = Arr.filter(self.revisions, (revision) => at === 'current' || revision.seq <= at)
  const baseIndex = Arr.findLastIndex(
    revisions,
    (revision) => revision.content._tag === 'base',
  ).pipe(Option.getOrElse(() => -1))
  const base = revisions[baseIndex]
  if (base === undefined || base.content._tag !== 'base')
    return yield* rejected('Document is missing a required base', CorruptError)
  let value = yield* detachedEffect(base.content.value)
  for (const revision of revisions.slice(baseIndex + 1)) {
    if (revision.content._tag !== 'delta' || revision.content.version !== base.content.version)
      return yield* rejected(
        'Document crosses a stored version boundary without a base',
        CorruptError,
      )
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

const applyWritesImpl = Effect.fnUntraced(function* (
  self: State,
  input: ReadonlyArray<Record.Write>,
): Effect.fn.Return<State, StorageError> {
  const writes = yield* validate(Schema.Array(Record.Write), input)
  const seq = yield* validate(Record.Seq, self.nextSeq)
  const conversations = MutableHashMap.fromIterable(
    (yield* detachedEffect(self.conversations)).map((item) => [item.id, item]),
  )
  const entries = MutableHashMap.fromIterable(
    (yield* detachedEffect(self.entries)).map((item) => [item.entry.id, item]),
  )
  const tasks = MutableHashMap.fromIterable(
    (yield* detachedEffect(self.tasks)).map((item) => [item.id, item]),
  )
  const submissions = MutableHashMap.fromIterable(
    (yield* detachedEffect(self.submissions)).map((item) => [item.id, item]),
  )
  const documents = MutableHashMap.fromIterable(
    (yield* detachedEffect(self.documents)).map((item) => [item.record.id, item]),
  )
  const ids = MutableHashMap.empty<number, string>()
  for (const [kind, values] of [
    ['conversation', self.conversations],
    ['task', self.tasks],
    ['submission', self.submissions],
  ] as const)
    for (const item of values) MutableHashMap.set(ids, item.id, kind)
  for (const item of self.entries) MutableHashMap.set(ids, item.entry.id, 'entry')
  for (const item of self.documents) MutableHashMap.set(ids, item.record.id, 'document')
  let nextId = self.nextId
  const changed = new Set(
    Arr.flatMap(writes, (write) => {
      if (write._tag === 'document.change' || write._tag === 'document.retire') return [write.id]
      if (write._tag === 'document.create' || write._tag === 'document.copy')
        return [write.record.id]
      return []
    }),
  )
  const contentCommands = new Set<number>()
  const retired = new Set<Record.DocumentId>()
  for (const write of writes) {
    if (write._tag === 'document.retire') {
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
      (write._tag !== 'conversation' ||
        write.value.parent !== undefined ||
        write.value.owner !== undefined)
    )
      return yield* rejected('ID 1 is reserved for the root conversation')
    const kind = write._tag.startsWith('document.') ? 'document' : write._tag
    if (write._tag !== 'document.change') {
      const existing = Option.getOrUndefined(MutableHashMap.get(ids, id))
      if (
        existing !== undefined &&
        (existing !== kind || kind === 'conversation' || kind === 'entry' || kind === 'document')
      )
        return yield* rejected(`ID ${id} already belongs to ${existing}`, ConflictError)
      MutableHashMap.set(ids, id, kind)
      nextId = Math.max(nextId, id + 1)
    }
    switch (write._tag) {
      case 'conversation':
        MutableHashMap.set(conversations, write.value.id, yield* detachedEffect(write.value))
        break
      case 'entry':
        MutableHashMap.set(entries, write.value.id, {
          entry: yield* detachedEffect(write.value),
          commitSeq: seq,
        })
        break
      case 'task':
        MutableHashMap.set(tasks, write.value.id, yield* detachedEffect(write.value))
        break
      case 'submission':
        MutableHashMap.set(submissions, write.value.id, yield* detachedEffect(write.value))
        break
      case 'document.create':
      case 'document.copy': {
        if (contentCommands.has(id))
          return yield* rejected('Document has more than one content command')
        contentCommands.add(id)
        if (write.record.scope._tag === 'conversation') {
          if (
            write.record.history === undefined ||
            write.record.fork === undefined ||
            (write.record.history === 'latest' && write.record.fork === 'asOf')
          )
            return yield* rejected('Invalid conversation document semantics')
        } else if (write.record.history !== undefined || write.record.fork !== undefined)
          return yield* rejected('Only conversation documents specify history and fork')
        let content: Record.Content
        if (write._tag === 'document.copy') {
          if (changed.has(write.source.id))
            return yield* rejected('Document copy source is changed in the copy batch')
          const source = Option.getOrUndefined(MutableHashMap.get(documents, write.source.id))
          if (source === undefined)
            return yield* rejected('Document copy source is absent', NotFoundError)
          const storedOption = yield* materialize(source, write.source.at)
          if (
            Option.isNone(storedOption) ||
            source.record.scope._tag !== 'conversation' ||
            write.record.scope._tag !== 'conversation' ||
            source.record.kind !== write.record.kind ||
            source.record.key !== write.record.key ||
            source.record.history !== write.record.history ||
            source.record.fork !== write.record.fork
          )
            return yield* rejected('Document copy source does not match')
          const stored = storedOption.value
          content = { _tag: 'base', version: stored.version, value: stored.value }
        } else content = write.content
        if (content._tag !== 'base') return yield* rejected('Document creation requires a base')
        MutableHashMap.set(documents, write.record.id, {
          record: { ...write.record, createdAt: seq },
          revisions: [{ seq, content: yield* detachedEffect(content) }],
        })
        break
      }
      case 'document.change': {
        if (contentCommands.has(id))
          return yield* rejected('Document has more than one content command')
        contentCommands.add(id)
        const previous = Option.getOrUndefined(MutableHashMap.get(documents, write.id))
        if (previous === undefined || previous.record.retiredAt !== undefined)
          return yield* rejected('Document is absent or retired', NotFoundError)
        const latest = previous.revisions.at(-1)
        if (
          write.content._tag === 'delta' &&
          (latest === undefined || latest.content.version !== write.content.version)
        )
          return yield* rejected('Document version transition requires a base')
        if (write.content._tag === 'delta') {
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
            write.content._tag === 'base'
              ? write.content.value
              : yield* applyOps(snapshot.value, write.content.ops)
          if (!Json.equals(published, persisted))
            return yield* rejected('Publication operations differ from persisted document')
        }
        const revisions =
          write.content._tag === 'base' && Record.isCurrentOnly(previous.record)
            ? []
            : previous.revisions
        MutableHashMap.set(documents, write.id, {
          record: previous.record,
          revisions: [...revisions, { seq, content: yield* detachedEffect(write.content) }],
        })
        break
      }
    }
  }
  for (const id of retired) {
    const previous = Option.getOrUndefined(MutableHashMap.get(documents, id))
    if (previous === undefined || previous.record.retiredAt !== undefined)
      return yield* rejected('Document is absent or retired', NotFoundError)
    MutableHashMap.set(documents, previous.record.id, {
      record: { ...previous.record, retiredAt: seq },
      revisions: Record.isCurrentOnly(previous.record) ? [] : previous.revisions,
    })
  }
  const addresses = new Set<string>()
  for (const document of MutableHashMap.values(documents)) {
    if (document.record.retiredAt !== undefined) continue
    const address = Record.addressKey(document.record)
    if (addresses.has(address))
      return yield* rejected('Document address already has a current incarnation', ConflictError)
    addresses.add(address)
  }
  return {
    ...self,
    nextId,
    nextSeq: seq + 1,
    conversations: [...MutableHashMap.values(conversations)],
    entries: [...MutableHashMap.values(entries)],
    tasks: [...MutableHashMap.values(tasks)],
    submissions: [...MutableHashMap.values(submissions)],
    documents: [...MutableHashMap.values(documents)],
  }
})
/** Validate arithmetic cursors without narrowing the allocator exhaustion sentinel. */
export const applyWrites: {
  (input: ReadonlyArray<Record.Write>): (self: State) => Effect.Effect<State, StorageError>
  (self: State, input: ReadonlyArray<Record.Write>): Effect.Effect<State, StorageError>
} = dual(2, applyWritesImpl)

import * as Json from 'effect-harness/Json'

export const applyOps: {
  (
    ops: ReadonlyArray<Record.Op>,
  ): (self: Schema.JsonObject) => Effect.Effect<Schema.JsonObject, StorageError>
  (
    self: Schema.JsonObject,
    ops: ReadonlyArray<Record.Op>,
  ): Effect.Effect<Schema.JsonObject, StorageError>
} = dual(2, applyOpsImpl)

export const materialize: {
  (
    at: Record.Point,
  ): (
    self: Record.StoredDocument,
  ) => Effect.Effect<Option.Option<Document.Document.Snapshot>, StorageError>
  (
    self: Record.StoredDocument,
    at: Record.Point,
  ): Effect.Effect<Option.Option<Document.Document.Snapshot>, StorageError>
} = dual(2, materializeImpl)

/** Transient batch validation state containing only the records touched by a commit. */
export const State = Schema.Struct({
  format: Schema.Literal(1),
  nextId: Schema.Finite.check(Schema.makeFilter((n: number) => Number.isInteger(n))).check(
    Schema.isBetween({ minimum: 2, maximum: Number.MAX_SAFE_INTEGER + 1 }),
  ),
  nextSeq: Schema.Finite.check(Schema.makeFilter((n: number) => Number.isInteger(n))).check(
    Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER + 1 }),
  ),
  conversations: Schema.Array(Record.Conversation),
  entries: Schema.Array(Schema.Struct({ entry: Record.Entry, commitSeq: Record.Seq })),
  tasks: Schema.Array(Record.Task),
  submissions: Schema.Array(Record.Submission),
  documents: Schema.Array(Record.StoredDocument),
})
export type State = typeof State.Type

/**
 * Creates an empty durable state with initial allocation counters.
 *
 * @category combinators
 */
export const emptyState = (): State => ({
  format: 1,
  nextId: 2,
  nextSeq: 1,
  conversations: [],
  entries: [],
  tasks: [],
  submissions: [],
  documents: [],
})
