/** Shared commit codec, recovery and retention policy over opaque persistence. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Persistence from './Persistence.js'
import * as Projection from './Projection.js'
import * as Kernel from './Kernel.js'
import * as Metadata from './Metadata.js'
import * as Row from './Row.js'
import * as Record from '../Record.js'
import * as Errors from '../StorageError.js'

const Inline = Schema.Struct({
  type: Schema.Literals(['conversation', 'entry', 'submission', 'task']),
  value: Schema.Unknown,
})
const Document = Schema.Struct({
  type: Schema.Literals(['document.create', 'document.change']),
  record: Row.StoredDocument.fields.record,
  firstRevision: Schema.NullOr(Record.Seq),
  ordinal: Schema.optionalKey(Schema.Natural),
})
const Task = Schema.Struct({
  type: Schema.Literal('task.sidecar'),
  id: Record.TaskId,
  ordinal: Schema.Natural,
})
const Write = Schema.Union([Inline, Document, Task])
const Envelope = Schema.Struct({
  format: Schema.Finite,
  type: Schema.Literal('commit'),
  seq: Metadata.Counter,
  metadata: Metadata.Metadata,
  writes: Schema.Array(Schema.Unknown),
})
const Frame = Envelope.mapFields((fields) => ({ ...fields, writes: Schema.Array(Write) }))
const Payload = Schema.Struct({
  format: Schema.Literal(1),
  type: Schema.Literal('record'),
  seq: Metadata.Counter,
  ordinal: Schema.Natural,
  payload: Schema.Unknown,
})
const Revision = Row.StoredDocument.fields.revisions.value

type Pointer = { readonly seq: number; readonly ordinal: number; readonly revision: number }
type Plan = {
  readonly kind: Row.Kind
  readonly row: Row.Stored | undefined
  readonly key: string | undefined
  readonly pointers: ReadonlyArray<Pointer>
}
const identity = (pointer: { readonly seq: number; readonly ordinal: number }) =>
  `${pointer.seq}:${pointer.ordinal}`
const decode = <S extends Schema.Top & { readonly DecodingServices: never }>(
  schema: S,
  text: string,
) =>
  Schema.decodeEffect(Schema.fromJsonString(schema))(text).pipe(
    Effect.mapError(Errors.corrupt('recover')),
  )

export const make = Effect.fnUntraced(function* (persistence: Persistence.Persistence) {
  const projection = yield* Projection.makeAccess
  yield* Effect.addFinalizer(() => projection.exclusive(Effect.void).pipe(Effect.orDie))
  let metadata = Metadata.initialMetadata
  let sequence = 0
  let plans = new Map<number, Plan>()
  const contents = new Map<string, Map<string, string>>()

  const project = Effect.fnUntraced(function* (frame: typeof Frame.Type) {
    if (
      frame.seq !== sequence + 1 ||
      frame.metadata.format < metadata.format ||
      (frame.metadata.minimumReader ?? 1) < (metadata.minimumReader ?? 1) ||
      frame.metadata.nextId < metadata.nextId ||
      (frame.metadata.nextSeq !== metadata.nextSeq &&
        frame.metadata.nextSeq !== metadata.nextSeq + 1) ||
      (frame.writes.length > 0 && frame.metadata.nextSeq !== metadata.nextSeq + 1)
    )
      return yield* Errors.make('corrupt', 'recover', 'Invalid commit sequence or metadata')
    const next = new Map(plans)
    const seen = new Set<number>()
    for (const write of frame.writes) {
      let id: number
      let plan: Plan
      const revision = frame.metadata.nextSeq - 1
      if (write.type === 'task.sidecar') {
        id = write.id
        plan = {
          kind: 'task',
          row: undefined,
          key: `task-${id}`,
          pointers: [{ seq: frame.seq, ordinal: write.ordinal, revision }],
        }
      } else if (write.type === 'document.create' || write.type === 'document.change') {
        id = write.record.id
        const old = plans.get(id)
        if ((write.type === 'document.create') !== (old === undefined))
          return yield* Errors.make('corrupt', 'recover', 'Invalid document creation or change')
        const pointers = (old?.pointers ?? []).filter(
          (p) => write.firstRevision !== null && p.revision >= write.firstRevision,
        )
        if (write.ordinal !== undefined)
          pointers.push({ seq: frame.seq, ordinal: write.ordinal, revision })
        if (
          (pointers[0]?.revision ?? null) !== write.firstRevision ||
          (!Record.isCurrentOnly(write.record) && write.firstRevision !== write.record.createdAt) ||
          (write.firstRevision === null && write.record.retiredAt !== revision)
        )
          return yield* Errors.make('corrupt', 'recover', 'Document has lost required history')
        plan = {
          kind: 'document',
          row: { _tag: 'document', value: { record: write.record, revisions: [] } },
          key: `doc-${id}`,
          pointers,
        }
      } else if ('value' in write) {
        const value =
          write.type === 'entry' ? { entry: write.value, commitSeq: revision } : write.value
        const row = yield* Schema.decodeUnknownEffect(Row.Stored)({ _tag: write.type, value }).pipe(
          Effect.mapError(Errors.corrupt('recover')),
        )
        if (row._tag === 'task' && row.value.state.status !== 'terminal')
          return yield* Errors.make('corrupt', 'recover', 'Active task must have a payload stream')
        id = Row.idOf(row)
        plan = { kind: row._tag, row, key: undefined, pointers: [] }
      } else return yield* Errors.make('corrupt', 'recover', 'Unknown stored operation')
      const old = plans.get(id)
      if (
        seen.has(id) ||
        id >= frame.metadata.nextId ||
        (old !== undefined &&
          (old.kind !== plan.kind || plan.kind === 'entry' || plan.kind === 'conversation'))
      )
        return yield* Errors.make('corrupt', 'recover', 'Conflicting record identity')
      seen.add(id)
      next.set(id, plan)
    }
    return next
  })

  // Read every format barrier before decoding domain rows; no disk changes until validation finishes.
  const envelopes = yield* Effect.forEach(yield* persistence.journal, (line) =>
    decode(Envelope, line),
  )
  for (const envelope of envelopes) {
    if (envelope.format !== 1)
      return yield* Errors.make('unsupportedFormat', 'recover', 'Unsupported commit format')
    yield* Metadata.requireSupported(envelope.metadata)
  }
  for (const envelope of envelopes) {
    const frame = yield* Schema.decodeUnknownEffect(Frame)(envelope).pipe(
      Effect.mapError(Errors.corrupt('recover')),
    )
    plans = yield* project(frame)
    metadata = frame.metadata
    sequence = frame.seq
  }
  const rows: Array<Row.Stored> = []
  for (const [id, plan] of plans) {
    const values: Array<typeof Payload.Type> = []
    let pruning = false
    if (plan.key !== undefined && plan.pointers.length > 0) {
      const selected = new Map<string, string>()
      const available = new Map<
        string,
        { readonly line: string; readonly value: typeof Payload.Type }
      >()
      for (const line of yield* persistence.read(plan.key)) {
        const value = yield* decode(Payload, line)
        const key = identity(value)
        if (available.has(key))
          return yield* Errors.make('corrupt', 'recover', 'Duplicate payload position')
        available.set(key, { line, value })
      }
      for (const pointer of plan.pointers) {
        const record = available.get(identity(pointer))
        if (record === undefined)
          return yield* Errors.make('corrupt', 'recover', 'Missing committed payload')
        values.push(record.value)
        selected.set(identity(pointer), record.line)
      }
      pruning = Array.from(available.values()).some(
        ({ value }) => value.seq < (plan.pointers[0]?.seq ?? 0),
      )
      contents.set(plan.key, selected)
    }
    if (plan.kind === 'task' && plan.row === undefined) {
      const row = yield* Schema.decodeUnknownEffect(Row.Stored)(values[0]?.payload).pipe(
        Effect.mapError(Errors.corrupt('recover')),
      )
      if (row._tag !== 'task' || row.value.id !== id || row.value.state.status === 'terminal')
        return yield* Errors.make('corrupt', 'recover', 'Task payload identity mismatch')
      rows.push(row)
    } else if (plan.row?._tag === 'document') {
      const revisions = yield* Effect.forEach(values, (value, index) =>
        Schema.decodeUnknownEffect(Revision)(value.payload).pipe(
          Effect.mapError(Errors.corrupt('recover')),
          Effect.filterOrElse(
            (revision) => revision.seq === plan.pointers[index]?.revision,
            () => Errors.make('corrupt', 'recover', 'Document revision identity mismatch'),
          ),
        ),
      )
      // A newer delta cannot authorize deleting the base it depends on.
      // Check headers only: obsolete bodies remain opaque to recovery.
      if (pruning) {
        let version: number | undefined
        for (const [index, revision] of revisions.entries()) {
          const header = yield* Schema.decodeUnknownEffect(Row.ContentHeader)(
            revision.content,
          ).pipe(Effect.mapError(Errors.corrupt('recover')))
          if (
            (index === 0 && header._tag !== 'base') ||
            (index > 0 && (header._tag !== 'delta' || header.version !== version))
          )
            return yield* Errors.make(
              'corrupt',
              'recover',
              'Pruned document has no continuous base',
            )
          version = header.version
        }
      }
      rows.push({ ...plan.row, value: { ...plan.row.value, revisions } })
    } else if (plan.row !== undefined) rows.push(plan.row)
  }
  const keys = yield* persistence.keys
  yield* persistence.accept
  yield* projection.save(rows, metadata)
  // Remove uncommitted tails and obsolete streams only after all live records validate.
  for (const key of keys)
    yield* persistence.replace({ key, lines: Array.from(contents.get(key)?.values() ?? []) })

  const save = Effect.fnUntraced(function* (
    changed: ReadonlyArray<Row.Stored>,
    nextMetadata: Metadata.Metadata,
  ) {
    // IDs become durable with their first commit. Uncommitted allocations may be reused after reopen.
    if (
      changed.length === 0 &&
      nextMetadata.format === metadata.format &&
      nextMetadata.minimumReader === metadata.minimumReader &&
      nextMetadata.nextSeq === metadata.nextSeq
    ) {
      yield* projection.save(changed, nextMetadata)
      return
    }
    const writes: Array<typeof Write.Type> = []
    const append: Array<{ key: string; lines: ReadonlyArray<string> }> = []
    const added = new Map<string, Map<string, string>>()
    let ordinal = 0
    const payload = (key: string, value: unknown) => {
      const record = {
        format: 1 as const,
        type: 'record' as const,
        seq: sequence + 1,
        ordinal: ordinal++,
        payload: value,
      }
      const line = JSON.stringify(record)
      append.push({ key, lines: [line] })
      const records = added.get(key) ?? new Map<string, string>()
      records.set(identity(record), line)
      added.set(key, records)
      return record.ordinal
    }
    for (const row of changed) {
      const id = Row.idOf(row)
      if (row._tag === 'document') {
        const revision = row.value.revisions.find((r) => r.seq === nextMetadata.nextSeq - 1)
        writes.push({
          type: plans.has(id) ? 'document.change' : 'document.create',
          record: row.value.record,
          firstRevision: row.value.revisions[0]?.seq ?? null,
          ...(revision === undefined ? {} : { ordinal: payload(`doc-${id}`, revision) }),
        })
      } else if (row._tag === 'task' && row.value.state.status !== 'terminal') {
        writes.push({ type: 'task.sidecar', id: row.value.id, ordinal: payload(`task-${id}`, row) })
      } else {
        writes.push({ type: row._tag, value: row._tag === 'entry' ? row.value.entry : row.value })
      }
    }
    const frame = {
      format: 1,
      type: 'commit' as const,
      seq: sequence + 1,
      metadata: nextMetadata,
      writes,
    }
    const next = yield* project(frame)
    const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Frame))(frame).pipe(
      Effect.mapError(Errors.invalid('commit')),
    )
    yield* persistence.append({ frame: encoded, append })
    yield* projection.save(changed, nextMetadata)
    const affected = new Set<string>()
    for (const row of changed) {
      const id = Row.idOf(row)
      const old = plans.get(id)
      const current = next.get(id)
      if (old?.key !== undefined) affected.add(old.key)
      if (current?.key !== undefined) affected.add(current.key)
    }
    plans = next
    metadata = nextMetadata
    sequence++
    for (const key of affected) {
      const records = new Map(contents.get(key))
      for (const [position, line] of added.get(key) ?? []) records.set(position, line)
      const plan = Array.from(plans.values()).find((p) => p.key === key)
      const keep = new Set(plan?.pointers.map(identity) ?? [])
      const retained = new Map(Array.from(records).filter(([position]) => keep.has(position)))
      contents.set(key, retained)
      // Maintenance failure cannot undo a confirmed commit. Recovery retries reclamation.
      if (retained.size !== records.size)
        yield* persistence
          .replace({ key, lines: Array.from(retained.values()) })
          .pipe(Effect.ignore)
    }
  })
  return yield* Kernel.make({
    ...projection,
    exclusive: (effect) =>
      projection.exclusive(persistence.checkWrite.pipe(Effect.andThen(effect))),
    save,
  })
})
