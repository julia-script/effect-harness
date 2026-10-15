/** A single-writer commit journal with immutable, reclaimable row and revision sidecars. */
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Ndjson from 'effect/encoding/Ndjson'
import type { Access } from './Kernel.js'
import * as Kernel from './Kernel.js'
import * as Metadata from './Metadata.js'
import * as Row from './Row.js'
import * as Memory from './Memory.js'
import * as Domain from '../Record.js'
import * as Errors from '../StorageError.js'

// This physical format is independent of runtime feature activation in metadata.format.
const Reference = Schema.Struct({
  id: Domain.StorageIdSchema,
  kind: Schema.Literals(['conversation', 'entry', 'task', 'submission', 'document']),
  seq: Domain.Seq,
  firstRevision: Schema.NullOr(Domain.Seq),
  revisions: Schema.Array(Domain.Seq),
})
type Reference = typeof Reference.Type
const Frame = Schema.Struct({
  persistence: Schema.Literal('sidecars-v1'),
  metadata: Metadata.Metadata,
  rows: Schema.Array(Reference),
})
const Envelope = Schema.Struct({
  persistence: Schema.optionalKey(Schema.String),
  metadata: Metadata.Metadata,
  rows: Schema.Array(Schema.Unknown),
})
const Revision = Row.StoredDocument.fields.revisions.value
const rowName = (ref: Reference) => `${ref.id}-${ref.seq}.row`
const revisionName = (id: number, seq: number) => `${id}-${seq}.revision`
const filesOf = (ref: Reference) => [
  rowName(ref),
  ...ref.revisions.map((seq) => revisionName(ref.id, seq)),
]
const io = <A, E, R>(operation: string, effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.mapError((cause) => Errors.make('io', operation, `JSONL ${operation} failed`, cause)),
  )

export const make = Effect.fnUntraced(function* (options: { readonly filePath: string }) {
  yield* Schema.decodeEffect(Schema.NonEmptyString)(options.filePath).pipe(
    Effect.mapError(Errors.invalid('initialize')),
  )
  const fs = yield* FileSystem.FileSystem
  const directory = `${options.filePath}.parts`
  if (
    !(yield* io('open', fs.exists(options.filePath))) &&
    (yield* io('open', fs.exists(directory)))
  )
    return yield* Errors.make('corrupt', 'recover', 'JSONL sidecars exist without their journal')
  const file = yield* io('open', fs.open(options.filePath, { flag: 'a+' }))
  const memory = yield* Memory.makeAccess
  const bytes = yield* io('read', fs.readFile(options.filePath))
  const end = bytes.lastIndexOf(10) + 1
  const complete = yield* Effect.try({
    try: () => new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(0, end)),
    catch: (cause) => Errors.make('corrupt', 'recover', 'JSONL contains invalid UTF-8', cause),
  })
  const frames = yield* Stream.make(complete).pipe(
    Stream.pipeThroughChannel(Ndjson.decodeSchemaString(Envelope)()),
    Stream.runCollect,
    Effect.mapError(Errors.corrupt('recover')),
  )
  let previous = Metadata.initialMetadata
  const references = new Map<number, Reference>()
  for (const envelope of frames) {
    yield* Metadata.requireSupported(envelope.metadata)
    if (
      envelope.metadata.format < previous.format ||
      (envelope.metadata.minimumReader ?? 1) < (previous.minimumReader ?? 1)
    )
      return yield* Errors.make('corrupt', 'recover', 'JSONL format marker decreased')
    if (envelope.persistence !== 'sidecars-v1')
      return yield* Errors.make(
        'unsupportedFormat',
        'recover',
        'Unsupported JSONL persistence format',
      )
    const frame = yield* Schema.decodeUnknownEffect(Frame)(envelope).pipe(
      Effect.mapError(Errors.corrupt('recover')),
    )
    if (
      frame.metadata.nextId < previous.nextId ||
      (frame.metadata.nextSeq !== previous.nextSeq &&
        frame.metadata.nextSeq !== previous.nextSeq + 1) ||
      (frame.rows.length > 0 && frame.metadata.nextSeq !== previous.nextSeq + 1)
    )
      return yield* Errors.make('corrupt', 'recover', 'JSONL allocation counters are inconsistent')
    const seen = new Set<number>()
    for (const ref of frame.rows) {
      const old = references.get(ref.id)
      if (
        seen.has(ref.id) ||
        ref.id >= frame.metadata.nextId ||
        ref.seq !== frame.metadata.nextSeq - 1 ||
        (old !== undefined && old.kind !== ref.kind) ||
        (old !== undefined && (ref.kind === 'entry' || ref.kind === 'conversation')) ||
        (ref.kind !== 'document' && (ref.firstRevision !== null || ref.revisions.length !== 0)) ||
        ref.revisions.some((seq) => seq !== ref.seq) ||
        ref.revisions.length > 1 ||
        (ref.firstRevision === null && ref.revisions.length !== 0)
      )
        return yield* Errors.make('corrupt', 'recover', 'JSONL contains invalid record references')
      seen.add(ref.id)
      const retained = (old?.revisions ?? []).filter(
        (seq) => ref.firstRevision !== null && seq >= ref.firstRevision,
      )
      const revisions = [...retained, ...ref.revisions]
      if (ref.kind === 'document' && (revisions[0] ?? null) !== ref.firstRevision)
        return yield* Errors.make('corrupt', 'recover', 'JSONL is missing a retained revision')
      references.set(ref.id, { ...ref, revisions })
    }
    previous = frame.metadata
  }
  // Validate the entire journal and every live sidecar before modifying recovery state.
  const read = <S extends Schema.Top & { readonly DecodingServices: never }>(
    name: string,
    schema: S,
  ) =>
    fs.readFile(`${directory}/${name}`).pipe(
      Effect.flatMap((bytes) =>
        Effect.try({
          try: () => new TextDecoder('utf-8', { fatal: true }).decode(bytes),
          catch: Errors.corrupt('recover'),
        }),
      ),
      Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(schema))),
      Effect.mapError(Errors.corrupt('recover')),
    )
  const rows: Array<Row.Stored> = []
  for (const ref of references.values()) {
    let row = yield* read(rowName(ref), Row.Stored)
    if (
      Row.idOf(row) !== ref.id ||
      row._tag !== ref.kind ||
      (row._tag === 'entry' && row.value.commitSeq !== ref.seq)
    )
      return yield* Errors.make(
        'corrupt',
        'recover',
        'JSONL sidecar identity does not match its marker',
      )
    if (row._tag === 'document') {
      if (
        (!Domain.isCurrentOnly(row.value.record) &&
          ref.firstRevision !== row.value.record.createdAt) ||
        (ref.firstRevision === null && row.value.record.retiredAt === undefined)
      )
        return yield* Errors.make('corrupt', 'recover', 'JSONL document has lost required history')
      if (row.value.revisions.length !== 0)
        return yield* Errors.make('corrupt', 'recover', 'Document row contains inline revisions')
      const revisions = []
      for (const seq of ref.revisions) {
        const revision = yield* read(revisionName(ref.id, seq), Revision)
        if (revision.seq !== seq)
          return yield* Errors.make(
            'corrupt',
            'recover',
            'JSONL revision identity does not match its marker',
          )
        revisions.push(revision)
      }
      row = { ...row, value: { ...row.value, revisions } }
    }
    rows.push(row)
  }
  yield* memory.save(rows, previous)
  const names = (yield* io('recover', fs.exists(directory)))
    ? yield* io('recover', fs.readDirectory(directory))
    : []
  if (names.some((name) => !/^\d+-\d+\.(row|revision)$/.test(name)))
    return yield* Errors.make('corrupt', 'recover', 'Unexpected file in JSONL sidecar directory')
  yield* io('initialize', fs.makeDirectory(directory, { recursive: true }))
  const syncDirectory = (path: string) =>
    Effect.scoped(fs.open(path, { flag: 'r' }).pipe(Effect.flatMap((directory) => directory.sync)))
  // Persist both directory entries (journal and sidecar directory) before admitting writes.
  yield* io('initialize', syncDirectory(`${directory}/..`))
  if (end !== bytes.length) {
    yield* io('truncate', file.truncate(end))
    yield* io('sync', file.sync)
  }
  const live = new Set(Array.from(references.values()).flatMap(filesOf))
  for (const name of names) {
    if (!live.has(name)) yield* io('recover', fs.remove(`${directory}/${name}`))
  }
  yield* io('recover', syncDirectory(directory))
  const write = (name: string, value: unknown) =>
    Effect.scoped(
      Effect.gen(function* () {
        const sidecar = yield* fs.open(`${directory}/${name}`, { flag: 'wx' })
        yield* sidecar.writeAll(new TextEncoder().encode(JSON.stringify(value)))
        yield* sidecar.sync
      }),
    )
  const access: Access = {
    ...memory,
    save: Effect.fnUntraced(function* (rows, metadata) {
      const changes: Array<Reference> = []
      const obsolete = new Set<string>()
      const next = new Map<number, Reference>()
      yield* Effect.gen(function* () {
        for (const row of rows) {
          const id = Row.idOf(row)
          const old = references.get(id)
          const revisions = row._tag === 'document' ? row.value.revisions : []
          const added = revisions.filter((revision) => revision.seq === metadata.nextSeq - 1)
          const ref: Reference = {
            id: yield* Schema.decodeEffect(Domain.StorageIdSchema)(id),
            kind: row._tag,
            seq: Domain.Seq.make(metadata.nextSeq - 1),
            firstRevision: revisions[0]?.seq ?? null,
            revisions: added.map((revision) => revision.seq),
          }
          changes.push(ref)
          const retained = { ...ref, revisions: revisions.map((revision) => revision.seq) }
          next.set(id, retained)
          const keep = new Set(filesOf(retained))
          for (const name of old === undefined ? [] : filesOf(old))
            if (!keep.has(name)) obsolete.add(name)
          yield* write(
            rowName(ref),
            row._tag === 'document'
              ? { ...row, value: { record: row.value.record, revisions: [] } }
              : row,
          )
          for (const revision of added) yield* write(revisionName(id, revision.seq), revision)
        }
        // A durable marker may only reference durable files and directory entries.
        if (rows.length > 0) yield* syncDirectory(directory)
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Frame))({
          persistence: 'sidecars-v1',
          rows: changes,
          metadata,
        })
        yield* file.writeAll(new TextEncoder().encode(`${encoded}\n`))
        yield* file.sync
        // The old files remain valid through any failed/partial marker append.
        for (const name of obsolete) yield* fs.remove(`${directory}/${name}`)
        if (obsolete.size > 0) yield* syncDirectory(directory)
      }).pipe(
        Effect.mapError((cause) =>
          Errors.make('uncertain', 'commit', 'JSONL write outcome is uncertain', cause),
        ),
      )
      for (const [id, ref] of next) references.set(id, ref)
      yield* memory.save(rows, metadata)
    }),
  }
  return yield* Kernel.make(access)
})
