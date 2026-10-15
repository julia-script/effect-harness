/** A single-writer commit journal with immutable, reclaimable row and revision sidecars. */
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Ndjson from 'effect/encoding/Ndjson'
import * as Hex from 'effect/encoding/Hex'
import type { Access } from './Kernel.js'
import * as Kernel from './Kernel.js'
import * as Metadata from './Metadata.js'
import * as Row from './Row.js'
import * as MediaRow from './MediaRow.js'
import * as Media from '../StorageMedia.js'
import * as Memory from './Memory.js'
import * as Domain from '../Record.js'
import * as Errors from '../StorageError.js'

// This physical format is independent of runtime feature activation in metadata.format.
const Reference = Schema.Struct({
  id: Domain.StorageIdSchema,
  kind: Schema.Literals(['conversation', 'entry', 'task', 'submission', 'document']),
  seq: Domain.Seq,
  firstRevision: Schema.NullOr(Domain.Seq),
  ownership: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  revisions: Schema.Array(Domain.Seq),
})
type Reference = typeof Reference.Type
const Frame = Schema.Struct({
  persistence: Schema.Literal('sidecars-v4'),
  metadata: Metadata.Metadata,
  rows: Schema.Array(Reference),
})
const Envelope = Schema.Struct({
  persistence: Schema.optionalKey(Schema.String),
  metadata: Metadata.Metadata,
  rows: Schema.Array(Schema.Unknown),
})
const Revision = Schema.Struct({ seq: Domain.Seq, content: Media.PackedBodySchema })
const RevisionHeader = Schema.Struct({
  _tag: Schema.Literals(['base', 'delta']),
  version: Domain.Content.members[0].fields.version,
})
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

// Commit markers independently bind every retained owner manifest. Checking only
// the references a sidecar happens to list cannot detect an omitted reference.
// Do not hash/decode bodies: malformed obsolete bodies must remain retireable.
const ownership = (row: MediaRow.PackedRow) => {
  const manifest = (refs: ReadonlyArray<Media.Reference>) =>
    refs.map((ref) => [
      ref.path,
      ref.digest,
      ref.byteLength,
      ref.presentation._tag,
      ref.presentation._tag === 'data-url' ? ref.presentation.prefix : null,
    ])
  const encoded = new TextEncoder().encode(
    JSON.stringify([
      'effect-harness/jsonl/ownership/1',
      manifest(row.value.media),
      row.revisions.map((revision) => [revision.seq, manifest(revision.content.media)]),
    ]),
  )
  return Effect.tryPromise({
    try: async () =>
      Hex.encode(new Uint8Array(await globalThis.crypto.subtle.digest('SHA-256', encoded))),
    catch: (cause) => Errors.make('io', 'media.hash', 'Cannot hash media ownership', cause),
  })
}

export const make = Effect.fnUntraced(function* (options: { readonly filePath: string }) {
  yield* Schema.decodeEffect(Schema.NonEmptyString)(options.filePath).pipe(
    Effect.mapError(Errors.invalid('initialize')),
  )
  const fs = yield* FileSystem.FileSystem
  const directory = `${options.filePath}.parts`
  const mediaDirectory = `${options.filePath}.media`
  if (
    !(yield* io('open', fs.exists(options.filePath))) &&
    ((yield* io('open', fs.exists(directory))) || (yield* io('open', fs.exists(mediaDirectory))))
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
    if (envelope.persistence !== 'sidecars-v4')
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
  const names = (yield* io('recover', fs.exists(directory)))
    ? yield* io('recover', fs.readDirectory(directory))
    : []
  if (names.some((name) => !/^\d+-\d+\.(row|revision)$/.test(name)))
    return yield* Errors.make('corrupt', 'recover', 'Unexpected file in JSONL sidecar directory')
  const pruning = new Set<number>()
  for (const name of names) {
    const match = /^(\d+)-(\d+)\.revision$/.exec(name)
    if (match === null) continue
    const id = Number(match[1])
    const first = references.get(id)?.firstRevision
    if (first !== undefined && first !== null && Number(match[2]) < first) pruning.add(id)
  }
  const rows = new Map<number, MediaRow.PackedRow>()
  const objects = new Map<string, Uint8Array>()
  for (const ref of references.values()) {
    const packed = yield* MediaRow.validate(yield* read(rowName(ref), MediaRow.PackedRow))
    let row = yield* MediaRow.skeleton(packed)
    const packedRevisions: Array<MediaRow.PackedRow['revisions'][number]> = []
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
        (ref.firstRevision === null && row.value.record.retiredAt !== ref.seq)
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
        yield* Media.manifest(revision.content)
        packedRevisions.push(revision)
        revisions.push({ seq: revision.seq, content: revision.content.body })
      }
      // Replacing a current-only base may prune earlier revisions, but a delta
      // cannot authorize that deletion. Retired bodies remain entirely opaque.
      if (pruning.has(ref.id)) {
        let version: number | undefined
        for (const [index, revision] of revisions.entries()) {
          const header = yield* Schema.decodeUnknownEffect(RevisionHeader)(revision.content).pipe(
            Effect.mapError(Errors.corrupt('recover')),
          )
          if (
            (index === 0 && header._tag !== 'base') ||
            (index > 0 && (header._tag !== 'delta' || header.version !== version))
          )
            return yield* Errors.make('corrupt', 'recover', 'JSONL prune has no continuous base')
          version = header.version
        }
      }
      row = { ...row, value: { ...row.value, revisions } }
    }
    const physical = { ...packed, revisions: packedRevisions }
    if ((yield* ownership(physical)) !== ref.ownership)
      return yield* Errors.make(
        'corrupt',
        'media.manifest',
        'Media ownership does not match its commit marker',
      )
    for (const ref of MediaRow.references(physical)) {
      let bytes = objects.get(ref.digest)
      if (bytes === undefined) {
        bytes = yield* fs
          .readFile(`${mediaDirectory}/${ref.digest}`)
          .pipe(
            Effect.mapError((cause) =>
              Errors.make('corrupt', 'media.read', 'Cannot read required media object', cause),
            ),
          )
        objects.set(ref.digest, bytes)
      }
      yield* Media.verify(ref, bytes)
    }
    rows.set(ref.id, physical)
  }
  const mediaNames = (yield* io('recover', fs.exists(mediaDirectory)))
    ? yield* io('recover', fs.readDirectory(mediaDirectory))
    : []
  for (const name of mediaNames)
    if (!/^[bl]-[a-f0-9]{64}$/.test(name))
      return yield* Errors.make('corrupt', 'media.manifest', 'Unexpected media object name')
  yield* memory.publish({ rows, objects, metadata: previous })
  yield* io('initialize', fs.makeDirectory(directory, { recursive: true }))
  yield* io('initialize', fs.makeDirectory(mediaDirectory, { recursive: true }))
  const syncDirectory = (path: string) =>
    Effect.scoped(fs.open(path, { flag: 'r' }).pipe(Effect.flatMap((directory) => directory.sync)))
  // Persist both directory entries (journal and sidecar directory) before admitting writes.
  yield* io('initialize', syncDirectory(`${directory}/..`))
  if (end !== bytes.length) yield* io('truncate', file.truncate(end))
  // A complete marker can survive a failed sync. Make recovery's chosen prefix
  // durable before deleting files needed by the previously durable prefix.
  yield* io('sync', file.sync)
  const live = new Set(Array.from(references.values()).flatMap(filesOf))
  for (const name of names) {
    if (!live.has(name)) yield* io('recover', fs.remove(`${directory}/${name}`))
  }
  yield* io('recover', syncDirectory(directory))
  for (const name of mediaNames) {
    if (!objects.has(name)) yield* io('recover', fs.remove(`${mediaDirectory}/${name}`))
  }
  yield* io('recover', syncDirectory(mediaDirectory))
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
      const current = yield* memory.snapshot
      const prepared = yield* memory.prepare(rows, metadata)
      const changes: Array<Reference> = []
      const obsolete = new Set<string>()
      const next = new Map<number, Reference>()
      yield* Effect.gen(function* () {
        let addedMedia = false
        for (const [digest, bytes] of prepared.objects) {
          if (current.objects.has(digest)) continue
          addedMedia = true
          const path = `${mediaDirectory}/${digest}`
          if (yield* fs.exists(path)) {
            const previous = yield* fs.readFile(path)
            if (!Media.sameObject(previous, bytes))
              return yield* Errors.make('corrupt', 'media.integrity', 'Media hash collision')
          } else {
            yield* Effect.scoped(
              Effect.gen(function* () {
                const object = yield* fs.open(path, { flag: 'wx' })
                yield* object.writeAll(bytes)
                yield* object.sync
              }),
            )
          }
        }
        if (addedMedia) yield* syncDirectory(mediaDirectory)
        for (const row of rows) {
          const id = Row.idOf(row)
          const old = references.get(id)
          const revisions = row._tag === 'document' ? row.value.revisions : []
          const added = revisions.filter((revision) => revision.seq === metadata.nextSeq - 1)
          const physical = prepared.rows.get(id)!
          const ref: Reference = {
            id: yield* Schema.decodeEffect(Domain.StorageIdSchema)(id),
            kind: row._tag,
            seq: Domain.Seq.make(metadata.nextSeq - 1),
            firstRevision: revisions[0]?.seq ?? null,
            revisions: added.map((revision) => revision.seq),
            ownership: yield* ownership(physical),
          }
          changes.push(ref)
          const retained = { ...ref, revisions: revisions.map((revision) => revision.seq) }
          next.set(id, retained)
          const keep = new Set(filesOf(retained))
          for (const name of old === undefined ? [] : filesOf(old))
            if (!keep.has(name)) obsolete.add(name)
          yield* write(rowName(ref), { ...physical, revisions: [] })
          for (const revision of physical.revisions) {
            if (revision.seq === metadata.nextSeq - 1)
              yield* write(revisionName(id, revision.seq), revision)
          }
        }
        // A durable marker may only reference durable files and directory entries.
        if (rows.length > 0) yield* syncDirectory(directory)
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Frame))({
          persistence: 'sidecars-v4',
          rows: changes,
          metadata,
        })
        yield* file.writeAll(new TextEncoder().encode(`${encoded}\n`))
        yield* file.sync
        // The old files remain valid through any failed/partial marker append.
        for (const name of obsolete) yield* fs.remove(`${directory}/${name}`)
        if (obsolete.size > 0) yield* syncDirectory(directory)
        let removedMedia = false
        for (const digest of current.objects.keys()) {
          if (!prepared.objects.has(digest)) {
            yield* fs.remove(`${mediaDirectory}/${digest}`)
            removedMedia = true
          }
        }
        if (removedMedia) yield* syncDirectory(mediaDirectory)
      }).pipe(
        Effect.mapError((cause) =>
          Errors.make('uncertain', 'commit', 'JSONL write outcome is uncertain', cause),
        ),
      )
      for (const [id, ref] of next) references.set(id, ref)
      yield* memory.publish(prepared)
    }),
  }
  return yield* Kernel.make(access)
})
