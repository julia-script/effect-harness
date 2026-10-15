/** Physical row/revision ownership shared by the built-in adapters. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Struct from 'effect/Struct'
import * as Media from '../StorageMedia.js'
import * as Errors from '../StorageError.js'
import * as Domain from '../Record.js'
import * as Row from './Row.js'
import * as Records from './records.js'
import * as Paths from './MediaPaths.js'

export const PackedRow = Schema.Struct({
  category: Schema.Literals(['conversation', 'entry', 'task', 'submission', 'document']),
  value: Media.PackedBodySchema,
  revisions: Schema.Array(Schema.Struct({ seq: Domain.Seq, content: Media.PackedBodySchema })),
})
export type PackedRow = typeof PackedRow.Type

export const skeleton = (row: PackedRow) =>
  Schema.decodeUnknownEffect(Row.Stored)({
    _tag: row.category,
    value:
      row.category === 'document'
        ? {
            ...Struct.assign(row.value.body as Schema.JsonObject, {}),
            revisions: row.revisions.map((revision) => ({
              seq: revision.seq,
              content: revision.content.body,
            })),
          }
        : row.value.body,
  }).pipe(Effect.mapError(Errors.corrupt('media.row')))

export const references = (row: PackedRow): ReadonlyArray<Media.Reference> => [
  ...row.value.media,
  ...row.revisions.flatMap((revision) => revision.content.media),
]

export const validate = Effect.fnUntraced(function* (input: unknown) {
  const row = yield* Schema.decodeUnknownEffect(PackedRow)(input).pipe(
    Effect.mapError(Errors.corrupt('media.row')),
  )
  yield* Media.manifest(row.value)
  for (const revision of row.revisions) yield* Media.manifest(revision.content)
  if (row.category !== 'document' && row.revisions.length !== 0)
    return yield* Errors.make('corrupt', 'media.row', 'Only documents own revisions')
  return row
})

export const pack = Effect.fnUntraced(function* (row: Row.Stored, previous?: PackedRow) {
  const value = row._tag === 'document' ? { record: row.value.record, revisions: [] } : row.value
  const encoded = yield* Schema.encodeEffect(Schema.Json)(value).pipe(
    Effect.mapError(Errors.invalid('media.pack')),
  )
  const header = yield* Media.pack(encoded, {
    category: row._tag === 'document' ? 'conversation' : row._tag,
  })
  const objects = [...header.objects]
  const revisions: Array<PackedRow['revisions'][number]> = []
  if (row._tag === 'document') {
    const existing = new Map(previous?.revisions.map((revision) => [revision.seq, revision]) ?? [])
    const added = row.value.revisions.some((revision) => !existing.has(revision.seq))
    let state: Schema.JsonObject = {}
    for (const revision of row.value.revisions) {
      const old = existing.get(revision.seq)
      if (old !== undefined) revisions.push(old)
      else {
        const packed = yield* Media.pack(revision.content, {
          category: 'document',
          kind: row.value.record.kind,
          previous: state,
        })
        revisions.push({ seq: revision.seq, content: packed.packed })
        objects.push(...packed.objects)
      }
      // Pure retirement preserves opaque historical bodies; it must not interpret them.
      if (added && Paths.ownsDocumentMedia(row.value.record.kind)) {
        const content = yield* Schema.decodeUnknownEffect(Domain.Content)(revision.content).pipe(
          Effect.mapError(Errors.corrupt('media.body')),
        )
        state =
          content._tag === 'base'
            ? content.value
            : yield* Records.applyOps(state, content.ops).pipe(
                Effect.mapError(Errors.corrupt('media.body')),
              )
      }
    }
  }
  return { row: { category: row._tag, value: header.packed, revisions }, objects }
})

export const hydrate = Effect.fnUntraced(function* (
  row: PackedRow,
  read: (digest: string) => Effect.Effect<Uint8Array, Errors.StorageError>,
  metadataOnly = false,
) {
  yield* validate(row)
  if (metadataOnly) return yield* skeleton(row)
  if (row.category === 'document' && references(row).length === 0) {
    const header = yield* skeleton(row)
    if (header._tag === 'document' && !Paths.ownsDocumentMedia(header.value.record.kind))
      return header
  }
  const value = yield* Media.hydrate(
    row.value,
    { category: row.category === 'document' ? 'conversation' : row.category },
    read,
  )
  if (row.category !== 'document')
    return yield* Schema.decodeUnknownEffect(Row.Stored)({ _tag: row.category, value }).pipe(
      Effect.mapError(Errors.corrupt('media.row')),
    )
  const header = yield* skeleton(row)
  if (header._tag !== 'document')
    return yield* Errors.make('corrupt', 'media.row', 'Invalid document header')
  const revisions = []
  let state: Schema.JsonObject = {}
  for (const revision of row.revisions) {
    const content = yield* Media.hydrate(
      revision.content,
      { category: 'document', kind: header.value.record.kind, previous: state },
      read,
    )
    revisions.push({ seq: revision.seq, content })
    const decoded = yield* Schema.decodeUnknownEffect(Domain.Content)(content).pipe(
      Effect.mapError(Errors.corrupt('media.body')),
    )
    state =
      decoded._tag === 'base'
        ? decoded.value
        : yield* Records.applyOps(state, decoded.ops).pipe(
            Effect.mapError(Errors.corrupt('media.body')),
          )
  }
  return { _tag: 'document' as const, value: { record: header.value.record, revisions } }
})
