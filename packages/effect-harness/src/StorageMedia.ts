/** Content-addressed physical bodies for adapters that can publish ownership atomically. */
import * as Effect from 'effect/Effect'
import * as Base64 from 'effect/encoding/Base64'
import * as Encoding from './internal/MediaEncoding.js'
import * as Hex from 'effect/encoding/Hex'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Record from './Record.js'
import * as Errors from './StorageError.js'
import * as Paths from './internal/MediaPaths.js'
import * as Records from './internal/records.js'

const Path = Schema.NonEmptyArray(
  Schema.Union([
    Schema.String,
    Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
  ]),
)
export const ReferenceSchema = Schema.Struct({
  path: Path,
  digest: Schema.String.check(Schema.isPattern(/^[bl]-[a-f0-9]{64}$/)),
  byteLength: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
  presentation: Schema.Union([
    Schema.TaggedStruct('base64', {}),
    Schema.TaggedStruct('data-url', {
      prefix: Schema.String.check(Schema.isPattern(/^data:[^,]*;base64,$/)),
    }),
    Schema.TaggedStruct('literal', {}),
  ]),
})
export type Reference = typeof ReferenceSchema.Type
/** This envelope is a physical record, never an application JSON marker. */
export const PackedBodySchema = Schema.Struct({
  version: Schema.Literal(2),
  coverage: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  body: Schema.Json,
  media: Schema.Array(ReferenceSchema),
})
export type PackedBody = typeof PackedBodySchema.Type
export interface ObjectData {
  readonly digest: string
  readonly bytes: Uint8Array
}
/** Records use their public JSON value; document bodies use Record.Content and prior materialization. */
export type Context =
  | { readonly category: 'conversation' | 'entry' | 'task' | 'submission' }
  | { readonly category: 'document'; readonly kind: string; readonly previous?: Schema.JsonObject }

const integrity = (message: string) => Errors.make('corrupt', 'media.integrity', message)
const starts = (path: Paths.Path, prefix: Paths.Path) =>
  prefix.length <= path.length && prefix.every((key, index) => String(path[index]) === String(key))
const select = Effect.fnUntraced(
  function* (value: Schema.Json, context: Context) {
    if (context.category !== 'document') return Paths.record(context.category, value)
    if (!Paths.ownsDocumentMedia(context.kind)) return []
    const content = yield* Schema.decodeUnknownEffect(Record.Content)(value)
    if (content._tag === 'base')
      return Paths.document(context.kind, content.value).map((path) => ['value', ...path])
    let state = context.previous ?? {}
    const selected: Array<Paths.Path> = []
    for (const [index, op] of content.ops.entries()) {
      state = yield* Records.applyOps(state, [op])
      for (const path of Paths.document(context.kind, state)) {
        if (op[0] === 'replace') selected.push(['ops', index, 1, ...path])
        else if (op[0] === 'set' && starts(path, op[1]))
          selected.push(['ops', index, 2, ...path.slice(op[1].length)])
        else if (op[0] === 'splice' && starts(path, op[1])) {
          const offset = path[op[1].length]
          if (typeof offset === 'number' && offset >= op[2] && offset < op[2] + op[4].length)
            selected.push(['ops', index, 4, offset - op[2], ...path.slice(op[1].length + 1)])
        }
      }
    }
    return selected
  },
  Effect.mapError(Errors.corrupt('media.body')),
)
const payload = (
  value: Schema.Json | undefined,
): { bytes: Uint8Array; presentation: Reference['presentation'] } | undefined => {
  if (typeof value !== 'string') return undefined
  const match = /^(data:[^,]*;base64,)(.*)$/s.exec(value)
  const base64 = match === null ? value : match[2]!
  const decoded = Base64.decode(base64)
  if (Result.isSuccess(decoded) && Encoding.encode(decoded.success) === base64)
    return {
      bytes: decoded.success,
      presentation: match === null ? { _tag: 'base64' } : { _tag: 'data-url', prefix: match[1]! },
    }
  // External references, including local file URLs, remain verbatim and are never resolved.
  if (/^[a-z][a-z0-9+.-]*:/i.test(value) && !value.startsWith('data:')) return undefined
  return {
    bytes: new TextEncoder().encode(JSON.stringify(value)),
    presentation: { _tag: 'literal' },
  }
}
const digest = (bytes: Uint8Array, literal: boolean) =>
  Effect.tryPromise({
    try: async () => {
      const hash = new Uint8Array(
        await globalThis.crypto.subtle.digest('SHA-256', new Uint8Array(bytes)),
      )
      return `${literal ? 'l' : 'b'}-${Hex.encode(hash)}`
    },
    catch: (cause) => Errors.make('io', 'media.hash', 'Cannot hash media bytes', cause),
  })
// Independent physical coverage includes every occurrence, not only unique digests.
// Stable tuples avoid dependence on object-key serialization order. This is a
// corruption check, not authentication against coordinated metadata rewrites.
const coverage = (media: ReadonlyArray<Reference>) => {
  if (media.length === 0)
    return Effect.succeed('4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945')
  const encoded = JSON.stringify(
    media.map((ref) => [
      ref.path,
      ref.digest,
      ref.byteLength,
      ref.presentation._tag,
      ref.presentation._tag === 'data-url' ? ref.presentation.prefix : null,
    ]),
  )
  return digest(new TextEncoder().encode(encoded), false).pipe(Effect.map((hash) => hash.slice(2)))
}

/** Verify every persisted object, including hash hits, before using its bytes. */
export const verify = Effect.fnUntraced(function* (
  reference: Pick<Reference, 'digest' | 'byteLength'>,
  bytes: Uint8Array,
) {
  if (
    bytes.length !== reference.byteLength ||
    (yield* digest(bytes, reference.digest.startsWith('l-'))) !== reference.digest
  )
    return yield* integrity('Media digest or length does not match its manifest')
})
/** Exact comparison on a hash hit fails closed, even if a hash primitive collides. */
export const sameObject = (left: Uint8Array, right: Uint8Array): boolean =>
  left.length === right.length && left.every((value, index) => value === right[index])

// JSON has occurrences, not object identity. structuredClone preserves aliases;
// punching one selected hole would otherwise also erase an opaque/unselected alias.
const copyJson = (value: Schema.Json): Schema.Json => {
  if (Array.isArray(value)) return value.map(copyJson)
  if (value !== null && typeof value === 'object')
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, copyJson(item)]))
  return value
}

/** Packs only known image slots, leaving arbitrary JSON, aliases, URLs and metadata untouched. */
export const pack = Effect.fnUntraced(function* (value: Schema.Json, context: Context) {
  const body = copyJson(value)
  const media: Array<Reference> = []
  const objects = new Map<string, ObjectData>()
  for (const path of yield* select(body, context)) {
    const data = payload(Paths.at(body, path))
    if (data === undefined) continue
    const key = yield* digest(data.bytes, data.presentation._tag === 'literal')
    const previous = objects.get(key)
    if (previous !== undefined && !sameObject(previous.bytes, data.bytes))
      return yield* integrity('Media hash collision')
    const reference = yield* Schema.decodeUnknownEffect(ReferenceSchema)({
      path,
      digest: key,
      byteLength: data.bytes.length,
      presentation: data.presentation,
    }).pipe(Effect.mapError(Errors.invalid('media.pack')))
    media.push(reference)
    objects.set(key, { digest: key, bytes: data.bytes })
    if (!Paths.set(body, path, null)) return yield* integrity('Invalid image slot')
  }
  return {
    packed: { version: 2 as const, coverage: yield* coverage(media), body, media },
    objects: [...objects.values()],
  }
})

/**
 * Validate listed references without decoding the document body.
 * The physical coverage checksum detects omitted or altered occurrences independently
 * of application envelope eligibility. Adapters still need atomic durable ownership
 * for reclamation; the checksum does not authenticate coordinated metadata edits.
 */
export const manifest = (value: unknown) =>
  Schema.decodeUnknownEffect(PackedBodySchema)(value).pipe(
    Effect.flatMap(
      Effect.fnUntraced(function* (packed) {
        if ((yield* coverage(packed.media)) !== packed.coverage)
          return yield* integrity('Media manifest does not match its physical coverage')
        const paths: Array<Paths.Path> = []
        for (const ref of packed.media) {
          if (
            (ref.presentation._tag === 'literal') !== ref.digest.startsWith('l-') ||
            paths.some((path) => starts(path, ref.path) || starts(ref.path, path)) ||
            Paths.at(packed.body, ref.path) !== null
          )
            return yield* integrity('Invalid, overlapping or missing media slot')
          paths.push(ref.path)
        }
        return packed
      }),
    ),
    Effect.mapError(Errors.corrupt('media.manifest')),
  )

/** Returns an owned, self-contained JSON body. Resolver work and failures stay below Storage. */
export const hydrate = Effect.fnUntraced(function* (
  value: PackedBody,
  context: Context,
  read: (digest: string) => Effect.Effect<Uint8Array, Errors.StorageError>,
) {
  const packed = yield* manifest(value)
  const body = copyJson(packed.body)
  for (const ref of packed.media) {
    const bytes = new Uint8Array(yield* read(ref.digest))
    yield* verify(ref, bytes)
    let restored: string
    if (ref.presentation._tag === 'literal') {
      const text = yield* Effect.try({
        try: () => new TextDecoder('utf-8', { fatal: true }).decode(bytes),
        catch: Errors.corrupt('media.integrity'),
      })
      restored = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.String))(text).pipe(
        Effect.mapError(Errors.corrupt('media.integrity')),
      )
    } else {
      const base64 = Encoding.encode(bytes)
      restored = ref.presentation._tag === 'data-url' ? ref.presentation.prefix + base64 : base64
    }
    if (!Paths.set(body, ref.path, restored)) return yield* integrity('Cannot restore media slot')
  }
  const allowed = yield* select(body, context)
  if (allowed.some((path) => Paths.at(body, path) === null))
    return yield* integrity('Unrestored media slot')
  const expected = new Set(
    allowed
      .filter((path) => payload(Paths.at(body, path)) !== undefined)
      .map((path) => JSON.stringify(path)),
  )
  if (
    expected.size !== packed.media.length ||
    packed.media.some((ref) => !expected.has(JSON.stringify(ref.path)))
  )
    return yield* integrity('Media manifest does not match the typed image slots')
  return body
})
