import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Ref from 'effect/Ref'
import * as Semaphore from 'effect/Semaphore'
import type { Access } from './Kernel.js'
import * as Kernel from './Kernel.js'
import * as Metadata from './Metadata.js'
import * as Row from './Row.js'
import * as MediaRow from './MediaRow.js'
import * as Media from '../StorageMedia.js'
import * as Errors from '../StorageError.js'

/** Immutable packed state and its object closure are published together. */
export const makeAccess = Effect.gen(function* () {
  const state = yield* Ref.make({
    metadata: Metadata.initialMetadata,
    rows: new Map<number, MediaRow.PackedRow>(),
    objects: new Map<string, Uint8Array>(),
    owners: new Map<string, number>(),
  })
  const lock = yield* Semaphore.make(1)
  const read = (objects: ReadonlyMap<string, Uint8Array>) => (digest: string) => {
    const bytes = objects.get(digest)
    return bytes === undefined
      ? Effect.fail(Errors.make('corrupt', 'media.read', 'Media object is absent'))
      : Effect.succeed(bytes)
  }
  const prepare = Effect.fnUntraced(function* (
    rows: ReadonlyArray<Row.Stored>,
    metadata: Metadata.Metadata,
  ) {
    const current = yield* Ref.get(state)
    const next = new Map(current.rows)
    const objects = new Map(current.objects)
    const owners = new Map(current.owners)
    for (const row of rows) {
      const id = Row.idOf(row)
      const packed = yield* MediaRow.pack(row, current.rows.get(id))
      for (const object of packed.objects) {
        const previous = objects.get(object.digest)
        if (previous !== undefined && !Media.sameObject(previous, object.bytes))
          return yield* Errors.make('corrupt', 'media.integrity', 'Media hash collision')
        objects.set(object.digest, object.bytes)
      }
      const old = current.rows.get(id)
      if (old !== undefined)
        for (const digest of new Set(MediaRow.references(old).map((ref) => ref.digest)))
          owners.set(digest, (owners.get(digest) ?? 0) - 1)
      for (const digest of new Set(MediaRow.references(packed.row).map((ref) => ref.digest)))
        owners.set(digest, (owners.get(digest) ?? 0) + 1)
      next.set(id, packed.row)
    }
    for (const [digest, count] of owners)
      if (count === 0) {
        objects.delete(digest)
        owners.delete(digest)
      }
    return { metadata, rows: next, objects, owners }
  })
  const access: Access = {
    metadata: Ref.get(state).pipe(Effect.map((state) => ({ ...state.metadata }))),
    get: Effect.fnUntraced(function* (id, metadataOnly = false) {
      const current = yield* Ref.get(state)
      const row = current.rows.get(id)
      return row === undefined
        ? Option.none()
        : Option.some(yield* MediaRow.hydrate(row, read(current.objects), metadataOnly))
    }),
    page: Effect.fnUntraced(function* (kind, after, limit, filter, order, metadataOnly = false) {
      const current = yield* Ref.get(state)
      const selected = []
      for (const [id, packed] of current.rows) {
        if (
          packed.category !== kind ||
          (after !== undefined && (order === 'ascending' ? id <= after : id >= after))
        )
          continue
        const row = yield* MediaRow.skeleton(packed)
        if (Row.matches(row, filter)) selected.push({ id, packed })
      }
      selected.sort((a, b) => (order === 'ascending' ? a.id - b.id : b.id - a.id))
      return yield* Effect.forEach(selected.slice(0, limit), ({ packed }) =>
        MediaRow.hydrate(packed, read(current.objects), metadataOnly),
      )
    }),
    save: (rows, metadata) =>
      prepare(rows, metadata).pipe(Effect.flatMap((next) => Ref.set(state, next))),
    exclusive: (effect) => lock.withPermit(effect),
  }
  return {
    ...access,
    snapshot: Ref.get(state),
    prepare,
    publish: (next: Omit<typeof state extends Ref.Ref<infer S> ? S : never, 'owners'>) => {
      const owners = new Map<string, number>()
      for (const row of next.rows.values())
        for (const digest of new Set(MediaRow.references(row).map((ref) => ref.digest)))
          owners.set(digest, (owners.get(digest) ?? 0) + 1)
      return Ref.set(state, { ...next, owners })
    },
  }
})

export const make = makeAccess.pipe(Effect.flatMap(Kernel.make))
