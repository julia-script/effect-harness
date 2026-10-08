/**
 * Append-only JSONL storage with recovery and compaction.
 */
import type * as Scope from 'effect/Scope'
import { identity } from 'effect/Function'
import * as Effect from 'effect/Effect'
import * as Ref from 'effect/Ref'
import * as Config from 'effect/Config'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Record from '../Record.ts'
import { rejected, uncertain, Io, Corrupt, Invalid, type StorageError } from '../StorageError.ts'
import { Store } from '../Store.ts'
import * as backend from './internal/backend.ts'
import { detachedEffect, validateState } from './internal/state.ts'

/**
 * Directory and durable-flushing policy for a single-writer JSONL Store.
 *
 * **Details**
 *
 * directory holds snapshot and journal files. fsync is enabled only when explicitly true.
 *
 * **Gotchas**
 *
 * Without fsync, successful writes do not establish crash durability. Directory ownership
 * must be coordinated outside the adapter.
 *
 * @category models
 */
export interface Options {
  readonly directory: string
  readonly fsync?: boolean | undefined
}
/**
 * Schema for a JSONL checkpoint containing authoritative state and retained frames.
 *
 * @category schemas
 */
export const SnapshotSchema = Schema.Struct({
  state: Record.State,
  frames: Schema.Array(Record.Frame),
})

/**
 * Acquires a single-writer Store backed by JSONL commit frames.
 *
 * **Details**
 *
 * Replays saved frames on acquisition, repairs an incomplete final line and rejects
 * malformed complete frames. fsync controls durable flushing.
 *
 * **Gotchas**
 *
 * Coordinate directory ownership externally. Disabling fsync gives no crash-durability
 * guarantee. An uncertain write poisons the open Store; reopen to inspect receipts.
 *
 * @category constructors
 */
export const make = Effect.fnUntraced(function* (
  options: Options,
): Effect.fn.Return<
  Store['Service'],
  StorageError,
  Scope.Scope | FileSystem.FileSystem | Path.Path
> {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const directory = path.resolve(options.directory)
  const file = path.join(directory, 'commits.jsonl')
  const temporary = path.join(directory, 'commits.reclaim')
  yield* fs
    .makeDirectory(directory, { recursive: true })
    .pipe(Effect.mapError((cause) => rejected('Cannot create JSONL directory', Io, cause)))
  const exists = yield* fs
    .exists(file)
    .pipe(Effect.mapError((cause) => rejected('Cannot inspect JSONL file', Io, cause)))
  let recovered: backend.Snapshot = { state: Record.emptyState(), frames: [] }
  if (exists) {
    const bytes = yield* fs
      .readFile(file)
      .pipe(Effect.mapError((cause) => rejected('Cannot read JSONL file', Io, cause)))
    const complete =
      bytes.length === 0 || bytes.at(-1) === 10 ? bytes.length : bytes.lastIndexOf(10) + 1
    if (complete !== bytes.length)
      yield* fs
        .truncate(file, complete)
        .pipe(Effect.mapError((cause) => rejected('Cannot repair torn JSONL tail', Io, cause)))
    let start = 0
    let previous = 0
    for (let end = 0; end < complete; end++) {
      if (bytes[end] !== 10) continue
      const text = yield* Effect.try({
        try: () => new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(start, end)),
        catch: (cause) => rejected('Malformed complete JSONL frame', Corrupt, cause),
      })
      const parsed = yield* Schema.decodeEffect(Schema.fromJsonString(SnapshotSchema))(text).pipe(
        Effect.mapError((cause) => rejected('Invalid complete JSONL frame', Corrupt, cause)),
      )
      const seq = parsed.state.nextSeq - 1
      if (seq <= previous || !Number.isSafeInteger(seq))
        return yield* rejected('JSONL commit sequences do not strictly increase', Corrupt)
      previous = seq
      recovered = { ...parsed, state: yield* validateState(parsed.state) }
      start = end + 1
    }
  }
  const snapshot = yield* Ref.make(recovered)
  yield* fs.remove(temporary, { force: true }).pipe(Effect.ignore)
  const flush = Effect.fnUntraced(function* (target: string) {
    const handle = yield* fs.open(target, { flag: 'r+' })
    yield* handle.sync
  }, Effect.scoped)
  const save = Effect.fnUntraced(function* (next: backend.Snapshot) {
    const text = yield* Schema.encodeEffect(Schema.fromJsonString(SnapshotSchema))(next).pipe(
      Effect.mapError((cause) => rejected('Cannot encode JSONL frame', Invalid, cause)),
    )
    const encoded = `${text}\n`
    yield* fs
      .writeFileString(file, encoded, { flag: 'a' })
      .pipe(Effect.mapError((cause) => uncertain('JSONL append settlement is uncertain', cause)))
    if (options.fsync === true)
      yield* flush(file).pipe(
        Effect.mapError((cause) => uncertain('JSONL flush settlement is uncertain', cause)),
      )
    yield* Ref.set(snapshot, yield* detachedEffect(next))
    // Publication has succeeded. Reclamation can fail safely and is retried on a later commit.
    yield* Effect.gen(function* () {
      yield* fs.writeFileString(temporary, encoded)
      if (options.fsync === true) yield* flush(temporary)
      yield* fs.rename(temporary, file)
    }).pipe(Effect.ignore)
  })
  return yield* backend.make({
    load: Ref.get(snapshot).pipe(Effect.flatMap(detachedEffect)),
    committed: Ref.get(snapshot).pipe(Effect.flatMap(detachedEffect)),
    save,
    atomic: identity,
  })
})
/**
 * Provides scoped JSONL storage from native FileSystem and Path services.
 *
 * **Details**
 *
 * Acquisition validates and recovers the directory before exposing the Store.
 *
 * @see {@link make} for durability and single-writer requirements.
 * @category layers
 */
export const layer = (
  options: Options,
): Layer.Layer<Store, StorageError, FileSystem.FileSystem | Path.Path> =>
  Layer.effect(Store, make(options))

/**
 * Resolves journal options through the caller's ConfigProvider without changing storage ownership.
 *
 * @category layers
 */
export const layerConfig = (
  config: Config.Wrap<Options>,
): Layer.Layer<Store, StorageError | Config.ConfigError, FileSystem.FileSystem | Path.Path> =>
  Layer.effect(Store, Config.unwrap(config).pipe(Effect.flatMap(make)))

/**
 * Canonical Jsonl Store layer.
 *
 * @category layers
 */
export const layerStoreJsonl: typeof layer = layer
/** Scoped memory alternative for the same Store service. */
/**
 * Scoped storage service, allocation accessors and memory acquisition.
 *
 * @category re-exports
 */
export { makeMemory, layerMemory, layerStoreMemory } from '../Store.ts'
