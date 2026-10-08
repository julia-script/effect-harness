/**
 * Append-only JSONL storage with recovery and compaction.
 */
import type * as Scope from 'effect/Scope'
import { identity } from 'effect/Function'
import { SnapshotPayload } from './internal/SnapshotPayload.ts'
import * as Effect from 'effect/Effect'
import * as Ref from 'effect/Ref'
import * as Config from 'effect/Config'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Record from '../Record.ts'
import {
  rejected,
  uncertain,
  IoError,
  CorruptError,
  InvalidError,
  type StorageError,
} from '../StorageError.ts'
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
 * must be coordinated outside the adapter. With fsync enabled, the FileSystem must support
 * opening directories in read mode and synchronizing their handles; unsupported synchronization
 * fails acquisition or leaves a commit uncertain instead of silently downgrading durability.
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
export const SnapshotSchema = SnapshotPayload

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
 * guarantee. fsync requires directory synchronization through FileSystem.open(directory,
 * { flag: 'r' }) and handle.sync. Unsupported mandatory synchronization fails acquisition
 * or poisons the open Store through an uncertain commit; reopen to inspect receipts.
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
  const missingDirectories: Array<string> = []
  if (options.fsync === true) {
    let current = directory
    while (
      !(yield* fs
        .exists(current)
        .pipe(
          Effect.mapError((cause) => rejected('Cannot inspect JSONL directory', IoError, cause)),
        ))
    ) {
      missingDirectories.push(current)
      const parent = path.dirname(current)
      if (parent === current) break
      current = parent
    }
  }
  yield* fs
    .makeDirectory(directory, { recursive: true })
    .pipe(Effect.mapError((cause) => rejected('Cannot create JSONL directory', IoError, cause)))
  const flushDirectory = Effect.fnUntraced(function* (target: string) {
    const handle = yield* fs.open(target, { flag: 'r' })
    yield* handle.sync
  }, Effect.scoped)
  if (options.fsync === true)
    for (const created of missingDirectories.toReversed())
      yield* flushDirectory(path.dirname(created)).pipe(
        Effect.mapError((cause) =>
          rejected('Cannot persist JSONL directory creation', IoError, cause),
        ),
      )
  const exists = yield* fs
    .exists(file)
    .pipe(Effect.mapError((cause) => rejected('Cannot inspect JSONL file', IoError, cause)))
  let recovered: backend.Backend.Snapshot = { state: Record.emptyState(), frames: [] }
  if (exists) {
    const bytes = yield* fs
      .readFile(file)
      .pipe(Effect.mapError((cause) => rejected('Cannot read JSONL file', IoError, cause)))
    const complete =
      bytes.length === 0 || bytes.at(-1) === 10 ? bytes.length : bytes.lastIndexOf(10) + 1
    if (complete !== bytes.length)
      yield* fs
        .truncate(file, complete)
        .pipe(Effect.mapError((cause) => rejected('Cannot repair torn JSONL tail', IoError, cause)))
    let start = 0
    let previous = 0
    for (let end = 0; end < complete; end++) {
      if (bytes[end] !== 10) continue
      const text = yield* Effect.try({
        try: () => new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(start, end)),
        catch: (cause) => rejected('Malformed complete JSONL frame', CorruptError, cause),
      })
      const parsed = yield* Schema.decodeEffect(Schema.fromJsonString(SnapshotSchema))(text).pipe(
        Effect.mapError((cause) => rejected('Invalid complete JSONL frame', CorruptError, cause)),
      )
      const seq = parsed.state.nextSeq - 1
      if (seq <= previous || !Number.isSafeInteger(seq))
        return yield* rejected('JSONL commit sequences do not strictly increase', CorruptError)
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
  const save = Effect.fnUntraced(function* (next: backend.Backend.Snapshot) {
    const text = yield* Schema.encodeEffect(Schema.fromJsonString(SnapshotSchema))(next).pipe(
      Effect.mapError((cause) => rejected('Cannot encode JSONL frame', InvalidError, cause)),
    )
    const encoded = `${text}\n`
    yield* fs
      .writeFileString(file, encoded, { flag: 'a' })
      .pipe(Effect.mapError((cause) => uncertain('JSONL append settlement is uncertain', cause)))
    if (options.fsync === true)
      yield* flush(file).pipe(
        Effect.andThen(flushDirectory(directory)),
        Effect.mapError((cause) => uncertain('JSONL flush settlement is uncertain', cause)),
      )
    yield* Ref.set(snapshot, yield* detachedEffect(next))
    // Failures before replacement preserve the already durable authoritative journal.
    let replacementAttempted = false
    yield* Effect.gen(function* () {
      yield* fs.writeFileString(temporary, encoded)
      if (options.fsync === true) yield* flush(temporary)
      replacementAttempted = true
      yield* fs.rename(temporary, file)
    }).pipe(Effect.ignore)
    if (options.fsync === true && replacementAttempted)
      yield* flushDirectory(directory).pipe(
        Effect.mapError((cause) => uncertain('JSONL replacement settlement is uncertain', cause)),
      )
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
/** Scoped memory alternative for the same Store service. */

/** Decoded value of the SnapshotSchema schema.
 * @category models
 */
export type SnapshotSchema = typeof SnapshotSchema.Type
