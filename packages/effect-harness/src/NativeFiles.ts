/** Portable native filesystem capabilities and bounded scoped reader contracts. */
import * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import type * as DateTime from 'effect/DateTime'
import * as Pipeable from 'effect/Pipeable'
import * as Inspectable from 'effect/Inspectable'
import * as Predicate from 'effect/Predicate'
import type * as Scope from 'effect/Scope'
import type * as Stream from 'effect/Stream'
import type { FileError } from './FileError.ts'

/**
 * File metadata returned by environment readers and directory scans.
 *
 * @category models
 */
export interface FileInfo {
  readonly name: string
  readonly path: string
  readonly kind: 'file' | 'directory' | 'symlink'
  readonly size: number
  /**
   * File modification instant as DateTime.Utc, preserving fractional epoch-millisecond
   * precision.
   */
  readonly mtimeMs: DateTime.Utc
  /** Native identity used by watch snapshots; adapters without stable identities may omit it. */
  readonly identity?: string | undefined
}
/**
 * Byte offsets and newline counts for a requested text window.
 *
 * @category models
 */
export interface LineScan {
  readonly newlines: number
  readonly start: number
  readonly end: number
  readonly firstLineEnd: number
  readonly lastLineStart: number
  readonly selectedBytes: number
  readonly firstLineBytes: number
}
const BinaryReaderTypeId = '~effect-harness/NativeFiles/BinaryReader'
/**
 * Scoped random-access file reader and line-window scanner.
 *
 * @category models
 */
export interface BinaryReader extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [BinaryReaderTypeId]: typeof BinaryReaderTypeId
  readonly info: Effect.Effect<FileInfo, FileError>
  readonly read: (offset: number, length: number) => Effect.Effect<Uint8Array, FileError>
  readonly scanLines: (options: {
    readonly startLine: number
    readonly endLine?: number | undefined
  }) => Effect.Effect<LineScan, FileError>
}
/**
 * Attaches handle identity without evaluating or changing capability getters.
 *
 * @category constructors
 */
export const makeBinaryReader = (
  input: Omit<
    BinaryReader,
    typeof BinaryReaderTypeId | keyof Pipeable.Pipeable | keyof Inspectable.Inspectable
  >,
): BinaryReader => {
  const handle: BinaryReader = Object.create(BinaryReaderProto)
  Object.defineProperties(handle, Object.getOwnPropertyDescriptors(input))
  Object.defineProperty(handle, BinaryReaderTypeId, {
    value: BinaryReaderTypeId,
    enumerable: false,
  })
  return handle
}
/**
 * Checks whether a value carries the nominal `BinaryReader` marker.
 *
 * **Gotchas**
 *
 * This checks library identity, not the validity of arbitrary fields or stored JSON.
 *
 * @category guards
 */
export const isBinaryReader = (u: unknown): u is BinaryReader =>
  Predicate.hasProperty(u, BinaryReaderTypeId)

const DirReaderTypeId = '~effect-harness/NativeFiles/DirReader'
/**
 * Scoped directory reader returning bounded batches.
 *
 * @category models
 */
export interface DirReader extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [DirReaderTypeId]: typeof DirReaderTypeId
  readonly next: (
    maxEntries: number,
  ) => Effect.Effect<
    { readonly entries: ReadonlyArray<FileInfo>; readonly done: boolean },
    FileError
  >
}
/**
 * Attaches handle identity without evaluating or changing capability getters.
 *
 * @category constructors
 */
export const makeDirReader = (
  input: Omit<
    DirReader,
    typeof DirReaderTypeId | keyof Pipeable.Pipeable | keyof Inspectable.Inspectable
  >,
): DirReader => {
  const handle: DirReader = Object.create(DirReaderProto)
  Object.defineProperties(handle, Object.getOwnPropertyDescriptors(input))
  Object.defineProperty(handle, DirReaderTypeId, { value: DirReaderTypeId, enumerable: false })
  return handle
}
/**
 * Checks whether a value carries the nominal `DirReader` marker.
 *
 * **Gotchas**
 *
 * This checks library identity, not the validity of arbitrary fields or stored JSON.
 *
 * @category guards
 */
export const isDirReader = (u: unknown): u is DirReader => Predicate.hasProperty(u, DirReaderTypeId)

/**
 * Scoped native directory notifications and installation readiness.
 *
 * **Details**
 *
 * Consuming changes installs the watcher. started reports installation success or failure,
 * allowing callers to await readiness before mutating files.
 *
 * **Gotchas**
 *
 * changes is single-consumer and owned by the consumption Scope. Closing that Scope joins
 * native producers.
 *
 * @category models
 */
export interface DirectoryNotifications {
  /**
   * Single-consumer stream of watcher changes owned by the consumption Scope.
   */
  readonly changes: Stream.Stream<string | undefined, FileError>
  /**
   * Waits for native watcher installation and reports installation failure before callers rely
   * on notifications.
   */
  readonly started: Effect.Effect<void, FileError>
}
/**
 * Service supplying native directory watches, lstat and bounded file readers.
 *
 * **Details**
 *
 * Adapters fill capabilities absent from the generic FileSystem contract. Supply this
 * service with the native platform filesystem/process Layers at the application boundary.
 *
 * @category services
 */
export class NativeFiles extends Context.Service<
  NativeFiles,
  {
    readonly lstat: (path: string) => Effect.Effect<FileInfo, FileError>
    readonly openBinaryReader: (
      path: string,
      options?: { readonly noFollow?: boolean | undefined },
    ) => Effect.Effect<BinaryReader, FileError, Scope.Scope>
    readonly openDirReader: (path: string) => Effect.Effect<DirReader, FileError, Scope.Scope>
    readonly watchDirectory: (path: string) => Effect.Effect<DirectoryNotifications, FileError>
  }
>()('effect-harness/NativeFiles') {}

const BinaryReaderProto = {
  ...Pipeable.Prototype,
  ...Inspectable.BaseProto,
  toJSON(): unknown {
    return { _id: '@effect-harness/harness/Env/BinaryReader' }
  },
}

const DirReaderProto = {
  ...Pipeable.Prototype,
  ...Inspectable.BaseProto,
  toJSON(): unknown {
    return { _id: '@effect-harness/harness/Env/DirReader' }
  },
}
