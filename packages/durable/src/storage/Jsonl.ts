import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Record from '../Record.ts'
import { rejected, uncertain, Io, Corrupt, Invalid } from '../StorageError.ts'
import { Store } from '../Store.ts'
import * as Backend from './Backend.ts'
import { detachedEffect, validateState, validate } from './State.ts'

export interface Options {
  readonly directory: string
  readonly fsync?: boolean
}
const SnapshotSchema = Schema.Struct({ state: Record.State, frames: Schema.Array(Record.Frame) })

export const make = Effect.fnUntraced(function* (options: Options) {
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
  let snapshot: Backend.Snapshot = { state: Record.emptyState(), frames: [] }
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
      const value = yield* Effect.try({
        try: () =>
          JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(start, end))),
        catch: (cause) => rejected('Malformed complete JSONL frame', Corrupt, cause),
      })
      const parsed = yield* validate(SnapshotSchema, value).pipe(
        Effect.mapError((cause) => rejected('Invalid complete JSONL frame', Corrupt, cause)),
      )
      const seq = parsed.state.nextSeq - 1
      if (seq <= previous || !Number.isSafeInteger(seq))
        return yield* rejected('JSONL commit sequences do not strictly increase', Corrupt)
      previous = seq
      snapshot = { ...parsed, state: yield* validateState(parsed.state) }
      start = end + 1
    }
  }
  yield* fs.remove(temporary, { force: true }).pipe(Effect.ignore)
  const flush = Effect.fnUntraced(function* (target: string) {
    return yield* Effect.scoped(
      Effect.gen(function* () {
        const handle = yield* fs.open(target, { flag: 'r+' })
        yield* handle.sync
      }),
    )
  })
  const save = Effect.fnUntraced(function* (next: Backend.Snapshot) {
    const encoded = yield* Effect.try({
      try: () => `${JSON.stringify(next)}\n`,
      catch: (cause) => rejected('Cannot encode JSONL frame', Invalid, cause),
    })
    yield* fs
      .writeFileString(file, encoded, { flag: 'a' })
      .pipe(Effect.mapError((cause) => uncertain('JSONL append settlement is uncertain', cause)))
    if (options.fsync === true)
      yield* flush(file).pipe(
        Effect.mapError((cause) => uncertain('JSONL flush settlement is uncertain', cause)),
      )
    snapshot = yield* detachedEffect(next)
    // Publication has succeeded. Reclamation can fail safely and is retried on a later commit.
    yield* Effect.gen(function* () {
      yield* fs.writeFileString(temporary, encoded)
      if (options.fsync === true) yield* flush(temporary)
      yield* fs.rename(temporary, file)
    }).pipe(Effect.ignore)
  })
  return yield* Effect.acquireRelease(
    Backend.make({
      load: Effect.suspend(() => detachedEffect(snapshot)),
      committed: Effect.suspend(() => detachedEffect(snapshot)),
      save,
      atomic: (effect) => effect,
      close: Effect.void,
    }),
    (store) => store.close.pipe(Effect.orDie),
  )
})
export const layer = (options: Options) => Layer.effect(Store, make(options))
