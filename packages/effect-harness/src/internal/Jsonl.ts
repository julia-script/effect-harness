/** A scoped, single-writer JSONL journal backed by the same indexed reads as memory. */
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
import * as Errors from '../StorageError.js'

const Frame = Schema.Struct({ metadata: Metadata.Metadata, rows: Schema.Array(Row.Row) })
const io = <A, E, R>(operation: string, effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.mapError((cause) => Errors.make('io', operation, `JSONL ${operation} failed`, cause)),
  )

export const make = Effect.fnUntraced(function* (options: { readonly filePath: string }) {
  yield* Schema.decodeEffect(Schema.NonEmptyString)(options.filePath).pipe(
    Effect.mapError(Errors.invalid('initialize')),
  )
  const fs = yield* FileSystem.FileSystem
  const file = yield* io('open', fs.open(options.filePath, { flag: 'a+' }))
  const memory = yield* Memory.makeAccess
  const bytes = yield* io('read', fs.readFile(options.filePath))
  const end = bytes.lastIndexOf(10) + 1
  const complete = yield* Effect.try({
    try: () => new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(0, end)),
    catch: (cause) => Errors.make('corrupt', 'recover', 'JSONL contains invalid UTF-8', cause),
  })
  const frames = yield* Stream.make(complete).pipe(
    Stream.pipeThroughChannel(Ndjson.decodeSchemaString(Frame)()),
    Stream.runCollect,
    Effect.mapError(Errors.corrupt('recover')),
  )
  let previous = Metadata.initialMetadata
  const identities = new Map<number, Row.Kind>()
  for (const frame of frames) {
    if (
      frame.metadata.nextId < previous.nextId ||
      (frame.metadata.nextSeq !== previous.nextSeq &&
        frame.metadata.nextSeq !== previous.nextSeq + 1) ||
      (frame.rows.length > 0 && frame.metadata.nextSeq !== previous.nextSeq + 1)
    )
      return yield* Errors.make('corrupt', 'recover', 'JSONL allocation counters are inconsistent')
    for (const row of frame.rows) {
      const id = Row.idOf(row)
      const existing = identities.get(id)
      if (id >= frame.metadata.nextId || (existing !== undefined && existing !== row._tag))
        return yield* Errors.make(
          'corrupt',
          'recover',
          'JSONL contains conflicting record identities',
        )
      if (row._tag === 'entry' && row.value.commitSeq !== frame.metadata.nextSeq - 1)
        return yield* Errors.make(
          'corrupt',
          'recover',
          'JSONL entry has an invalid commit sequence',
        )
      identities.set(id, row._tag)
    }
    yield* memory.save(frame.rows, frame.metadata)
    previous = frame.metadata
  }
  // Newline completes a frame. Discard an unterminated tail even when it is valid JSON.
  if (end !== bytes.length) {
    yield* io('truncate', file.truncate(end))
    yield* io('sync', file.sync)
  }
  const access: Access = {
    ...memory,
    save: Effect.fnUntraced(function* (rows, metadata) {
      const encoded = yield* Stream.make({ rows, metadata }).pipe(
        Stream.pipeThroughChannel(Ndjson.encodeSchema(Frame)()),
        Stream.runCollect,
        Effect.mapError((cause) =>
          Errors.make('invalid', 'commit', 'Cannot encode JSONL frame', cause),
        ),
      )
      yield* Effect.forEach(encoded, (bytes) => file.writeAll(bytes), { discard: true }).pipe(
        Effect.andThen(file.sync),
        Effect.mapError((cause) =>
          Errors.make('uncertain', 'commit', 'JSONL write outcome is uncertain', cause),
        ),
      )
      yield* memory.save(rows, metadata)
    }),
  }
  return yield* Kernel.make(access)
})
