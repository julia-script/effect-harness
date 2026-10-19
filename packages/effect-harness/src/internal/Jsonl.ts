/** Filesystem implementation of an opaque log with named payload streams. */
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import type { Persistence } from './Persistence.js'
import * as Errors from '../StorageError.js'

const io = <A, E, R>(operation: string, effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.mapError((cause) => Errors.make('io', operation, `JSONL ${operation} failed`, cause)),
  )

export const make = Effect.fnUntraced(function* (options: { readonly filePath: string }) {
  const fs = yield* FileSystem.FileSystem
  if (options.filePath.length === 0)
    return yield* Errors.make('invalid', 'initialize', 'JSONL path must not be empty')
  if (
    !(yield* io('open', fs.exists(options.filePath))) &&
    (yield* io('open', fs.exists(`${options.filePath}.parts`)))
  )
    return yield* Errors.make('corrupt', 'recover', 'Payload streams exist without their log')
  const file = yield* io('open', fs.open(options.filePath, { flag: 'a+' }))
  const path = yield* io('open', fs.realPath(options.filePath))
  const directory = `${path}.parts`
  yield* io('initialize', fs.makeDirectory(directory, { recursive: true }))
  const syncDirectory = (path: string) =>
    Effect.scoped(fs.open(path, { flag: 'r' }).pipe(Effect.flatMap((handle) => handle.sync)))
  yield* io('initialize', syncDirectory(`${directory}/..`))
  const readLines = (path: string) =>
    Effect.gen(function* () {
      const bytes = yield* io('read', fs.readFile(path))
      const end = bytes.lastIndexOf(10) + 1
      const text = yield* Effect.try({
        try: () => new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(0, end)),
        catch: Errors.corrupt('recover'),
      })
      return {
        lines: text.split('\n').filter((line) => line.length > 0),
        end,
        length: bytes.length,
      }
    })
  const initial = yield* readLines(path)
  let repaired = false
  const repair = Effect.gen(function* () {
    if (repaired) return
    if (initial.end !== initial.length) yield* io('truncate', file.truncate(initial.end))
    yield* io('sync', file.sync)
    repaired = true
  })
  const location = (key: string) =>
    /^[a-z]+-\d+$/.test(key)
      ? Effect.succeed(`${directory}/${key}.jsonl`)
      : Errors.make('corrupt', 'recover', 'Invalid payload stream key')
  const write = (path: string, text: string, flag: 'a' | 'w') =>
    Effect.scoped(
      Effect.gen(function* () {
        const handle = yield* fs.open(path, { flag })
        yield* handle.writeAll(new TextEncoder().encode(text))
        yield* handle.sync
      }),
    )
  return {
    checkWrite: Effect.void,
    accept: repair,
    journal: Effect.succeed(initial.lines),
    keys: io('read', fs.readDirectory(directory)).pipe(
      Effect.flatMap((names) =>
        Effect.forEach(names, (name) => {
          if (!/^[a-z]+-\d+\.jsonl(?:\.tmp)?$/.test(name))
            return Errors.make('corrupt', 'recover', 'Unexpected payload file')
          return Effect.succeed(name.replace(/\.jsonl(?:\.tmp)?$/, ''))
        }),
      ),
      Effect.map((keys) => Array.from(new Set(keys))),
    ),
    read: (key) =>
      Effect.gen(function* () {
        const path = yield* location(key)
        if (!(yield* io('read', fs.exists(path)))) return []
        return (yield* readLines(path)).lines
      }),
    append: (batch) =>
      Effect.gen(function* () {
        yield* repair
        yield* Effect.gen(function* () {
          for (const stream of batch.append) {
            const path = yield* location(stream.key)
            yield* write(path, `${stream.lines.join('\n')}\n`, 'a')
          }
          if (batch.append.length > 0) yield* syncDirectory(directory)
          yield* file.writeAll(new TextEncoder().encode(`${batch.frame}\n`))
          yield* file.sync
        }).pipe(
          Effect.mapError((cause) =>
            Errors.make(
              'uncertain',
              'commit',
              'JSONL append outcome is uncertain; reopen storage',
              cause,
            ),
          ),
        )
      }),
    replace: ({ key, lines }) =>
      Effect.gen(function* () {
        yield* repair
        const path = yield* location(key)
        yield* io(
          'reclaim',
          Effect.gen(function* () {
            if (lines.length === 0) yield* fs.remove(path, { force: true })
            else {
              yield* write(`${path}.tmp`, `${lines.join('\n')}\n`, 'w')
              yield* fs.rename(`${path}.tmp`, path)
            }
            yield* fs.remove(`${path}.tmp`, { force: true })
            yield* syncDirectory(directory)
          }),
        )
      }),
  } satisfies Persistence
})
