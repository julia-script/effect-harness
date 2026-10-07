import { makeBinaryReader } from '../../src/Env.ts'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as DateTime from 'effect/DateTime'
import * as Fsp from 'node:fs/promises'
import * as Ref from 'effect/Ref'
import { Env, FileError, FilePermissionDenied } from '../../src/Env.ts'
import * as Time from '../../src/Time.ts'
import * as Read from '../../src/tools/Read.ts'
import { withEnv, message } from './Helpers.ts'

const sample = Effect.fnUntraced(function* (
  mtimes: ReadonlyArray<number>,
  fail?: 'info' | 'read',
  growth = false,
) {
  const env = yield* Env
  yield* env.writeFile('text', 'one\ntwo')
  const opens = yield* Ref.make(0)
  const releases = yield* Ref.make(0)
  const samples = yield* Ref.make(0)
  const reads = yield* Ref.make(0)
  const cause = new Error('actual native I/O failure')
  const failure = new FileError({
    reason: new FilePermissionDenied({ message: cause.message, cause }),
  })
  const custom = Env.of({
    ...env,
    openBinaryReader: Effect.fnUntraced(function* (path, options) {
      yield* Ref.update(opens, (value) => value + 1)
      const reader = yield* env.openBinaryReader(path, options)
      yield* Effect.addFinalizer(() => Ref.update(releases, (value) => value + 1))
      return makeBinaryReader({
        ...reader,
        info: Effect.gen(function* () {
          const index = yield* Ref.getAndUpdate(samples, (value) => value + 1)
          if (fail === 'info') return yield* failure
          const info = yield* reader.info
          return {
            ...info,
            size: info.size + (growth && index === 1 ? 1 : 0),
            mtimeMs: Time.fromEpochMillis(mtimes[index] ?? 0),
          }
        }),
        read: (offset, length) =>
          Ref.update(reads, (value) => value + 1).pipe(
            Effect.andThen(fail === 'read' ? Effect.fail(failure) : reader.read(offset, length)),
          ),
      })
    }),
  })
  const outcome = yield* Effect.result(
    Read.handler({ path: 'text' }).pipe(Effect.provideService(Env, custom)),
  )
  return {
    outcome,
    failure,
    opens: yield* Ref.get(opens),
    releases: yield* Ref.get(releases),
    samples: yield* Ref.get(samples),
    reads: yield* Ref.get(reads),
  }
})

describe('Read consistency retry', () => {
  it.live(
    'native file metadata preserves fractional mtime and each content/metadata observation is fresh',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          yield* env.writeFile('fractional', 'a')
          const path = yield* env.absolutePath('fractional')
          yield* Effect.promise(() => Fsp.utimes(path, 1.0005, 1.0005))
          const native = yield* Effect.promise(() => Fsp.lstat(path))
          const before = yield* env.fileInfo(path)
          assert.notStrictEqual(native.mtimeMs % 1, 0)
          assert.strictEqual(DateTime.toEpochMillis(before.mtimeMs), native.mtimeMs)
          yield* env.writeFile(path, 'replacement')
          assert.strictEqual((yield* env.fileInfo(path)).size, 11)
          assert.strictEqual(
            new TextDecoder().decode(yield* env.readBinaryFile(path)),
            'replacement',
          )
        }),
      ),
  )
  it.effect(
    'retries one fractional metadata inconsistency on the same opened reader and releases once',
    () =>
      withEnv(
        Effect.gen(function* () {
          const result = yield* sample([0, 0.5, 1, 1])
          assert.strictEqual(result.outcome._tag, 'Success')
          if (result.outcome._tag === 'Success')
            assert.strictEqual(message(result.outcome.success), 'one\ntwo')
          assert.strictEqual(result.samples, 4)
          assert.strictEqual(result.opens, 1)
          assert.strictEqual(result.releases, 1)
        }),
      ),
  )
  it.effect(
    'accepted growth does not retry, while two changes exhaust exactly one retry without inventing an I/O cause',
    () =>
      withEnv(
        Effect.gen(function* () {
          const growth = yield* sample([0, 1], undefined, true)
          assert.strictEqual(growth.outcome._tag, 'Success')
          assert.strictEqual(growth.samples, 2)
          const changed = yield* sample([0, 0.5, 1, 1.5])
          assert.strictEqual(changed.outcome._tag, 'Failure')
          if (changed.outcome._tag === 'Failure') {
            assert.match(changed.outcome.failure.message, /changed while it was read/)
            assert.strictEqual(changed.outcome.failure.cause, undefined)
          }
          assert.strictEqual(changed.samples, 4)
          assert.strictEqual(changed.opens, 1)
          assert.strictEqual(changed.releases, 1)
        }),
      ),
  )
  it.effect(
    'genuine metadata and read I/O failures propagate their exact cause without retry or reopening',
    () =>
      withEnv(
        Effect.gen(function* () {
          for (const operation of ['info', 'read'] as const) {
            const result = yield* sample([], operation)
            assert.strictEqual(result.outcome._tag, 'Failure')
            if (result.outcome._tag === 'Failure')
              assert.strictEqual(result.outcome.failure.cause, result.failure)
            assert.strictEqual(result.opens, 1)
            assert.strictEqual(result.releases, 1)
            assert.strictEqual(result.samples, 1)
            assert.strictEqual(result.reads, operation === 'read' ? 1 : 0)
          }
        }),
      ),
  )
})
