import { assertSuccess, assertFailure } from '@effect/vitest/utils'
import * as Result from 'effect/Result'
import { ToolError, ToolExecutionError } from 'effect-harness/ToolError'
import { makeBinaryReader } from 'effect-harness/NativeFiles'

import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as DateTime from 'effect/DateTime'

import * as promises from 'node:fs/promises'

import * as Ref from 'effect/Ref'

import { Env } from 'effect-harness/Env'
import { FileError, FilePermissionDeniedError } from 'effect-harness/FileError'

import * as Time from 'effect-harness/Time'

import * as Read from 'effect-harness/tools/Read'

import { withEnv } from '../EnvFixture.ts'

import { message } from '../ToolResultText.ts'

describe('ReadRetry', () => {
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
      reason: new FilePermissionDeniedError({ message: cause.message, cause }),
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
    // Real filesystem acquisition and release callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'native file metadata preserves fractional mtime and each content/metadata observation is fresh',
      () =>
        withEnv(
          Effect.gen(function* () {
            const env = yield* Env
            yield* env.writeFile('fractional', 'a')
            const path = yield* env.absolutePath('fractional')
            yield* Effect.promise(() => promises.utimes(path, 1.0005, 1.0005))
            const native = yield* Effect.promise(() => promises.lstat(path))
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
            assertSuccess(Result.map(result.outcome, message), 'one\ntwo')
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
            assertSuccess(Result.map(growth.outcome, message), 'one\ntwo')
            assert.strictEqual(growth.samples, 2)
            const changed = yield* sample([0, 0.5, 1, 1.5])
            assertFailure(
              changed.outcome,
              new ToolError({
                reason: new ToolExecutionError({
                  name: 'read',
                  message: 'text changed while it was read',
                }),
              }),
            )
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
              assertFailure(
                result.outcome,
                new ToolError({
                  reason: new ToolExecutionError({
                    name: 'read',
                    message: 'actual native I/O failure',
                    cause: result.failure,
                  }),
                }),
              )
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
})
