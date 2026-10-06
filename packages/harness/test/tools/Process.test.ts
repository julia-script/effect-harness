import { assert, describe, it } from '@effect/vitest'
import * as Cause from 'effect/Cause'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Path from 'effect/Path'
import * as PlatformError from 'effect/PlatformError'
import * as Ref from 'effect/Ref'
import { ChildProcessSpawner, type ChildProcessHandle } from 'effect/process/ChildProcessSpawner'
import { TestClock } from 'effect/testing'
import {
  Env,
  ExecutionError,
  ExecutionCallbackError,
  ExecutionShellUnavailable,
} from '../../src/Env.ts'
import * as Exec from '../../src/env/Exec.ts'
import { withEnv } from './Helpers.ts'

describe('native process boundaries and complete spill output', () => {
  it.live(
    'direct argv stays literal and reports stdout/stderr with explicit cwd/env and nonzero exit',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const events = yield* Ref.make<ReadonlyArray<{ text: string; stream: string }>>([])
          const result = yield* env.exec(
            [
              '/bin/sh',
              '-c',
              'printf "%s\\n" "$1" "$MARK"; printf err >&2; exit 7',
              'argv',
              '$(not-a-command) quoted',
            ],
            {
              env: { MARK: 'explicit' },
              inheritEnv: false,
              onOutput: (text, info) =>
                Ref.update(events, (values) => [...values, { text, stream: info.stream }]),
            },
          )
          assert.strictEqual(result.exitCode, 7)
          const values = yield* Ref.get(events)
          assert.strictEqual(
            values.some((value) => value.stream === 'stderr' && value.text === 'err'),
            true,
          )
          assert.strictEqual(
            values
              .filter((value) => value.stream === 'stdout')
              .map((value) => value.text)
              .join(''),
            '$(not-a-command) quoted\nexplicit\n',
          )
          const cwd = yield* Ref.make('')
          yield* env.exec('pwd', { onOutput: (text) => Ref.set(cwd, text.trim()) })
          assert.strictEqual(yield* Ref.get(cwd), yield* env.canonicalPath(env.cwd))
          assert.strictEqual((yield* Effect.flip(env.exec([]))).code, 'spawn_error')
          assert.strictEqual(
            (yield* Effect.flip(env.exec(['/definitely/no/program']))).code,
            'spawn_error',
          )
        }),
      ),
  )
  it.live(
    'complete raw bytes including invalid UTF8 prefix spill only beyond either threshold; paths survive cleanup',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const output = yield* Ref.make('')
          const result = yield* env.exec(
            ['/bin/sh', '-c', 'printf "\\357\\273\\277hello\\342\\202"'],
            {
              spill: { afterBytes: 4, afterLines: 100 },
              onOutput: (text) => Ref.update(output, (old) => old + text),
            },
          )
          assert.isDefined(result.spillPath)
          if (result.spillPath === undefined) return yield* Effect.die('Missing spill')
          assert.deepStrictEqual(
            Array.from(yield* env.readBinaryFile(result.spillPath)),
            [239, 187, 191, 104, 101, 108, 108, 111, 226, 130],
          )
          assert.strictEqual(yield* Ref.get(output), 'hello�')
          yield* env.cleanup
          assert.strictEqual(yield* env.exists(result.spillPath), true)
          yield* env.remove(result.spillPath)
          assert.strictEqual(
            (yield* env.exec('printf 1234', { spill: { afterBytes: 4, afterLines: 1 } })).spillPath,
            undefined,
          )
          const lines = yield* env.exec('printf "a\\nb"', {
            spill: { afterBytes: 100, afterLines: 1 },
          })
          assert.isDefined(lines.spillPath)
          if (lines.spillPath !== undefined) yield* env.remove(lines.spillPath)
        }),
      ),
  )
  it.live(
    'real timeout before output has no spill; caller cancellation remains interruption and kills only owned command',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const failure = yield* Effect.flip(
            env.exec('sleep 10', { timeout: 0.03, spill: { afterBytes: 2, afterLines: 100 } }),
          )
          assert.strictEqual(failure.code, 'timeout')
          assert.strictEqual(failure.spillPath, undefined)
          const started = yield* Deferred.make<void>()
          const first = yield* env
            .exec('printf started; sleep 10', {
              onOutput: () => Deferred.succeed(started, undefined).pipe(Effect.asVoid),
            })
            .pipe(Effect.forkChild)
          yield* Deferred.await(started)
          yield* Fiber.interrupt(first)
          const exit = yield* Fiber.await(first)
          assert.strictEqual(exit._tag, 'Failure')
          if (exit._tag === 'Failure') assert.strictEqual(Cause.hasInterrupts(exit.cause), true)
          assert.strictEqual((yield* env.exec('exit 0')).exitCode, 0)
        }),
      ),
  )
  it.effect(
    'timeout after spill publication retains native output and settles its owned child',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const nativeSpawner = yield* ChildProcessSpawner
          const handles: ChildProcessHandle[] = []
          const spawner: ChildProcessSpawner['Service'] = {
            ...nativeSpawner,
            // The native acquisition and its registered cleanup keep the live clock;
            // only Exec's semantic timeout is driven by the test clock.
            spawn: (command) =>
              TestClock.withLive(nativeSpawner.spawn(command)).pipe(
                Effect.tap((handle) =>
                  Effect.sync(() => {
                    handles.push(handle)
                  }),
                ),
              ),
          }
          const custom = yield* Exec.make(fs, path, spawner, {
            id: 'admitted-timeout',
            cwd: env.cwd,
            shell: '/bin/sh',
          })
          const published = yield* Deferred.make<string>()
          const settled = yield* Ref.make(false)
          const running = yield* custom
            .exec('printf prefix; sleep 10', {
              timeout: 0.03,
              spill: { afterBytes: 2, afterLines: 100 },
              onSpill: (file) => Deferred.succeed(published, file).pipe(Effect.asVoid),
            })
            .pipe(
              Effect.onExit(() => Ref.set(settled, true)),
              Effect.flip,
              Effect.forkChild,
            )
          // onSpill runs after the prefix write settles, so host scheduling cannot
          // consume the deadline before the file whose retention is being tested exists.
          const file = yield* Deferred.await(published)
          assert.strictEqual(yield* env.readTextFile(file), 'prefix')
          yield* TestClock.adjust(29)
          assert.strictEqual(yield* Ref.get(settled), false)
          yield* TestClock.adjust(1)
          const failure = yield* Fiber.join(running)
          assert.strictEqual(failure.reason._tag, 'ExecutionTimeout')
          assert.strictEqual(failure.spillPath, file)
          assert.strictEqual(yield* env.readTextFile(file), 'prefix')
          assert.strictEqual(handles.length, 1)
          const handle = handles[0]
          assert.ok(handle)
          assert.strictEqual(yield* handle.isRunning, false)
          yield* env.remove(file)
        }),
      ),
  )
  it.live(
    'callback failures stop command with semantic callback error; spill failure is never silent',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const callbackFailure = yield* Effect.flip(
            env.exec('printf x; sleep 10', {
              onOutput: () =>
                Effect.fail(
                  new ExecutionError({
                    reason: new ExecutionCallbackError({ message: 'observer rejected' }),
                  }),
                ),
            }),
          )
          assert.strictEqual(callbackFailure.code, 'callback_error')
          assert.strictEqual(
            (yield* Effect.flip(env.exec('exit 0', { timeout: -1 }))).code,
            'timeout',
          )
        }),
      ),
  )
  it.live('inherited stdio closes after idle grace while admitted writes have settled', () =>
    withEnv(
      Effect.gen(function* () {
        const env = yield* Env
        const output = yield* Ref.make('')
        const result = yield* env.exec('printf finished; sleep 0.5 &', {
          onOutput: (text) => Ref.update(output, (old) => old + text),
        })
        assert.strictEqual(result.exitCode, 0)
        assert.strictEqual(yield* Ref.get(output), 'finished')
      }),
    ),
  )
  it.live(
    'spill failures and callback defects terminate a running command; decoder flush is a callback boundary',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const spawner = yield* ChildProcessSpawner
          const denied: FileSystem.FileSystem = {
            ...fs,
            makeTempFile: () =>
              Effect.fail(
                PlatformError.badArgument({
                  module: 'FileSystem',
                  method: 'makeTempFile',
                  description: 'spill denied',
                }),
              ),
          }
          const custom = yield* Exec.make(denied, path, spawner, {
            id: 'failure',
            cwd: env.cwd,
            shell: '/bin/sh',
          })
          const failure = yield* Effect.flip(
            custom.exec('printf x; sleep 10', { spill: { afterBytes: 0, afterLines: 10 } }),
          ).pipe(Effect.timeout(2000))
          assert.match(failure.message, /spill denied/)
          const defect = yield* Effect.flip(
            env.exec('printf x; sleep 10', { onOutput: () => Effect.die('broken observer') }),
          ).pipe(Effect.timeout(2000))
          assert.strictEqual(defect.code, 'callback_error')
          const flush = yield* Effect.flip(
            env.exec(['/bin/sh', '-c', 'printf "\\342\\202"'], {
              onOutput: () => Effect.die('flush observer'),
            }),
          )
          assert.strictEqual(flush.code, 'callback_error')
          const spillObserver = yield* Effect.flip(
            env.exec('printf x; sleep 10', {
              spill: { afterBytes: 0, afterLines: 10 },
              onSpill: () => Effect.die('spill observer'),
            }),
          ).pipe(Effect.timeout(2000))
          assert.strictEqual(spillObserver.code, 'callback_error')
          if (spillObserver.spillPath !== undefined) yield* env.remove(spillObserver.spillPath)
        }),
      ),
  )
  it.live(
    'cancellation waits for admitted spill initialization and retains its complete prefix',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const spawner = yield* ChildProcessSpawner
          const admitted = yield* Deferred.make<string>()
          const release = yield* Deferred.make<void>()
          const slow: FileSystem.FileSystem = {
            ...fs,
            writeFile: (file, bytes, options) =>
              Effect.gen(function* () {
                yield* Deferred.succeed(admitted, file)
                yield* Deferred.await(release)
                yield* fs.writeFile(file, bytes, options)
              }),
          }
          const custom = yield* Exec.make(slow, path, spawner, {
            id: 'slow',
            cwd: env.cwd,
            shell: '/bin/sh',
          })
          const running = yield* custom
            .exec('printf prefix; sleep 10', { spill: { afterBytes: 0, afterLines: 10 } })
            .pipe(Effect.forkChild)
          const file = yield* Deferred.await(admitted)
          yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))
          const stopped = yield* Ref.make(false)
          const stopping = yield* Fiber.interrupt(running).pipe(
            Effect.andThen(Ref.set(stopped, true)),
            Effect.forkChild,
          )
          yield* Effect.sleep(20)
          assert.strictEqual(yield* Ref.get(stopped), false)
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(stopping)
          assert.strictEqual(yield* env.readTextFile(file), 'prefix')
          yield* env.remove(file)
        }),
      ),
  )
  it.live(
    'owner cleanup stops every active child; custom missing shell and invalid cwd remain typed',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const spawner = yield* ChildProcessSpawner
          const starts = yield* Ref.make(0)
          const ready = yield* Deferred.make<void>()
          const callback = () =>
            Ref.updateAndGet(starts, (n) => n + 1).pipe(
              Effect.flatMap((n) =>
                n === 2 ? Deferred.succeed(ready, undefined).pipe(Effect.asVoid) : Effect.void,
              ),
            )
          const first = yield* env
            .exec('printf started; sleep 10', { onOutput: callback })
            .pipe(Effect.forkChild)
          const second = yield* env
            .exec('printf started; sleep 10', { onOutput: callback })
            .pipe(Effect.forkChild)
          yield* Deferred.await(ready)
          yield* env.cleanup
          yield* Fiber.await(first).pipe(Effect.timeout(2000))
          yield* Fiber.await(second).pipe(Effect.timeout(2000))
          assert.strictEqual((yield* env.exec('exit 0')).exitCode, 0)
          const missing = yield* Exec.make(fs, path, spawner, {
            id: 'missing',
            cwd: env.cwd,
            resolveShell: Effect.fail(
              new ExecutionError({
                reason: new ExecutionShellUnavailable({ message: 'No shell' }),
              }),
            ),
          })
          assert.strictEqual((yield* Effect.flip(missing.exec('x'))).code, 'shell_unavailable')
          assert.strictEqual(
            (yield* Effect.flip(env.exec('x', { cwd: '/definitely/no/cwd' }))).code,
            'spawn_error',
          )
        }),
      ),
  )
})
