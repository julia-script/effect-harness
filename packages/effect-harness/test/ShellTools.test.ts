import { assert, describe, it } from '@effect/vitest'
import * as NodeChildProcessSpawner from '@effect/platform-node/NodeChildProcessSpawner'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as Deferred from 'effect/Deferred'
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as PlatformError from 'effect/PlatformError'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Sink from 'effect/Sink'
import * as Stream from 'effect/Stream'
import * as TestClock from 'effect/testing/TestClock'
import type * as ChildProcess from 'effect/process/ChildProcess'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import * as Identity from '../src/Identity.ts'
import * as Record from '../src/Record.ts'
import * as Tool from '../src/Tool.ts'
import { ToolExecution } from '../src/ToolExecution.ts'
import * as ToolSpill from '../src/ToolSpill.ts'
import * as ToolkitFileSystem from '../src/ToolkitFileSystem.ts'
import * as ToolkitProcess from '../src/ToolkitProcess.ts'
import * as Bash from '../src/coding/Bash.ts'
import * as PowerShell from '../src/coding/PowerShell.ts'
import * as ShellProcess from '../src/coding/ShellProcess.ts'

const files = Layer.merge(NodeFileSystem.layer, NodePath.layer)
const native = NodeChildProcessSpawner.layer.pipe(Layer.provideMerge(files))
const encoder = new TextEncoder()
const reporter = () => {
  const chunks: string[] = []
  const details: Schema.Json[] = []
  const diagnostics: string[] = []
  const service = ToolExecution.of({
    conversationId: Schema.decodeSync(Identity.ConversationId)(1),
    taskId: Schema.decodeSync(Record.TaskId)(1),
    callId: 'shell-test',
    snapshot: () => Effect.succeedNone,
    watch: () => Stream.empty,
    commit: () => Effect.die('Unexpected document commit'),
    output: (text) =>
      Effect.sync(() => {
        chunks.push(text)
      }),
    details: (value) =>
      Effect.sync(() => {
        details.push(value)
      }),
    diagnostic: (value) =>
      Effect.sync(() => {
        diagnostics.push(value.code ?? '')
      }),
  })
  return { chunks, details, diagnostics, service }
}
const bound = (root: string, backend = native) => {
  const services = Layer.merge(
    ToolkitFileSystem.layer({ root }),
    ToolkitProcess.layer({ root }),
  ).pipe(Layer.provide(backend))
  return Layer.merge(
    ShellProcess.layerNative,
    ToolSpill.layer({ provider: root, directory: `${root}/spills`, lifetime: 'persistent' }),
  ).pipe(Layer.provideMerge(services))
}
const temp = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  return yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
})
const expectError = <A, E>(result: Result.Result<A, E>) => {
  if (Result.isFailure(result) && result.failure instanceof Bash.ShellError) return result.failure
  return assert.fail('Expected typed shell failure')
}

const fakeHandle = (
  stdout: Stream.Stream<Uint8Array, PlatformError.PlatformError>,
  stderr: Stream.Stream<Uint8Array, PlatformError.PlatformError> = Stream.empty,
  exitCode: Effect.Effect<
    ChildProcessSpawner.ExitCode,
    PlatformError.PlatformError
  > = Effect.succeed(ChildProcessSpawner.ExitCode(0)),
) =>
  ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(99),
    stdout,
    stderr,
    all: Stream.die('Raw merged stream must never be consumed'),
    exitCode,
    isRunning: Effect.succeed(true),
    kill: () => Effect.void,
    stdin: Sink.drain,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
    unref: Effect.succeed(Effect.void),
  })
describe('ready-made shell tools', () => {
  it.effect(
    'real Bash executes selected string semantics and hostile direct argv literally with bound cwd/env',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const root = yield* temp
          const progress = reporter()
          const hostile = '$(touch SHOULD_NOT_EXIST); "quoted" & * | > output'
          yield* Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            yield* fs.makeDirectory('sub')
            const result = yield* Bash.run({
              command: ['/bin/bash', '-c', 'printf "%s" "$1"', 'name', hostile],
              cwd: 'sub',
            })
            assert.strictEqual(result.structuredOutput.output, hostile)
            assert.isFalse(yield* fs.exists('sub/SHOULD_NOT_EXIST'))
            const scripted = yield* Bash.run(
              {
                command: 'printf "%s:%s:%s" "$VALUE" "$MARK" "$PWD"',
                env: { VALUE: 'caller' },
                extendEnv: false,
              },
              {
                programs: ['/bin/bash'],
                commandPrefix: 'MARK=prefix',
                env: { VALUE: 'default', OMIT: 'x' },
                prepare: (execution) =>
                  Effect.succeed({
                    ...execution,
                    cwd: `${root}/sub`,
                    env: { ...execution.env, PREPARED: 'yes' },
                  }),
              },
            )
            assert.strictEqual(scripted.structuredOutput.output, `caller:prefix:${root}/sub`)
            assert.strictEqual(progress.chunks.join(''), hostile + scripted.structuredOutput.output)
            assert.strictEqual(progress.details.length, 2)
            assert.deepStrictEqual(yield* fs.readDirectory('spills'), [])
          }).pipe(
            Effect.provide(bound(root)),
            Effect.provideService(ToolExecution, progress.service),
          )
          assert.deepStrictEqual(Tool.policy(Bash.tool), {
            replay: 'unsafe',
            executionMode: 'sequential',
          })
          assert.strictEqual(Tool.policy(PowerShell.tool).replay, 'unsafe')
        }),
      ).pipe(Effect.provide(files)),
  )

  it.effect('real bounded flood spills every raw stdout/stderr byte and survives call Scope', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const root = yield* temp
        const progress = reporter()
        yield* Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const provider = yield* ToolSpill.ToolSpill
          const out = '😀'.repeat(20000) + '\nlast stdout\n'
          const err = 'é'.repeat(20000) + '\nlast stderr\n'
          const result = yield* Bash.run(
            {
              command: [
                process.execPath,
                '-e',
                'process.stdout.write("😀".repeat(20000)+"\\nlast stdout\\n"); process.stderr.write("é".repeat(20000)+"\\nlast stderr\\n")',
              ],
            },
            { retention: { retain: 'tail', maxBytes: 64, maxLines: 2 } },
          )
          const value = result.structuredOutput
          assert.isTrue(value.truncated)
          assert.isAtMost(value.retention.retainedBytes, 64)
          assert.isAtMost(value.retention.retainedLines, 2)
          assert.strictEqual(value.retention.totalBytes, encoder.encode(out + err).length)
          assert.strictEqual(progress.chunks.join('').length, (out + err).length)
          const spill = value.spill
          if (spill === undefined) return assert.fail('Truncated output must have a complete spill')
          assert.deepStrictEqual(yield* provider.revalidate(spill), spill)
          assert.strictEqual(yield* fs.readFileString(spill.stdoutPath), out)
          assert.strictEqual(yield* fs.readFileString(spill.stderrPath), err)
          assert.strictEqual((yield* fs.stat(spill.path)).size, BigInt(spill.bytes))
          assert.deepStrictEqual(progress.details.at(-1), value)
          assert.isTrue(progress.diagnostics.includes('full_output'))
        }).pipe(Effect.provide(bound(root)), Effect.provideService(ToolExecution, progress.service))
      }),
    ).pipe(Effect.provide(files)),
  )

  it.effect(
    'nonzero exit has typed retained diagnostics and ready-made toolkit encodes failure',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const root = yield* temp
          const progress = reporter()
          yield* Effect.gen(function* () {
            const error = expectError(
              yield* Bash.run(
                { command: 'printf out; printf err >&2; exit 7' },
                { programs: ['/bin/bash'] },
              ).pipe(Effect.result),
            )
            assert.strictEqual(error.reason, 'nonzero')
            assert.strictEqual(error.outcome?.exitCode, 7)
            assert.include(error.outcome?.output ?? '', 'out')
            assert.include(error.outcome?.output ?? '', 'err')
            const toolkit = yield* Bash.toolkit.pipe(
              Effect.provide(Bash.layer({ programs: ['/bin/bash'] })),
            )
            const result = yield* toolkit.invoke({
              id: 'exit',
              name: 'bash',
              arguments: { command: 'printf latest; exit 9' },
            })
            assert.isTrue(result.isError)
            assert.include(JSON.stringify(result.structuredOutput), 'nonzero')
            assert.include(JSON.stringify(result.content), 'latest')
            assert.strictEqual(progress.details.length, 2)
          }).pipe(
            Effect.provide(bound(root)),
            Effect.provideService(ToolExecution, progress.service),
          )
        }),
      ).pipe(Effect.provide(files)),
  )

  it.effect(
    'independent stream decoders preserve Unicode split across arbitrary hostile interleaving',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const root = yield* temp
          const progress = reporter()
          let released = 0
          const backend = Layer.succeed(
            ChildProcessSpawner.ChildProcessSpawner,
            ChildProcessSpawner.make(() =>
              Effect.acquireRelease(
                Effect.succeed(
                  fakeHandle(
                    Stream.make(new Uint8Array([0xf0, 0x9f]), new Uint8Array([0x98, 0x80])),
                    Stream.make(new Uint8Array([0xc3]), new Uint8Array([0xa9])),
                  ),
                ),
                () =>
                  Effect.sync(() => {
                    released++
                  }),
              ),
            ),
          ).pipe(Layer.provideMerge(files))
          const result = yield* Bash.run({ command: ['remote', '$(evil)', 'a b', ';'] }).pipe(
            Effect.provide(bound(root, backend)),
            Effect.provideService(ToolExecution, progress.service),
          )
          assert.include(result.structuredOutput.output, '😀')
          assert.include(result.structuredOutput.output, 'é')
          assert.notInclude(result.structuredOutput.output, '�')
          assert.strictEqual(progress.chunks.join(''), result.structuredOutput.output)
          assert.strictEqual(released, 1)
        }),
      ).pipe(Effect.provide(files)),
  )

  it.effect(
    'timeout differs from caller interruption; final UTF8 flush and cleanup run for both',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const root = yield* temp
          const progress = reporter()
          let released = 0
          let ready = yield* Deferred.make<void>()
          const backend = Layer.succeed(
            ChildProcessSpawner.ChildProcessSpawner,
            ChildProcessSpawner.make(() =>
              Effect.acquireRelease(
                Effect.succeed(
                  fakeHandle(
                    Stream.concat(
                      Stream.make(new Uint8Array([0xf0, 0x9f])),
                      Stream.fromEffect(
                        Deferred.succeed(ready, undefined).pipe(Effect.andThen(Effect.never)),
                      ),
                    ),
                    Stream.empty,
                    Effect.never,
                  ),
                ),
                () =>
                  Effect.sync(() => {
                    released++
                  }),
              ),
            ),
          ).pipe(Layer.provideMerge(files))
          yield* Effect.gen(function* () {
            const run = Bash.run({ command: 'wait', timeout: 1 })
            const fiber = yield* run.pipe(Effect.result, Effect.forkChild)
            yield* Deferred.await(ready)
            yield* TestClock.adjust('2 seconds')
            const error = expectError(yield* Fiber.join(fiber))
            assert.strictEqual(error.reason, 'timeout')
            assert.strictEqual(error.outcome?.output, '�')
            assert.strictEqual(released, 1)
            ready = yield* Deferred.make<void>()
            const caller = yield* Bash.run({ command: 'wait' }).pipe(Effect.forkChild)
            yield* Deferred.await(ready)
            yield* Fiber.interrupt(caller)
            const exit = yield* Fiber.await(caller)
            assert.isTrue(Exit.isFailure(exit))
            if (Exit.isFailure(exit)) assert.isTrue(Cause.hasInterrupts(exit.cause))
            assert.strictEqual(released, 2)
            assert.strictEqual(progress.details.length, 2)
          }).pipe(
            Effect.provide(bound(root, backend)),
            Effect.provideService(ToolExecution, progress.service),
          )
        }),
      ).pipe(Effect.provide(files)),
  )

  it.effect(
    'strict owned-tree cleanup refuses native before spawning and accepts an owned adapter',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const root = yield* temp
          const progress = reporter()
          let started = 0
          let released = 0
          const backend = Layer.succeed(
            ChildProcessSpawner.ChildProcessSpawner,
            ChildProcessSpawner.make(() => {
              started++
              return Effect.succeed(fakeHandle(Stream.empty))
            }),
          ).pipe(Layer.provideMerge(files))
          const refused = yield* Bash.run(
            { command: 'must-not-start' },
            { requireOwnedTree: true },
          ).pipe(
            Effect.result,
            Effect.provide(bound(root, backend)),
            Effect.provideService(ToolExecution, progress.service),
          )
          assert.strictEqual(expectError(refused).reason, 'unsupportedCleanup')
          assert.strictEqual(started, 0)
          const strict = Layer.succeed(
            ShellProcess.ShellProcess,
            ShellProcess.ShellProcess.of({
              cleanup: 'owned-tree',
              spawn: () =>
                Effect.acquireRelease(
                  Effect.sync(() => {
                    started++
                    return fakeHandle(Stream.make(encoder.encode('strict adapter')))
                  }),
                  () =>
                    Effect.sync(() => {
                      released++
                    }),
                ),
            }),
          )
          const result = yield* Bash.run({ command: 'owned' }, { requireOwnedTree: true }).pipe(
            Effect.provide(Layer.merge(bound(root), strict)),
            Effect.provideService(ToolExecution, progress.service),
          )
          assert.strictEqual(result.structuredOutput.cleanup, 'owned-tree')
          assert.strictEqual(result.structuredOutput.output, 'strict adapter')
          assert.strictEqual(started, 1)
          assert.strictEqual(released, 1)
        }),
      ).pipe(Effect.provide(files)),
  )

  it.effect(
    'PowerShell custom executable fallback, UTF8 prefix and prepared env are deterministic',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const root = yield* temp
          const progress = reporter()
          const seen: ChildProcess.StandardCommand[] = []
          const backend = Layer.succeed(
            ChildProcessSpawner.ChildProcessSpawner,
            ChildProcessSpawner.make((command) => {
              if (command._tag !== 'StandardCommand') return Effect.die('Unexpected pipeline')
              seen.push(command)
              if (command.command === 'missing')
                return Effect.fail(
                  PlatformError.systemError({
                    _tag: 'NotFound',
                    module: 'remote',
                    method: 'spawn',
                  }),
                )
              return Effect.succeed(fakeHandle(Stream.make(encoder.encode('PowerShell output'))))
            }),
          ).pipe(Layer.provideMerge(files))
          const result = yield* PowerShell.run(
            { command: 'Write-Output $x', env: { VALUE: 'explicit' } },
            {
              programs: ['missing', 'custom-pwsh'],
              commandPrefix: '$x="prepared"',
              prepare: (execution) =>
                Effect.succeed({ ...execution, env: { ...execution.env, PREPARED: 'yes' } }),
            },
          ).pipe(
            Effect.provide(bound(root, backend)),
            Effect.provideService(ToolExecution, progress.service),
          )
          assert.strictEqual(result.structuredOutput.output, 'PowerShell output')
          assert.deepStrictEqual(
            seen.map((c) => c.command),
            ['missing', 'custom-pwsh'],
          )
          const command = seen.at(-1)
          if (command === undefined) return assert.fail('Missing recorded command')
          assert.deepStrictEqual(command.args.slice(0, -1), [
            '-NoProfile',
            '-NonInteractive',
            '-ExecutionPolicy',
            'Bypass',
            '-Command',
          ])
          assert.include(command.args.at(-1) ?? '', '[Console]::OutputEncoding')
          assert.include(command.args.at(-1) ?? '', '$x="prepared"\nWrite-Output $x')
          assert.deepStrictEqual(command.options.env, { VALUE: 'explicit', PREPARED: 'yes' })
          assert.strictEqual(command.options.cwd, root)
          assert.strictEqual(command.options.shell, false)
        }),
      ).pipe(Effect.provide(files)),
  )

  it.effect('missing PowerShell runtime is explicit and never silently omitted', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const root = yield* temp
        const progress = reporter()
        const result = yield* PowerShell.run({ command: 'Write-Output "runtime-probe"' }).pipe(
          Effect.result,
          Effect.provide(bound(root)),
          Effect.provideService(ToolExecution, progress.service),
        )
        if (Result.isFailure(result)) {
          const error = expectError(result)
          assert.strictEqual(error.reason, 'unavailable')
          assert.include(error.message, 'supplied spawner')
          yield* Effect.log(
            'PowerShell actual runtime unavailable; deterministic executable contract tested separately.',
          )
        } else assert.include(result.success.structuredOutput.output, 'runtime-probe')
      }),
    ).pipe(Effect.provide(files)),
  )

  it.effect(
    'full overflow and nonzero failure still publish latest bounded state and complete raw spill',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const root = yield* temp
          const progress = reporter()
          yield* Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            for (const retain of ['full', 'tail'] as const) {
              const error = expectError(
                yield* Bash.run(
                  { command: 'printf abcdef; printf stderr >&2; exit 5' },
                  { programs: ['/bin/bash'], retention: { retain, maxBytes: 4, maxLines: 1 } },
                ).pipe(Effect.result),
              )
              assert.strictEqual(error.reason, retain === 'full' ? 'retention' : 'nonzero')
              assert.isTrue(error.outcome?.truncated)
              const spill = error.outcome?.spill
              if (spill === undefined)
                return assert.fail('Failure output needs a complete raw spill')
              assert.strictEqual(yield* fs.readFileString(spill.stdoutPath), 'abcdef')
              assert.strictEqual(yield* fs.readFileString(spill.stderrPath), 'stderr')
              assert.deepStrictEqual(progress.details.at(-1), error.outcome)
            }
          }).pipe(
            Effect.provide(bound(root)),
            Effect.provideService(ToolExecution, progress.service),
          )
        }),
      ).pipe(Effect.provide(files)),
  )

  it.effect(
    'capture per-toolkit preparation services and mutable defaults without cwd/env bleed',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          class Credentials extends Context.Service<Credentials, string>()(
            'test/ShellTools/BindingCredentials',
          ) {}
          const fs = yield* FileSystem.FileSystem
          const rootA = yield* temp
          const rootB = yield* temp
          const progress = reporter()
          const invoke = Effect.fnUntraced(function* (value: string) {
            const programs = ['/bin/bash']
            const options: Bash.Options<never, Credentials> = {
              programs,
              env: { VALUE: 'default' },
              prepare: (input) =>
                Effect.map(Credentials, (credential) => ({
                  ...input,
                  env: { ...input.env, VALUE: credential },
                })),
            }
            const handlers = Bash.layer(options).pipe(
              Layer.provide(Layer.succeed(Credentials, value)),
            )
            programs[0] = '/does-not-exist'
            const toolkit = yield* Bash.toolkit.pipe(Effect.provide(handlers))
            return yield* toolkit.invoke({
              id: value,
              name: 'bash',
              arguments: {
                command: 'printf "%s:%s" "$VALUE" "$PWD"; printf "%s" "$VALUE" > same.txt',
              },
            })
          })
          const results = yield* Effect.all(
            [
              invoke('a').pipe(Effect.provide(bound(rootA))),
              invoke('b').pipe(Effect.provide(bound(rootB))),
            ],
            { concurrency: 'unbounded' },
          ).pipe(Effect.provideService(ToolExecution, progress.service))
          assert.include(JSON.stringify(results[0]?.content), `a:${rootA}`)
          assert.include(JSON.stringify(results[1]?.content), `b:${rootB}`)
          assert.strictEqual(yield* fs.readFileString(`${rootA}/same.txt`), 'a')
          assert.strictEqual(yield* fs.readFileString(`${rootB}/same.txt`), 'b')
        }),
      ).pipe(Effect.provide(files)),
  )

  it.effect(
    'rejects invalid timeout, empty argv and script prefixes on direct argv before spawning',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const root = yield* temp
          const progress = reporter()
          let started = 0
          const backend = Layer.succeed(
            ChildProcessSpawner.ChildProcessSpawner,
            ChildProcessSpawner.make(() => {
              started++
              return Effect.succeed(fakeHandle(Stream.empty))
            }),
          ).pipe(Layer.provideMerge(files))
          yield* Effect.gen(function* () {
            for (const input of [
              { command: [] },
              { command: [''] },
              { command: 'echo', timeout: -1 },
              { command: 'echo', timeout: Infinity },
              { command: 'echo', timeout: 2147484 },
            ])
              assert.strictEqual(
                expectError(yield* Bash.run(input).pipe(Effect.result)).reason,
                'invalidParameters',
              )
            assert.strictEqual(
              expectError(
                yield* Bash.run({ command: ['literal', 'a b'] }, { commandPrefix: 'prefix' }).pipe(
                  Effect.result,
                ),
              ).reason,
              'invalidParameters',
            )
            assert.strictEqual(started, 0)
          }).pipe(
            Effect.provide(bound(root, backend)),
            Effect.provideService(ToolExecution, progress.service),
          )
        }),
      ).pipe(Effect.provide(files)),
  )

  it.effect(
    'preparation uses the current invocation reporter and scope, not construction-time services',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const root = yield* temp
          const old = reporter()
          const current = reporter()
          let cleaned = 0
          const handlers = Bash.layer({
            programs: ['/bin/bash'],
            prepare: (input) =>
              Effect.gen(function* () {
                const execution = yield* ToolExecution
                yield* execution.output(`prepared:${execution.callId}:`)
                return yield* Effect.acquireRelease(Effect.succeed(input), () =>
                  Effect.sync(() => {
                    cleaned++
                  }),
                )
              }),
          }).pipe(Layer.provideMerge(bound(root)))
          const context = yield* Layer.build(handlers).pipe(
            Effect.provideService(ToolExecution, { ...old.service, callId: 'old' }),
          )
          const toolkit = yield* Bash.toolkit.pipe(Effect.provideContext(context))
          yield* Effect.scoped(
            toolkit.invoke({
              id: 'current',
              name: 'bash',
              arguments: { command: 'printf executed' },
            }),
          ).pipe(Effect.provideService(ToolExecution, { ...current.service, callId: 'current' }))
          assert.strictEqual(current.chunks.join(''), 'prepared:current:executed')
          assert.deepStrictEqual(old.chunks, [])
          assert.strictEqual(cleaned, 1)
        }),
      ).pipe(Effect.provide(files)),
  )
})
