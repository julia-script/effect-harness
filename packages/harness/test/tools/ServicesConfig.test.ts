import { assert, describe, it } from '@effect/vitest'
import * as Config from 'effect/Config'
import * as ConfigProvider from 'effect/ConfigProvider'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as PlatformError from 'effect/PlatformError'
import * as Ref from 'effect/Ref'
import * as Scope from 'effect/Scope'
import * as Exit from 'effect/Exit'
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as Spawner from 'effect/process/ChildProcessSpawner'
import { Env } from '../../src/Env.ts'
import * as NodeEnv from '../../src/env/Node.ts'
import * as Invocation from '../../src/Invocation.ts'
import * as Tools from '../../src/tools/index.ts'
import { withEnv } from './Helpers.ts'

const host: NodeEnv.Host = {
  platform: 'linux',
  cwd: '/injected-cwd',
  home: '/injected-home',
  searchPathDelimiter: '|',
}
const capture = Effect.gen(function* () {
  const base = yield* Path.Path
  const attempts: string[] = []
  const joins: ReadonlyArray<string>[] = []
  const commands: ChildProcess.StandardCommand[] = []
  const available = new Set<string>([host.cwd])
  const failure = PlatformError.systemError({
    _tag: 'Unknown',
    module: 'ChildProcess',
    method: 'spawn',
    cause: new Error('captured replacement spawn'),
  })
  const fs = FileSystem.makeNoop({
    access: (name) =>
      Effect.suspend(() => {
        attempts.push(name)
        return available.has(name)
          ? Effect.void
          : Effect.fail(
              PlatformError.systemError({
                _tag: 'NotFound',
                module: 'FileSystem',
                method: 'access',
                pathOrDescriptor: name,
              }),
            )
      }),
    readFile: () => Effect.succeed(new TextEncoder().encode('replacement filesystem')),
  })
  const path = Path.Path.of({
    ...base,
    join: (...parts) => {
      joins.push(parts)
      return base.join(...parts)
    },
  })
  const spawner = Spawner.make((command) =>
    Effect.suspend(() => {
      assert.strictEqual(ChildProcess.isStandardCommand(command), true)
      if (ChildProcess.isStandardCommand(command)) commands.push(command)
      return Effect.fail(failure)
    }),
  )
  const platform = Layer.mergeAll(
    Layer.succeed(FileSystem.FileSystem, fs),
    Layer.succeed(Path.Path, path),
    Layer.succeed(Spawner.ChildProcessSpawner, spawner),
  )
  return { fs, path, spawner, platform, failure, attempts, joins, commands, available }
})
const build = (
  options: NodeEnv.Options,
  platform: Layer.Layer<FileSystem.FileSystem | Path.Path | Spawner.ChildProcessSpawner>,
) =>
  Layer.build(NodeEnv.layer(options).pipe(Layer.provide(platform))).pipe(
    Effect.map((context) => Context.get(context, Env)),
  )

describe('ServicesConfig', () => {
  it.effect(
    'Node library preserves replacement FileSystem, Path and spawner identities and open types',
    () =>
      Effect.gen(function* () {
        const replacement = yield* capture
        const node = NodeEnv.layer({ host, shell: '/chosen-shell' })
        const requirements: Layer.Services<typeof node> extends
          | FileSystem.FileSystem
          | Path.Path
          | Spawner.ChildProcessSpawner
          ? true
          : false = true
        const filesystem: FileSystem.FileSystem extends Layer.Services<typeof node> ? true : false =
          true
        const paths: Path.Path extends Layer.Services<typeof node> ? true : false = true
        const spawn: Spawner.ChildProcessSpawner extends Layer.Services<typeof node>
          ? true
          : false = true
        void requirements
        void filesystem
        void paths
        void spawn
        replacement.available.add('/chosen-shell')
        const env = yield* build({ host, shell: '/chosen-shell' }, replacement.platform)
        assert.strictEqual(env.path, replacement.path)
        assert.strictEqual(env.cwd, host.cwd)
        assert.strictEqual(yield* env.absolutePath('~/file'), '/injected-home/file')
        assert.strictEqual(
          new TextDecoder().decode(yield* env.readBinaryFile('file')),
          'replacement filesystem',
        )
        const error = yield* Effect.flip(env.exec('literal command'))
        assert.strictEqual(error.cause, replacement.failure)
        assert.strictEqual(error.reason._tag, 'ExecutionSpawnError')
        assert.deepStrictEqual(replacement.attempts, [host.cwd, '/chosen-shell'])
        assert.strictEqual(replacement.commands[0]?.command, '/chosen-shell')
        assert.deepStrictEqual(replacement.commands[0]?.args, ['-c', 'literal command'])
      }).pipe(Effect.provide(Path.layer)),
  )
  it.effect(
    'ConfigProvider PATH is decoded then split with injected host delimiter and captured Path.join',
    () =>
      Effect.gen(function* () {
        const replacement = yield* capture
        replacement.available.add('/config/second/bash')
        const env = yield* build({ host }, replacement.platform)
        const error = yield* Effect.flip(
          env
            .exec('select shell')
            .pipe(
              Effect.provideService(
                ConfigProvider.ConfigProvider,
                ConfigProvider.fromUnknown({ PATH: '/config/first|/config/second' }),
              ),
            ),
        )
        assert.strictEqual(error.cause, replacement.failure)
        assert.deepStrictEqual(replacement.attempts, [
          host.cwd,
          '/bin/bash',
          '/config/first/bash',
          '/config/second/bash',
        ])
        assert.deepStrictEqual(replacement.joins, [
          ['/config/first', 'bash'],
          ['/config/second', 'bash'],
        ])
        assert.strictEqual(replacement.commands[0]?.command, '/config/second/bash')
        // Both dependencies are captured at construction; unrelated per-call replacements cannot change discovery.
        const unavailable = FileSystem.makeNoop({})
        yield* Effect.flip(
          env
            .exec('same services')
            .pipe(
              Effect.provideService(FileSystem.FileSystem, unavailable),
              Effect.provideService(
                Path.Path,
                Path.Path.of({ ...replacement.path, join: () => '/wrong' }),
              ),
              Effect.provideService(
                ConfigProvider.ConfigProvider,
                ConfigProvider.fromUnknown({ PATH: '/config/second' }),
              ),
            ),
        )
        assert.strictEqual(replacement.commands[1]?.command, '/config/second/bash')
      }).pipe(Effect.provide(Path.layer)),
  )
  it.effect('missing PATH retains empty search and Unix sh fallback', () =>
    Effect.gen(function* () {
      const replacement = yield* capture
      const env = yield* build({ host }, replacement.platform)
      yield* Effect.flip(
        env
          .exec('fallback')
          .pipe(
            Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({})),
          ),
      )
      assert.deepStrictEqual(replacement.attempts, [host.cwd, '/bin/bash', 'bash'])
      assert.strictEqual(replacement.commands[0]?.command, 'sh')
    }).pipe(Effect.provide(Path.layer)),
  )
  it.effect(
    'Windows discovery uses decoded ProgramFiles and preserves system Bash stdin protocol',
    () =>
      Effect.gen(function* () {
        const replacement = yield* capture
        const windows = { ...host, platform: 'win32' }
        replacement.available.add('/programs86/Git/bin/bash.exe')
        const env = yield* build({ host: windows }, replacement.platform)
        yield* Effect.flip(
          env.exec('windows').pipe(
            Effect.provideService(
              ConfigProvider.ConfigProvider,
              ConfigProvider.fromUnknown({
                ProgramFiles: '/programs',
                'ProgramFiles(x86)': '/programs86',
                PATH: '/ignored',
              }),
            ),
          ),
        )
        assert.strictEqual(replacement.commands[0]?.command, '/programs86/Git/bin/bash.exe')
        assert.deepStrictEqual(replacement.attempts, [
          host.cwd,
          '/programs/Git/bin/bash.exe',
          '/programs86/Git/bin/bash.exe',
        ])
        replacement.available.add('C:\\Windows\\System32\\bash.exe')
        const system = yield* build(
          { host: windows, shell: 'C:\\Windows\\System32\\bash.exe' },
          replacement.platform,
        )
        yield* Effect.flip(system.exec('stdin command'))
        const command = replacement.commands[1]
        assert.ok(command)
        assert.deepStrictEqual(command.args, ['-s'])
        assert.ok(typeof command.options.stdin === 'object' && 'stream' in command.options.stdin)
        if (
          typeof command.options.stdin === 'object' &&
          'stream' in command.options.stdin &&
          Stream.isStream(command.options.stdin.stream)
        )
          assert.strictEqual(
            new TextDecoder().decode((yield* Stream.runCollect(command.options.stdin.stream))[0]),
            'stdin command\n',
          )
      }).pipe(Effect.provide(Path.layer)),
  )
  it.effect(
    'Config source failure becomes semantic ExecutionError with the actual ConfigError cause and no spawn',
    () =>
      Effect.gen(function* () {
        const replacement = yield* capture
        const env = yield* build({ host }, replacement.platform)
        const cause = new Error('configuration transport failed')
        const source = new ConfigProvider.SourceError({ message: 'source unavailable', cause })
        const provider = ConfigProvider.make(() => Effect.fail(source))
        const error = yield* Effect.flip(
          env.exec('blocked').pipe(Effect.provideService(ConfigProvider.ConfigProvider, provider)),
        )
        assert.strictEqual(error._tag, 'ExecutionError')
        assert.strictEqual(error.reason._tag, 'ExecutionShellUnavailable')
        assert.ok(error.cause instanceof Config.ConfigError)
        if (error.cause instanceof Config.ConfigError) assert.strictEqual(error.cause.cause, source)
        assert.strictEqual(replacement.commands.length, 0)
        assert.deepStrictEqual(replacement.attempts, [host.cwd])
      }).pipe(Effect.provide(Path.layer)),
  )
  it.effect(
    'custom shell/watch overrides bypass discovery Config and child environment remains verbatim',
    () =>
      Effect.gen(function* () {
        const replacement = yield* capture
        let watchCalls = 0
        const env = yield* build(
          {
            host,
            env: { EXISTING: 'keep', PATH: 'child-path' },
            resolveShell: Effect.succeed({
              program: '/override',
              args: ['custom'],
              commandOnStdin: false,
            }),
            resolveWatchMode: () =>
              Effect.sync(() => {
                watchCalls++
                return 'polling' as const
              }),
          },
          replacement.platform,
        )
        const broken = ConfigProvider.make(() =>
          Effect.fail(new ConfigProvider.SourceError({ message: 'must not read' })),
        )
        yield* Effect.flip(
          env
            .exec('override', { env: { ADDED: 'one' } })
            .pipe(Effect.provideService(ConfigProvider.ConfigProvider, broken)),
        )
        assert.strictEqual(replacement.commands[0]?.command, '/override')
        assert.deepStrictEqual(replacement.commands[0]?.options.env, {
          EXISTING: 'keep',
          PATH: 'child-path',
          ADDED: 'one',
        })
        assert.strictEqual(replacement.commands[0]?.options.extendEnv, true)
        yield* Effect.flip(env.exec('isolated', { inheritEnv: false, env: { ONLY: 'two' } }))
        assert.deepStrictEqual(replacement.commands[1]?.options.env, { ONLY: 'two' })
        assert.strictEqual(replacement.commands[1]?.options.extendEnv, false)
        const owner = yield* Scope.fork(yield* Scope.Scope)
        const watcher = yield* env
          .watch([{ path: '/missing-injected-target' }])
          .pipe(Scope.provide(owner))
        assert.strictEqual(watcher.mode, 'polling')
        assert.strictEqual(watchCalls, 1)
        yield* Scope.close(owner, Exit.void)
      }).pipe(Effect.provide(Path.layer)),
  )
  it.effect(
    'layerConfig accepts complete Config.Wrap options, retains open types and reports ConfigError',
    () =>
      Effect.gen(function* () {
        const replacement = yield* capture
        replacement.available.add('/configured-shell')
        const layer = NodeEnv.layerConfig({
          host: Config.succeed(host),
          id: Config.String('ID'),
          shell: Config.String('SHELL'),
        })
        const errors: Config.ConfigError extends Layer.Error<typeof layer> ? true : false = true
        const requirements:
          | FileSystem.FileSystem
          | Path.Path
          | Spawner.ChildProcessSpawner extends Layer.Services<typeof layer>
          ? true
          : false = true
        void errors
        void requirements
        const loaded = yield* Layer.build(layer.pipe(Layer.provide(replacement.platform))).pipe(
          Effect.provideService(
            ConfigProvider.ConfigProvider,
            ConfigProvider.fromUnknown({ ID: 'configured', SHELL: '/configured-shell' }),
          ),
        )
        yield* Effect.flip(Context.get(loaded, Env).exec('configured'))
        assert.strictEqual(replacement.commands[0]?.command, '/configured-shell')
        const error = yield* Effect.flip(
          Layer.build(layer.pipe(Layer.provide(replacement.platform))).pipe(
            Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({})),
          ),
        )
        assert.ok(error instanceof Config.ConfigError)
      }).pipe(Effect.provide(Path.layer)),
  )
  it.effect(
    'direct Invocation bindings retain exact request identity and layerSilent is stable',
    () =>
      Effect.gen(function* () {
        const reports = yield* Ref.make<ReadonlyArray<string>>([])
        const first = Invocation.Invocation.of({
          cwd: '/first',
          report: () => Ref.update(reports, (values) => [...values, 'first']),
          progress: () => Effect.void,
        })
        const second = Invocation.Invocation.of({
          cwd: '/second',
          report: () => Ref.update(reports, (values) => [...values, 'second']),
          progress: () => Effect.void,
        })
        for (const selected of [first, second])
          yield* Effect.gen(function* () {
            const current = yield* Invocation.Invocation
            assert.strictEqual(current, selected)
            yield* current.report(new Error('reported'))
          }).pipe(
            Effect.provide(
              Layer.succeed(Invocation.Invocation, Invocation.Invocation.of(selected)),
            ),
          )
        assert.deepStrictEqual(yield* Ref.get(reports), ['first', 'second'])
        const a = Context.get(yield* Layer.build(Invocation.layerSilent), Invocation.Invocation)
        const b = Context.get(yield* Layer.build(Invocation.layerSilent), Invocation.Invocation)
        assert.strictEqual(a, b)
        assert.strictEqual(a.cwd, '.')
        yield* a.report(new Error('ignored'))
        yield* a.progress({ output: 'ignored' })
        assert.deepStrictEqual(yield* Ref.get(reports), ['first', 'second'])
      }),
  )
  it.effect(
    'CodingTools service keeps executable registration payload and PowerShell remains Extension data',
    () =>
      withEnv(
        Effect.gen(function* () {
          const context = yield* Layer.build(Tools.layer())
          const tools = Context.get(context, Tools.CodingTools)
          assert.strictEqual(tools.name, 'coding-tools')
          assert.deepStrictEqual(
            tools.tools?.map((registration) => registration.tool.name),
            ['read', 'write', 'edit', 'bash'],
          )
          const again = Context.get(
            yield* Layer.build(Layer.succeed(Tools.CodingTools, tools)),
            Tools.CodingTools,
          )
          assert.strictEqual(again, tools)
          const powershell = yield* Tools.makePowerShell()
          assert.strictEqual(powershell.name, 'powershell')
          assert.deepStrictEqual(
            powershell.tools?.map((registration) => registration.tool.name),
            ['powershell'],
          )
        }),
      ),
  )
})
