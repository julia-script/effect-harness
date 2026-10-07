import * as Config from 'effect/Config'
import * as ConfigProvider from 'effect/ConfigProvider'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Sink from 'effect/Sink'
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as Spawner from 'effect/process/ChildProcessSpawner'
import * as Cli from '../src/Cli.ts'

const request: Cli.Request = {
  model: 'sonnet',
  system: 'System',
  content: [{ type: 'text', text: 'Hi' }],
  cwd: '/tmp',
  effort: 'high',
}
const fixture = (options?: {
  readonly status?: string
  readonly output?: string
  readonly exit?: number
  readonly hang?: boolean
  readonly policyTrust?: 'trusted-installed-cli' | undefined
  readonly limit?: number
}) => {
  const commands: Array<ChildProcess.StandardCommand> = []
  const inputs: Array<string> = []
  let released = 0
  let inferenceReleased = 0
  const spawn = Spawner.make(
    Effect.fnUntraced(function* (command) {
      if (!ChildProcess.isStandardCommand(command)) return yield* Effect.die('Unexpected pipeline')
      commands.push(command)
      const isStatus = command.args[0] === 'auth'
      const input = command.options.stdin
      if (Stream.isStream(input)) inputs.push(yield* input.pipe(Stream.decodeText, Stream.mkString))
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          released++
          if (!isStatus) inferenceReleased++
        }),
      )
      let stdout = Stream.empty as Stream.Stream<Uint8Array>
      if (isStatus)
        stdout = Stream.succeed(
          new TextEncoder().encode(
            options?.status ??
              '{"loggedIn":true,"authMethod":"claude.ai","apiProvider":"firstParty","email":"private@example.com"}',
          ),
        )
      else if (options?.hang) stdout = Stream.never
      else
        stdout = Stream.succeed(
          new TextEncoder().encode(
            options?.output ?? '{"type":"system","subtype":"init","tools":[]}\n',
          ),
        )
      return Spawner.makeHandle({
        pid: Spawner.ProcessId(123),
        stdin: Sink.drain,
        stdout,
        stderr: Stream.succeed(new TextEncoder().encode('secret stderr')),
        all: stdout,
        exitCode: Effect.succeed(Spawner.ExitCode(isStatus ? 0 : (options?.exit ?? 0))),
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
        unref: Effect.succeed(Effect.void),
      })
    }),
  )
  return {
    commands,
    spawn,
    inputs,
    get released() {
      return released
    },
    get inferenceReleased() {
      return inferenceReleased
    },
    layer: Cli.layer({
      executable: '/installed/claude',
      policyTrust: options?.policyTrust,
      maxOutputBytes: options?.limit,
    }).pipe(Layer.provide(Layer.succeed(Spawner.ChildProcessSpawner, spawn))),
  }
}
const trusted = () => fixture({ policyTrust: 'trusted-installed-cli' })

describe('Claude Code portable process boundary', () => {
  it.effect('login status returns booleans only without extracting credentials', () =>
    Effect.gen(function* () {
      const f = fixture()
      const status = yield* Cli.Cli.use((cli) => cli.status).pipe(Effect.provide(f.layer))
      assert.deepEqual(status, { loggedIn: true, account: true })
      assert.strictEqual(f.released, 1)
      assert.deepEqual(f.commands[0]?.args, ['auth', 'status', '--json'])
    }),
  )
  it.effect('requires explicit managed-policy trust before inference', () =>
    Effect.gen(function* () {
      const f = fixture()
      const error = yield* Cli.Cli.use((cli) => Stream.runDrain(cli.run(request))).pipe(
        Effect.provide(f.layer),
        Effect.flip,
      )
      assert.strictEqual(error.reason._tag, 'InvalidRequestError')
      assert.strictEqual(f.commands.length, 0)
    }),
  )
  it.effect(
    'text invocation disables builtins, uses owned OAuth login, and sends structured stdin',
    () =>
      Effect.gen(function* () {
        const f = trusted()
        yield* Cli.Cli.use((cli) => Stream.runDrain(cli.run(request))).pipe(Effect.provide(f.layer))
        const command = f.commands[1]
        assert.isDefined(command)
        assert.include(command?.args, '--safe-mode')
        assert.notInclude(command?.args, '--bare')
        assert.notInclude(command?.args, '--dangerously-skip-permissions')
        assert.strictEqual(command?.args[(command?.args.indexOf('--tools') ?? 0) + 1], '')
        assert.strictEqual(command?.args[(command?.args.indexOf('--max-turns') ?? 0) + 1], '1')
        assert.strictEqual(command?.options.shell, false)
        assert.strictEqual(command?.options.env?.ANTHROPIC_API_KEY, undefined)
        assert.strictEqual(command?.options.env?.CLAUDE_CODE_OAUTH_TOKEN, undefined)
        assert.deepEqual(JSON.parse(f.inputs[0] ?? '{}'), {
          type: 'user',
          session_id: '',
          parent_tool_use_id: null,
          message: { role: 'user', content: request.content },
        })
        assert.strictEqual(f.inferenceReleased, 1)
      }),
  )
  it.effect('MCP invocation isolates configuration and permits only descriptor aliases', () =>
    Effect.gen(function* () {
      const f = trusted()
      yield* Cli.Cli.use((cli) =>
        Stream.runDrain(
          cli.run({
            ...request,
            mcp: { url: 'http://127.0.0.1:42/mcp/id', aliases: ['mcp__harness__tool_0'] },
          }),
        ),
      ).pipe(Effect.provide(f.layer))
      const args = f.commands[1]?.args ?? []
      assert.include(args, '--restricted')
      assert.include(args, '--strict-mcp-config')
      assert.strictEqual(args[args.indexOf('--setting-sources') + 1], '')
      assert.deepEqual(JSON.parse(args[args.indexOf('--settings') + 1] ?? '{}'), {
        disableAllHooks: true,
        autoMemoryEnabled: false,
      })
      assert.strictEqual(args[args.indexOf('--allowedTools') + 1], 'mcp__harness__tool_0')
    }),
  )
  it.effect('rejects missing and alternate authentication before starting inference', () =>
    Effect.gen(function* () {
      for (const status of [
        '{"loggedIn":false}',
        '{"loggedIn":true,"authMethod":"api_key","apiProvider":"firstParty"}',
        '{"loggedIn":true,"authMethod":"claude.ai","apiProvider":"bedrock"}',
      ]) {
        const f = fixture({ status, policyTrust: 'trusted-installed-cli' })
        assert.strictEqual(
          (yield* Cli.Cli.use((cli) => Stream.runDrain(cli.run(request))).pipe(
            Effect.provide(f.layer),
            Effect.flip,
          )).reason._tag,
          'AuthenticationError',
        )
        assert.strictEqual(f.commands.length, 1)
      }
    }),
  )
  it.effect('sanitizes malformed frames and nonzero process exits; bounds status and stdout', () =>
    Effect.gen(function* () {
      for (const options of [
        { output: 'private secret\n' },
        { exit: 1 },
        { output: '{"type":"unknown","secret":"private"}\n' },
        { limit: 4 },
        { limit: 200, output: 'x'.repeat(201) },
      ]) {
        const f = fixture({ ...options, policyTrust: 'trusted-installed-cli' })
        const error = yield* Cli.Cli.use((cli) => Stream.runDrain(cli.run(request))).pipe(
          Effect.provide(f.layer),
          Effect.flip,
        )
        assert.notInclude(JSON.stringify(error), 'private secret')
        assert.notInclude(JSON.stringify(error), 'secret stderr')
        assert.strictEqual(error._tag, 'AiError')
        assert.strictEqual(f.released, f.commands.length)
      }
    }),
  )
  it.effect('interruption releases the process scope while stdout is blocked', () =>
    Effect.gen(function* () {
      const f = fixture({ hang: true, policyTrust: 'trusted-installed-cli' })
      const fiber = yield* Effect.forkChild(
        Cli.Cli.use((cli) => cli.run(request).pipe(Stream.runDrain)).pipe(Effect.provide(f.layer)),
      )
      while (f.commands.length < 2) yield* Effect.yieldNow
      yield* Fiber.interrupt(fiber)
      assert.strictEqual(f.inferenceReleased, 1)
    }),
  )
})

describe('CLI ConfigProvider boundary', () => {
  it.effect('configured executable and output limits use the substituted process service', () => {
    const f = fixture()
    const layer = Cli.layerConfig({
      executable: Config.String('CLI_PATH'),
      maxOutputBytes: Config.Int('OUTPUT_LIMIT'),
    }).pipe(Layer.provide(Layer.succeed(Spawner.ChildProcessSpawner, f.spawn)))
    const run = (limit: number) =>
      Cli.Cli.use((cli) => cli.status).pipe(
        Effect.provide(layer),
        Effect.provideService(
          ConfigProvider.ConfigProvider,
          ConfigProvider.fromUnknown({ CLI_PATH: '/configured/claude', OUTPUT_LIMIT: limit }),
        ),
      )
    return Effect.gen(function* () {
      assert.deepStrictEqual(yield* run(1000), { loggedIn: true, account: true })
      assert.strictEqual(f.commands[0]?.command, '/configured/claude')
      const limited = yield* run(1).pipe(Effect.flip)
      assert.strictEqual(limited._tag, 'AiError')
      assert.isFalse(JSON.stringify(limited).includes('private@example.com'))
      const missing = yield* Effect.scoped(Layer.build(layer)).pipe(
        Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({})),
        Effect.flip,
      )
      assert.strictEqual(missing._tag, 'ConfigError')
      assert.strictEqual(f.commands.length, 2)
      assert.strictEqual(f.released, 2)
    })
  })
})
