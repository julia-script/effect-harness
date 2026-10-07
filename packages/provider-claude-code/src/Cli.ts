import * as Config from 'effect/Config'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import type * as AiError from 'effect/ai/AiError'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import { authentication, processError, protocol, unsupported } from './Error.ts'
import * as Protocol from './Protocol.ts'
import * as Prompt from './Prompt.ts'

export interface Request {
  readonly model: string
  readonly system: string
  readonly content: ReadonlyArray<Prompt.ContentBlock>
  readonly sessionId?: string | undefined
  readonly thinkingEnabled?: boolean | undefined
  readonly maxTokens?: number | undefined
  readonly cache?: 'none' | 'short' | 'long' | undefined
  readonly autoCompact?: false | undefined
  readonly cwd?: string | undefined
  readonly effort?: 'low' | 'medium' | 'high' | 'xhigh' | 'max' | undefined
  readonly mcp?: { readonly url: string; readonly aliases: ReadonlyArray<string> } | undefined
}
export interface AccountStatus {
  readonly loggedIn: boolean
  readonly account: boolean
}
export class Cli extends Context.Service<
  Cli,
  {
    readonly status: Effect.Effect<AccountStatus, AiError.AiError>
    readonly run: (request: Request) => Stream.Stream<Protocol.Event, AiError.AiError>
  }
>()('@effect-harness/provider-claude-code/Cli') {}
const Status = Schema.Struct({
  loggedIn: Schema.Boolean,
  authMethod: Schema.optionalKey(Schema.String),
  apiProvider: Schema.optionalKey(Schema.String),
})
const clearedProviderEnvironment: Record<string, string | undefined> = {
  ANTHROPIC_API_KEY: undefined,
  ANTHROPIC_AUTH_TOKEN: undefined,
  ANTHROPIC_BASE_URL: undefined,
  CLAUDE_CODE_OAUTH_TOKEN: undefined,
  CLAUDE_CODE_USE_BEDROCK: undefined,
  CLAUDE_CODE_USE_VERTEX: undefined,
  CLAUDE_CODE_USE_FOUNDRY: undefined,
  CLAUDE_CODE_SIMPLE: undefined,
  CLAUDE_CODE_SAFE_MODE: undefined,
  CLAUDE_CODE_STARTUP_FAILURE_RESULTS: '1',
  ENABLE_TOOL_SEARCH: 'false',
}

/** Portable process boundary; the application provides its native ChildProcessSpawner Layer. */
export const layer = (options?: {
  readonly executable?: string | undefined
  readonly maxOutputBytes?: number | undefined
  readonly policyTrust?: 'trusted-installed-cli' | undefined
}) =>
  Layer.effect(Cli)(
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
      const executable = options?.executable ?? 'claude'
      const limit = options?.maxOutputBytes ?? 16 * 1024 * 1024
      if (executable.length === 0 || !Number.isSafeInteger(limit) || limit <= 0)
        return yield* unsupported('invalid executable or output limit')
      const commandOptions = {
        env: clearedProviderEnvironment,
        extendEnv: true,
        shell: false,
        killSignal: 'SIGTERM',
        forceKillAfter: '2 seconds',
      } as const
      const status = Effect.scoped(
        Effect.gen(function* () {
          const handle = yield* spawner
            .spawn(
              ChildProcess.make(executable, ['auth', 'status', '--json'], {
                ...commandOptions,
                stdin: 'ignore',
              }),
            )
            .pipe(Effect.mapError(() => processError('Claude Code could not be started')))
          yield* Effect.forkScoped(handle.stderr.pipe(Stream.runDrain, Effect.ignore))
          let statusSize = 0
          const text = yield* handle.stdout.pipe(
            Stream.mapEffect((bytes) => {
              statusSize += bytes.length
              return statusSize > limit
                ? Effect.fail(protocol('Claude Code status exceeded its configured limit'))
                : Effect.succeed(bytes)
            }),
            Stream.decodeText,
            Stream.mkString,
            Effect.mapError(() =>
              processError('Claude Code authentication status could not be read'),
            ),
          )
          const exitCode = yield* handle.exitCode.pipe(
            Effect.mapError(() => processError('Claude Code authentication status failed')),
          )
          if (exitCode !== 0) return { loggedIn: false, account: false }
          const raw = yield* Effect.try({
            try: () => JSON.parse(text) as unknown,
            catch: () => protocol('Malformed Claude Code authentication status'),
          })
          const value = yield* Schema.decodeUnknownEffect(Status)(raw).pipe(
            Effect.mapError(() => protocol('Malformed Claude Code authentication status')),
          )
          return {
            loggedIn: value.loggedIn,
            account: value.authMethod === 'claude.ai' && value.apiProvider === 'firstParty',
          }
        }),
      )
      return Cli.of({
        status,
        run: (request) =>
          Stream.unwrap(
            Effect.gen(function* () {
              if (options?.policyTrust !== 'trusted-installed-cli')
                return yield* unsupported(
                  'untrusted managed CLI policy; explicitly select policyTrust: trusted-installed-cli after auditing host policy',
                )
              if (request.sessionId !== undefined)
                yield* Schema.decodeEffect(Schema.String.check(Schema.isUUID(7)))(
                  request.sessionId,
                ).pipe(Effect.mapError(() => unsupported('invalid UUID7 session identity')))
              if (
                request.maxTokens !== undefined &&
                (!Number.isSafeInteger(request.maxTokens) || request.maxTokens <= 0)
              )
                return yield* unsupported('invalid output token limit')
              const settings = JSON.stringify({
                disableAllHooks: true,
                autoMemoryEnabled: false,
                ...(request.thinkingEnabled === undefined
                  ? {}
                  : { alwaysThinkingEnabled: request.thinkingEnabled }),
                ...(request.autoCompact === undefined
                  ? {}
                  : { autoCompactEnabled: request.autoCompact }),
              })
              const account = yield* status
              if (!account.loggedIn || !account.account) return yield* authentication()
              const args = [
                '--print',
                '--input-format',
                'stream-json',
                '--output-format',
                'stream-json',
                '--verbose',
                '--include-partial-messages',
                '--no-session-persistence',
                '--max-turns',
                '1',
                '--tools',
                '',
                '--strict-mcp-config',
                '--disable-slash-commands',
                '--no-chrome',
                '--system-prompt',
                request.system,
                '--model',
                request.model,
                '--permission-mode',
                'dontAsk',
                '--permission-prompts',
                'none',
              ]
              if (request.mcp === undefined)
                args.push(
                  '--safe-mode',
                  '--settings',
                  settings,
                  '--mcp-config',
                  '{"mcpServers":{}}',
                )
              else
                args.push(
                  '--restricted',
                  '--setting-sources',
                  '',
                  '--settings',
                  settings,
                  '--mcp-config',
                  JSON.stringify({
                    mcpServers: { harness: { type: 'http', url: request.mcp.url } },
                  }),
                  '--allowedTools',
                  request.mcp.aliases.join(','),
                )
              if (request.sessionId !== undefined) args.push('--session-id', request.sessionId)
              if (request.effort !== undefined) args.push('--effort', request.effort)
              const frame = yield* Prompt.encodeUserFrame({
                type: 'user',
                session_id: request.sessionId ?? '',
                parent_tool_use_id: null,
                message: { role: 'user', content: request.content },
              }).pipe(Effect.mapError(() => unsupported('non-serializable user frame')))
              const input = frame + '\n'
              if (new TextEncoder().encode(input).length > 10 * 1024 * 1024)
                return yield* unsupported('input exceeding the documented 10MB CLI limit')
              let cacheTtl: string | undefined
              if (request.cache === 'long') cacheTtl = '1h'
              else if (request.cache === 'short') cacheTtl = '5m'
              const handle = yield* spawner
                .spawn(
                  ChildProcess.make(executable, args, {
                    ...commandOptions,
                    env: {
                      ...clearedProviderEnvironment,
                      ...(request.maxTokens === undefined
                        ? {}
                        : { CLAUDE_CODE_MAX_OUTPUT_TOKENS: String(request.maxTokens) }),
                      ...(request.thinkingEnabled === undefined
                        ? {}
                        : { MAX_THINKING_TOKENS: request.thinkingEnabled ? undefined : '0' }),
                      ...(request.cache === undefined
                        ? {}
                        : {
                            DISABLE_PROMPT_CACHING: request.cache === 'none' ? '1' : undefined,
                            DISABLE_PROMPT_CACHING_HAIKU: undefined,
                            DISABLE_PROMPT_CACHING_SONNET: undefined,
                            DISABLE_PROMPT_CACHING_OPUS: undefined,
                            DISABLE_PROMPT_CACHING_FABLE: undefined,
                            CLAUDE_CODE_PROMPT_CACHE_TTL: cacheTtl,
                          }),
                    },
                    cwd: request.cwd,
                    stdin: Stream.succeed(new TextEncoder().encode(input)),
                  }),
                )
                .pipe(Effect.mapError(() => processError('Claude Code could not be started')))
              yield* Effect.forkScoped(handle.stderr.pipe(Stream.runDrain, Effect.ignore))
              let size = 0
              const lines = handle.stdout.pipe(
                Stream.mapEffect((chunk) => {
                  size += chunk.length
                  return size > limit
                    ? Effect.fail(protocol('Claude Code output exceeded its configured limit'))
                    : Effect.succeed(chunk)
                }),
                Stream.mapError(() =>
                  protocol('Claude Code output could not be read within its limit'),
                ),
                Stream.decodeText,
                Stream.splitLines,
                Stream.filter((line) => line.length !== 0),
                Stream.mapEffect(Protocol.decode),
              )
              return lines.pipe(
                Stream.concat(
                  Stream.fromEffect(
                    handle.exitCode.pipe(
                      Effect.mapError(() =>
                        processError('Claude Code exit status could not be read'),
                      ),
                      Effect.flatMap((code) =>
                        code === 0
                          ? Effect.void
                          : Effect.fail(processError('Claude Code exited unsuccessfully')),
                      ),
                    ),
                  ).pipe(Stream.drain),
                ),
              )
            }),
          ),
      })
    }),
  )

/** Resolves all layer options through the caller's ConfigProvider. */
export const layerConfig = (
  config: Config.Wrap<NonNullable<Parameters<typeof layer>[0]>>,
): Layer.Layer<
  Cli,
  AiError.AiError | Config.ConfigError,
  ChildProcessSpawner.ChildProcessSpawner
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layer(yield* Config.unwrap(config))
    }),
  )
