/**
 * Scoped intent-only MCP sessions with tool execution blocked at the provider boundary.
 */
import * as Arr from 'effect/Array'
import * as Ref from 'effect/Ref'
import * as HashMap from 'effect/HashMap'
import * as Option from 'effect/Option'
import * as Context from 'effect/Context'
import * as Crypto from 'effect/Crypto'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import type * as AiError from 'effect/ai/AiError'
import * as McpProtocol from 'effect/ai/McpProtocol'
import * as McpSchema from 'effect/ai/McpSchema'
import * as McpServer from 'effect/ai/McpServer'
import * as Tool from 'effect/ai/Tool'
import * as HttpRouter from 'effect/http/HttpRouter'
import * as HttpServer from 'effect/http/HttpServer'
import * as HttpServerRequest from 'effect/http/HttpServerRequest'
import * as HttpServerResponse from 'effect/http/HttpServerResponse'
import { processError, unsupported } from './ClaudeCodeError.ts'

/**
 * Scoped MCP endpoint and CLI-alias mapping for offered native tools.
 *
 * @category models
 */
export interface Session {
  readonly url: string
  /** CLI alias -> original native Effect Tool name. */
  readonly aliases: ReadonlyMap<string, string>
}
/**
 * Service opening a scoped MCP endpoint that captures native tool intents.
 *
 * **Details**
 *
 * The CLI can request offered tools but the harness executes their handlers. Aliases map
 * CLI-visible names back to native Tool names.
 *
 * @category services
 */
export class IntentServer extends Context.Service<
  IntentServer,
  {
    readonly open: (
      tools: ReadonlyArray<Tool.Any>,
    ) => Effect.Effect<Session, AiError.AiError, Scope.Scope>
  }
>()('@effect-harness/provider-claude-code/IntentServer') {}

/**
 * Provides an intent server that rejects requests offering tools.
 *
 * **When to use**
 *
 * Use when CLI requests contain no tools and need no loopback server.
 *
 * @category layers
 */
export const layerDisabled: Layer.Layer<IntentServer, never, never> = Layer.succeed(
  IntentServer,
  IntentServer.of({
    open: () => Effect.fail(unsupported('tools without a scoped loopback IntentServer Layer')),
  }),
)

/**
 * Provides scoped MCP tool-intent sessions through a loopback HttpServer.
 *
 * **Details**
 *
 * Each session exposes offered tools and retains their alias mapping. The server records
 * intent rather than executing handlers inside the CLI.
 *
 * **Gotchas**
 *
 * Keep the supplied HttpServer and acquisition Scope alive until the request finishes.
 *
 * @category layers
 */
export const layer: Layer.Layer<
  IntentServer,
  AiError.AiError,
  Crypto.Crypto | HttpServer.HttpServer
> = Layer.effect(IntentServer)(
  Effect.gen(function* () {
    const server = yield* HttpServer.HttpServer
    const crypto = yield* Crypto.Crypto
    if (
      server.address._tag !== 'InetAddressV4' ||
      server.address.address.toString() !== '127.0.0.1'
    )
      return yield* unsupported('an MCP listener bound outside 127.0.0.1')
    const port = server.address.port
    type Handler = Effect.Effect<
      HttpServerResponse.HttpServerResponse,
      never,
      HttpServerRequest.HttpServerRequest | Scope.Scope
    >
    const sessions = yield* Ref.make(HashMap.empty<string, Handler>())
    yield* server.serve(
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest
        const token = Option.flatMap(
          Option.fromNullishOr(/^\/mcp\/([a-f0-9-]+)(?:\?|$)/.exec(request.url)),
          (match) => Arr.get(match, 1),
        )
        const handler = Option.isNone(token)
          ? Option.none<Handler>()
          : HashMap.get(yield* Ref.get(sessions), token.value)
        return Option.isNone(handler)
          ? HttpServerResponse.empty({ status: 404 })
          : yield* handler.value
      }),
    )
    return IntentServer.of({
      open: Effect.fnUntraced(function* (tools) {
        const id = yield* crypto.randomUUIDv4.pipe(
          Effect.mapError(() => processError('MCP session identity generation failed')),
        )
        const path = `/mcp/${id}` as const
        const aliases = new Map<string, string>()
        const definitions: Array<McpSchema.Tool> = []
        for (const [index, tool] of tools.entries()) {
          if (Tool.isProviderDefined(tool)) return yield* unsupported('provider-executed tools')
          const name = `tool_${index}`
          const schema = yield* Effect.try({
            try: () => Tool.getJsonSchema(tool),
            catch: () => unsupported('the supplied tool schema'),
          })
          const inputSchema = yield* Schema.decodeUnknownEffect(McpSchema.ToolJson)(schema).pipe(
            Effect.mapError(() => unsupported('non-object MCP tool input schemas')),
          )
          aliases.set(`mcp__harness__${name}`, tool.name)
          definitions.push(
            new McpSchema.Tool({
              name,
              inputSchema,
              ...(tool.description === undefined ? {} : { description: tool.description }),
            }),
          )
        }
        const registration = Layer.effectDiscard(
          Effect.gen(function* () {
            const registry = yield* McpServer.McpServer
            for (const tool of definitions)
              yield* registry.addTool({
                tool,
                annotations: Context.empty(),
                handle: () => Effect.never,
              })
          }),
        ).pipe(
          Layer.provide(
            McpServer.layerHttp({
              name: 'effect-harness-intents',
              version: '1',
              path,
              protocols: [McpProtocol.v2025_11_25, McpProtocol.v2025_03_26],
            }),
          ),
        )
        const handler = yield* HttpRouter.toHttpEffect(registration).pipe(
          Effect.mapError(() => processError('Native MCP session could not be started')),
        )
        yield* Effect.acquireRelease(
          Ref.update(
            sessions,
            HashMap.set(
              id,
              handler.pipe(
                Effect.catchCause(() => Effect.succeed(HttpServerResponse.empty({ status: 500 }))),
              ),
            ),
          ),
          () => Ref.update(sessions, HashMap.remove(id)),
        )
        return { url: `http://127.0.0.1:${port}${path}`, aliases }
      }),
    })
  }),
)
