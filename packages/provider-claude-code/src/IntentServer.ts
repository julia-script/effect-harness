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
import { processError, unsupported } from './Error.ts'

export interface Session {
  readonly url: string
  /** CLI alias -> original native Effect Tool name. */
  readonly aliases: ReadonlyMap<string, string>
}
export class IntentServer extends Context.Service<
  IntentServer,
  {
    readonly open: (
      tools: ReadonlyArray<Tool.Any>,
    ) => Effect.Effect<Session, AiError.AiError, Scope.Scope>
  }
>()('@effect-harness/provider-claude-code/IntentServer') {}

/** Explicit text-only capability: attempts to supply tools produce a native typed error. */
export const layerDisabled = Layer.succeed(IntentServer, {
  open: () => Effect.fail(unsupported('tools without a scoped loopback IntentServer Layer')),
})

/** Serves native MCP descriptors; every tools/call handler waits forever and never runs a real tool. */
export const layer = Layer.effect(IntentServer)(
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
    const sessions = new Map<string, Handler>()
    yield* server.serve(
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest
        const match = /^\/mcp\/([a-f0-9-]+)(?:\?|$)/.exec(request.url)
        const handler = match?.[1] === undefined ? undefined : sessions.get(match[1])
        return handler === undefined ? HttpServerResponse.empty({ status: 404 }) : yield* handler
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
        sessions.set(
          id,
          handler.pipe(
            Effect.catchCause(() => Effect.succeed(HttpServerResponse.empty({ status: 500 }))),
          ),
        )
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            sessions.delete(id)
          }),
        )
        return { url: `http://127.0.0.1:${port}${path}`, aliases }
      }),
    })
  }),
)
