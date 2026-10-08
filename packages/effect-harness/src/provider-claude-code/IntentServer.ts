const SessionTypeId = '~effect-harness/provider-claude-code/IntentServer/Session'

/**
 * Scoped intent-only MCP sessions with tool execution blocked at the provider boundary.
 */
import * as Pipeable from 'effect/Pipeable'
import * as Inspectable from 'effect/Inspectable'
import * as Predicate from 'effect/Predicate'
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
 * **Details**
 *
 * This owned handle supports piping and bounded inspection. `toJSON` is a diagnostic
 * projection; use the original fields for protocol values and resource references.
 *
 * @category models
 */
export interface Session extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [SessionTypeId]: typeof SessionTypeId
  readonly url: string
  /** CLI alias -> original native Effect Tool name. */
  readonly aliases: ReadonlyMap<string, string>
}

/**
 * Checks the established nominal `Session` marker; it does not validate arbitrary payload fields.
 *
 * @category guards
 */
export const isSession = (u: unknown): u is Session =>
  Predicate.hasProperty(u, SessionTypeId) && u[SessionTypeId] === SessionTypeId

/**
 * Owns a `Session` handle while preserving payload descriptors and exact resource references.
 *
 * **Details**
 *
 * Construction and diagnostics do not evaluate payload accessors. Inspection is a bounded
 * diagnostic projection; read the original fields for protocol values.
 *
 * @category constructors
 */
export const makeSession = (
  input: Omit<
    Session,
    typeof SessionTypeId | keyof Pipeable.Pipeable | keyof Inspectable.Inspectable
  >,
): Session => {
  const handle: Session = Object.create(SessionProto)
  const descriptors = Object.getOwnPropertyDescriptors(input)
  // The owned protocol cannot be replaced by extra runtime payload keys.
  for (const key of [SessionTypeId, 'pipe', 'toJSON', 'toString', Inspectable.NodeInspectSymbol])
    Reflect.deleteProperty(descriptors, key)
  Object.defineProperties(handle, descriptors)
  Object.defineProperty(handle, SessionTypeId, { value: SessionTypeId, enumerable: false })
  return handle
}

const SessionProto = {
  ...Pipeable.Prototype,
  ...Inspectable.BaseProto,
  toJSON(): unknown {
    return {
      _id: 'effect-harness/provider-claude-code/IntentServer/Session',
      url: '<scoped endpoint>',
      aliases: '<ReadonlyMap>',
    }
  },
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
>()('effect-harness/provider-claude-code/IntentServer') {}

/**
 * Provides an intent server that rejects requests offering tools.
 *
 * **When to use**
 *
 * Use when CLI requests contain no tools and need no loopback server.
 *
 * @category layers
 */
export const layerDisabled: Layer.Layer<IntentServer> = Layer.succeed(
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
        const handler = yield* Option.match(token, {
          onNone: () => Effect.succeedNone,
          onSome: (token) =>
            Ref.get(sessions).pipe(Effect.map((sessions) => HashMap.get(sessions, token))),
        })
        return yield* Option.match(handler, {
          onNone: () => Effect.succeed(HttpServerResponse.empty({ status: 404 })),
          onSome: (handler) => handler,
        })
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
        return makeSession({ url: `http://127.0.0.1:${port}${path}`, aliases })
      }),
    })
  }),
)
