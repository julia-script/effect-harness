import * as IntentServer from 'effect-harness/provider-claude-code/IntentServer'
import { IntentServerFixture } from './IntentServerFixture.ts'
import * as Exit from 'effect/Exit'
import * as Scope from 'effect/Scope'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Schema from 'effect/Schema'
import * as Tool from 'effect/ai/Tool'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'

const layer = IntentServerFixture.layer
const json = Schema.Struct({
  jsonrpc: Schema.Literal('2.0'),
  id: Schema.Int,
  result: Schema.JsonObject,
})
const tool = Tool.make('write.document', {
  parameters: Schema.Struct({ value: Schema.String }),
  success: Schema.String,
})
const post = (
  url: string,
  id: number,
  method: string,
  params: Schema.JsonObject,
  sessionId?: string,
) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient
    const request = yield* HttpClientRequest.post(url).pipe(
      HttpClientRequest.setHeaders({
        accept: 'application/json, text/event-stream',
        'mcp-protocol-version': '2025-03-26',
        ...(sessionId === undefined ? {} : { 'mcp-session-id': sessionId }),
      }),
      HttpClientRequest.bodyJson({ jsonrpc: '2.0', id, method, params }),
    )
    const response = yield* client.execute(request)
    return {
      status: response.status,
      sessionId: response.headers['mcp-session-id'],
      body: yield* response.json,
    }
  })

describe('IntentServer', () => {
  describe('native intent-only MCP bridge', () => {
    it.effect('closing one registered MCP scope preserves another active session', () =>
      Effect.gen(function* () {
        const server = (yield* IntentServerFixture).server
        const firstScope = yield* Scope.make()
        const secondScope = yield* Scope.make()
        yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
        yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void))
        const first = yield* server
          .open([tool])
          .pipe(Effect.provideService(Scope.Scope, firstScope))
        const second = yield* server
          .open([tool])
          .pipe(Effect.provideService(Scope.Scope, secondScope))
        const initialize = (url: string) =>
          post(url, 1, 'initialize', {
            protocolVersion: '2025-03-26',
            capabilities: {},
            clientInfo: { name: 'test', version: '1' },
          })
        assert.strictEqual((yield* initialize(first.url)).status, 200)
        const initialized = yield* initialize(second.url)
        yield* Scope.close(firstScope, Exit.void)
        assert.strictEqual((yield* (yield* HttpClient.HttpClient).get(first.url)).status, 404)
        assert.strictEqual(
          (yield* post(second.url, 2, 'tools/list', {}, initialized.sessionId)).status,
          200,
        )
        yield* Scope.close(secondScope, Exit.void)
        assert.strictEqual((yield* (yield* HttpClient.HttpClient).get(second.url)).status, 404)
      }).pipe(Effect.provide(layer)),
    )

    it.effect(
      'lists real tool schemas, blocks tools/call, and removes sessions at scope close',
      () =>
        Effect.gen(function* () {
          const server = (yield* IntentServerFixture).server
          let url = ''
          yield* Effect.scoped(
            Effect.gen(function* () {
              const session = yield* server.open([tool])
              url = session.url
              assert.strictEqual(session.aliases.get('mcp__harness__tool_0'), 'write.document')
              const initialized = yield* post(url, 1, 'initialize', {
                protocolVersion: '2025-03-26',
                capabilities: {},
                clientInfo: { name: 'test', version: '1' },
              })
              assert.strictEqual(initialized.status, 200)
              const listed = yield* post(url, 2, 'tools/list', {}, initialized.sessionId)
              assert.strictEqual(listed.status, 200)
              const result = yield* Schema.decodeUnknownEffect(json)(listed.body)
              assert.deepStrictEqual(result.result.tools, [
                {
                  name: 'tool_0',
                  inputSchema: {
                    type: 'object',
                    properties: { value: { type: 'string' } },
                    required: ['value'],
                  },
                },
              ])
              const call = yield* Effect.forkChild(
                post(
                  url,
                  3,
                  'tools/call',
                  { name: 'tool_0', arguments: { value: 'must-never-execute' } },
                  initialized.sessionId,
                ),
              )
              yield* Effect.yieldNow
              assert.isUndefined(call.pollUnsafe())
              yield* Fiber.interrupt(call)
            }),
          )
          const client = yield* HttpClient.HttpClient
          assert.strictEqual((yield* client.get(url)).status, 404)
        }).pipe(Effect.provide(layer)),
    )
    it.effect('disabled MCP capability is a typed native failure', () =>
      IntentServer.IntentServer.use((server) => server.open([tool])).pipe(
        Effect.provide(IntentServer.layerDisabled),
        Effect.flip,
        Effect.map((error) => assert.strictEqual(error.reason._tag, 'InvalidRequestError')),
      ),
    )
  })
})
