/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * The bundled tools through the OpenAI and Codex providers, against a stand-in that refuses tool
 * schemas the way the Responses API does: a call that leaves out optional arguments, and the
 * request that continues from its result.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, Redacted } from 'effect'
import { Base64Url } from 'effect/encoding'
import { FetchHttpClient } from 'effect/http'
import { Codemode } from '../../codemode/index.ts'
import { CodingTools } from '../../coding/index.ts'
import { Credentials } from '../../credentials/index.ts'
import { ToolSearch } from '../../harness/index.ts'
import { Model } from '../../model/index.ts'
import type { Message } from '../../model/index.ts'
import { serve } from '../../model/test/fakeApi.ts'
import type { Recorded } from '../../model/test/fakeApi.ts'
import { violations } from '../../model/test/strictSchema.ts'
import { Tool } from '../../tool/index.ts'
import { OpenAi, OpenAiCodex } from '../index.ts'
import { events, toolCallEvents } from './support.ts'

const definitions = [
  CodingTools.Read,
  CodingTools.Write,
  CodingTools.Edit,
  CodingTools.Bash,
  ToolSearch.ToolSearch,
  Codemode.Codemode,
].map(Tool.definition)

interface FunctionTool {
  readonly name: string
  readonly parameters: unknown
  readonly strict?: boolean
}

interface Body {
  readonly input?: unknown
  readonly tools?: ReadonlyArray<FunctionTool>
}

const bodyOf = (request: Recorded | undefined) => (request?.body ?? {}) as Body

const toolsOf = (request: Recorded) =>
  (bodyOf(request).tools ?? []).filter((tool) => 'name' in tool)

/** The first reason the Responses API would refuse `request`'s tools, as it words it. */
const refusal = (request: Recorded) =>
  toolsOf(request)
    .filter((tool) => tool.strict === true)
    .flatMap((tool) =>
      violations(tool.parameters).map(
        (reason) => `Invalid schema for function '${tool.name}': ${reason}`,
      ),
    )[0]

/** Refuse strict tools it would refuse; otherwise call `read` with only its path, then answer. */
const respond = (request: Recorded) => {
  const refused = refusal(request)
  if (refused !== undefined) {
    return {
      status: 400,
      json: {
        error: {
          message: refused,
          type: 'invalid_request_error',
          param: 'tools',
          code: 'invalid_function_parameters',
        },
      },
    }
  }
  const input = JSON.stringify(bodyOf(request).input)
  return input.includes('function_call_output')
    ? events
    : toolCallEvents('call_1', 'read', '{"path":"notes.md"}')
}

const system: Message.Message = {
  role: 'system',
  content: 'Be brief.',
  toolsAdded: definitions,
  timestamp: 0,
}
const user: Message.Message = { role: 'user', content: 'read notes.md', timestamp: 1 }

/** Ask for a tool call through `provider`, run it, and ask again with its result. */
const roundTrip = <R>(provider: Model.Provider<R>, ref: Model.Ref) =>
  Effect.gen(function* () {
    const model = yield* Model.make([provider])
    const info = yield* model.info(ref)
    if (info === undefined) return yield* Effect.die('no model')
    const call = yield* Model.collect(
      model.stream({ model: ref, messages: [system, user], options: { sessionId: 'conv_1' } }),
      info,
    )
    const result: Message.Message = {
      role: 'toolResult',
      toolCallId: 'call_1',
      toolName: 'read',
      content: [{ type: 'text', text: 'hello from notes' }],
      isError: false,
      timestamp: 3,
    }
    const answer = yield* Model.collect(
      model.stream({
        model: ref,
        messages: [system, user, call, result],
        options: { sessionId: 'conv_1' },
      }),
      info,
    )
    return { call, answer }
  })

/** A token whose claims carry `account`. */
const jwt = (account: string) =>
  [
    Base64Url.encode(JSON.stringify({ alg: 'none' })),
    Base64Url.encode(
      JSON.stringify({ 'https://api.openai.com/auth': { chatgpt_account_id: account } }),
    ),
    'signature',
  ].join('.')

const providers = {
  OpenAi: (url: string) =>
    Effect.succeed({
      provider: OpenAi.provider({ apiKey: Redacted.make('sk-given'), apiUrl: `${url}/v1` }),
      ref: OpenAi.ref('gpt-5.5'),
    }),
  OpenAiCodex: (url: string) =>
    Effect.gen(function* () {
      const store = yield* Credentials.CredentialStore
      yield* store.set('openai-codex', {
        type: 'oauth',
        access: Redacted.make(jwt('acct_1')),
        refresh: Redacted.make('refresh_1'),
        expires: Date.now() + 3_600_000,
      })
      return {
        provider: OpenAiCodex.provider({ baseUrl: url, authUrl: url }),
        ref: OpenAiCodex.ref('gpt-5.5'),
      }
    }),
}

describe('bundled tools over the Responses API', () => {
  for (const [name, make] of Object.entries(providers)) {
    test(`${name} sends schemas it can take, and continues after a call without optional arguments`, () =>
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const api = yield* serve(respond)
            const { provider, ref } = yield* make(api.url)
            const { call, answer } = yield* roundTrip(provider, ref)

            expect(call.stopReason).toBe('toolUse')
            expect(call.content).toContainEqual({
              type: 'toolCall',
              id: 'call_1',
              name: 'read',
              arguments: { path: 'notes.md' },
            })
            expect(answer).toMatchObject({
              stopReason: 'stop',
              content: [{ type: 'text', text: 'Hello' }],
            })

            expect(api.requests).toHaveLength(2)
            for (const request of api.requests) {
              expect(refusal(request)).toBeUndefined()
              const tools = toolsOf(request)
              expect(tools.map((tool) => tool.name)).toEqual(definitions.map((tool) => tool.name))
              for (const tool of tools) {
                expect(tool.strict).toBe(false)
                expect(tool.parameters).toEqual(
                  definitions.find((found) => found.name === tool.name)?.parameters,
                )
              }
            }
            const input = JSON.stringify(bodyOf(api.requests[1]).input)
            expect(input).toContain('"type":"function_call"')
            expect(input).toContain('"arguments":"{\\"path\\":\\"notes.md\\"}"')
            expect(input).toContain('"type":"function_call_output"')
            expect(input).toContain('hello from notes')
          }),
        ).pipe(
          // oxlint-disable-next-line effecttsgo/strict-effect-provide -- each test is an entry point
          Effect.provide([FetchHttpClient.layer, Credentials.layerMemory()]),
          Effect.orDie,
        ),
      ))
  }

  test('the stand-in refuses the bundled read tool in strict mode, as the Responses API does', () => {
    const read = definitions.find((tool) => tool.name === 'read')
    expect(violations(read?.parameters)).toContain(
      'At schema path /, additionalProperties must be false for a strict schema',
    )
  })
})
