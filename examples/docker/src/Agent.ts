/**
 * The harness in a container, deployed with Alchemy as a Docker Swarm service. Each name under
 * `/agents/` is its own loop, as each Durable Object is in the Cloudflare example: its
 * conversations, accounts and credentials in `/data/<name>/harness.db`, and the coding tools
 * working in `/data/<name>/workspace`. `/data` is a Docker volume, so a crash, a restart or a
 * redeploy finds every loop where it stopped, and resumes the ones that were running.
 */
import { BunServices } from '@effect/platform-bun'
import { SqliteClient } from '@effect/sql-sqlite-bun'
import * as Docker from 'alchemy/Docker'
import { ServerHost } from 'alchemy/Server'
import { Config, Effect, Layer, Redacted, Scope } from 'effect'
import { FetchHttpClient, HttpServerRequest, HttpServerResponse } from 'effect/http'
import { KeyValueStore } from 'effect/persistence'
import { RpcSerialization, RpcServer } from 'effect/rpc'
import { Accounts } from 'effect-harness/accounts'
import { Anthropic } from 'effect-harness/anthropic'
import { CodingHandlers, CodingTools } from 'effect-harness/coding'
import { Credentials } from 'effect-harness/credentials'
import { Extension, Harness, HarnessServer } from 'effect-harness/harness'
import { OpenAi, OpenAiCodex } from 'effect-harness/openai'
import { Storage } from 'effect-harness/storage'
import { adapters, Rpcs } from './adapters.ts'

const DATA = '/data'
const NAME = /^[A-Za-z0-9_-]{1,64}$/u

/** The loop of one name, with the RPC handlers its clients call. */
const loop = (name: string) => {
  const cwd = { cwd: `${DATA}/${name}/workspace` }
  const coding = Extension.make('coding-tools', {
    toolkit: CodingTools.toolkit,
    config: { read: cwd, write: cwd, edit: cwd, bash: cwd },
  })
  const store = Layer.mergeAll(Storage.layerSql(), KeyValueStore.layerSql()).pipe(
    Layer.provide(SqliteClient.layer({ filename: `${DATA}/${name}/harness.db` })),
  )
  return Layer.mergeAll(HarnessServer.layerHandlers(), Accounts.layerHandlers).pipe(
    Layer.provide(Harness.layer({ extensions: [coding] })),
    Layer.provide(Accounts.layerModel),
    Layer.provideMerge(Accounts.layer(adapters)),
    Layer.provideMerge(Credentials.layerKeyValueStore()),
    Layer.provide(store),
    Layer.provide([
      CodingHandlers.layer,
      OpenAiCodex.layerAdapter(),
      OpenAi.layerAdapter,
      Anthropic.layerAdapter,
    ]),
    Layer.provide([FetchHttpClient.layer, BunServices.layer]),
  )
}

type Endpoint = Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  never,
  Scope.Scope | HttpServerRequest.HttpServerRequest
>

/** Build the loop of `name` for the rest of the process's life, and its WebSocket endpoint. */
const build = (name: string): Effect.Effect<Endpoint> =>
  Effect.gen(function* () {
    yield* Effect.promise(() => Bun.$`mkdir -p ${DATA}/${name}/workspace`.quiet())
    const scope = yield* Scope.make()
    const context = yield* Layer.buildWithScope(loop(name), scope)
    return yield* RpcServer.toHttpEffectWebsocket(Rpcs).pipe(
      Effect.provide(Layer.mergeAll(Layer.succeedContext(context), RpcSerialization.layerJson)),
      Scope.provide(scope),
    )
  }).pipe(Effect.orDie)

/** Each name's endpoint, built once, on first use. */
const makeLoops = () => {
  const built = new Map<string, Effect.Effect<Endpoint>>()
  return (name: string) => {
    const existing = built.get(name)
    if (existing !== undefined) return existing
    const once = Effect.runSync(Effect.cached(build(name)))
    built.set(name, once)
    return once
  }
}

/** Compare without stopping at the first difference, so timing does not reveal the token. */
const same = (given: string, expected: string) => {
  let difference = given.length ^ expected.length
  for (let i = 0; i < expected.length; i++)
    difference |= (given.codePointAt(i) ?? 0) ^ (expected.codePointAt(i) ?? 0)
  return difference === 0
}

export const Swarm = Docker.Swarm('swarm')
export const Data = Docker.Volume('data')

export default class Agent extends Docker.Service<Agent>()(
  'Agent',
  Effect.gen(function* () {
    const swarm = yield* Swarm
    const data = yield* Data
    return {
      context: swarm,
      main: import.meta.url,
      image: 'oven/bun:1.4.2',
      port: 3000,
      // Host mode publishes on the node itself, without the routing mesh.
      ports: [{ external: 3000, internal: 3000, mode: 'host' as const }],
      volumes: [{ hostPath: data.name, containerPath: DATA }],
    }
  }),
  Effect.gen(function* () {
    const host = yield* ServerHost
    const token = yield* Config.Redacted('HARNESS_TOKEN')
    const loops = makeLoops()
    // Open every loop the volume holds at start, so the ones a crash interrupted resume now.
    yield* host.run(
      Effect.gen(function* () {
        const names = yield* Effect.promise(() =>
          Bun.$`ls ${DATA}`
            .nothrow()
            .quiet()
            .then((out) => out.text().split('\n')),
        )
        yield* Effect.forEach(
          names.filter((name) => NAME.test(name)),
          (name) => Effect.asVoid(loops(name)),
          { discard: true },
        )
      }),
    )
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest
        const url = new URL(request.url, 'http://agent')
        const name = url.pathname.startsWith('/agents/')
          ? url.pathname.slice('/agents/'.length)
          : ''
        if (!NAME.test(name)) return HttpServerResponse.empty({ status: 404 })
        const header = request.headers['authorization']
        const given = header?.startsWith('Bearer ')
          ? header.slice('Bearer '.length)
          : url.searchParams.get('token')
        if (given === null || !same(given, Redacted.value(token)))
          return HttpServerResponse.empty({ status: 401 })
        // Plain requests only learn that the name exists; the loop speaks over a WebSocket.
        if (request.headers['upgrade']?.toLowerCase() !== 'websocket')
          return HttpServerResponse.empty({ status: 200 })
        return yield* Effect.flatten(loops(name))
      }).pipe(Effect.scoped),
    }
  }),
) {}
