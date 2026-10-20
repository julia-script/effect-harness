/**
 * A command-line client for the example, over one WebSocket to the object named `HARNESS_NAME`
 * (default `julia`) at `HARNESS_URL` (default `http://localhost:1337`), with `HARNESS_TOKEN`.
 *
 *   bun examples/cloudflare/chat.ts codex                 # sign in with ChatGPT
 *   bun examples/cloudflare/chat.ts openai                # add OPENAI_API_KEY (OPENAI_BASE_URL optional)
 *   bun examples/cloudflare/chat.ts anthropic             # add ANTHROPIC_API_KEY
 *   bun examples/cloudflare/chat.ts openrouter            # add OPENROUTER_API_KEY
 *   bun examples/cloudflare/chat.ts models                # every account's models
 *   bun examples/cloudflare/chat.ts use codex gpt-5.5     # answer with that account's model
 *   bun examples/cloudflare/chat.ts "Hi there"            # send a message, print the answer
 */
import { BunRuntime } from '@effect/platform-bun'
import { Config, Console, Data, Effect, Layer, Option, Redacted, Stream } from 'effect'
import { RpcClient } from 'effect/rpc'
import { AccountsRpcs } from 'effect-harness/accounts'
import { HarnessClient, HarnessTransport } from 'effect-harness/client'

const Settings = Config.all({
  url: Config.String('HARNESS_URL').pipe(Config.withDefault('http://localhost:1337')),
  name: Config.String('HARNESS_NAME').pipe(Config.withDefault('julia')),
  token: Config.Redacted('HARNESS_TOKEN'),
})

class ChatError extends Data.TaggedError('ChatError')<{ readonly message: string }> {}

/** The object's clients, while the scope is open. */
const connect = Effect.gen(function* () {
  const { url, name, token } = yield* Settings
  const object = `${url}/agents/${name}?token=${Redacted.value(token)}`
  // The WebSocket retries a refused connection forever: ask once first, to fail with the reason.
  const reached = yield* Effect.tryPromise({
    try: () => fetch(object),
    catch: () =>
      new ChatError({
        message: `Nothing answers at ${url}: start it with \`bun run example:cloudflare\`, or set HARNESS_URL`,
      }),
  })
  if (reached.status === 401)
    return yield* new ChatError({ message: 'HARNESS_TOKEN is not the token' })
  if (reached.status === 404)
    return yield* new ChatError({ message: `No object at ${url}/agents/${name}` })
  const ws = object.replace(/^http/u, 'ws')
  const transport = yield* Layer.build(HarnessTransport.layerWebSocket(ws))
  const harness = yield* Effect.provideContext(HarnessClient.make, transport)
  const accounts = yield* Effect.provideContext(
    RpcClient.make(AccountsRpcs.AccountsRpcs),
    transport,
  )
  return { harness, accounts }
})

type Clients = Effect.Success<typeof connect>

const show = (account: {
  readonly name: string
  readonly status: string
  readonly error?: string
}) =>
  Console.log(
    `${account.name}: ${account.status}${account.error === undefined ? '' : ` (${account.error})`}`,
  )

const signIn = ({ accounts }: Clients) =>
  accounts
    .SignIn({ name: 'codex', adapter: 'openai-codex', label: 'ChatGPT' })
    .pipe(
      Stream.runForEach((event) =>
        event.type === 'code'
          ? Console.log(`Open ${event.url} and enter ${event.code}`)
          : show(event.account),
      ),
    )

/** The keyed accounts this client adds: the adapter, and where its key and URL come from. */
const keyed = {
  openai: { adapter: 'openai', env: 'OPENAI', apiUrl: undefined },
  anthropic: { adapter: 'anthropic', env: 'ANTHROPIC', apiUrl: undefined },
  // OpenRouter speaks the OpenAI Responses API, so it is an OpenAI account at another URL.
  openrouter: { adapter: 'openai', env: 'OPENROUTER', apiUrl: 'https://openrouter.ai/api/v1' },
} as const

const addKey = ({ accounts }: Clients, name: keyof typeof keyed) =>
  Effect.gen(function* () {
    const { adapter, env, apiUrl } = keyed[name]
    const apiKey = yield* Config.Redacted(`${env}_API_KEY`)
    const url = yield* Config.option(Config.String(`${env}_BASE_URL`))
    const settings = Option.getOrUndefined(url) ?? apiUrl
    const added = yield* accounts.Add({
      adapter,
      name,
      label: name,
      settings: settings === undefined ? {} : { apiUrl: settings },
      apiKey,
    })
    yield* show(added)
  })

const models = ({ accounts }: Clients) =>
  Effect.gen(function* () {
    for (const group of yield* accounts.Models()) {
      yield* show(group.account)
      for (const model of group.models) yield* Console.log(`  ${model.id}`)
    }
  })

const use = ({ harness }: Clients, provider: string, modelId: string) =>
  Effect.flatMap(harness.root(), (root) => root.configure({ model: { provider, modelId } }))

/** Send `content` to the root conversation and print the answer's text. */
const send = ({ harness }: Clients, content: string) =>
  Effect.gen(function* () {
    const root = yield* harness.root()
    const settled = yield* Effect.flatMap(root.submit({ type: 'input', content }), (s) => s.wait)
    if (settled.status === 'unanswered') return yield* Console.error(`No answer: ${settled.reason}`)
    const page = yield* root.entries({ order: 'descending' }, 10)
    const answer = page.items.find((entry) => entry.kind === '$assistant')
    const text: unknown = answer?.model?.[0]?.['content']
    const blocks: ReadonlyArray<{ readonly text?: string }> = Array.isArray(text) ? text : []
    return yield* Console.log(blocks.map((block) => block.text ?? '').join(''))
  })

const main = Effect.gen(function* () {
  const [command = 'models', ...rest] = process.argv.slice(2)
  const clients = yield* connect
  switch (command) {
    case 'codex':
      return yield* signIn(clients)
    case 'openai':
    case 'anthropic':
    case 'openrouter':
      return yield* addKey(clients, command)
    case 'models':
      return yield* models(clients)
    case 'use':
      return yield* use(clients, rest[0] ?? '', rest[1] ?? '')
    default:
      return yield* send(clients, [command, ...rest].join(' '))
  }
})

BunRuntime.runMain(Effect.scoped(main))
