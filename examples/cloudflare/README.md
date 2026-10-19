# Cloudflare example

The harness in a Durable Object, deployed with Alchemy. Each name under `/agents/` is its own
loop, with its conversations, accounts and credentials in that object's SQLite. People add
accounts at run time: Codex through a ChatGPT sign-in, or an OpenAI or Anthropic key.

```
chat.ts ──wss /agents/<name>?token──▶ Api (Worker) ──▶ Agent (Durable Object, one per name)
                                      checks token     HarnessObject.layer: loop + accounts + SQLite
```

- `src/adapters.ts`: the providers people can add accounts of.
- `src/Agent.ts`: the Durable Object, `HarnessObject.layer({ adapters })` plus their implementations.
- `src/Api.ts`: the Worker, `HarnessObject.route(agents, { token })`.
- `alchemy.run.ts`: the stack.
- `chat.ts`: a command-line client.

## Run it locally

`alchemy dev` runs the stack in a local workerd, with no Cloudflare account. From the repo root:

```bash
bun install
bun run build
HARNESS_TOKEN=dev-token bun run example:cloudflare   # serves http://localhost:1337
```

Then, in another shell:

```bash
export HARNESS_TOKEN=dev-token
bun examples/cloudflare/chat.ts codex              # prints a URL and a code to enter
bun examples/cloudflare/chat.ts models             # the account's models
bun examples/cloudflare/chat.ts use codex 'MODEL_ID' # replace MODEL_ID with an ID from models
bun examples/cloudflare/chat.ts "Hi there"         # prints the answer
```

With a key instead of a sign-in, `OPENAI_API_KEY=… bun examples/cloudflare/chat.ts openai` adds
an `openai` account (`OPENAI_BASE_URL` points it at a server supporting the OpenAI Responses API), and
`ANTHROPIC_API_KEY=… bun examples/cloudflare/chat.ts anthropic` adds an `anthropic` one.
`OPENROUTER_API_KEY=… bun examples/cloudflare/chat.ts openrouter` adds an `openrouter` account,
an OpenAI one at `https://openrouter.ai/api/v1`; then `chat.ts use openrouter <model-id>` selects
a model from that account's model list.

`HARNESS_NAME` picks the object (default `julia`): another name is another harness, with its own
accounts and conversations. Objects keep their SQLite between `alchemy dev` runs.

The example imports the root package's built exports. Run `bun run build` again after changing
library code in `src/`.

## Deploy it

```bash
HARNESS_TOKEN=<a long random string> bunx alchemy deploy examples/cloudflare/alchemy.run.ts
```

Alchemy uses your Cloudflare profile (`bunx alchemy profile edit` to set one up) and prints the
Worker's URL. Point the client at it with `HARNESS_URL=https://…workers.dev`. The stack keeps its
state in `.alchemy/`, which is ignored by git; `bunx alchemy destroy examples/cloudflare/alchemy.run.ts`
removes what it deployed.

The coding tools and code mode don't run in a Worker, which has no file system or processes.
