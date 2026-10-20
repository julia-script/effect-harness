# Docker example

The harness in a container, deployed with Alchemy as a Docker Swarm service. It serves the same
routes and RPCs as the [Cloudflare example](../cloudflare/README.md), so the same client talks to
either: each name under `/agents/` is its own loop, with its conversations, accounts and
credentials in `/data/<name>/harness.db`. Unlike a Worker, a container has a file system and
processes, so each loop also gets the coding tools (`read`, `write`, `edit`, `bash`), working in
`/data/<name>/workspace`.

```
chat.ts ──ws /agents/<name>?token──▶ Agent (Docker service, Bun)
                                     checks token, one loop per name
                                     /data (Docker volume): harness.db + workspace per name
```

- `src/adapters.ts`: the providers people can add accounts of, and the RPCs each loop serves.
- `src/Agent.ts`: the service: one loop per name over SQLite, opened again at start so a run a
  crash interrupted resumes without waiting for a client.
- `alchemy.run.ts`: the stack: a single-node swarm, the volume, and the service.
- `agent.test.ts`: coding sessions against the deployed service, with a simulated model.

## Run it

You need a Docker engine. From the repo root:

```bash
bun install
bun run setup
bun run build
HARNESS_TOKEN=dev-token bunx alchemy deploy examples/docker/alchemy.run.ts   # serves http://localhost:3000
```

Alchemy turns the engine into a single-node swarm, builds an image from `src/Agent.ts` (bundled,
on `oven/bun`), and starts the service. Then use the Cloudflare example's client:

```bash
export HARNESS_URL=http://localhost:3000 HARNESS_TOKEN=dev-token
OPENAI_API_KEY=… bun examples/cloudflare/chat.ts openai
bun examples/cloudflare/chat.ts use openai gpt-5.5
bun examples/cloudflare/chat.ts "Write a fizzbuzz in fizz.ts and run it"
```

The service publishes its port in host mode, on the node itself, rather than through the swarm's
routing mesh: the mesh needs the kernel's IPVS module, which some machines, CI runners and
sandboxes lack. Redeploys replace the container; the volume, and every loop in it, stays.
`bunx alchemy destroy examples/docker/alchemy.run.ts` removes the service, the volume and the
swarm.

## Test it

With the stack deployed:

```bash
HARNESS_TOKEN=dev-token bun test examples/docker
```

The tests run multi-turn coding sessions with a simulated model on this machine, which the
container reaches through Docker's bridge (`MODEL_HOST`, default `172.17.0.1`), and kill the
container in the middle of a command to see the run resume once the swarm has started it again.

## Elsewhere

The service is an Alchemy effectful platform: `main: import.meta.url` and an Effect returning
`{ fetch }`. Point `context` at a remote Docker context to run it on another machine. Other
Alchemy hosts take the same shape (`AWS.ECS.Service`, `Fly`, `GCP` Cloud Run, `Railway`,
`Hetzner`), but this example has only been run on Docker.

A host like this runs one process, so it serves each loop from one place. Don't run more than
one replica over the same volume: a loop must be the only writer of its store.
