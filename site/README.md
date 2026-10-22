# Docs site

The documentation site for effect-harness: a [Foldkit](https://foldkit.dev) app styled with Tailwind, deployed to Cloudflare with [Alchemy](https://alchemy.run).

The guides are the files in [`docs/`](../docs), rendered at build time with their code highlighted by Shiki and their Mermaid diagrams drawn in the page. Edit a guide there and the site follows.

The demos on the home page and in the playground run the real loop in the browser: `HarnessServer` and `HarnessClient` over in-memory storage, with a scripted Faux model in place of a provider. Crashing a demo closes the loop's scope; restarting it builds a new loop over the same storage.

## Develop

```sh
bun run site:dev     # http://localhost:5173
bun run site:build   # site/dist
```

The site imports the library from `src/` directly, so changes there show up without a build.

| Path             | What it holds                                                              |
| ---------------- | -------------------------------------------------------------------------- |
| `src/main.ts`    | The application: model, update, view, routing                              |
| `src/page/`      | One view per page                                                          |
| `src/demo/`      | The demos: the loop in a scope (`lab.ts`), the transcript fold, the agents |
| `src/markdown/`  | How a guide renders                                                        |
| `plugins/`       | Vite plugins: GitHub markdown to Foldkit markdown, Shiki highlighting      |
| `alchemy.run.ts` | The Cloudflare stack                                                       |

## Deploy

```sh
bun run site:deploy
```

The first deploy signs you in to Cloudflare and offers to set up Alchemy's state store in your account, so later deploys, from a laptop or from CI, share the same state.

Pushes to `main` that touch the site, the guides or the library deploy the `production` stage through [`.github/workflows/docs.yml`](../.github/workflows/docs.yml). It reads two repository settings:

- `CLOUDFLARE_ACCOUNT_ID`, a repository variable: the account to deploy to.
- `CLOUDFLARE_API_TOKEN`, a repository secret: an API token for that account with **Workers Scripts: Edit** and **Secrets Store: Edit** (the state store keeps its token in Secrets Store).
