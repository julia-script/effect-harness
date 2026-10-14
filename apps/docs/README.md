# Effect Harness documentation app

[Live documentation](https://effect-harness-docs-docs-prod-xywgv64k67qqnujx.juliaortiz-js.workers.dev).

Foldkit + Tailwind documentation, deployed as Cloudflare Worker assets through Alchemy. The audience and page map are in [CONTENT_PLAN.md](CONTENT_PLAN.md).

From the repository root:

```sh
bun install
bun run build
bun run --cwd apps/docs dev
```

The dev server listens on http://localhost:5173. `bun run --cwd apps/docs preview` serves the production build. Foldkit preserves UI state during development reloads; use the production preview to verify fresh direct navigation.

## Content

Markdown lives in `content/`. `src/Content.ts` assigns page paths and embeds checked TypeScript from `src/examples/` and `apps/example/src/`. Composition, recovery, and provider material reuse the repository's existing documentation.

Each page has one primary Diátaxis purpose. The lab is supplementary interactive learning material. Run the tutorial against its pinned published packages when changing it; keep expected output in sync.

## Controlled model lab

`src/Lab.ts` runs the actual local harness with an offline `LanguageModel` and an in-memory store. `Deferred` gates hold each tool intent, tool result, and text delta until autoplay or the reader advances. Commands own operations; a Foldkit Subscription polls committed snapshots. The chat autoplays one gate per tick by default. “Under the hood” reveals a three-step explanation, playback controls, and runtime recovery; a further disclosure contains raw data, retries, and forks. Follow-up questions about a fix receive a scripted code example. Chat history is decoded from stored entries, with a separate live bubble while the answer streams.

Closing the runtime Scope interrupts work without discarding the acquired storage. Reopening reacquires conversation and submission handles. Reset acquires a fresh store. Refreshing the page clears everything. No credentials, paid inference, or backend service are involved in these demos.

## Verification

```sh
bun run check
bun run --cwd apps/docs test
bun run --cwd apps/example portable
bun run --cwd apps/example tour
```

The docs integration tests cover provider gating, admission reuse, fork isolation, safe-tool replay, completed-tool recovery, abort/reset cleanup, and composition of the tool/observation guide modules.

## Deploy

The stack is `effect-harness-docs`, the resource is `Docs`, and the production stage is `prod`. Alchemy's `Cloudflare.Website.Foldkit` builds the Vite app and serves assets with SPA deep-link fallback. Its Cloudflare state backend owns the deployment state.

```sh
bun run --cwd apps/docs alchemy profile show
bun run --cwd apps/docs deploy
```

If no profile is connected, configure one with `bun run --cwd apps/docs alchemy profile edit --add Cloudflare`. CI can use `CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_API_TOKEN`; never put credentials in `VITE_` variables. `.alchemy` and `.wrangler` artifacts are ignored.

See the [Alchemy Foldkit documentation](https://alchemy.run/cloudflare/frontend/foldkit) for the deployment resource. Foldkit 0.168.0 declares Effect 4.0.0 peers; this workspace resolves Effect and platform-browser 4.0.1, validated by the docs tests and production browser checks.
