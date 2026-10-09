# Examples

These programs exercise the public package imports using an offline native Effect AI model. No API key is needed.

```sh
bun install
bun run build
bun run --cwd apps/example start
bun run --cwd apps/example tour
bun run --cwd apps/example portable
```

The development commands use Bun; the built example entrypoints run with Node. The library's core and portable example also bundle for a browser without host services.

- [main.ts](src/main.ts): submit input through Harness and wait for the settled record.
- [Application.ts](src/Application.ts): compose tool handlers and a model, leaving Storage to the application.
- [Uppercase.ts](src/Uppercase.ts): schema declarations separated from handler Layers, with durable output.
- [DemoModel.ts](src/DemoModel.ts): an offline native LanguageModel returning tool intents.
- [Tour](TOUR.md): forks, documents, extensions, and recovery.
- [Portable.ts](src/Portable.ts): the same client/tool/runtime APIs with in-memory storage, including reopening the same store.

## Recovery

```sh
bun run --cwd apps/example recovery
```

[Recovery.ts](src/tour/Recovery.ts) opens `./agent.sqlite` in the command's working directory. Run it again to retrieve the same `job-42` submission. If the process was interrupted, the new runtime resumes its checkpoint. The offline uppercase handler finishes quickly; the integration tests kill a worker while a tool is blocked to verify recovery during execution.

## Portable runtime example

After building:

```sh
node apps/example/dist/PortableMain.js
bun apps/example/dist/PortableMain.js
deno run --allow-read apps/example/dist/PortableMain.js
```

The portability tests also bundle this entrypoint with `bun build --target=browser` and evaluate it without Node, Bun, or Deno globals. SQL and JSONL applications choose compatible SqlClient/FileSystem Layers at their application edge.
