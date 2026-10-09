# Conversation and task examples

This tour adapts the examples in [Pi Durable](https://earendil.com/posts/pi-durable/) to the embedded Effect harness. Each program uses the public package imports, runs offline, and checks the behavior it demonstrates. No model credentials are needed. Payments, deployments, issue searches and approval decisions use local simulations.

From the repository root:

```sh
bun install
bun run build
bun run --cwd apps/example tour -- all
```

Run an individual example by name:

```sh
bun run --cwd apps/example tour -- recovery
bun run --cwd apps/example tour -- documents
bun run --cwd apps/example tour -- --help
```

Each successful example prints its name and a JSON result. Failed assertions fail the command. Generated databases and workspace files live in scoped temporary directories and are removed when the example exits.

## Pick an example

| Command       | Source                                    | What it checks                                                                                                                                    |
| ------------- | ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `quickstart`  | [Quickstart.ts](src/tour/Quickstart.ts)   | SQLite persistence, a coding tool reading a real workspace file, and stable request identity returning the same submission.                       |
| `recovery`    | [Recovery.ts](src/tour/Recovery.ts)       | Open SQLite, resume interrupted work, and submit with a stable request identity. Run the file with `start` and `resume` to try a process restart. |
| `forks`       | [Forks.ts](src/tour/Forks.ts)             | Create a thread at a saved answer and run both conversations concurrently with their inherited history.                                           |
| `extensions`  | [Extensions.ts](src/tour/Extensions.ts)   | Selected tools, safe and unsafe replay policies, managed context sections, tool timing, registry replacement, and a saved approval decision.      |
| `subagent`    | [Subagent.ts](src/tour/Subagent.ts)       | A tool owns a triage conversation; save its identity atomically, close and reopen, and reuse its settled request.                                 |
| `checkout`    | [Checkout.ts](src/tour/Checkout.ts)       | Join parallel payment tasks, fail fast on a decline, and compensate charges with idempotency keys.                                                |
| `reminder`    | [Reminder.ts](src/tour/Reminder.ts)       | Save an absolute deadline, reopen without restarting the delay, and explicitly cancel background work.                                            |
| `context`     | [Context.ts](src/tour/Context.ts)         | Compact model context, reset it, and still search the saved transcript.                                                                           |
| `documents`   | [Documents.ts](src/tour/Documents.ts)     | Commit typed Todo and progress documents with a transcript entry, render Todos into context, and fork their earlier value.                        |
| `multiplayer` | [Multiplayer.ts](src/tour/Multiplayer.ts) | Two local observers see committed changes; a late observer sees active work and submits steering input.                                           |

## Read and reuse the code

[Runtime.ts](src/tour/Runtime.ts) supplies the shared scoped harness setup and a deterministic native Effect AI model. Each scenario exports a `run` effect and a `Result` schema. Tasks declare input, checkpoint and result schemas; documents declare their value schemas. The observation examples encode and decode `Observation.Change`, which an application can reuse at a transport boundary.

The examples intentionally have different recovery boundaries. Run `Recovery.ts` with a SQLite filename and `start`, send `SIGKILL` after it prints "Paused", then run that same file and filename with `resume`. The automated process-kill driver lives in [test/fixtures/Recovery.ts](test/fixtures/Recovery.ts); the tour command runs the completion path in memory. `extensions`, `subagent` and `reminder` close and reopen a harness over the same in-memory persistence service inside one process. Those examples exercise scheduler recovery; their memory store does not survive process termination.

Safe replay can execute a tool again after interruption. The recovery test checks that resuming replays the safe tool, saves one tool result, and returns the cached submission on another reopen. An unsafe deployment uses a simulated action and a saved approval memo; approval hooks reuse the committed decision after reopening.

In checkout, an aborted live payment runs its refund handler. A payment that already completed remains terminal, so the checkout task explicitly compensates those charges too. The fake bank deduplicates charge and refund keys; a real integration needs equivalent behavior from its external service.

Both multiplayer clients call the same local harness. To expose it remotely, an application would provide its own API or transport around these operations and schema-encoded observations.

## Verify

```sh
bun run check
bun run build
bun run test
```

The tour tests live in [test/](test/), alongside the original embedded application tests. The recovery test launches and kills actual Bun subprocesses.
