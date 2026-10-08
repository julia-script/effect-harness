# Run your first durable conversation

The repository example supplies a deterministic native model and an uppercase tool. You need Bun and a checkout of this repository.

## Build and run

From the repository root:

```sh
bun install
bun run build
bun apps/example/dist/main.js
```

The application prints:

```text
Hello, Effect
HELLO
embedded-harness-example-ok
```

The first line comes from a custom checkpoint task. The second comes from a conversation: the model requests the uppercase tool, the harness commits its result, then the model returns the answer.

## Follow the conversation

Open [main.ts](../../apps/example/src/main.ts). The program obtains `Harness.Harness`, selects its root conversation and calls `Conversation.submit`. `Submission.wait` waits for the saved submission to settle. `Conversation.snapshot` returns committed entries, tasks, submissions and documents.

[Application.ts](../../apps/example/src/Application.ts) supplies the Layers. [DemoModel.ts](../../apps/example/src/DemoModel.ts) implements native Effect AI calls, and [Uppercase.ts](../../apps/example/src/Uppercase.ts) binds the Toolkit handler through the registry.

The request identity `uppercase-v1` prevents duplicate admission when the same conversation receives the request again. It identifies the submission within its store.

## Run the integration checks

```sh
bun run --cwd apps/example test
```

The test reopens the database and checks that model and tool call counts stay unchanged. Continue with [persistent state](persistent-state.md) to retain the example database between separate process runs.
