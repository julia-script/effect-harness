# Effect Harness

Embedded durable AI conversations, built with Effect.

An application opens a scoped `Harness` with a model catalogue, a tool registry and a `Persistence` Layer. Conversations, task checkpoints, tool results and typed documents share atomic commits. One harness process owns each store; clients call that process's conversation APIs.

- **Native Effect AI.** Models use `LanguageModel`, `Prompt`, `Tool` and `Toolkit`.
- **Checkpoint tasks.** Named, versioned task definitions run phases outside transactions and save their next checkpoint or outcome.
- **Atomic application state.** A task can commit its output, document edits and terminal state together.
- **Explicit tool recovery.** Replay policies distinguish safe repeatable work from interrupted external actions.
- **Committed observations.** Watches start with a snapshot and stream subsequent commits.

## Runtime support

The core uses standard JavaScript, Web APIs and Effect services. It has no Bun or Node runtime dependency and can run in Node, Bun, Deno or a browser. Persistence, model transport and tool capabilities come from your application; choose adapters that your host supports. Bun is the repository's development tool.

Runtime-specific adapters are optional leaf imports: `NodeEnv`, `NodeNativeFiles`, `storage/SqliteNode`, `storage/JsonlNode`, `storage/SqliteBun` and `storage/JsonlBun`. Importing the package root, portable tools or `storage/Memory` does not load them.

The [portable example](apps/example/src/Portable.ts) runs eight conversation and task scenarios without platform services. After building, run it under Node:

```sh
node apps/example/dist/PortableMain.js
```

See [the runtime commands](apps/example/README.md#portable-runtime-example) for Bun, Deno and browser bundling.

## Run the example

```sh
bun install
bun run build
bun run --cwd apps/example test
```

The offline example calls an uppercase tool, returns `HELLO`, and runs a custom greeting task. It requires no API key. [Run your first conversation](docs/tutorials/first-conversation.md) explains the application.

The [conversation and task tour](apps/example/TOUR.md) provides ten more runnable examples, including process recovery, parallel forks, subagents, compensating tasks and typed documents:

```sh
bun run --cwd apps/example tour -- all
```

## Examples

These programs expect a configured `Harness.Harness` service from your application's Layer. The [example application](apps/example/src/Application.ts) composes SQLite, a native Effect AI model and a tool registry; [main.ts](apps/example/src/main.ts) supplies the Bun platform and runs the program.

### Submit and resume a conversation

Resume saved work when the host environment is ready, then submit a message. Reusing the request ID in this conversation returns the saved submission. `Submission.wait` returns its settled state; an `InputDone` result identifies the committed answer entry.

```ts
import * as Effect from 'effect/Effect'
import * as Harness from 'effect-harness/Harness'
import * as Conversation from 'effect-harness/Conversation'
import * as Submission from 'effect-harness/Submission'
import * as Identity from 'effect-harness/Identity'

export const program = Effect.gen(function* () {
  const harness = yield* Harness.Harness
  yield* harness.resume
  const conversation = yield* harness.root
  const submission = yield* Conversation.submit(conversation, 'Read notes.txt', {
    requestId: Identity.RequestId.make('read-notes-v1'),
  })
  return yield* Submission.wait(submission)
})
```

The [quickstart](apps/example/src/tour/Quickstart.ts) reads a real workspace file. The [recovery example](apps/example/src/tour/Recovery.ts) kills a process and resumes the same SQLite-backed submission in a fresh process.

### Fork at an answer and explore in parallel

A fork inherits the saved history through the selected entry. The parent and fork can then accept independent requests.

```ts
import * as Effect from 'effect/Effect'
import * as Harness from 'effect-harness/Harness'
import * as Conversation from 'effect-harness/Conversation'
import * as Submission from 'effect-harness/Submission'

export const program = Effect.gen(function* () {
  const harness = yield* Harness.Harness
  const channel = yield* harness.root
  const question = yield* Conversation.submit(channel, 'Why did the deployment fail?')
  const answer = yield* Submission.wait(question)
  if (answer._tag !== 'InputDone') return answer

  const thread = yield* Conversation.fork(channel, answer.answer)
  yield* Conversation.configure(thread, { instructions: 'Review rollback options.' })

  return yield* Effect.all(
    [
      Conversation.submit(thread, 'Can we roll it back?').pipe(Effect.flatMap(Submission.wait)),
      Conversation.submit(channel, 'Who is on call?').pipe(Effect.flatMap(Submission.wait)),
    ],
    { concurrency: 'unbounded' },
  )
})
```

See the [parallel forks example](apps/example/src/tour/Forks.ts) for a runnable version that checks concurrent model requests and inherited history.

### Commit typed document state with the transcript

A schema defines the document's value. A transaction can edit its draft and append an entry atomically. With `rewindable` history and `asOf` forks, a historical fork receives the document value at its cutoff.

```ts
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Harness from 'effect-harness/Harness'
import * as Conversation from 'effect-harness/Conversation'
import * as Document from 'effect-harness/Document'

const TodoData = Schema.Struct({
  items: Schema.Array(Schema.Struct({ text: Schema.String, done: Schema.Boolean })),
})
const TodoAdded = Schema.Struct({ text: Schema.String })
const Todos = Document.defineUnsafe({
  kind: 'app.todos',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  schema: TodoData,
  initial: () => ({ items: [] }),
})

export const program = Effect.gen(function* () {
  const harness = yield* Harness.Harness
  const conversation = yield* harness.root
  yield* harness.transaction(
    Effect.fn(function* (tx) {
      const todos = yield* tx.doc(Todos, { owner: conversation.id })
      todos.items.push({ text: 'Add a smoke test', done: false })
      yield* tx.appendEntry(conversation.id, {
        kind: 'app.todo-added',
        data: yield* Schema.encodeEffect(TodoAdded)({ text: 'Add a smoke test' }),
      })
    }),
  )
  return yield* Conversation.document(conversation, Todos)
})
```

The [documents example](apps/example/src/tour/Documents.ts) also renders saved Todos into model context and watches the atomic commit. For custom checkpoint tasks, see [checkout](apps/example/src/tour/Checkout.ts) and [reminders](apps/example/src/tour/Reminder.ts).

## Choose your imports

```ts
import * as Harness from 'effect-harness/Harness'
import * as Conversation from 'effect-harness/Conversation'
import * as Submission from 'effect-harness/Submission'
import * as SqliteBun from 'effect-harness/storage/SqliteBun'
```

The package root exposes concept namespaces. Leaf imports select individual modules. Storage and provider adapters have dedicated subpaths:

| Import                                            | Purpose                                                    |
| ------------------------------------------------- | ---------------------------------------------------------- |
| `effect-harness`                                  | Harness, conversations, documents, tasks, models and tools |
| `effect-harness/storage/Memory`                   | Process-local persistence                                  |
| `effect-harness/storage/SqliteBun` / `SqliteNode` | Indexed persistent records                                 |
| `effect-harness/storage/JsonlBun` / `JsonlNode`   | Framed JSONL commits                                       |
| `effect-harness/provider-openai`                  | Native API-key OpenAI models and catalogues                |
| `effect-harness/provider-anthropic`               | Native API-key Anthropic models and catalogues             |
| `effect-harness/tools`                            | Portable coding tools                                      |

Your application supplies platform services and the execution environment. Recovery requires that the store and the resources used by tools remain available. See [recovery](docs/explanation/recovery.md).

## Build an application

[Connect a provider](docs/providers.md), [register tools](docs/tools.md), [declare tasks](docs/tasks.md), [persist state](docs/persistence.md), or [observe committed changes](docs/observations.md). The [documentation index](docs/README.md) links the contracts and examples.

External clients use an application-owned API or transport around the embedded harness. Storage ownership and tool scheduling remain inside the harness process.

## Develop

```sh
bun run check
bun run test
bun run build
```

`check` verifies formatting, lint, source and test types, and public type assertions. [The repository example](apps/example/README.md) exercises the published package exports with SQLite. Package changes use [Changesets](.changeset/README.md).

## Thanks

This project adapts [Pi Durable](https://github.com/earendil-works/pi)'s embedded conversation and task design using Effect services and scoped fibers. Upstream attribution is retained in [NOTICE](packages/effect-harness/NOTICE).
