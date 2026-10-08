# Save application state across restarts

We will save a counter in a typed Session document, stop the process and reopen it. Repeating the same transaction key will leave the counter at `1`; a new key will increment it to `2`.

Start in an empty directory with Node.js and a package manager of your choice. This lesson assumes Effect knowledge. This lesson uses a single-writer JSONL Store and does not run a model or WorkflowEngine.

The shell commands use npm syntax. You can use the equivalent commands from your preferred package manager; execution uses Node.js and `tsx`.

## 1. Install the packages

```sh
mkdir persistent-state
cd persistent-state
npm init -y
npm pkg set type=module
npm install effect @effect/platform-node effect-harness
npm install --save-dev typescript tsx @types/node
```

Create `tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ESNext",
    "module": "Preserve",
    "moduleResolution": "Bundler",
    "strict": true,
    "noEmit": true,
    "allowImportingTsExtensions": true,
    "skipLibCheck": true,
    "types": ["node"]
  },
  "include": ["*.ts"]
}
```

## 2. Define the document and transaction

Create `counter.ts`:

```ts
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as Document from 'effect-harness/durable/Document'
import * as Session from 'effect-harness/durable/Session'
import * as JsonlStore from 'effect-harness/durable/storage/JsonlStore'
import * as Config from 'effect/Config'
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'

const Counter = Document.defineUnsafe({
  kind: 'example.counter',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ count: Schema.Number }),
  initial: () => ({ count: 0 }),
})

const SessionLive = Session.layer.pipe(
  Layer.provide(JsonlStore.layer({ directory: './data', fsync: true })),
  Layer.provide(NodeServices.layer),
)

const program = Effect.gen(function* () {
  const key = yield* Config.String('COUNTER_REQUEST').pipe(Config.withDefault('increment-v1'))
  const session = yield* Session.Session
  const result = yield* session.transaction(
    Effect.fnUntraced(function* (tx) {
      const counter = yield* tx.doc(Counter)
      counter.count += 1
      return counter.count
    }),
    { key },
  )
  const snapshot = yield* session.snapshot(Counter)
  const count = Option.map(snapshot, (saved) => saved.value.count)
  yield* Console.log(`Transaction result: ${result}`)
  yield* Console.log(`Saved count: ${Option.getOrElse(count, () => 0)}`)
})

await Effect.runPromise(program.pipe(Effect.provide(SessionLive)))
```

The document token fixes its schema and session address. `tx.doc` acquires a mutable draft inside the transaction; `snapshot` reads the committed value afterward.

## 3. Reopen the saved state

```sh
npm exec tsc
node --import tsx counter.ts
node --import tsx counter.ts
```

Both processes should print:

```text
Transaction result: 1
Saved count: 1
```

The second process loads `data/commits.jsonl`. Its transaction key is still `increment-v1`, so it returns the saved result without running the increment again.

## 4. Make a new change

```sh
COUNTER_REQUEST=increment-v2 node --import tsx counter.ts
```

You should see:

```text
Transaction result: 2
Saved count: 2
```

Keep `data/` for the next run. Run only one writer against this directory at a time.

You now have a persisted document and a replayable transaction result. The [document reference](../reference/documents-and-storage.md) describes conversation scopes, families and migrations. For model conversations, [persist both domain state and the native engine](../persistence.md).
