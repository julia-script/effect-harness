# Run your first durable conversation

We will build a small application whose local model asks an `uppercase` tool to transform `hello`. We will submit the same request twice and see that the second submission returns the original answer receipt.

Start in an empty directory with Node.js and a package manager of your choice. This lesson assumes Effect knowledge. The model is local and deterministic; it makes no HTTP requests. Storage and the WorkflowEngine will be in memory for this lesson.

The shell commands use npm syntax. You can use the equivalent commands from your preferred package manager; execution uses Node.js and `tsx`.

## 1. Install the packages

```sh
mkdir first-conversation
cd first-conversation
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

## 2. Bind an ordinary AI Toolkit

Create `Tools.ts`:

```ts
import * as Registry from 'effect-harness/Registry'
import * as ToolRegistration from 'effect-harness/ToolRegistration'
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'

const Uppercase = Tool.make('uppercase', {
  description: 'Convert text to uppercase.',
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.String,
})
const toolkit = Toolkit.make(Uppercase)
const handlers = toolkit.toLayer({
  uppercase: ({ text }) =>
    Console.log(`uppercase: ${text.toUpperCase()}`).pipe(Effect.as(text.toUpperCase())),
})

export const layer = Layer.unwrap(
  ToolRegistration.bind(toolkit, { uppercase: { replay: 'safe' } }).pipe(
    Effect.map((tools) => Registry.layer([{ name: 'text-tools', tools }])),
  ),
).pipe(Layer.provide(handlers))
```

This handler only transforms text and logs the result. We permit it to run again during recovery with `replay: 'safe'`. See [tool recovery policy](../reference/configuration.md#tool-policy) before applying that policy to external actions.

## 3. Supply a local model

Create `Model.ts`:

```ts
import * as Model from 'effect-harness/Model'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Response from 'effect/ai/Response'

export const ref = { provider: 'local', modelId: 'uppercase-demo' }

const finish = (reason: Response.FinishReason): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 5, text: 5, reasoning: undefined },
  },
  response: undefined,
})

const native = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: () => Effect.succeed([{ type: 'text', text: 'HELLO' }, finish('stop')]),
    streamText: ({ prompt }) => {
      const hasResult = prompt.content.some(
        (message) =>
          message.role === 'tool' &&
          message.content.some((part) => part.type === 'tool-result' && part.name === 'uppercase'),
      )
      return Stream.fromIterable<Response.StreamPartEncoded>(
        hasResult
          ? [
              { type: 'text-start', id: 'answer' },
              { type: 'text-delta', id: 'answer', delta: 'HELLO' },
              { type: 'text-end', id: 'answer' },
              finish('stop'),
            ]
          : [
              {
                type: 'tool-call',
                id: 'uppercase-call',
                name: 'uppercase',
                params: { text: 'hello' },
                providerExecuted: false,
              },
              finish('tool-calls'),
            ],
      )
    },
  }),
)

export const layer = Layer.unwrap(
  Effect.map(LanguageModel.LanguageModel, (model) =>
    Model.layer([
      {
        ref,
        model,
        contextWindow: 100000,
        maxOutputTokens: 1000,
        configure: () => Effect.succeed(Context.empty()),
      },
    ]),
  ),
).pipe(Layer.provide(native))
```

The local model requests one tool call. Once its prompt contains that tool's result, it emits the fixed answer `HELLO`.

## 4. Compose the executor Layers

Create `Runtime.ts`:

```ts
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as Conversation from 'effect-harness/durable/Conversation'
import * as DurableExecutor from 'effect-harness/durable/Executor'
import * as Identity from 'effect-harness/durable/Identity'
import * as Session from 'effect-harness/durable/Session'
import * as SessionDirectory from 'effect-harness/durable/SessionDirectory'
import * as Store from 'effect-harness/durable/Store'
import * as HarnessExecutor from 'effect-harness/Executor'
import * as Layer from 'effect/Layer'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Model from './Model.ts'
import * as Tools from './Tools.ts'

export const sessionId = Identity.SessionId.make('first-conversation')

const Creation = Conversation.layer({
  settings: {
    retry: { enabled: false },
    compaction: { enabled: false },
  },
})
const SessionLive = Session.layer.pipe(
  Layer.provideMerge(Store.layerMemory),
  Layer.provideMerge(Creation),
)
const Directory = SessionDirectory.layerSingle(sessionId).pipe(Layer.provideMerge(SessionLive))

export const layer = DurableExecutor.layer.pipe(
  Layer.provide(HarnessExecutor.layer),
  Layer.provideMerge(Directory),
  Layer.provideMerge(WorkflowEngine.layerMemory),
  Layer.provide(Layer.mergeAll(Model.layer, Tools.layer)),
  Layer.provide(NodeServices.layer),
)
```

The application supplies one Session and a native WorkflowEngine. `DurableExecutor.layer` registers the built-in Workflows with that engine.

## 5. Submit and replay a request

Create `main.ts`:

```ts
import * as Conversation from 'effect-harness/durable/Conversation'
import * as Identity from 'effect-harness/durable/Identity'
import * as Session from 'effect-harness/durable/Session'
import { Submission } from 'effect-harness/durable/workflow/Submission'
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Prompt from 'effect/ai/Prompt'
import * as Model from './Model.ts'
import * as Runtime from './Runtime.ts'

const program = Effect.gen(function* () {
  const session = yield* Session.Session
  const root = yield* session.root()
  yield* session.transaction(
    Effect.fnUntraced(function* (tx) {
      const agent = yield* tx.doc(Conversation.AgentDoc, { owner: root.id })
      agent.model = Model.ref
    }),
  )

  const payload: typeof Submission.payloadSchema.Type = {
    sessionId: Runtime.sessionId,
    conversationId: root.id,
    requestId: Identity.RequestId.make('hello-v1'),
    submission: {
      _tag: 'input',
      type: 'input',
      message: Prompt.userMessage({ content: [Prompt.textPart({ text: 'uppercase hello' })] }),
    },
  }
  const first = yield* Submission.execute(payload)
  const second = yield* Submission.execute(payload)
  yield* Console.log(first._tag)
  yield* Console.log(`Same receipt: ${JSON.stringify(first) === JSON.stringify(second)}`)
})

await Effect.runPromise(program.pipe(Effect.provide(Runtime.layer)))
```

Run the typechecker and application:

```sh
npm exec tsc
node --import tsx main.ts
```

You should see:

```text
uppercase: HELLO
InputDone
Same receipt: true
```

Notice that the tool logs once, even though we execute the submission twice. The request ID selects the same native execution and saved receipt. A new message needs a new request ID.

This lesson's state disappears when the process ends. Continue with [persistent application state](persistent-state.md), then [persistent conversations and execution](../persistence.md). To replace the local model, follow [connect a model provider](../providers.md).
