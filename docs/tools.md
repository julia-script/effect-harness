# Tools

`Tool.make` declares a tool's name, description, parameter schema, success schema, failure schema, and replay policy. `Toolkit.make` combines declarations; `Toolkit.merge` combines toolkits.

```ts
const tools = Toolkit.make(
  Tool.make('search_issues', {
    description: 'Search the issue tracker',
    parameters: Schema.Struct({ query: Schema.String }),
    success: Schema.Array(Schema.Struct({ id: Schema.String, title: Schema.String })),
    replay: 'safe',
  }),
)

const ToolsLive = tools.toLayer(
  Effect.gen(function* () {
    const tracker = yield* IssueTracker
    return { search_issues: ({ query }) => tracker.search(query) }
  }),
)
```

`IssueTracker` is an application service. Supply its Layer to `ToolsLive`; the builder captures the service for subsequent calls. No registry or environment service is needed. Different handler Effects can provide different Layers for filesystem, network, or other capabilities.

Handlers receive decoded parameters. The toolkit encodes their success values and declared domain failures through the schemas. A domain failure becomes a failed tool result visible to the model. Infrastructure failures and defects fail execution rather than pretending to be domain results.

`ToolResult.Result.content` is the model-visible text and media. After-tool hooks can replace this content without deleting `details`: details remain durable application data and do not override the model-visible content. The local loop encodes final content in `ToolResult.Envelope`; the provider adapters expand it into native tool-result blocks. Handlers whose success schema is `ToolResult.Envelope` expose that envelope's content to after-tool hooks before projection.

Each handler invocation has its own Scope. The runtime supplies `ToolExecution`, which exposes the current conversation, task and call identifiers; durable output and diagnostics; document snapshots and watches; and `commit` for atomic entries/document changes. These capabilities are revoked when the invocation ends.

Tools default to unsafe replay and sequential scheduling metadata. Set `replay: 'safe'` only when repeating the entire handler after a crash is safe. A durable document update alone does not make an arbitrary handler replay safe.

The local loop currently executes tool calls in response order. The `executionMode` declaration is metadata; parallel tool scheduling is not implemented.

`Extension.make` statically bundles tools, hooks, and prompt sections. `Extension.provide` defers a Layer to runtime construction. Hooks can transform prompts, block tool calls, replace arguments or results, and continue a yielded response. See the [extension example](../apps/example/src/tour/Extensions.ts).

The [uppercase tool](../apps/example/src/Uppercase.ts) is a complete declaration/handler example. The [documents example](../apps/example/src/tour/Documents.ts) writes application state from a handler.
