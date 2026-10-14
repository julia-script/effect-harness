# Add a tool to a conversation

Give a model a schema-backed tool and supply the handler through a Layer. This guide assumes an existing application and a native Effect AI `LanguageModel` Layer.

## Declare the contract and handler

Put this module in your application. The declaration accepts a `text` string and returns a string. The handler also writes progress through its invocation's `ToolExecution` service.

<!-- example:tools -->

## Provide your model

Call the exported `withModel` function with your model Layer, then provide the returned Layer to your client program. Submit an input that asks the model to uppercase some text. The [provider guide](/guides/providers) describes model Layers.

The model must return a tool-call intent naming `uppercase`; the harness then invokes the handler. The harness owns tool execution. A provider must not execute the declared tool itself.

## Choose the replay policy

This handler can be repeated safely, so it declares `replay: 'safe'`. For a handler that sends a message, charges an account, or increments application state, keep the default unsafe policy unless repeating the entire operation is safe.

If your handler depends on an application service, acquire it in an Effect passed to `tools.toLayer` and provide that service's Layer to the resulting handler Layer. Keep credentials and network resources in those service Layers.

## Check the result

After `Submission.wait` returns `InputDone`, read the final answer from its entry ID. The committed history also contains the tool intent, progress, and result. The [observation guide](/guides/observation) includes a complete answer-reading function.

For atomic application state changes, use the [document example](/examples/documents). For interruption behavior, read [restart and recovery](/explanation/recovery).
