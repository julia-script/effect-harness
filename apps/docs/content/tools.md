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

## Return an image from a custom tool

Call `ToolImage.prepare(bytes, mimeType)` inside your handler. It uses your bound
`ImagePreparation` codec service to validate, orient, resize, and convert the
image, then returns a `ToolResult` output with a native image part and a text
note describing transformations. `ToolImage.Metadata` is the success schema;
its structured output contains dimensions and byte counts, without another copy
of the image.

The screenshot example below supplies the same image services as `Read.withImages`.
Provide your webview service and the resulting `ToolsLive` Layer to the harness.
The example is compiled with the documentation app.

<!-- example:tool-images -->

For harness-selected models, declare `capabilities.images` and explicit
`imageLimits` on `Model.Definition`. The request-pinned policy in `ToolExecution`
wins over the fallback `coding/ImageReadPolicy` resolver. A selected model without
vision or explicit image limits cannot receive prepared images. Direct toolkit
calls use the bound resolver, which runs for each invocation with `ToolExecution`.
The helper does not rediscover or reselect the model.

`maxWidth` and `maxHeight` bound output pixels; `maxInputPixels` guards decoder
allocation. `maxBytes` bounds **padded base64 ASCII bytes**, excluding a data URL
prefix: three binary bytes require four base64 bytes. These image limits are
independent of text `OutputRetention`. The helper does not bound the allocation
that created your screenshot bytes; apply source limits in your capture service.
Use the Node, Bun, or WASM image Layer appropriate to your runtime. No image
processor is installed implicitly and other tool results are not processed.

For screen coordinates, `dimensions` is the image the model sees, and `oriented`
is the upright source before resizing. Multiply x by
`oriented.width / dimensions.width` and y by
`oriented.height / dimensions.height` to map continuous coordinates back to the
upright source. The model-visible note includes both scale factors. `original`
and `orientation` describe the encoded source; map through its EXIF orientation
separately if your application uses encoded-image coordinates. Screenshot tools
may also need to account for viewport offsets and device pixel ratio.

Unsupported vision fails with `ImageModelUnsupported` before the codec runs.
Preparation preserves the typed errors from `ImagePreparation` (including
`InvalidImageLimits`, `ImageDecodeFailed`, and `ImageCannotFit`); invalid output
from a replacement service fails with `InvalidImageOutput`. The example maps
those failures into declared tool error results. No failure silently drops an
image or reports successful text-only output.
