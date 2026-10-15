import { Context, Effect, Layer, Schema } from 'effect'
import * as Prompt from 'effect/ai/Prompt'
import * as Images from 'effect-harness/ImagePreparation'
import * as Tool from 'effect-harness/Tool'
import * as ToolImage from 'effect-harness/ToolImage'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Policy from 'effect-harness/coding/ImageReadPolicy'
import * as NodeImages from 'effect-harness/images-node'

// Supply your webview's screenshot implementation as this service.
export class Webview extends Context.Service<
  Webview,
  { readonly screenshot: Effect.Effect<Uint8Array, Error> }
>()('example/Webview') {}

export const tools = Toolkit.make(
  Tool.makeResult('screenshot', {
    description: 'Capture the current webview as an image',
    success: ToolImage.Metadata,
    failure: Schema.String,
    resultValidation: 'strict',
    replay: 'safe',
  })
    .addDependency(Webview)
    .addDependency(Images.ImagePreparation)
    .addDependency(Policy.ImageReadPolicy),
)

export const handlers = tools.toLayer({
  screenshot: () =>
    Effect.gen(function* () {
      const webview = yield* Webview
      return yield* ToolImage.prepare(yield* webview.screenshot, 'image/png')
    }).pipe(
      Effect.mapError((error) => ({
        content: [Prompt.textPart({ text: error.message })],
        structuredOutput: error.message,
        diagnostics: [{ severity: 'error', message: error.message }],
      })),
    ),
})

// Direct toolkit calls use this fallback. Harness-selected requests instead use
// the pinned Model.Definition capabilities.images and imageLimits.
export const fallback = Policy.layer({
  vision: true,
  limits: {
    maxWidth: 2000,
    maxHeight: 2000,
    maxBytes: 4.5 * 1024 * 1024, // Padded base64 bytes, not binary bytes.
    maxInputPixels: 40_000_000,
  },
})
export const ToolsLive = handlers.pipe(
  Layer.provideMerge(NodeImages.layer),
  Layer.provideMerge(fallback),
)
// Bind Webview with ToolsLive.pipe(Layer.provideMerge(WebviewLive)), then
// provide that Layer to Harness.layerLocal({ tools }).
