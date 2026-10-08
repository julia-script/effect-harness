import { expect, test } from 'tstyche'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Layer from 'effect/Layer'
import * as AiTool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import type * as NativeModel from 'effect/ai/LanguageModel'
import * as CodingTools from 'effect-harness/tools/CodingTools'
import * as Bash from 'effect-harness/tools/Bash'
import * as Read from 'effect-harness/tools/Read'
import * as Write from 'effect-harness/tools/Write'
import * as Edit from 'effect-harness/tools/Edit'
// effect-review-allow P8-tests-import-public-specifiers: path is private tool wiring; only its internal effect channels are asserted here.
import * as Path from '../../src/tools/internal/path.ts'
import * as Image from 'effect-harness/tools/Image'
import * as Tool from 'effect-harness/Tool'
import type { Env, FileError } from 'effect-harness/Env'
import type { MutationLocks } from 'effect-harness/MutationLocks'
import type { Invocation, ToolCall, ToolResult } from 'effect-harness/Invocation'
import type { ToolError } from 'effect-harness/ToolError'
import type * as Prompt from 'effect/ai/Prompt'
import type * as Option from 'effect/Option'

declare const registration: Tool.Registration
declare const model: NativeModel.LanguageModel
const known = AiTool.make('known', {
  parameters: Schema.Struct({ value: Schema.Finite }),
  success: Schema.String,
})
const toolkit = Toolkit.make(known)

test('native definition-only requests retain toolkit and binding types', () => {
  expect(model.generateText).type.toBeCallableWith({
    prompt: 'request',
    toolkit,
    disableToolCallResolution: true,
  })
  expect(Tool.bind(toolkit)).type.toBe<
    Effect.Effect<Array<Tool.Registration>, never, AiTool.HandlersFor<{ known: typeof known }>>
  >()
})

test('coding toolkit binds at the host edge while invocation services remain invocation-time', () => {
  expect(CodingTools.layerHandlers()).type.toBe<
    Layer.Layer<AiTool.HandlersFor<Toolkit.Tools<typeof CodingTools.toolkit>>>
  >()
  expect(CodingTools.make()).type.toBe<
    Effect.Effect<import('effect-harness/Extension').Extension, never, Env | MutationLocks>
  >()
  expect(Bash.handler()({ command: 'true' })).type.toBe<
    Effect.Effect<ToolResult, ToolError, Env | Invocation | ToolCall>
  >()
  expect(Bash.powerShellHandler()({ command: 'true' })).type.toBe<
    Effect.Effect<ToolResult, ToolError, Env | Invocation | ToolCall>
  >()
  expect(Read.handler({ path: 'file' })).type.toBe<
    Effect.Effect<ToolResult, ToolError, Env | Invocation>
  >()
  expect(Write.handler({ path: 'file', content: 'data' })).type.toBe<
    Effect.Effect<{ content: Array<Prompt.TextPart> }, ToolError, Env | Invocation | MutationLocks>
  >()
  expect(Edit.handler({ path: 'file', edits: [] })).type.toBe<
    Effect.Effect<
      {
        content: Array<Prompt.TextPart>
        details: { diff: string; patch: string; firstChangedLine?: number }
      },
      ToolError,
      Env | Invocation | MutationLocks
    >
  >()
  expect(Path.resolve('file')).type.toBe<Effect.Effect<string, FileError, Env | Invocation>>()
  expect(Path.resolveRead('file')).type.toBe<Effect.Effect<string, FileError, Env | Invocation>>()
  expect(Tool.makeIntent(registration, { id: 'c', decoded: {} })).type.toBe<
    Effect.Effect<Tool.Intent, ToolError>
  >()
  expect(Image.detectSupportedImageMimeType(new Uint8Array())).type.toBe<Option.Option<string>>()
  expect(
    Image.detectSupportedImageMimeTypeOf({ size: 0, read: () => Effect.fail('read') }),
  ).type.toBe<Effect.Effect<Option.Option<string>, string>>()
  expect(Read.handler).type.not.toBeCallableWith({ path: 1 })
  expect(Write.handler).type.not.toBeCallableWith({ path: 'file' })
})
